import { ms } from "./ms.ts"
import { compactChanges, CrrColumn, saveChanges, saveFractionalIndexCols, sqlExplainExec, pkEncodingOfRow, Change, reconstructRowFromHistory, Client, sqlDetermineOperation, sqlAsSelectStmt } from "./change.ts";
import { insertTablesStmt } from "./tables.ts";
import { assert } from "./utils.ts";

type MessageType = 'dbClose' | 'exec' | 'select' | 'change';

type SqliteUpdateHookChange = {
    updateType: 'delete' | 'insert' | 'update',
    dbName: string,
    tableName: string,
    rowid: bigint
}

export type SqliteColumnInfo = {
    cid: number
    dflt_value: string | null
    name: string
    notnull: number
    pk: number
    type: string
}

export type SqliteForeignKey = {
    table: string
    from: string
    to: string
    on_delete: 'CASCADE' | 'RESTRICT'
}

export class SqliteDB {
    name: string;
    siteId = "";
    pks: { [tblName: string]: string[] } = {};
    crrColumns: { [tbl_name: string]: CrrColumn[] } = {};

    #debug = false;
    #mp: MessagePort
    #channelTableChange: BroadcastChannel

    constructor(name:string, mp: MessagePort) {
        this.name = name;
        this.#mp = mp;

        // Broadcast channel to notify dependent queries to re-run
        this.#channelTableChange = new BroadcastChannel("table_change");

        // Broadcast for update_hook()
        const bcUpdateHook = new BroadcastChannel("update_hook");

        // Broadcast any events handled by the worker to any of the functions below waiting for a result
        const bc = new BroadcastChannel("message_bus");
        this.#mp.addEventListener('message', (event) => {
            if (event.data.type === 'change') {
                bcUpdateHook.postMessage(event.data.change);
            }
            bc.postMessage(event.data);
        });

        // Enable foreign key constraints
        this.exec(`PRAGMA foreign_keys = ON`, []);

        // Extract the primary keys of tables to be used in changes
        this.extractPks();
    }

    async close(): Promise<Error> {
        const data = await this.send('dbClose');
        if (!data.error) {
            console.log(`Closed database connection ...`);
        }
        this.#channelTableChange.close();
        return data.error;
    }

    async exec(sql: string, params: any[], options: { notify?: boolean } = { notify: true }) {
        const data = await this.send('exec', { sql, params });
        if (options.notify) {
            const tblName = sqlExplainExec(sql);
            this.#channelTableChange.postMessage(tblName);
        };
        if (data.error) {
            console.error(`Failed executing`, sql, params, data.error);
        }
        return data.error as Error | undefined;
    }

    async execOrThrow(sql: string, params: any[], options: { notify?: boolean } = { notify: true }) {
        const err = await this.exec(sql, params, options);
        if (err) throw err;
    }

    async execTrackChanges(sql: string, params: any[]) {
        try {
            const tblName = sqlExplainExec(sql);
            const operation = sqlDetermineOperation(sql);

            const deleteRowidToPk: {[rowid: string] : string} = {};
            if (operation === 'delete') {
                // Turn the delete stmt into a select stmt to get which rows are being affected.
                const deleteAsSelectQuery = sqlAsSelectStmt(sql) as string;
                const rows = await this.select<{rowid: bigint}[]>(deleteAsSelectQuery, params);
                for (const row of rows) {
                    const pk = pkEncodingOfRow(this, tblName, row);
                    deleteRowidToPk['' + row.rowid] = pk;
                }
            }

            await this.exec(`BEGIN EXCLUSIVE TRANSACTION;`, [], { notify: false });
            const updateHook = new BroadcastChannel("update_hook");
            const change: SqliteUpdateHookChange = await new Promise((resolve, reject) => {
                updateHook.addEventListener('message', (event) => {
                    resolve(event.data);
                });
                this.exec(sql, params, { notify: false }).catch(err => reject(err));
            });

            const changeSet = await this.getChangeSetFromUpdate(change, deleteRowidToPk);
            await saveFractionalIndexCols(this, changeSet);

            await saveChanges(this, changeSet);
            await compactChanges(this, changeSet);
            await this.exec(`COMMIT;`, [], { notify: false });

            // @Incomplete - A delete might cascade over multiple tables, so only notifying hooks of the deleted row is not sufficient.
            // We should get all the table names that are affected by the delete
            this.#channelTableChange.postMessage(tblName);
            this.#channelTableChange.postMessage("crr_changes");
        } catch (e) {
            console.error(e);
            return e;
        }
    }

    async first<T>(sql: string, params: any[]) {
        const { data: results, error } = await this.selectWithError<T>(sql, params);
        if (error) throw new Error(error);
        if (results.length === 0) {
            return undefined;
        }
        return results[0] as T;
    }

    async select<T>(sql: string, params: any[]) {
        const { data: results, error } = await this.selectWithError(sql, params);
        if (error) throw new Error(error);
        return results as T;
    }

    async selectWithError<T>(sql: string, params: any[]) {
        if (this.#debug) console.info(sql);

        const { results, error } = await this.send('select', { sql, params });
        return { data: results as T, error };
    }

    async upgradeAllTablesToCrr() {
        const frameworkMadeTables = ["crr_changes", "crr_clients", "crr_columns"];
        const tables = await this.select<{ name: string }[]>(`SELECT name FROM sqlite_master WHERE type='table' ORDER BY name`, []);
        for (const table of tables) {
            if (frameworkMadeTables.includes(table.name)) continue;
            await this.upgradeTableToCrr(table.name);
        }
    }

    async upgradeTableToCrr(tblName: string, deleteWinsAfter: string = '10s') {
        const dwaMs = ms(deleteWinsAfter);
        const columns = await this.select<any[]>(`PRAGMA table_info('${tblName}')`, []);
        const fks = await this.select<SqliteForeignKey[]>(`PRAGMA foreign_key_list('${tblName}')`, []);
        const values = columns.map(c => {
            const fk = this.fkOrNull(c, fks);
            if (fk) return `('${tblName}', '${c.name}', 'lww', '${fk.table}|${fk.to}', '${fk.on_delete}', '${dwaMs}', null)`;
            else return `('${tblName}', '${c.name}', 'lww', null, null, '${dwaMs}', null)`;
        }).join(',');
        const err = await this.exec(`
            INSERT INTO "crr_columns" (tbl_name, col_id, type, fk, fk_on_delete, delete_wins_after, parent_col_id)
            VALUES ${values}
            ON CONFLICT DO NOTHING
        `, []);
        if (err) return console.error(err);
    }

    async upgradeColumnToFractionalIndex(tblName: string, colId: string, parentColId: string) {
        const err = await this.exec(`
            UPDATE "crr_columns" 
            SET type = 'fractional_index', parent_col_id = '${parentColId}'
            WHERE tbl_name = '${tblName}' AND col_id = '${colId}'
        `, []);
        if (err) return console.error(err);
    }

    async finalizeUpgrades() {
        const crrColumns = await this.select<CrrColumn[]>(`SELECT * FROM "crr_columns"`, []);
        this.crrColumns = Object.groupBy(crrColumns, ({ tbl_name }) => tbl_name) as { [tbl_name: string]: CrrColumn[] };
    }

    private fkOrNull(col: any, fks: SqliteForeignKey[]): SqliteForeignKey | null {
        const fk = fks.find(fk => fk.from === col.name);
        if (fk === undefined) return null;
        return fk
    }

    private async getChangeSetFromUpdate(change: SqliteUpdateHookChange, deleteRowidToPk: {[rowid: string] : string}): Promise<Change[]> {
        switch (change.updateType) {
            case "insert": {
                const row = await this.first<any>(`SELECT rowid, * FROM "${change.tableName}" WHERE rowid = ${change.rowid}`, []);
                if (row === undefined) {
                    console.error(`Failed to get just inserted row in table '${change.tableName}' with rowid '${change.rowid}'`);
                    return [];
                }

                const pk = pkEncodingOfRow(this, change.tableName, row);
                const now = (new Date()).getTime();

                const changeSet = [];
                for (const [key, value] of Object.entries(row)) {
                    if (key === "rowid") continue;
                    changeSet.push({ type: change.updateType, tbl_name: change.tableName, col_id: key, pk, value, site_id: this.siteId, created_at: now, applied_at: 0 });
                }
                return changeSet;
            }
            case "delete": {
                const tblName = change.tableName;
                const now = (new Date()).getTime();
                
                // We use the computed deleteRowidToPk mapping to lookup the primary-key that got deleted
                const pk = deleteRowidToPk['' + change.rowid];
                assert(pk);
                
                return [{ type: 'delete', tbl_name: tblName, col_id: "tombstone", pk, value: 1, site_id: this.siteId, created_at: now, applied_at: 0 }];
            };
            case "update": {
                const row = await this.first<any>(`SELECT * FROM "${change.tableName}" WHERE rowid = ${change.rowid}`, []);
                if (row === undefined) {
                    console.error(`No row was found for update`);
                    return [];
                };

                const pk = pkEncodingOfRow(this, change.tableName, row);
                const lastVersionOfRow = await reconstructRowFromHistory(this, change.tableName, pk);
                if (lastVersionOfRow === undefined || Object.keys(lastVersionOfRow).length === 0) {
                    console.log(`No prior changes was found for row with pk ${pk} in table ${change.tableName} while receiving a new update in getChangesetFromUpdate()`);
                    return [];
                }

                const now = (new Date()).getTime();

                const changeSet = [];
                for (const [key, value] of Object.entries(row)) {
                    const lastValue = lastVersionOfRow[key];
                    if (value !== lastValue) {
                        changeSet.push({ type: change.updateType, tbl_name: change.tableName, col_id: key, pk, value, site_id: this.siteId, created_at: now, applied_at: 0 });

                        // :ModifyForeignKeyInserts
                        // @Temporary - If the update changes the foreign-key, also update the original insert change
                        // NOTE: We might in the future have a structure such that each column of a row
                        // only has 1 associated change row. Then there couldn't be another change to the foreign-key
                        const fkCols = this.crrColumns[change.tableName].filter(col => col.fk);
                        if (fkCols.length === 0) continue;
                        const fkCol = fkCols.find(col => col.col_id === key);
                        if (fkCol) {
                            await this.exec(`UPDATE "crr_changes" SET value = ? WHERE type = 'insert' AND tbl_name = ? AND pk = ? AND col_id = ?`, [value, change.tableName, pk, fkCol.col_id]);
                        }
                    }
                }

                return changeSet;
            };
            default:
                return [];
        }
    }

    private async extractPks() {
        const pks: { [tblName: string]: string[] } = {};
        const tables = await this.select<{ name: string }[]>(`SELECT name FROM sqlite_master WHERE type='table' ORDER BY name`, []);
        for (const table of tables) {
            const columns = await this.select<{ name: string, pk: number }[]>(`PRAGMA table_info('${table.name}')`, []);
            pks[table.name] = columns.filter(c => c.pk !== 0).map(c => c.name);
        }
        this.pks = pks;
    }

    private async send(type: MessageType, payload?: any): Promise<any> {
        const id = crypto.randomUUID();
        const bc = new BroadcastChannel('message_bus');
        this.#mp.postMessage({ type: type, id, ...payload });
        const data = await new Promise(resolve => {
            bc.addEventListener('message', (event) => {
                if (event.data.id === id) {
                    // Only resolve the promise once we see a reply from the message that we posted!
                    resolve(event.data);
                }
            });
        });
        bc.close();
        return data;
    }
}

export const createDb = async (name: string = 'main'): Promise<SqliteDB> => {
    const workerScriptPath = `./sqlite-worker.js?dbName=${name}`;

    // Setup up a message channel to communicate with worker thread
    const { port1, port2 } = new MessageChannel();
    port1.start();

    const workerURL = new URL(workerScriptPath, import.meta.url);
    const w = new Worker(workerURL, { type: 'module' });
    w.postMessage('messagePort', [port2]);

    // Wait for database to be ready
    const ok = await new Promise(resolve => {
        port1.addEventListener('message', event => {
            if (event.data === 'dbReady') {
                resolve(true);
            } else {
                resolve(false)
            }
        }, { once: true });
    });

    if (!ok) {
        throw new Error("Failed to connect to the database");
    }

    console.log(`Connected to the database ...`);
    const db = new SqliteDB(name, port1);

    // Setup tables
    const err = await db.exec(insertTablesStmt, []);
    if (err) {
        throw new Error("Failed to insert necessary tables. " + err);
    }

    // Get or assign a unique site_id
    const me = await db.first<Client>(`SELECT * FROM "crr_clients" WHERE is_me = true`, [])
    if (!me) {
        const id = crypto.randomUUID();
        const err = await db.exec(`INSERT INTO "crr_clients" (site_id, is_me) VALUES (?, true)`, [id])
        if (err) {
            throw new Error(`Failed to assign this browser a site_id. Maybe try refreshing the browser`)
        }

        console.log("Assigned a new site_id");
        db.siteId = id;
    } else {
        db.siteId = me.site_id;
    }

    return db;
}



