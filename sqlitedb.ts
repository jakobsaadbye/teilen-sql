import { ms } from "./ms.ts"
import { compactChanges, CrrColumn, saveChanges, saveFractionalIndexCols, sqlExplainExec, pkEncodingOfRow, Change, reconstructRowFromHistory, Client, getRelatedChanges } from "./change.ts";
import { insertTablesStmt } from "./tables.ts";

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


export type ForeignKey = {
    table: string
    from: string
    to: string
    on_delete: 'CASCADE' | 'RESTRICT'
}

export class SqliteDB {
    #debug = false;
    #mp: MessagePort
    #channelTableChange: BroadcastChannel
    siteId = "";
    pks: { [tblName: string]: string[] } = {};
    crrColumns: { [tbl_name: string]: CrrColumn[] } = {};

    constructor(mp: MessagePort) {
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

    async execTrackChanges(sql: string, params: any[]) {
        try {
            const tblName = sqlExplainExec(sql);

            await this.exec(`BEGIN EXCLUSIVE TRANSACTION;`, [], { notify: false });
            const updateHook = new BroadcastChannel("update_hook");
            this.exec(sql, params, { notify: false });
            const change: SqliteUpdateHookChange = await new Promise(resolve => {
                updateHook.addEventListener('message', (event) => {
                    resolve(event.data);
                });
            });

            const changeSet = await this.getChangeSetFromUpdate(change);
            await saveFractionalIndexCols(this, changeSet);

            const err = await saveChanges(this, changeSet);
            await compactChanges(this, changeSet);
            await this.exec(`COMMIT;`, [], { notify: false });

            // @Incomplete - A delete might cascade over multiple tables, so only notifying hooks of the deleted row is not sufficient.
            // We should get all the table names that are affected by the delete
            this.#channelTableChange.postMessage(tblName);
            this.#channelTableChange.postMessage("crr_changes");
            return err;
        } catch (e) {
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
        const fks = await this.select<ForeignKey[]>(`PRAGMA foreign_key_list('${tblName}')`, []);
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

    private fkOrNull(col: any, fks: ForeignKey[]): ForeignKey | null {
        const fk = fks.find(fk => fk.from === col.name);
        if (fk === undefined) return null;
        return fk
    }

    private async getChangeSetFromUpdate(change: SqliteUpdateHookChange): Promise<Change[]> {
        switch (change.updateType) {
            case "insert": {
                const row = await this.first<any>(`SELECT rowid, * FROM "${change.tableName}" WHERE rowid = ${change.rowid}`, []);
                if (row === undefined) {
                    console.error(`Failed to get just inserted row in table '${change.tableName}' with rowid '${change.rowid}'`);
                    return [];
                }

                const rowId = row["rowid"];
                const pk = pkEncodingOfRow(this, change.tableName, row);
                const now = (new Date()).getTime();

                const changeSet = [];
                for (const [key, value] of Object.entries(row)) {
                    if (key === "rowid") continue;
                    changeSet.push({ row_id: rowId, type: change.updateType, tbl_name: change.tableName, col_id: key, pk, value, site_id: this.siteId, created_at: now, applied_at: 0 });
                }
                return changeSet;
            }
            case "delete": {
                // Look for a previous insert to get what primary-key we are deleting as we can't rely on rowids generated from sqlite
                const rowChange = await this.first<Change>(`
                    SELECT * FROM "crr_changes" 
                    WHERE type = 'insert' AND tbl_name = ? AND row_id = ? 
                    ORDER BY created_at DESC
                `, [change.tableName, change.rowid]);
                if (rowChange === undefined) {
                    console.error(`No previous change was found to row before delete`);
                    return [];
                }

                const tblName = rowChange.tbl_name;
                const pk = rowChange.pk;
                const now = (new Date()).getTime();
                
                return [{ row_id: change.rowid, type: 'delete', tbl_name: tblName, col_id: null, pk, value: null, site_id: this.siteId, created_at: now, applied_at: 0 }];
            };
            case "update": {
                const result = await this.first<any>(`SELECT rowid, * FROM "${change.tableName}" WHERE rowid = ${change.rowid}`, []);
                if (result === undefined) {
                    console.error(`No row was found for update`);
                    return [];
                };

                const { rowid, ...row } = result;
                if (row === undefined) {
                    console.error(`No version of row exists with the current changes`);
                    return [];
                };

                const pk = pkEncodingOfRow(this, change.tableName, row);
                const lastVersionOfRow = await reconstructRowFromHistory(this, change.tableName, pk);
                if (lastVersionOfRow === undefined || Object.keys(lastVersionOfRow).length === 0) {
                    console.log(`No prior changes was found for row with pk ${pk} in table ${change.tableName} while receiving a new update in getChangesetFromUpdate()`);
                    return [];
                }

                const rowId = rowid;
                const now = (new Date()).getTime();

                const changeSet = [];
                for (const [key, value] of Object.entries(row)) {
                    const lastValue = lastVersionOfRow[key];
                    if (value !== lastValue) {
                        changeSet.push({ row_id: rowId, type: change.updateType, tbl_name: change.tableName, col_id: key, pk, value, site_id: this.siteId, created_at: now, applied_at: 0 })
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

export const createDb = async (dbName: string = 'main'): Promise<SqliteDB> => {
    const workerScriptPath = `./sqlite-worker.js?dbName=${dbName}`;

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
    const db = new SqliteDB(port1);

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



