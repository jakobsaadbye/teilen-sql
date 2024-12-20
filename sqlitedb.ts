import { assert } from "jsr:@std/assert@0.217/assert";
import { compactChanges, sqlAsSelectStmt, CrrColumn, diff, OpType, saveChanges, saveFractionalIndexCols, sqlExplainExec, pkEncodingOfRow, Change } from "./change.ts";

type MessageType = 'dbClose' | 'exec' | 'select' | 'change';

type UpdateHookChange = {
    updateType: 'delete' | 'insert' | 'update',
    dbName: string,
    tableName: string,
    rowid: bigint
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

        // Get or assign a unique site_id
        this.select(`SELECT * FROM "crr_client"`, [])
            .then(rows => {
                if (rows.length > 0) {
                    this.siteId = rows[0].site_id;
                } else {
                    const id = crypto.randomUUID();
                    this.exec(`INSERT INTO "crr_client" (site_id) VALUES ($1)`, [id])
                        .then(() => {
                            console.log("Assigned a new site_id");
                            this.siteId = id;
                        })
                        .catch(e => console.error("Failed to insert a new site_id", e));
                }
            })
            .catch(e => console.error("Failed to get site_id", e));

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
        console.log(sql);
        const data = await this.send('exec', { sql, params });
        if (options.notify) {
            const tblName = sqlExplainExec(sql);
            this.#channelTableChange.postMessage(tblName);
        };
        return data.error as Error;
    }

    async execTrackChanges(sql: string, params: any[]) {
        try {
            const tblName = sqlExplainExec(sql);

            const updateHook = new BroadcastChannel("update_hook");
            this.exec(sql, params, { notify: false });
            const change: UpdateHookChange = await new Promise(resolve => {
                updateHook.addEventListener('message', (event) => {
                    resolve(event.data);
                });
            });

            const changeSet = await this.getChangesetFromUpdate(change);
            await saveFractionalIndexCols(this, changeSet);

            const err = await saveChanges(this, changeSet);
            await compactChanges(this);

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
        const frameworkMadeTables = ["crr_changes", "crr_client", "crr_columns", "crr_frac_index"];
        const tables = await this.select<{ name: string }[]>(`SELECT name FROM sqlite_master WHERE type='table' ORDER BY name`, []);
        for (const table of tables) {
            if (frameworkMadeTables.includes(table.name)) continue;
            await this.upgradeTableToCrr(table.name);
        }
    }

    async upgradeTableToCrr(tblName: string) {
        const columns = await this.select<any[]>(`PRAGMA table_info('${tblName}')`, []);
        const values = columns.map(c => `('${tblName}', '${c.name}', 'lww', 'null')`).join(',');
        const err = await this.exec(`
            INSERT INTO "crr_columns"
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

    private async getChangesetFromUpdate(change: UpdateHookChange): Promise<Change[]> {
        switch (change.updateType) {
            case "insert": {
                const row = await this.first<any>(`SELECT rowid, * FROM "${change.tableName}" WHERE rowid = ${change.rowid}`, []);
                assert(row !== undefined, `Failed to get just inserted row in table '${change.tableName}' with rowid '${change.rowid}'`);

                const id = crypto.randomUUID();
                const rowId = row["rowid"];
                const pk = pkEncodingOfRow(this, change.tableName, row);
                const created_at = (new Date()).getTime();

                let seq = 0;
                const changeSet = [];
                for (const [key, value] of Object.entries(row)) {
                    if (key === "rowid") continue;
                    changeSet.push({ id, row_id: rowId, type: change.updateType, tbl_name: change.tableName, col_id: key, pk, value, site_id: this.siteId, created_at, applied_at: 0, seq });
                    seq += 1;
                }
                return changeSet;
            }
            case "delete": {
                const rowChange = await this.first<Change | undefined>(`SELECT * FROM "crr_changes" WHERE row_id = ${change.rowid}`, []);
                assert(rowChange !== undefined, `No previous change was found to row before delete'`);

                const id = crypto.randomUUID();
                const pk = rowChange.pk;
                const rowId = rowChange.row_id;
                const created_at = (new Date()).getTime();

                return [{ id, row_id: rowId, type: change.updateType, tbl_name: change.tableName, col_id: null, pk, value: null, site_id: this.siteId, created_at, applied_at: 0, seq: 0 }];
            };
            case "update": {
                const result = await this.first<any>(`SELECT rowid, * FROM "${change.tableName}" WHERE rowid = ${change.rowid}`, []);
                assert(result !== undefined, `No row was found for update`);

                const { rowid, ...row } = result;

                const lastVersionOfRow = await this.reconstructRowFromCurrentChanges(change.tableName, row);
                assert(row !== undefined, `No version of row exists with the current changes`);

                const id = crypto.randomUUID();
                const pk = pkEncodingOfRow(this, change.tableName, row);
                const rowId = rowid;
                const created_at = (new Date()).getTime();

                let seq = 0;
                const changeSet = [];
                for (const [key, value] of Object.entries(row)) {
                    const lastValue = lastVersionOfRow[key];
                    if (value !== lastValue) {
                        changeSet.push({ id, row_id: rowId, type: change.updateType, tbl_name: change.tableName, col_id: key, pk, value, site_id: this.siteId, created_at, applied_at: 0, seq })
                        seq += 1;
                    }
                }

                return changeSet;
            };
            default:
                return [];
        }
    }

    private reconstructRowFromCurrentChanges = async (tblName: string, row: any): Promise<any> => {
        const pk = pkEncodingOfRow(this, tblName, row);
        const latestChanges = await this.select<Change[]>(`SELECT * FROM "crr_changes" WHERE tbl_name = ? AND pk = ? ORDER BY created_at DESC`, [tblName, pk]);
        if (latestChanges.length === 0) return;
        let constructed = {};
        for (const key of Object.keys(row)) {
            const col = latestChanges.find(c => c.col_id === key);
            constructed[key] = col !== undefined ? col.value : null;
        }
        return constructed;
    }

    private async extractPks() {
        const pks: { [tblName: string]: string[] } = {};
        const tables = await this.select<{ name: string }[]>(`SELECT name FROM sqlite_master WHERE type='table' ORDER BY name`, []);
        for (const table of tables) {
            const columns = await this.select(`PRAGMA table_info('${table.name}')`, []);
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

export const createDb = async (workerScriptPath: string): Promise<SqliteDB> => {
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
        console.log(`Failed to receive ready event`);
        throw new Error("Failed to create the database");
    }

    console.log(`Succesfully connected to database ...`);
    return new SqliteDB(port1);
}



