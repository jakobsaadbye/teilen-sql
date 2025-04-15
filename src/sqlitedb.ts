import { saveChanges, saveFractionalIndexCols, reconstructRowFromHistory, CrrColumn, Change, Client, attachChangeGenerationTriggers, detachChangeGenerationTriggers } from "./change.ts";
import { pkEncodingOfRow, sqlExplainExec, generateUniqueId } from "./utils.ts"
import { insertCrrTablesStmt } from "./tables.ts";
import { assert } from "./utils.ts";
import { checkout, Commit, commit, discardChanges, Document, preparePullCommits as preparePullCommits, preparePushCommits as preparePushCommits, PullRequest, PushRequest, receivePullCommits, receivePushCommits as receivePushCommits } from "./versioning.ts";

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
    channelTableChange: BroadcastChannel
    ready: boolean

    #debug = false;
    mp: MessagePort
    deleteRowidToPk: { [rowid: string]: string }

    constructor(name: string, mp: MessagePort) {
        this.name = name;
        this.mp = mp;
        this.deleteRowidToPk = {};
        this.ready = false;

        // Broadcast channel to notify dependent queries to re-run
        this.channelTableChange = new BroadcastChannel("table_change");

        // Broadcast for update_hook()
        const bcUpdateHook = new BroadcastChannel("update_hook");

        // Broadcast any events handled by the worker to any of the functions below waiting for a result
        const bc = new BroadcastChannel("message_bus");
        this.mp.addEventListener('message', (event) => {
            if (event.data.type === 'change') {
                bcUpdateHook.postMessage(event.data.change);
            }
            bc.postMessage(event.data);
        });

        // Enable foreign key constraints
        // this.exec(`PRAGMA foreign_keys = ON`, []);
    }

    async close(): Promise<Error> {
        const data = await this.send('dbClose');
        if (!data.error) {
            console.log(`Closed database connection ...`);
        }
        this.channelTableChange.close();
        return data.error;
    }

    async exec(sql: string, params: any[], options: { notify?: boolean } = { notify: true }) {
        const data = await this.send('exec', { sql, params });
        if (options.notify) {
            const tblName = sqlExplainExec(sql);
            this.channelTableChange.postMessage(tblName);
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

    async execTrackChanges(sql: string, params: any[], documentId = "main") {
        const err = await execTrackChangesHelper(this, sql, params, documentId);
        if (err) return err;

        const tblName = sqlExplainExec(sql);

        this.channelTableChange.postMessage(tblName);
        this.channelTableChange.postMessage("crr_changes");
    }

    async first<T>(sql: string, params: any[]) {
        const { data: results, error } = await this.selectWithError<T[]>(sql, params);
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
        const frameworkMadeTables = ["crr_changes", "crr_clients", "crr_columns", "crr_commits", "crr_temp", "crr_documents"];
        const tables = await this.select<{ name: string }[]>(`SELECT name FROM sqlite_master WHERE type='table' ORDER BY name`, []);
        for (const table of tables) {
            if (frameworkMadeTables.includes(table.name)) continue;
            await this.upgradeTableToCrr(table.name);
        }
    }

    async upgradeTableToCrr(tblName: string) {
        const columns = await this.select<any[]>(`PRAGMA table_info('${tblName}')`, []);
        if (columns.length === 0) {
            console.error(`'${tblName}' is not a recognized table. Make sure it exists in the database before upgrading it to a crr`);
            return;
        }
        const fks = await this.select<SqliteForeignKey[]>(`PRAGMA foreign_key_list('${tblName}')`, []);
        const values = columns.map(c => {
            const fk = this.fkOrNull(c, fks);
            if (fk) return `('${tblName}', '${c.name}', 'lww', '${fk.table}|${fk.to}', '${fk.on_delete}', null)`;
            else return `('${tblName}', '${c.name}', 'lww', null, null, null)`;
        }).join(',');
        const err = await this.exec(`
            INSERT INTO "crr_columns" (tbl_name, col_id, type, fk, fk_on_delete, parent_col_id)
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

    async finalize() {
        const crrColumns = await this.select<CrrColumn[]>(`SELECT * FROM "crr_columns"`, []);
        this.crrColumns = Object.groupBy(crrColumns, ({ tbl_name }) => tbl_name) as { [tbl_name: string]: CrrColumn[] };

        // Extract the primary keys of tables to be used in changes
        await this.extractPks();

        await detachChangeGenerationTriggers(this);
        await attachChangeGenerationTriggers(this);

        this.ready = true;
    }

    async getUncommittedChanges(documentId = "main") {
        const doc = await this.first<Document>(`SELECT * FROM "crr_documents" WHERE id = ?`, [documentId]);
        if (!doc) return [];
        
        const uncommittedChanges = await this.select<Change[]>(`SELECT * FROM "crr_changes" WHERE version = '0' AND document = ? ORDER BY created_at ASC`, [doc.id]);
        return uncommittedChanges;
    }
    async getUncommittedChangeCount(documentId = "main") {
        const doc = await this.first<Document>(`SELECT * FROM "crr_documents" WHERE id = ?`, [documentId]);
        if (!doc) return 0;
        
        const row = await this.first<{count: number}>(`SELECT COUNT(*) as count FROM "crr_changes" WHERE version = '0' AND document = ? ORDER BY created_at ASC`, [doc.id]);
        return row!.count;
    }

    async preparePushCommits(documentId = "main") {
        return await preparePushCommits(this, documentId);
    }

    async preparePullCommits() {
        return await preparePullCommits(this);
    }

    /** @Important Should only be called by a server database */
    async receivePushCommits(push: PushRequest) {
        return await receivePushCommits(this, push);
    }

    /** @Important Should only be called by a server database */
    async receivePullCommits(pull: PullRequest) {
        return await receivePullCommits(this, pull);
    }

    async commit(message: string, documentId = "main") {
        return await commit(this, message, documentId);
    }

    async checkout(commitId: string) {
        await checkout(this, commitId);
    }

    async discardChanges(documentId = "main") {
        await discardChanges(this, documentId);
    }

    private fkOrNull(col: any, fks: SqliteForeignKey[]): SqliteForeignKey | null {
        const fk = fks.find(fk => fk.from === col.name);
        if (fk === undefined) return null;
        return fk
    }

    private async generateChanges(change: SqliteUpdateHookChange, deleteRowidToPk: { [rowid: string]: string }): Promise<Change[]> {
        switch (change.updateType) {
            case "insert": {
                const row = await this.first<any>(`SELECT rowid, * FROM "${change.tableName}" WHERE rowid = ${change.rowid}`, []);
                if (row === undefined) {
                    console.error(`Failed to get just inserted row in table '${change.tableName}' with rowid '${change.rowid}'`);
                    return [];
                }

                const pk = pkEncodingOfRow(this, change.tableName, row);
                const now = (new Date()).getTime();
                const version = "0";

                const changeSet = [];
                for (const [key, value] of Object.entries(row)) {
                    if (key === "rowid") continue;
                    changeSet.push({ type: change.updateType, tbl_name: change.tableName, col_id: key, pk, value, site_id: this.siteId, created_at: now, applied_at: 0, version });
                }
                return changeSet;
            }
            case "delete": {
                const tblName = change.tableName;
                const now = (new Date()).getTime();
                const version = "0";

                // We use the computed deleteRowidToPk mapping to lookup the primary-key that got deleted
                const pk = deleteRowidToPk['' + change.rowid];
                assert(pk);

                return [{ type: 'delete', tbl_name: tblName, col_id: "tombstone", pk, value: 1, site_id: this.siteId, created_at: now, applied_at: 0, version: version }];
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
                const version = "0";

                const changeSet = [];
                for (const [key, value] of Object.entries(row)) {
                    const lastValue = lastVersionOfRow[key];
                    if (value !== lastValue) {
                        changeSet.push({ type: change.updateType, tbl_name: change.tableName, col_id: key, pk, value, site_id: this.siteId, created_at: now, applied_at: 0, version });

                        // :ModifyForeignKeyInserts
                        // @Temporary - If the update changes the foreign-key, also update the original insert change
                        // NOTE: We might in the future have a structure such that each column of a row
                        // only has 1 associated change type for a row (instead of insert and update). Then there couldn't be another change to the foreign-key
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
        this.mp.postMessage({ type: type, id, ...payload });
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

/**
 * Creates a new local browser database
 * @param name Name of the database file to store in OPFS
 */
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
    const err = await db.exec(insertCrrTablesStmt, []);
    if (err) {
        throw new Error("Failed to insert necessary tables. " + err);
    }

    // Get or assign a unique site_id
    await assignSiteId(db);

    return db;
}

export const assignSiteId = async (db: SqliteDB) => {
    const me = await db.first<Client>(`SELECT * FROM "crr_clients" WHERE is_me = true`, [])
    if (!me) {
        const id = generateUniqueId();
        const err = await db.exec(`INSERT INTO "crr_clients" (site_id, is_me) VALUES (?, true)`, [id])
        if (err) {
            throw new Error(`Failed to assign this browser a site_id. Maybe try refreshing the browser`)
        }

        db.siteId = id;
    } else {
        db.siteId = me.site_id;
    }
}

export const execTrackChangesHelper = async (db: SqliteDB, sql: string, params: any[], documentId = "main") => {
    // @TODO: This will become a place where we would actually create a new hybrid logical clock.

    try {
        const doc = await db.first<Document>(`SELECT * FROM "crr_documents" WHERE id = ?`, [documentId]);
        if (!doc) {
            await db.exec(`INSERT OR IGNORE INTO "crr_documents" (id, head) VALUES (?, ?)`, [documentId, null]);
        }
        const now = (new Date).getTime();
        await db.execOrThrow(`INSERT OR REPLACE INTO "crr_temp" (time, document) VALUES (?, ?)`, [now, documentId]);

        const tblName = sqlExplainExec(sql);

        if (db.crrColumns[tblName] === undefined) {
            console.error(`Table '${tblName}' have not been upgraded to a crr table. Upgrade the table with upgradeTableToCrr("${tblName}") to begin tracking changes on it`);
            return;
        }

        await db.exec(`BEGIN EXCLUSIVE TRANSACTION;`, [], { notify: false });
        await db.exec(sql, params, { notify: false });
        await db.exec(`COMMIT;`, [], { notify: false });

        const appliedChanges = await db.select<Change[]>(`SELECT * FROM "crr_changes" WHERE created_at = ? AND site_id = ?`, [now, db.siteId]);

        await saveFractionalIndexCols(db, appliedChanges);
        await saveChanges(db, appliedChanges);
        // await compactChanges(db, appliedChanges);
    } catch (e) {
        await db.exec(`ROLLBACK;`, [], { notify: false });
        return e;
    }
}