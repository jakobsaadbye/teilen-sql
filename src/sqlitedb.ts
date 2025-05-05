import { saveChanges, saveFractionalIndexCols, CrrColumn, Change, Client, attachChangeGenerationTriggers, detachChangeGenerationTriggers } from "./change.ts";
import { sqlExplainExec, generateUniqueId } from "./utils.ts"
import { insertCrrTablesStmt } from "./tables.ts";
import { checkout, Commit, commit, discardChanges, Document, getConflicts, preparePullCommits as preparePullCommits, preparePushCommits as preparePushCommits, PullRequest, PushRequest, receivePullCommits, receivePushCommits as receivePushCommits, ConflictChoice, resolveConflict, getPushCount, getHead } from "./versioning.ts";
import { getDocumentSnapshot } from "./snapshot.ts";
import { createTimestamp } from "./hlc.ts";
import { upgradeTableToCrr } from "./sqlitedbCommon.ts";

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
    on_delete: null | 'CASCADE' | 'RESTRICT' | "NO ACTION"
}

export type TemporaryData = {
    lotr: number
    clock: string    // Encoded hybrid-logical-clock
    time_travelling: boolean
    document: string
}

type UpgradeOptions = {
    /** Weather or not the table or certain columns should be replicated to other clients 
     * @Important Non replicated columns should contain a default sql value
    */
    replicate?: "all" | "none" | {
        include?: string[],
        exclude?: string[]
    }

    /** Weather or not concurrent updates to the same cell are handled manually or resolved automatically through last-writer-wins
     * @Note Only applicable for git-style versioning
     */
    manualConflict?: "all" | "none" | {
        include?: string[],
        exclude?: string[]
    }
}

export const defaultUpgradeOptions: UpgradeOptions = {
    replicate: "all",
    manualConflict: "none"
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
        const frameworkMadeTables = ["crr_changes", "crr_clients", "crr_columns", "crr_commits", "crr_temp", "crr_documents", "crr_conflicts"];
        const tables = await this.select<{ name: string }[]>(`SELECT name FROM sqlite_master WHERE type='table' ORDER BY name`, []);
        for (const table of tables) {
            if (frameworkMadeTables.includes(table.name)) continue;
            await this.upgradeTableToCrr(table.name);
        }
    }

    async upgradeTableToCrr(tblName: string, options = defaultUpgradeOptions) {
        return await upgradeTableToCrr(this, tblName, options);
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

    //////////////////////////
    //  Git-style versioning
    //////////////////////////
    async getUncommittedChanges(documentId = "main") {
        const doc = await this.first<Document>(`SELECT * FROM "crr_documents" WHERE id = ?`, [documentId]);
        if (!doc) return [];

        const uncommittedChanges = await this.select<Change[]>(`SELECT * FROM "crr_changes" WHERE version = '0' AND document = ? ORDER BY created_at ASC`, [doc.id]);
        return uncommittedChanges;
    }
    async getUncommittedChangeCount(documentId = "main") {
        const doc = await this.first<Document>(`SELECT * FROM "crr_documents" WHERE id = ?`, [documentId]);
        if (!doc) return 0;

        const row = await this.first<{ count: number }>(`SELECT COUNT(*) as count FROM "crr_changes" WHERE version = '0' AND document = ? ORDER BY created_at ASC`, [doc.id]);
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

    /** Gets conflicts for a specific table in a document */
    async getConflicts<T>(table: string, documentId = "main") {
        return await getConflicts<T>(this, table, documentId);
    }

    /**
     * Resolve a single conflict
     * @param pk Encoded primary-key of the conflicting row. *Hint*: You can use ```pkEncodingOfRow``` to obtain it if the table contains multiple primary-keys.
     */
    async resolveConflict(table: string, pk: string, documentId = "main", choice: ConflictChoice) {
        return await resolveConflict(this, table, pk, documentId, choice);
    }

    /** Gets a snapshot of a document at a certain commit */
    async getDocumentSnapshot(commit: Commit) {
        return await getDocumentSnapshot(this, commit);
    }

    /** Gets the non-pushed commit count for a given document */
    async getPushCount(documentId = "main") {
        return await getPushCount(this, documentId);
    }

    /** Returns the HEAD commit of the document */
    async getHead(documentId = "main") {
        return await getHead(this, documentId);
    }

    //////////////////////////

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
    try {
        const doc = await db.first<Document>(`SELECT * FROM "crr_documents" WHERE id = ?`, [documentId]);
        if (!doc) {
            await db.exec(`INSERT OR IGNORE INTO "crr_documents" (id, head) VALUES (?, ?)`, [documentId, null]);
        }

        // Produce a new clock value for the changes
        const clock = await createTimestamp(db);

        // Update the document to which we are adding changes
        await db.exec(`UPDATE "crr_temp" SET document = ?`, [documentId]);

        const tblName = sqlExplainExec(sql);

        if (db.crrColumns[tblName] === undefined) {
            console.error(`Table '${tblName}' have not been upgraded to a crr table. Upgrade the table with upgradeTableToCrr("${tblName}") to begin tracking changes on it`);
            return;
        }

        await db.exec(`BEGIN EXCLUSIVE TRANSACTION;`, [], { notify: false });
        await db.exec(sql, params, { notify: false });
        await db.exec(`COMMIT;`, [], { notify: false });

        const appliedChanges = await db.select<Change[]>(`SELECT * FROM "crr_changes" WHERE created_at = ? AND site_id = ?`, [clock, db.siteId]);

        await saveFractionalIndexCols(db, appliedChanges);
        await saveChanges(db, appliedChanges);
        // await compactChanges(db, appliedChanges);
    } catch (e) {
        await db.exec(`ROLLBACK;`, [], { notify: false });
        return e;
    }
}