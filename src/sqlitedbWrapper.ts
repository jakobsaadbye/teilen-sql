import { Database } from "jsr:@db/sqlite@0.12";
import { Change, CrrColumn } from "./change.ts";
import { SqliteForeignKey, SqliteDB, assignSiteId, execTrackChangesHelper, defaultUpgradeOptions } from "./sqlitedb.ts";
import { insertCrrTablesStmt, } from "./tables.ts";
import { checkout, Commit, commit, ConflictChoice, discardChanges, Document, getConflicts, getHead, getPushCount, preparePullCommits, preparePushCommits, PullRequest, PushRequest, receivePullCommits, receivePushCommits, resolveConflict } from "./versioning.ts";
import { getDocumentSnapshot } from "./snapshot.ts";
import { upgradeTableToCrr } from "./sqlitedbCommon.ts";

/**
 * A simple wrapper on-top of Deno sqlite3 to let javascript server-side code
 * run the applyChanges() function without the applyChanges() function knowing which database driver is being used 
 * underneath
 * 
 * NOTE: We might make the SqliteDB into some kind of typescript interface
 * so that it will be easier (and more typesafe) to make wrappers for other sqlite3
 * drivers other than Denos, e.g in node.js.
 */
export class SqliteDBWrapper {
    #db: Database
    siteId = "";
    pks: { [tblName: string]: string[] } = {};
    crrColumns: { [tbl_name: string]: CrrColumn[] } = {};

    constructor(db: Database) {
        this.#db = db;
        this.siteId = "";
        this.pks = {};
        this.crrColumns = {};

        this.exec(`PRAGMA foreign_keys = OFF`, []);
    }

    async exec(sql: string, params: any[], options: { notify?: boolean } = { notify: true }) {
        try {
            // console.log(sql, params);
            this.#db.exec(sql, ...params);
        } catch (e) {
            console.error(e);
            return e;
        }
    }

    async execOrThrow(sql: string, params: any[], options: { notify?: boolean } = { notify: true }) {
        const err = await this.exec(sql, params, options);
        if (err) {
            console.error(err);
            throw err
        };
    }

    async execTrackChanges(sql: string, params: any[], documentId = "main") {
        const err = await execTrackChangesHelper(this, sql, params, documentId);
        return err;
    }

    async first<T>(sql: string, params: any[]): Promise<T | undefined> {
        return await this.#db.prepare(sql).get(...params) as (T | undefined);
    }

    async select<T>(sql: string, params: any[]): Promise<T> {
        return await this.#db.prepare(sql).all(...params) as T;
    }

    async selectWithError<T>(sql: string, params: any[]): Promise<{ data: T, error?: Error }> {
        try {
            const data = await this.#db.prepare(sql).all(...params) as T;
            return { data, error: undefined };
        } catch (e) {
            return { data: undefined, error: e };
        }
    }

    async tx<T>(fn: () => T) : Promise<T> {
        await this.exec(`BEGIN;`, []);
        try {
            const result = await fn();
            await this.exec(`COMMIT;`, []);
            return result;
        } catch (e) {
            await this.exec(`ROLLBACK;`, []);
            throw e;
        }
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

        await this.extractPks();
    }

    /** 
     * Gets non-pushed change count for a document.
     * @Note Use this function when doing synchronous/real-time style updates as opposed to getUncommittedChangeCount() 
     *  which assummes a commit based model
    */
    async getChangeCount(documentId = "main") {
        const doc = await this.first<Document>(`SELECT * FROM "crr_documents" WHERE id = ?`, [documentId]);
        if (!doc) return 0;

        const row = await this.first<{ count: number }>(`SELECT COUNT(*) as count FROM "crr_changes" WHERE version = '0' AND site_id = ? AND applied_at > ? AND document = ? ORDER BY created_at ASC`, [this.siteId, doc.last_pushed_at, doc.id]);
        return row!.count;
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

    /** Return the document with given id */
    async getDocument(documentId: string) {
        return this.first<Document>(`SELECT * FROM "crr_documents" WHERE id = ?`, [documentId]);
    }

    //////////////////////////

    private async extractPks() {
        const pks: { [tblName: string]: string[] } = {};
        const tables = await this.select<{ name: string }[]>(`SELECT name FROM sqlite_master WHERE type='table' ORDER BY name`, []);
        for (const table of tables) {
            const columns = await this.select<{ pk: number, name: string }[]>(`PRAGMA table_info('${table.name}')`, []);
            pks[table.name] = columns.filter(c => c.pk !== 0).map(c => c.name);
        }
        this.pks = pks;
    }
}

export const createServerDb = async (db: Database) => {
    const wDb = new SqliteDBWrapper(db) as SqliteDB;

    await wDb.exec(insertCrrTablesStmt, []);

    await assignSiteId(wDb);

    return wDb;
}