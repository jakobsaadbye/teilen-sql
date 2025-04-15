import { Database } from "jsr:@db/sqlite@0.12";
import { Change, Client, CrrColumn } from "./change.ts";
import { SqliteForeignKey, SqliteDB, assignSiteId, execTrackChangesHelper } from "./sqlitedb.ts";
import { insertCrrTablesStmt, } from "./tables.ts";
import { checkout, commit, discardChanges, Document, preparePullCommits, preparePushCommits, PullRequest, PushRequest, receivePullCommits, receivePushCommits } from "./versioning.ts";

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
        // console.log(sql, params);
        return await this.#db.prepare(sql).all(...params) as T;
    }

    async selectWithError<T>(sql: string, params: any[]): Promise<{ data: T, error?: Error }> {
        // console.log(sql, params);
        try {
            const data = await this.#db.prepare(sql).all(...params) as T;
            return { data, error: undefined };
        } catch (e) {
            return { data: undefined, error: e };
        }
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

        await this.extractPks();
    }

    async getUncommittedChanges(documentId = "main") {
        const doc = await this.first<Document>(`SELECT * FROM "crr_documents" WHERE id = ?`, [documentId]);
        if (!doc) return [];

        const uncommittedChanges = await this.select<Change[]>(`SELECT * FROM "crr_changes" WHERE version = '0' AND document = ? ORDER BY created_at ASC`, [doc.id]);
        return uncommittedChanges;
    }

    async preparePushCommits(documentId = "main") {
        return await preparePushCommits(this, documentId);
    }

    async preparePull() {
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