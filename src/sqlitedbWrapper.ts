import { Database } from "jsr:@db/sqlite@0.12";
import { Change, Client, CrrColumn } from "./change.ts";
import { SqliteForeignKey, SqliteDB, assignSiteId, execTrackChangesHelper } from "./sqlitedb.ts";
import { insertCrrTablesStmt, } from "./tables.ts";

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
        if (err) throw err;
    }

    async execTrackChanges(sql: string, params: any[]) {
        // TODO: This will become a place where we would actually create a new hybrid logical clock.
        // Note:
        //       It is only here in the wrapper for the server as we don't have access to the updateHook, so change
        //       generation is purely done in triggers which greatly limits what we can do. Thus we insert things into tables before
        //       the triggers run so they can query computed values
        const now = (new Date).getTime();
        await this.exec(`INSERT OR REPLACE INTO "crr_hlc" (time) VALUES (?)`, [now]);

        await execTrackChangesHelper(this, sql, params);
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
        const frameworkMadeTables = ["crr_changes", "crr_clients", "crr_columns", "crr_hlc"];
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

    async getMyChanges() {
        const client = await this.first<Client>(`SELECT * FROM "crr_clients" WHERE site_id = ?`, [this.siteId]);
        if (!client) return;

        const lastPushedAt = client.last_pushed_at;
        const changes = await this.select<Change[]>(`SELECT * FROM "crr_changes" WHERE applied_at > ? AND site_id = ? ORDER BY created_at ASC`, [lastPushedAt, this.siteId]);
        return changes;
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