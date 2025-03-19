import { Database } from "jsr:@db/sqlite@0.12";
import { CrrColumn } from "./change.ts";
import { SqliteForeignKey } from "./sqlitedb.ts";

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
    pks: { [tblName: string]: string[] } = {};
    crrColumns: { [tbl_name: string]: CrrColumn[] } = {};

    constructor(db: Database) {
        this.#db = db;

        this.exec(`PRAGMA foreign_keys = OFF`, []);
    }

    async exec(sql: string, params: any[], options: { notify?: boolean } = { notify: true }) {
        try {
            // console.log(sql, params);
            this.#db.exec(sql, ...params);
        } catch (e) {
            return e;
        }
    }

    async execOrThrow(sql: string, params: any[], options: { notify?: boolean } = { notify: true }) {
        const err = await this.exec(sql, params, options);
        if (err) throw err;
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
        const frameworkMadeTables = ["crr_changes", "crr_clients", "crr_columns"];
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

    private async finalize() {
        const crrColumns = await this.select<CrrColumn[]>(`SELECT * FROM "crr_columns"`, []);
        this.crrColumns = Object.groupBy(crrColumns, ({ tbl_name }) => tbl_name) as { [tbl_name: string]: CrrColumn[] };

        await this.extractPks();
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