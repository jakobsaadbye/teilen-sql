import { Database } from "jsr:@db/sqlite@0.12";
import { CrrColumn } from "./change.ts";
import { ForeignKey } from "./sqlitedb.ts";
import { ms } from "./ms.ts";

export class SqliteDBWrapper {
    #db: Database
    pks: { [tblName: string]: string[] } = {};
    crrColumns: { [tbl_name: string]: CrrColumn[] } = {};

    constructor(db: Database) {
        this.#db = db;

        this.exec(`PRAGMA foreign_keys = ON`, []);

        this.extractPks().catch(e => console.error(e));
        this.finalizeUpgrades().catch(e => console.error(e));
    }

    async exec(sql: string, params: any[]) {
        try {
            console.log(sql, params);
            this.#db.exec(sql, ...params);
        } catch (e) {
            return e;
        }
    }

    async first<T>(sql: string, params: any[]): Promise<T | undefined> {
        return await this.#db.prepare(sql).get(...params) as (T | undefined);
    }

    async select<T>(sql: string, params: any[]): Promise<T> {
        console.log(sql, params);
        return await this.#db.prepare(sql).all(...params) as T;
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
        const deleteWinsAfterMs = ms(deleteWinsAfter);
        const columns = await this.select<any[]>(`PRAGMA table_info('${tblName}')`, []);
        const fks = await this.select<ForeignKey[]>(`PRAGMA foreign_key_list('${tblName}')`, []);
        const values = columns.map(c => {
            const fk = this.fkOrNull(c, fks);
            if (fk) return `('${tblName}', '${c.name}', 'lww', '${fk.table}|${fk.to}', '${fk.on_delete}', '${deleteWinsAfterMs}', null)`;
            else return `('${tblName}', '${c.name}', 'lww', null, null, '${deleteWinsAfterMs}', null)`;
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

    private async finalizeUpgrades() {
        const crrColumns = await this.select<CrrColumn[]>(`SELECT * FROM "crr_columns"`, []);
        this.crrColumns = Object.groupBy(crrColumns, ({ tbl_name }) => tbl_name) as { [tbl_name: string]: CrrColumn[] };
    }

    private fkOrNull(col: any, fks: ForeignKey[]): ForeignKey | null {
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