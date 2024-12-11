import { Database } from "jsr:@db/sqlite@0.12";

export class SqliteDBWrapper {
    #db: Database
    pks: { [tblName: string]: string[] } = {};

    constructor(db: Database) {
        this.#db = db;

        this.extractPks().catch(e => console.error(e));
    }

    async exec(sql: string, params: any[]) {
        try {
            await this.#db.exec(sql, ...params);
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

    private async extractPks() {
        const pks: { [tblName: string]: string[] } = {};
        const tables = await this.select<{ name: string }[]>(`SELECT name FROM sqlite_master WHERE type='table' ORDER BY name`, []);
        for (const table of tables) {
            const columns = await this.select(`PRAGMA table_info('${table.name}')`, []);
            pks[table.name] = columns.filter(c => c.pk !== 0).map(c => c.name);
        }
        this.pks = pks;
    }
}