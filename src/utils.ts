import type { SqliteDB } from "@/src/sqlitedb.ts";

type SqlOperation = "none" | "select" | "insert" | "update" | "delete" | "pragma" | "explain";

export const sqlAsSelectStmt = (sql: string) => {
    const s = sql.trim().split(' ');
    switch (s[0].toLowerCase()) {
        case "insert": {
            s.shift();
            s.shift();
            const tableName = s[0];
            return `SELECT rowid, * FROM ${tableName}`;
        }
        case "update": {
            s.shift();
            const tableName = s[0];
            const whereIndex = s.findIndex(tok => tok.toLowerCase() === "where");
            if (whereIndex === -1) {
                console.error("Missign WHERE clause in UPDATE statement");
                return null;
            }
            const condition = s.slice(whereIndex + 1).join(' ');
            return `SELECT rowid, * FROM ${tableName} WHERE ${condition}`;
        }
        case "delete": {
            s.shift();
            s.shift();
            const tableName = s[0];
            s.shift();
            s.shift();
            const condition = s.join(' ');
            return `SELECT rowid, * FROM ${tableName} WHERE ${condition}`;
        }
        default:
            console.error(`Unknown start of sql statement in sqlAsSelectStmt(). Starts with ${s[0]}`);
            return null;
    }
}

/* Generates a comma seperated list of placeholders for the length of the array */
export const sqlPlaceholders = (a: any[]) => {
    return `${a.map(_ => `?`).join(',')}`;
}

/** Generates a set of placeholders each the length of the amount of keys in objects of a.
   
    Useful when populating values inside a sql statement

   @Example ```
      const a = [
        { id: '1', 'title' : 'hello' }, 
        { id: '2', 'title' : 'world' }
      ]
      
      Calling sqlPlaceholdersMulti(a) -> (?, ?) (?, ?)
    ```
*/
export const sqlPlaceholdersMulti = (a: any[]) => {
    if (a.length === 0) return "()";
    const nKeys = Object.keys(a[0]);
    return `${a.map(_ => '(' + sqlPlaceholders(nKeys) + ') ')}`;
}

export const sqlDetermineOperation = (sql: string) : SqlOperation => {
    const s = sql.trim().split(' ');
    switch (s[0].toLowerCase()) {
        case "select": return "select";
        case "pragma": return "pragma";
        case "explain": return "explain";
        case "insert": return "insert";
        case "update": return "update";
        case "delete": return "delete";
        default: 
            return "none"
    }
}

export const sqlExplainExec = (sql: string): string => {
    const s = sql.trim().split(' ');
    switch (s[0].toLowerCase()) {
        case "insert": {
            s.shift();
            s.shift();
            const tableName = s[0];
            return tableName.replaceAll(`"`, '').trim();
        }
        case "update": {
            s.shift();
            const tableName = s[0];
            return tableName.replaceAll(`"`, '').trim();
        }
        case "delete": {
            s.shift();
            s.shift();
            const tableName = s[0];
            return tableName.replaceAll(`"`, '').trim();
        }
        default: {
            return "";
        }
    }
}

export const sqlExplainQuery = async (db: SqliteDB, sql: string): Promise<string[]> => {
    const s = sql.trim().split(' ');
    switch (s[0].toLowerCase()) {
        case "pragma": {
            s.shift();
            if (s[0].includes("table_info")) {
                const split = s[0].split("(");
                if (split.length > 1) {
                    const tblName = split[1].replaceAll("'", "").replaceAll(")", "");
                    return [tblName]
                }
            }
            return [];
        }
        case "select": {
            const rows = await db.select<{ detail: string }[]>(`EXPLAIN QUERY PLAN ${sql}`, []);
            if (rows.length === 0) return [];
            const tblNames = [];
            for (const row of rows) {
                if (row.detail.includes("SCAN")) {
                    const tblName = row.detail.split(" ")[1];
                    tblNames.push(tblName)
                }
            }
            return tblNames;
        }
        default: {
            return [];
        }
    }
}

export const pkEncodingOfRow = (db: SqliteDB, tblName: string, row: any) => {
    const pkCols = db.pks[tblName];
    assert(pkCols && pkCols.length > 0, `No known primary-keys for table '${tblName}'`);
    return Object.entries(row).filter(([colId, _]) => pkCols.includes(colId)).map(([_, value]) => value).join('|');
}

export const assert = (expr: unknown, msg?: string): asserts expr => {
    if (!expr) {
        throw new Error(msg ?? "Assertion failed");
    }
}

export const unique = (arr: any[]) => {
    return [...new Set(arr)];
}

export const generateUniqueId = () => {
    return crypto.randomUUID().split("-")[0];
}

const insertRowsHelper = async (db: SqliteDB, tblName: string, rows: any[]) => {
    const pkCols = db.pks[tblName];
    const cols = Object.keys(rows[0]);

    const valueSets = rows.map(row => Object.values(row));
    const allVals = valueSets.reduce((allVals, vals) => [...allVals, ...vals], []);

    const updateStr = cols.filter(col => !pkCols.includes(col)).map(col => `${col} = EXCLUDED.${col}`).join(",\n")
    
    await db.execOrThrow(`
        INSERT INTO "${tblName}" (${cols.join(', ')})
        VALUES ${valueSets.map(vals => `(${sqlPlaceholders(vals)})`).join(",")}
        ON CONFLICT DO UPDATE SET
            ${updateStr}
    `, allVals);
}

export const insertRows = async (db: SqliteDB, tblName: string, rows: any[]) => {
    if (rows.length === 0) return;

    const rounds = Math.ceil(rows.length / 1_000);
    for (let i = 0; i < rounds; i++) {
        const start = i * 1_000;
        let end = start + 1_000;
        if (end >= rows.length) end = rows.length;

        const batch = rows.slice(start, end);
        await insertRowsHelper(db, tblName, batch);
    }
}