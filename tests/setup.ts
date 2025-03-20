import { Database } from "jsr:@db/sqlite@0.12";
import { SqliteDB, SqliteDBWrapper, applyChanges, insertCrrTablesStmt, createServerDb, CrrColumn, assert } from "../index.ts";
import { assignSiteId } from "../src/sqlitedb.ts";
import { assertEquals, assertLess, assertGreater, assertExists } from "jsr:@std/assert";

export const setupTwoDatabases = async (tables: string) => {
    const _A = new Database("./tests/db_A.db", { int64: true });
    const _B = new Database("./tests/db_B.db", { int64: true });

    const A = new SqliteDBWrapper(_A) as SqliteDB;
    const B = new SqliteDBWrapper(_B) as SqliteDB;

    await dropAllTables(A);
    await dropAllTables(B);

    await A.exec(insertCrrTablesStmt, []);
    await B.exec(insertCrrTablesStmt, []);
    await assignSiteId(A);
    await assignSiteId(B);

    
    await A.exec(tables, []);
    await B.exec(tables, []);
    
    await A.upgradeAllTablesToCrr();
    await B.upgradeAllTablesToCrr();
    await A.finalize();
    await B.finalize();
    await attachTriggers(A);
    await attachTriggers(B);

    return [A, B];
}

export const setupTwoNonFinalizedDatabases = async (tables: string) => {
    const _A = new Database("./tests/db_A.db", { int64: true });
    const _B = new Database("./tests/db_B.db", { int64: true });

    const A = new SqliteDBWrapper(_A) as SqliteDB;
    const B = new SqliteDBWrapper(_B) as SqliteDB;

    await dropAllTables(A);
    await dropAllTables(B);

    await A.exec(insertCrrTablesStmt, []);
    await B.exec(insertCrrTablesStmt, []);
    await assignSiteId(A);
    await assignSiteId(B);

    await A.exec(tables, []);
    await B.exec(tables, []);

    return [A, B];
}

export const attachTriggers = async (db: SqliteDB) => {
    const crrColumns = db.crrColumns;
    for (const [tblName, columns] of Object.entries(crrColumns)) {

        const getPk = (type: "insert" | "update" | "delete") => {
            const pkCols = db.pks[tblName];
            const prefix = type === "delete" ? "OLD" : "NEW";
            let pk = `${prefix}.${pkCols[0]}`;
            if (pkCols.length > 1) {
                const pkValue = pkCols.map(colId => `${prefix}.${colId}`).join(",'|',");
                pk = `concat(${pkValue})`;
            }
            return pk;
        }

        const columnInsert = (col: CrrColumn) => {
            return `
                INSERT OR IGNORE INTO crr_changes(type, tbl_name, col_id, pk, value, site_id, created_at, applied_at)
                VALUES ('insert', '${tblName}', '${col.col_id}', ${getPk("insert")}, NEW.${col.col_id}, '${db.siteId}', 
                        (SELECT time FROM crr_hlc LIMIT 1), 
                        (SELECT time FROM crr_hlc LIMIT 1)
                );
            `
        }

        const columnUpdate = (col: CrrColumn) => {
            return `
                INSERT OR REPLACE INTO crr_changes(type, tbl_name, col_id, pk, value, site_id, created_at, applied_at)
                SELECT 'update', '${tblName}', '${col.col_id}', ${getPk("update")}, NEW.${col.col_id}, '${db.siteId}',
                        (SELECT time FROM crr_hlc LIMIT 1), 
                        (SELECT time FROM crr_hlc LIMIT 1)
                WHERE OLD.${col.col_id} != NEW.${col.col_id}
                ;
            `
        }

        const columnInserts = columns.map(col => columnInsert(col)).join("\n");
        const columnUpdates = columns.map(col => columnUpdate(col)).join("\n");

        const insertTrigger = `
            CREATE TRIGGER IF NOT EXISTS _${tblName}_insert
            AFTER INSERT ON ${tblName}
            FOR EACH ROW
            BEGIN
                ${columnInserts}
            END;
        `;

        const updateTrigger = `
            CREATE TRIGGER IF NOT EXISTS _${tblName}_update
            AFTER UPDATE ON ${tblName}
            FOR EACH ROW
            BEGIN
                ${columnUpdates}
            END;
        `

        const deleteTrigger = `
            CREATE TRIGGER IF NOT EXISTS _${tblName}_delete
            AFTER DELETE ON ${tblName}
            FOR EACH ROW
            BEGIN
                INSERT OR IGNORE INTO crr_changes(type, tbl_name, col_id, pk, value, site_id, created_at, applied_at)
                VALUES ('delete', '${tblName}', 'tombstone', ${getPk("delete")}, 1, '${db.siteId}', 
                        (SELECT time FROM crr_hlc LIMIT 1), 
                        (SELECT time FROM crr_hlc LIMIT 1) 
                );
            END;
        `;

        await db.exec(insertTrigger, []);
        await db.exec(updateTrigger, []);
        await db.exec(deleteTrigger, []);
    }

}

const dropAllTables = async (db: SqliteDB) => {
    const tables = await db.select<{ name: string }[]>(`SELECT name FROM sqlite_master WHERE type='table' ORDER BY name`, []);
    for (const table of tables) {
        await db.exec(`DROP TABLE IF EXISTS "${table.name}"`, []);
    }
}