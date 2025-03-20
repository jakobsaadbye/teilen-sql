import { Database } from "jsr:@db/sqlite@0.12";
import { SqliteDB, SqliteDBWrapper, applyChanges, insertCrrTablesStmt, createServerDb, CrrColumn, assert } from "../index.ts";
import { assignSiteId } from "../src/sqlitedb.ts";
import { assertEquals, assertLess, assertGreater, assertExists } from "jsr:@std/assert";

const setupTwoDatabases = async (tables: string) => {
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

Deno.test("change generation", async () => {
    const tables = `
        CREATE TABLE IF NOT EXISTS "todos" (
            id primary key,
            name,
            finished
        );
    `;

    const [A, B] = await setupTwoDatabases(tables);

    await A.execTrackChanges(`INSERT INTO "todos" VALUES (1, 'A', 0)`, []);
    await A.execTrackChanges(`UPDATE "todos" SET name='B' WHERE id = 1`, []);

    // Check that we don't replace an update change if the value doesn't change. 
    // Even though we set name='B' again in the update, the change row should still contain
    // the old timestamp
    await delay(50);
    await A.execTrackChanges(`UPDATE "todos" SET name='B', finished=1 WHERE id = 1`, []);

    const prevUpdate = (await A.select<{ applied_at: number }[]>(`SELECT applied_at FROM "crr_changes" WHERE type = 'update' AND col_id = 'name'`, []))[0];
    const newUpdate = (await A.select<{ applied_at: number }[]>(`SELECT applied_at FROM "crr_changes" WHERE type = 'update' AND col_id = 'finished'`, []))[0];

    assertLess(prevUpdate.applied_at, newUpdate.applied_at);
});

Deno.test("merge A into B", async () => {
    const tables = `
        CREATE TABLE IF NOT EXISTS "todos" (
            id primary key,
            name,
            finished
        );
    `;

    const [A, B] = await setupTwoDatabases(tables);

    await A.execTrackChanges(`INSERT INTO "todos" VALUES (1, 'A', 0)`, []);
    const changes = await A.getMyChanges();

    await applyChanges(B, changes);

    const todo = await B.first(`SELECT * FROM "todos" WHERE id = 1`, []);
    
    assertExists(todo);
    assertEquals(todo.id, 1);
    assertEquals(todo.name, "A");
    assertEquals(todo.finished, 0);
});



const delay = async (milliseconds: number) => {
    await new Promise((resolve, reject) => {
        setTimeout(() => {
            resolve(true);
        }, milliseconds);
    });
}

const attachTriggers = async (db: SqliteDB) => {
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