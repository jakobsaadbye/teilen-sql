import { Database } from "jsr:@db/sqlite@0.12";
import { SqliteDB, SqliteDBWrapper, applyChanges, insertCrrTablesStmt, createServerDb, CrrColumn, assert } from "../index.ts";
import { assignSiteId } from "../src/sqlitedb.ts";
import { assertEquals, assertLess, assertGreater, assertExists } from "jsr:@std/assert";
import { attachTriggers, setupTwoDatabases, setupTwoNonFinalizedDatabases } from "@/tests/setup.ts";

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

Deno.test.only("fractional indexing", async () => {
    const tables = `
        CREATE TABLE IF NOT EXISTS "columns" (
            id text primary key
        );

        CREATE TABLE IF NOT EXISTS "todos" (
            id text primary key,
            column_id references columns(id),
            name,
            position
        );
    `;

    const [A, B] = await setupTwoNonFinalizedDatabases(tables);

    await A.upgradeAllTablesToCrr();
    await B.upgradeAllTablesToCrr();
    await A.upgradeColumnToFractionalIndex("todos", "position", "column_id");
    await B.upgradeColumnToFractionalIndex("todos", "position", "column_id");
    await A.finalize();
    await B.finalize();
    await attachTriggers(A);
    await attachTriggers(B);

    await A.execTrackChanges(`INSERT INTO "columns" VALUES ('4')`, []);
    await A.execTrackChanges(`INSERT INTO "todos" VALUES ('1', '4', 'A', ?)`, ["|append"]);
    await A.execTrackChanges(`INSERT INTO "todos" VALUES ('2', '4', 'B', ?)`, ["|append"]);
    await A.execTrackChanges(`INSERT INTO "todos" VALUES ('3', '4', 'C', ?)`, ["1"]);

    const todos = await A.select<{id: string, position: string}[]>(`SELECT * FROM "todos" ORDER BY position`, []);
    assertEquals(todos.length, 3);
    assertEquals(todos[0].id, '1');
    assertEquals(todos[1].id, '3');
    assertEquals(todos[2].id, '2');
})


const delay = async (milliseconds: number) => {
    await new Promise((resolve, reject) => {
        setTimeout(() => {
            resolve(true);
        }, milliseconds);
    });
}

