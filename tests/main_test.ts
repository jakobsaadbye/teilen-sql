import { SqliteDB, applyChanges } from "../index.ts";
import { assertEquals, assertLess, assertExists } from "jsr:@std/assert";
import { delay, setupTwoDatabases } from "./util.ts";
import { Commit } from "../src/versioning.ts";


export type Todo = {
    id: string
    name: string
    finished: boolean
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
    await delay(5);
    await A.execTrackChanges(`UPDATE "todos" SET name='B', finished = 1 WHERE id = 1`, []);

    const prevUpdate = (await A.select<{ applied_at: number }[]>(`SELECT applied_at FROM "crr_changes" WHERE type = 'update' AND col_id = 'name'`, []))[0];
    const newUpdate = (await A.select<{ applied_at: number }[]>(`SELECT applied_at FROM "crr_changes" WHERE type = 'update' AND col_id = 'finished'`, []))[0];

    assertLess(prevUpdate.applied_at, newUpdate.applied_at);
});

Deno.test("simple merge of A into empty B", async () => {
    const tables = `
        CREATE TABLE IF NOT EXISTS "todos" (
            id primary key,
            name,
            finished
        );
    `;

    const [A, B] = await setupTwoDatabases(tables);

    await A.execTrackChanges(`INSERT INTO "todos" VALUES (1, 'A', 0)`, []);
    const changes = await A.getUncommittedChanges();

    await applyChanges(B, changes);

    const todo = await B.first(`SELECT * FROM "todos" WHERE id = 1`, []);

    assertExists(todo);
    assertEquals(todo.id, 1);
    assertEquals(todo.name, "A");
    assertEquals(todo.finished, 0);
});



Deno.test("simple combined merge of A U B", async () => {
    const tables = `
        CREATE TABLE IF NOT EXISTS "todos" (
            id text primary key,
            name text,
            finished boolean
        );
    `;

    const [A, B] = await setupTwoDatabases(tables);

    await A.execTrackChanges(`INSERT INTO "todos" VALUES ('1', 'Buy milk', 0)`, []);
    const initialChanges = await A.getUncommittedChanges();
    await applyChanges(B, initialChanges);

    await A.execTrackChanges(`UPDATE "todos" SET name='Buy Coffee' WHERE id = '1'`, []);
    await B.execTrackChanges(`UPDATE "todos" SET finished=1 WHERE id = '1'`, []);

    const AsChanges = await A.getUncommittedChanges();
    const BsChanges = await B.getUncommittedChanges();

    await applyChanges(B, AsChanges);
    await applyChanges(A, BsChanges);

    const AsTodo = await A.select(`SELECT * FROM "todos" WHERE id = '1'`, []);
    const BsTodo = await B.select(`SELECT * FROM "todos" WHERE id = '1'`, []);

    const AsTodoStr = JSON.stringify(AsTodo);
    const BsTodoStr = JSON.stringify(BsTodo);

    assertEquals(AsTodoStr, BsTodoStr);
});



