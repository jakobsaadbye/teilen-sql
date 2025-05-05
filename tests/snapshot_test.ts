import { setupTwoDatabases, Todo, todoTable } from "@/tests/_common.ts";
import { assert } from "jsr:@std/assert@0.217/assert";
import { assertEquals } from "jsr:@std/assert@0.221/assert-equals";
import { assertExists } from "jsr:@std/assert@0.221/assert-exists";

Deno.test("Snapshot contains row before delete", async () => {
    const [A, B] = await setupTwoDatabases(todoTable);

    await A.execTrackChanges(`INSERT INTO "todos" VALUES ('1', 'Buy milk', 0)`, []);
    await A.execTrackChanges(`UPDATE "todos" SET finished = 1 WHERE id = '1'`, []);
    const updateCommit = await A.commit("update");
    await A.execTrackChanges(`DELETE FROM "todos" WHERE id = '1'`, []);

    assert(updateCommit);
    
    const doc = await A.getDocumentSnapshot(updateCommit);
    
    const row = doc.getRow<Todo>("todos", "1");
    assertExists(row);
    assertEquals(row.finished, 1);
});

Deno.test("Row is removed from snapshot after delete", async () => {
    const [A, B] = await setupTwoDatabases(todoTable);

    await A.execTrackChanges(`INSERT INTO "todos" VALUES ('1', 'Buy milk', 0)`, []);
    await A.execTrackChanges(`UPDATE "todos" SET finished = 1 WHERE id = '1'`, []);
    await A.execTrackChanges(`DELETE FROM "todos" WHERE id = '1'`, []);
    const deleteCommit = await A.commit("delete");

    assert(deleteCommit);
    
    const doc = await A.getDocumentSnapshot(deleteCommit);
    
    const row = doc.getRow<Todo>("todos", "1");
    assertEquals(row, undefined);
});