import { setupTwoDatabases, todoTable } from "@/tests/_common.ts";
import { assert } from "jsr:@std/assert@0.217/assert";
import { getDocumentSnapshot } from "@/src/versioning.ts";
import { assertEquals } from "jsr:@std/assert@0.221/assert-equals";

Deno.test("Get snapshot of a document", async () => {
    const [A, B] = await setupTwoDatabases(todoTable);

    await A.execTrackChanges(`INSERT INTO "todos" VALUES ('1', 'Buy milk', 0)`, []);
    const insertCommit = await A.commit("insert");
    await A.execTrackChanges(`UPDATE "todos" SET finished = 1 WHERE id = '1'`, []);
    const updateCommit = await A.commit("update");
    await A.execTrackChanges(`DELETE FROM "todos" WHERE id = '1'`, []);
    await A.commit("delete");

    assert(insertCommit && updateCommit);

    const doc = await getDocumentSnapshot(A, updateCommit);
    assertEquals(doc["todos"]["1"]["finished"], 1);
})