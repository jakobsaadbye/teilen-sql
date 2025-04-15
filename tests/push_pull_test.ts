import { setupThreeDatabases } from "@/tests/util.ts";
import { assertEquals } from "jsr:@std/assert/assert-equals";
import { applyPull, receivePushResponse } from "@/src/versioning.ts";
import { assertExists } from "jsr:@std/assert/assert-exists";
import { SqliteDB } from "@/src/sqlitedb.ts";
import { Todo } from "./main_test.ts";

Deno.test.ignore("push changes (A is up-to-date)", async () => {
    const tables = `
        CREATE TABLE IF NOT EXISTS "todos" (
            id text primary key,
            name text,
            finished boolean
        );
    `;

    const [A, B, S] = await setupThreeDatabases(tables);

    await A.execTrackChanges(`INSERT INTO "todos" VALUES ('1', 'Buy Milk', 0)`, []);
    await A.commit("We need milk");

    const Apush = await A.preparePushCommits();
    const response = await S.receivePush(Apush);
    assertEquals(response.status, "ok");
    await receivePushResponse(A, S, response);

    
    const AchangeCount = (await A.first<{ count: number }>(`SELECT COUNT(*) as count FROM "crr_changes"`, [])).count;
    const SchangeCount = (await S.first<{ count: number }>(`SELECT COUNT(*) as count FROM "crr_changes"`, [])).count;
    
    assertEquals(AchangeCount, SchangeCount);
});

Deno.test.ignore("push and pull changes simple", async () => {
    const tables = `
        CREATE TABLE IF NOT EXISTS "todos" (
            id text primary key,
            name text,
            finished boolean
        );
    `;

    const [A, B, S] = await setupThreeDatabases(tables);

    await A.execTrackChanges(`INSERT INTO "todos" VALUES ('1', 'Buy Milk', 0)`, []);
    await A.commit("We need milk");
    
    await quickExchangeA2B(A, B, S);
    
    await A.execTrackChanges(`UPDATE "todos" SET name = '2 Gallons of Milk' WHERE id = '1'`, []);
    await A.commit("It was 2 gallons of milk");

    await B.execTrackChanges(`UPDATE "todos" SET name = 'Buy Milk & Sugar' WHERE id = '1'`, []);
    await B.commit("We also needed sugar");
    
    await quickExchangeA2B(A, B, S);

    const todoA = await A.first<Todo>(`SELECT * FROM "todos" WHERE id = '1'`, []);
    const todoB = await A.first<Todo>(`SELECT * FROM "todos" WHERE id = '1'`, []);
    const todoS = await A.first<Todo>(`SELECT * FROM "todos" WHERE id = '1'`, []);

    assertExists(todoA);
    assertExists(todoB);
    assertExists(todoS);

    const todoAstr = JSON.stringify(todoA);
    const todoBstr = JSON.stringify(todoB);
    const todoSstr = JSON.stringify(todoS);

    assertEquals(todoAstr, todoBstr);
    assertEquals(todoAstr, todoSstr);
});


const quickExchangeA2B = async (A: SqliteDB, B: SqliteDB, S: SqliteDB) => {
    const Apush = await A.preparePushCommits();
    
    let response = await S.receivePush(Apush);
    await receivePushResponse(A, S, response);

    const Bpush = await B.preparePushCommits();
    response = await S.receivePush(Bpush);
    await receivePushResponse(B, S, response);

    const Apull = await A.preparePullCommits();
    const pull = await S.receivePull(Apull);
    await applyPull(A, pull);
}