import { countCommits, delay, maybeAutoPull, pullCommits, pushCommits, setupThreeDatabases, todosAsStr } from "./_common.ts";
import { assertEquals } from "jsr:@std/assert/assert-equals";
import { todoTable } from "./_common.ts";

Deno.test("Push changes (A is up-to-date)", async () => {
    const [A, _, S] = await setupThreeDatabases(todoTable);

    await A.execTrackChanges(`INSERT INTO "todos" VALUES ('1', 'Buy Milk', 0)`, []);
    await A.commit("We need milk");

    const Apush = await A.preparePushCommits();
    const response = await S.receivePushCommits(Apush);
    assertEquals(response.status, "ok");

    await maybeAutoPull(A, S, response);

    const AchangeCount = (await A.first<{ count: number }>(`SELECT COUNT(*) as count FROM "crr_changes"`, []))?.count ?? -99;
    const SchangeCount = (await S.first<{ count: number }>(`SELECT COUNT(*) as count FROM "crr_changes"`, []))?.count ?? 99;

    assertEquals(AchangeCount, SchangeCount);
});

Deno.test("Pull new commits", async () => {
    const [A, B, S] = await setupThreeDatabases(todoTable);

    await A.execTrackChanges(`INSERT INTO "todos" VALUES ('1', 'Buy Milk', 0)`, []);
    await A.commit("A");
    await pushCommits(A, S);

    await delay(5);

    await pullCommits(B, S);
    await B.execTrackChanges(`INSERT INTO "todos" VALUES ('2', 'Buy Coffee', 0)`, []);
    await B.commit("B");
    await pushCommits(B, S);
    
    await pullCommits(A, S); // We should get the pushed commit of B
    
    assertEquals(await countCommits(A), 2);
});



