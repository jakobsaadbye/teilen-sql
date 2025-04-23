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

Deno.test("A gets commits before its latest commit", async () => {
    // Description:
    //   Checks that client A gets commits from remote that were made prior to A
    //   pushing.

    const [A, B, S] = await setupThreeDatabases(todoTable);
    
    await B.execTrackChanges(`INSERT INTO "todos" VALUES ('2', 'Buy Coffee', 0)`, []);
    await B.commit("X");

    await delay(5);
    
    await A.execTrackChanges(`INSERT INTO "todos" VALUES ('1', 'Buy Milk', 0)`, []);
    await A.commit("A");
    await delay(5);
    await A.execTrackChanges(`INSERT INTO "todos" VALUES ('3', 'Buy Banannas', 0)`, []);
    await A.commit("B");

    await pushCommits(A, S);
    await pushCommits(B, S); // Expected to result in a merge
    
    // Expect the pull to contain the merge and commit X from B that was made earlier
    await pullCommits(A, S);
    
    assertEquals(await countCommits(A), 4);
    assertEquals(await countCommits(B), 4);
    assertEquals(await countCommits(S), 4);

    assertEquals(await todosAsStr(A), await todosAsStr(B));
    assertEquals(await todosAsStr(A), await todosAsStr(S));
});



