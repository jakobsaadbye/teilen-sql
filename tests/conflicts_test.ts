import { finalizeDatabases, pullCommits, pushCommits, setupThreeDatabases, setupThreeNonFinalizedDatabases, Todo, todosMatch } from "./_common.ts";
import { assertEquals } from "jsr:@std/assert@0.221/assert-equals";
import { RowConflict } from "../index.ts";

Deno.test("Concurrent changes to cell produces a conflict", async () => {
    const tables = `
        CREATE TABLE IF NOT EXISTS "todos" (
            id text primary key,
            name text,
            finished boolean
        );
    `;

    const [A, B, S] = await setupThreeDatabases(tables);

    for (const db of [A, B, S]) {
        await db.upgradeTableToCrr("todos", { 
            manualConflict: "all"
        });
    }
    await finalizeDatabases(A, B, S);

    await A.execTrackChanges(`INSERT INTO "todos" VALUES ('1', 'Buy milk', 0)`, []);
    await A.commit("We need milk");

    await pushCommits(A, S);
    await pullCommits(B, S);

    // Both update the name of the todo
    await A.execTrackChanges(`UPDATE "todos" SET name = 'Buy 2 jugs of milk'`, []);
    await A.commit("We actually need 2");
    await B.execTrackChanges(`UPDATE "todos" SET name = 'Buy coffee'`, []);
    await B.commit("I looked in the fridge, we have enough milk, but we need coffee!");

    await pushCommits(A, S);
    const results = await pullCommits(B, S);

    assertEquals(results.length, 1); // 1 result for the main document

    // 1 conflict is expected to be produced on the "name"
    const conflicts = results[0].conflicts as RowConflict<Todo>[];

    assertEquals(conflicts.length, 1);
    assertEquals(conflicts[0].columns.length, 1);
    assertEquals(conflicts[0].columns[0], "name");
});

Deno.test("Resolving a manual conflict", async () => {
    const tables = `
        CREATE TABLE IF NOT EXISTS "todos" (
            id text primary key,
            name text,
            finished boolean
        );
    `;

    const [A, B, S] = await setupThreeNonFinalizedDatabases(tables);

    // Have manual conflict resolution on the 'name' column
    for (const db of [A, B, S]) {
        await db.upgradeTableToCrr("todos", { 
            manualConflict: "all"
        });
    }
    await finalizeDatabases(A, B, S);

    // Both update the name of a todo to create a conflict
    await A.execTrackChanges(`INSERT INTO "todos" VALUES ('1', 'Buy milk', 0)`, []);
    await A.commit("We need milk");

    await pushCommits(A, S);
    await pullCommits(B, S);
    
    await A.execTrackChanges(`UPDATE "todos" SET name = 'Buy 2 jugs of milk'`, []);
    await A.commit("We actually need 2");
    await B.execTrackChanges(`UPDATE "todos" SET name = 'Buy coffee'`, []);
    await B.commit("I looked in the fridge, we have enough milk, but we need coffee!");

    await pushCommits(A, S);

    // Pulling here should yield a conflict for B
    await pullCommits(B, S);

    const conflicts = await B.getConflicts<Todo>("todos");
    assertEquals(conflicts.length, 1);

    return;
    // Accept A's change
    await B.resolveConflict("todos", "1", "main", "their");

    await pushCommits(B, S);
    await pullCommits(A, S);

    assertEquals(await todosMatch(A, B), true);
    assertEquals(await todosMatch(A, S), true);
})