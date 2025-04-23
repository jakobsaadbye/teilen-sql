import { assertEquals } from "jsr:@std/assert/assert-equals";
import { delay, setupThreeNonFinalizedDatabases } from "./_common.ts";
import { attachChangeGenerationTriggers } from "@/src/change.ts";

Deno.test("Fractional indexing", async () => {
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

    const [A, B] = await setupThreeNonFinalizedDatabases(tables);

    await A.upgradeAllTablesToCrr();
    await B.upgradeAllTablesToCrr();
    await A.upgradeColumnToFractionalIndex("todos", "position", "column_id");
    await B.upgradeColumnToFractionalIndex("todos", "position", "column_id");
    await A.finalize();
    await B.finalize();
    await attachChangeGenerationTriggers(A);
    await attachChangeGenerationTriggers(B);

    await A.execTrackChanges(`INSERT INTO "columns" VALUES ('4')`, []);
    await A.execTrackChanges(`INSERT INTO "todos" VALUES ('1', '4', 'A', ?)`, ["|append"]);
    await delay(10);
    await A.execTrackChanges(`INSERT INTO "todos" VALUES ('2', '4', 'B', ?)`, ["|append"]);
    await delay(10);
    await A.execTrackChanges(`INSERT INTO "todos" VALUES ('3', '4', 'C', ?)`, ["1"]);

    const todos = await A.select<{ id: string, position: string }[]>(`SELECT * FROM "todos" ORDER BY position`, []);
    assertEquals(todos.length, 3);
    assertEquals(todos[0].id, '1');
    assertEquals(todos[1].id, '3');
    assertEquals(todos[2].id, '2');
});