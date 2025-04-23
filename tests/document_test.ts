import { setupThreeDatabases } from "./_common.ts";

Deno.test("Adding changes to different documents", async () => {
    const tables = `
        CREATE TABLE IF NOT EXISTS "todos" (
            id text primary key,
            name text,
            finished boolean
        );
    `;

    const [A, B, S] = await setupThreeDatabases(tables);

    await A.execTrackChanges(`INSERT INTO "todos" VALUES ('1', 'Buy Milk', 0)`, [], "A");
    await A.commit("We need milk", "A");
    await A.execTrackChanges(`INSERT INTO "todos" VALUES ('2', 'Buy Juice', 0)`, [], "B");
    await A.commit("We need juice", "B");
});