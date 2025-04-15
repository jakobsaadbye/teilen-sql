import { setupThreeDatabases } from "./util.ts";

Deno.test("adding changes to different documents", async () => {
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

Deno.test("asdjasdjasdasd", async () => {
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
    // await A.execTrackChanges(`UPDATE "todos" SET finished = 1 WHERE id = '1'`, []);
    // await A.commit("Got it!");
})

