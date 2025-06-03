import { insertRandomTodos, pullCommits, pushCommits, saveTodo, setupThreeDatabases, todoTable } from "@/tests/_common.ts";
import { SqliteDB } from "@/index.ts";


// @Incomplete
Deno.test("Databases remain eventual consistent after merge", async () => {
    const [A, B, S] = await setupThreeDatabases(todoTable);

    await insertRandomTodos(A, 5);
    await A.commit("We got a lot of work people!");

    await syncLeft2Right(A, B, S);
})

const syncLeft2Right = async (A: SqliteDB, B: SqliteDB, S: SqliteDB) => {
    await pushCommits(A, S);
    await pullCommits(B, S);
}