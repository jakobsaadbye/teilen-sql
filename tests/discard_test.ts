import { SqliteDB, SqliteDBWrapper, applyChanges, insertCrrTablesStmt, createServerDb, CrrColumn, assert, generateUniqueId, sqlPlaceholdersMulti } from "../index.ts";
import { assertEquals, assertLess, assertGreater, assertExists } from "jsr:@std/assert";
import { setupThreeDatabases, setupTwoDatabases, setupTwoNonFinalizedDatabases } from "./util.ts";
import { receivePushCommits, Commit, applyPull, receivePushResponse } from "../src/versioning.ts";
import { attachChangeGenerationTriggers, Change } from "../src/change.ts";
import { Todo } from "@/tests/main_test.ts";

Deno.test("discard changes small", async () => {
    const tables = `
        CREATE TABLE IF NOT EXISTS "todos" (
            id text primary key,
            name text,
            finished boolean
        );
    `;

    const [A, B] = await setupTwoDatabases(tables);

    await A.execTrackChanges(`INSERT INTO "todos" VALUES ('1', 'Buy milk', 0)`, []);
    await A.commit("inserted a todo");
    await A.execTrackChanges(`DELETE FROM "todos" WHERE 1`, []);
    await A.discardChanges();

    const deletes = await A.select<Change[]>(`SELECT * FROM "crr_changes" WHERE type = 'delete'`, []);
    assertEquals(deletes.length, 0);

    const todo = await A.first(`SELECT * FROM "todos" WHERE id = '1'`, []);
    assertExists(todo);
});

Deno.test("discard changes two documents", async () => {
    const tables = `
        CREATE TABLE IF NOT EXISTS "todos" (
            id text primary key,
            name text,
            finished boolean
        );
    `;

    const [A, B] = await setupTwoDatabases(tables);

    await A.execTrackChanges(`INSERT INTO "todos" VALUES ('1', 'Buy milk', 0)`, [], "A");
    await A.execTrackChanges(`INSERT INTO "todos" VALUES ('2', 'Buy juice', 0)`, [], "B");
    await A.commit("inserted a todo", "B");

    // Discarding changes here should remove the uncomitted insert of 'Buy Milk'
    await A.discardChanges("A");

    const todos = await A.select<Todo[]>(`SELECT * FROM "todos"`, []);
    assertEquals(todos.length, 1);
    assertEquals(todos[0].id, '2');

    const docAChanges = await A.select<Change[]>(`SELECT * FROM "crr_changes" WHERE document = 'A'`, []);
    assertEquals(docAChanges.length, 0);
});

Deno.test.ignore("discard changes big", async () => {
    const tables = `
        CREATE TABLE IF NOT EXISTS "todos" (
            id text primary key,
            name text,
            finished boolean
        );
    `;

    const [A, B] = await setupTwoDatabases(tables);

    const generateTodos = (n: number): Todo[] => {
        const todos = new Array<{ id: string, name: string, finished: boolean }>(n);
        for (let i = 0; i < n; i++) {
            todos[i] = {
                id: "x" + generateUniqueId(),
                name: "test",
                finished: false
            };
        }
        return todos;
    }

    const saveTodosHelper = async (db: SqliteDB, todos: Todo[]) => {
        const values = [];
        for (const t of todos) {
            values.push(t.id, t.name, t.finished);
        }
        await db.execTrackChanges(`
            INSERT INTO "todos" 
            VALUES ${sqlPlaceholdersMulti(todos)}
            ON CONFLICT DO UPDATE SET 
                name = EXCLUDED.name,
                finished = EXCLUDED.finished
        `, values)
    }

    const saveTodos = async (db: SqliteDB, todos: Todo[]) => {
        // We need to save the todos in batches to not exceed SQLITE_MAX_VARIABLE_NUMBER of 999
        const rounds = Math.floor(todos.length / 1_000);
        for (let i = 0; i < rounds; i++) {
            let end = i * 1_000 + 1000;
            if (end > todos.length) end = todos.length - 1;
            const batch = todos.slice(i * 1_000, end);
            await saveTodosHelper(db, batch);
        }
    }

    const N = 25_000;
    const todos = generateTodos(N);

    console.time("insert-todos");
    await saveTodos(A, todos);
    console.timeEnd("insert-todos");

    await A.commit("inserted a big stack of todos. We have work to do people ...");

    // Mark all of them finished
    for (const todo of todos) {
        todo.finished = true;
    }

    console.time("update-todos");
    await saveTodos(A, todos);
    console.timeEnd("update-todos");

    const changeCountBefore = (await A.first<{ count: number }>(`SELECT COUNT(*) as count FROM "crr_changes"`, [])).count;
    assertEquals(changeCountBefore, N * 3 + N);

    // Discard the previous update
    console.time("discard-changes");
    await A.discardChanges();
    console.timeEnd("discard-changes");

    const changeCountAfter = (await A.first<{ count: number }>(`SELECT COUNT(*) as count FROM "crr_changes"`, [])).count;
    assertEquals(changeCountAfter, N * 3);
})