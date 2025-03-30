import { SqliteDB, SqliteDBWrapper, applyChanges, insertCrrTablesStmt, createServerDb, CrrColumn, assert, generateUniqueId, sqlPlaceholdersMulti } from "../index.ts";
import { assertEquals, assertLess, assertGreater, assertExists } from "jsr:@std/assert";
import { setupThreeDatabases, setupTwoDatabases, setupTwoNonFinalizedDatabases } from "@/tests/setup.ts";
import { receivePush, Commit, applyPull, receivePushResponse } from "../src/versioning.ts";
import { attachChangeGenerationTriggers } from "../src/change.ts";


type Todo = {
    id: string
    name: string
    finished: boolean
}


Deno.test.only("change generation", async () => {
    const tables = `
        CREATE TABLE IF NOT EXISTS "todos" (
            id primary key,
            name,
            finished
        );
    `;

    const [A, B] = await setupTwoDatabases(tables);

    await A.execTrackChanges(`INSERT INTO "todos" VALUES (1, 'A', 0)`, []);
    await A.execTrackChanges(`UPDATE "todos" SET name='B' WHERE id = 1`, []);

    // Check that we don't replace an update change if the value doesn't change. 
    // Even though we set name='B' again in the update, the change row should still contain
    // the old timestamp
    await delay(5);
    await A.execTrackChanges(`UPDATE "todos" SET name='C' WHERE id = 1`, []);

    const prevUpdate = (await A.select<{ applied_at: number }[]>(`SELECT applied_at FROM "crr_changes" WHERE type = 'update' AND col_id = 'name'`, []))[0];
    const newUpdate = (await A.select<{ applied_at: number }[]>(`SELECT applied_at FROM "crr_changes" WHERE type = 'update' AND col_id = 'finished'`, []))[0];

    assertLess(prevUpdate.applied_at, newUpdate.applied_at);
});

Deno.test("simple merge of A into empty B", async () => {
    const tables = `
        CREATE TABLE IF NOT EXISTS "todos" (
            id primary key,
            name,
            finished
        );
    `;

    const [A, B] = await setupTwoDatabases(tables);

    await A.execTrackChanges(`INSERT INTO "todos" VALUES (1, 'A', 0)`, []);
    const changes = await A.getMyChanges();

    await applyChanges(B, changes);

    const todo = await B.first(`SELECT * FROM "todos" WHERE id = 1`, []);

    assertExists(todo);
    assertEquals(todo.id, 1);
    assertEquals(todo.name, "A");
    assertEquals(todo.finished, 0);
});

Deno.test("fractional indexing", async () => {
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

    const [A, B] = await setupTwoNonFinalizedDatabases(tables);

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

Deno.test("simple combined merge of A U B", async () => {
    const tables = `
        CREATE TABLE IF NOT EXISTS "todos" (
            id text primary key,
            name text,
            finished boolean
        );
    `;

    const [A, B] = await setupTwoDatabases(tables);

    await A.execTrackChanges(`INSERT INTO "todos" VALUES ('1', 'Buy milk', 0)`, []);
    const initialChanges = await A.getMyChanges();
    await applyChanges(B, initialChanges);

    await A.execTrackChanges(`UPDATE "todos" SET name='Buy Coffee' WHERE id = '1'`, []);
    await B.execTrackChanges(`UPDATE "todos" SET finished=1 WHERE id = '1'`, []);

    const AsChanges = await A.getMyChanges();
    const BsChanges = await B.getMyChanges();

    await applyChanges(B, AsChanges);
    await applyChanges(A, BsChanges);

    const AsTodo = await A.select(`SELECT * FROM "todos" WHERE id = '1'`, []);
    const BsTodo = await B.select(`SELECT * FROM "todos" WHERE id = '1'`, []);

    const AsTodoStr = JSON.stringify(AsTodo);
    const BsTodoStr = JSON.stringify(BsTodo);

    assertEquals(AsTodoStr, BsTodoStr);
});

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
})

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

    await saveTodos(A, todos);

    await A.commit("inserted a big stack of todos. We have work to do people ...");

    // Mark all of them finished
    for (const todo of todos) {
        todo.finished = true;
    }

    await saveTodos(A, todos);

    const changeCountBefore = (await A.first<{ count: number }>(`SELECT COUNT(*) as count FROM "crr_changes"`, [])).count;
    assertEquals(changeCountBefore, N * 3 + N);

    // Discard the previous update
    console.time("discard-changes");
    await A.discardChanges();
    console.timeEnd("discard-changes");

    const changeCountAfter = (await A.first<{ count: number }>(`SELECT COUNT(*) as count FROM "crr_changes"`, [])).count;
    assertEquals(changeCountAfter, N * 3);
})

Deno.test("checkout back and forth", async () => {
    const tables = `
        CREATE TABLE IF NOT EXISTS "todos" (
            id text primary key,
            name text,
            finished boolean
        );
    `;

    const [A, B] = await setupTwoDatabases(tables);

    await A.execTrackChanges(`INSERT INTO "todos" VALUES ('1', 'Buy milk', 0)`, []);
    const firstCommit = await A.commit("Added a todo");

    await A.execTrackChanges(`UPDATE "todos" SET name='Buy Coffee', finished=1 WHERE id = '1'`, []);
    const latestCommit = await A.commit("Updated status of todo to finished. Jahuuu");

    await A.checkout(firstCommit.id);

    let todo = await A.first(`SELECT * FROM "todos" WHERE id = '1'`, []);
    assertExists(todo);
    assertEquals(todo.name, "Buy milk");
    assertEquals(todo.finished, 0);

    await A.checkout(latestCommit.id);

    todo = await A.first(`SELECT * FROM "todos" WHERE id = '1'`, []);
    assertExists(todo);
    assertEquals(todo.name, "Buy Coffee");
    assertEquals(todo.finished, 1);

    const head = await A.first<Commit>(`SELECT * FROM "crr_commits" WHERE id = (SELECT head FROM "crr_clients" WHERE is_me = 1)`, []);
    assertExists(head);
    assertEquals(head.id, latestCommit.id);
})

Deno.test("push changes (A is up-to-date)", async () => {
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

    const Apush = await A.preparePush();
    const response = await S.receivePush(Apush);
    assertEquals(response.status, "ok");
    await receivePushResponse(A, S, response);

    
    const AchangeCount = (await A.first<{ count: number }>(`SELECT COUNT(*) as count FROM "crr_changes"`, [])).count;
    const SchangeCount = (await S.first<{ count: number }>(`SELECT COUNT(*) as count FROM "crr_changes"`, [])).count;
    
    assertEquals(AchangeCount, SchangeCount);
});

Deno.test("push and pull changes simple", async () => {
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
    const Apush = await A.preparePush();
    
    let response = await S.receivePush(Apush);
    await receivePushResponse(A, S, response);

    const Bpush = await B.preparePush();
    response = await S.receivePush(Bpush);
    await receivePushResponse(B, S, response);

    const Apull = await A.preparePull();
    const pull = await S.receivePull(Apull);
    await applyPull(A, pull);
}

const delay = async (milliseconds: number) => {
    await new Promise((resolve, reject) => {
        setTimeout(() => {
            resolve(true);
        }, milliseconds);
    });
}

