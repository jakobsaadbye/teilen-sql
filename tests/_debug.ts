import { assertExists } from "jsr:@std/assert@0.221/assert-exists";
import { RowConflict } from "../index.ts";
import { finalizeDatabases, pullCommits, pushCommits, randomTodoStrings, setupTwoDatabases, Todo, todoTable } from "./_common.ts";
import { setupThreeDatabases, delay } from "./_common.ts";
import { assertEquals } from "jsr:@std/assert@0.221/assert-equals";
import { assert } from "jsr:@std/assert@0.217/assert";

//
//  Code in this file can be debugged in vscodes' debugger as the task "Debug a test"
////////

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

assert(firstCommit);
assert(latestCommit);

await A.checkout(firstCommit.id);

// let todo = await A.first<Todo>(`SELECT * FROM "todos" WHERE id = '1'`, []);
// assertExists(todo);
// assertEquals(todo.name, "Buy milk");
// assertEquals(todo.finished, 0);

// await A.checkout(latestCommit.id);

// todo = await A.first(`SELECT * FROM "todos" WHERE id = '1'`, []);
// assertExists(todo);
// assertEquals(todo.name, "Buy Coffee");
// assertEquals(todo.finished, 1);

// const head = await A.first<Commit>(`SELECT * FROM "crr_commits" WHERE id = (SELECT head FROM "crr_documents" WHERE id = ?)`, [latestCommit?.document]);
// assertExists(head);
// assertEquals(head.id, latestCommit.id);