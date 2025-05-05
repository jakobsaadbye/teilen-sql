import { assertExists } from "jsr:@std/assert@0.221/assert-exists";
import { Change, RowConflict } from "../index.ts";
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