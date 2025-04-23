import { assertExists } from "jsr:@std/assert@0.221/assert-exists";
import { getCommitGraph, printCommitGraph } from "@/index.ts";
import { pullCommits, pushCommits, randomTodoStrings, setupTwoDatabases, todoTable } from "./_common.ts";
import { setupThreeDatabases, delay } from "./_common.ts";

//
//  Code in this file can be debugged in vscodes' debugger as the task "Debug a test"
////////

const [A, B, S] = await setupThreeDatabases(todoTable);

const ACommitMsgs = ["A", "B", "C", "D", "E", "F"];
const BCommitMsgs = ["X", "Y", "Z", "S", "T", "U"];

await A.execTrackChanges(`INSERT INTO "todos" VALUES ('1', 'X', 0)`, []);
await A.commit("A");

await pushCommits(A, S);
await pullCommits(B, S);

await A.execTrackChanges(`INSERT INTO "todos" VALUES ('2', 'X', 0)`, []);
await A.commit("B");
await B.execTrackChanges(`INSERT INTO "todos" VALUES ('3', 'X', 0)`, []);
await B.commit("X");
await A.execTrackChanges(`INSERT INTO "todos" VALUES ('4', 'X', 0)`, []);
await A.commit("C");

await pushCommits(A, S);

await B.execTrackChanges(`INSERT INTO "todos" VALUES ('5', 'X', 0)`, []);
await B.commit("Y");

await pullCommits(B, S);

const G = await getCommitGraph(B);
assertExists(G);
printCommitGraph(G);