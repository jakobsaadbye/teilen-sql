import { Database } from "jsr:@db/sqlite@0.12";
import { SqliteDB, SqliteDBWrapper, applyChanges, insertCrrTablesStmt, createServerDb, CrrColumn, assert, PushResponse, applyPull } from "../index.ts";
import { assignSiteId } from "../src/sqlitedb.ts";
import { attachChangeGenerationTriggers } from "../src/change.ts";

//
// Common tables + types
//
export type Todo = {
    id: string
    name: string
    finished: boolean
}

export const todoTable = `
    CREATE TABLE IF NOT EXISTS "todos" (
        id text primary key,
        name text,
        finished boolean
    );
`;

//
// Common setup stuff
//
export const setupTwoDatabases = async (tables: string) => {
    const _A = new Database("./tests/db/A.db", { int64: true });
    const _B = new Database("./tests/db/B.db", { int64: true });

    const A = new SqliteDBWrapper(_A) as SqliteDB;
    const B = new SqliteDBWrapper(_B) as SqliteDB;

    A.name = "A";
    B.name = "B";

    await dropAllTables(A);
    await dropAllTables(B);

    await A.exec(insertCrrTablesStmt, []);
    await B.exec(insertCrrTablesStmt, []);
    await assignSiteId(A);
    await assignSiteId(B);

    await A.exec(tables, []);
    await B.exec(tables, []);
    
    await A.upgradeAllTablesToCrr();
    await B.upgradeAllTablesToCrr();
    await A.finalize();
    await B.finalize();
    await attachChangeGenerationTriggers(A);
    await attachChangeGenerationTriggers(B);

    return [A, B];
}

export const setupThreeDatabases = async (tables: string) => {
    const _A = new Database("./tests/db/A.db", { int64: true });
    const _B = new Database("./tests/db/B.db", { int64: true });
    const _S = new Database("./tests/db/S.db", { int64: true });

    const A = new SqliteDBWrapper(_A) as SqliteDB;
    const B = new SqliteDBWrapper(_B) as SqliteDB;
    const S = new SqliteDBWrapper(_S) as SqliteDB;

    A.name = "A";
    B.name = "B";
    S.name = "S";
    
    await dropAllTables(A);
    await dropAllTables(B);
    await dropAllTables(S);

    await A.exec(insertCrrTablesStmt, []);
    await B.exec(insertCrrTablesStmt, []);
    await S.exec(insertCrrTablesStmt, []);
    await assignSiteId(A);
    await assignSiteId(B);
    await assignSiteId(S);

    await A.exec(tables, []);
    await B.exec(tables, []);
    await S.exec(tables, []);
    
    await A.upgradeAllTablesToCrr();
    await B.upgradeAllTablesToCrr();
    await S.upgradeAllTablesToCrr();
    await A.finalize();
    await B.finalize();
    await S.finalize();
    await attachChangeGenerationTriggers(A);
    await attachChangeGenerationTriggers(B);
    await attachChangeGenerationTriggers(S);

    return [A, B, S];
}

export const setupThreeNonFinalizedDatabases = async (tables: string) => {
    const _A = new Database("./tests/db/A.db", { int64: true });
    const _B = new Database("./tests/db/B.db", { int64: true });
    const _S = new Database("./tests/db/S.db", { int64: true });

    const A = new SqliteDBWrapper(_A) as SqliteDB;
    const B = new SqliteDBWrapper(_B) as SqliteDB;
    const S = new SqliteDBWrapper(_S) as SqliteDB;

    A.name = "A";
    B.name = "B";
    S.name = "S";

    await dropAllTables(A);
    await dropAllTables(B);
    await dropAllTables(S);

    await A.exec(insertCrrTablesStmt, []);
    await B.exec(insertCrrTablesStmt, []);
    await S.exec(insertCrrTablesStmt, []);
    await assignSiteId(A);
    await assignSiteId(B);
    await assignSiteId(S);

    await A.exec(tables, []);
    await B.exec(tables, []);
    await S.exec(tables, []);

    return [A, B, S];
}

export const finalizeDatabases = async (...dbs: SqliteDB[]) => {
    for (const db of dbs) {
        await db.finalize();
        await attachChangeGenerationTriggers(db);
    }
}

const dropAllTables = async (db: SqliteDB) => {
    const tables = await db.select<{ name: string }[]>(`SELECT name FROM sqlite_master WHERE type='table' ORDER BY name`, []);
    for (const table of tables) {
        await db.exec(`DROP TABLE IF EXISTS "${table.name}"`, []);
    }
}

//
// Common utility functions
//
export const delay = async (milliseconds: number) => {
    await new Promise((resolve, reject) => {
        setTimeout(() => {
            resolve(true);
        }, milliseconds);
    });
}

export const countCommits = async (db: SqliteDB) => {
    const row = await db.first<{ count: number }>(`SELECT COUNT(*) as count FROM "crr_commits"`, []);
    return row?.count ?? 0;
}

export const countChanges = async (db: SqliteDB) => {
    const row = await db.first<{ count: number }>(`SELECT COUNT(*) as count FROM "crr_changes"`, []);
    return row?.count ?? 0;
}

export const todosMatch = async (A: SqliteDB, B: SqliteDB) => {
    return (await todosAsStr(A)) === (await todosAsStr(B));
}

export const todosAsStr = async (db: SqliteDB) => {
    const todosA = await db.select<Todo[]>(`SELECT * FROM "todos" ORDER BY id`, []);
    return JSON.stringify(todosA);
}

export const pushCommits = async (db: SqliteDB, remote: SqliteDB) => {
    const push = await db.preparePushCommits();
    const pushResponse = await remote.receivePushCommits(push);
    await maybeAutoPull(db, remote, pushResponse);
}

export const pullCommits = async (db: SqliteDB, remote: SqliteDB) => {
    const pull = await db.preparePullCommits();
    const pullResponse = await remote.receivePullCommits(pull);
    const results = await applyPull(db, pullResponse);
    return results;
}

export const maybeAutoPull = async (db: SqliteDB, remote: SqliteDB, response: PushResponse) => {
    switch (response.status) {
        case "ok": {
            assert(response.appliedAt);
            await db.exec(`
                UPDATE "crr_documents" SET 
                    last_pulled_at = ?,
                    last_pushed_commit = head, 
                    last_pulled_commit = head 
                WHERE id = ?
            `, [response.appliedAt, response.documentId]);
            return;
        }
        case "needs-pull": {
            // Here we just auto pull down any changes
            await pullCommits(db, remote);

            const secondPush = await db.preparePushCommits();

            const secondResponse = await remote.receivePushCommits(secondPush);
            if (secondResponse.status !== "ok" && secondResponse.status !== "request-contained-no-commits") {
                console.error(`Remote is still ahead after a pull and push`, secondResponse);
                assert(false);
            }
            break;
        }
        case "request-contained-no-commits": {
            console.log(`Nothing got pushed`)
            break;
        }
        case "request-malformed": {
            console.error(`Push was malformed`);
            break;
        }
    }
}

// Random data

export const randomTodoStrings = [
    "Pet the cat like it owes me money",
    "Google “how to be productive” again",
    "Rearrange fridge magnets for feng shui",
    "Stare into the void (5 mins max)",
    "Water the plants—apologize for the drought",
    "Pretend to be a burrito for self-care",
    "Practice evil laugh (volume: 80%)",
    "Check fridge for the 4th time",
    "Write list of things I’ll never do",
    "Blink dramatically at nothing",
    "Organize socks by emotional damage",
    "Talk to the spider in the corner",
    "Create elaborate escape plan (from what?)",
    "Watch one video—fall into rabbit hole",
]