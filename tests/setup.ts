import { Database } from "jsr:@db/sqlite@0.12";
import { SqliteDB, SqliteDBWrapper, applyChanges, insertCrrTablesStmt, createServerDb, CrrColumn, assert } from "../index.ts";
import { assignSiteId } from "../src/sqlitedb.ts";
import { attachChangeGenerationTriggers } from "../src/change.ts";

export const setupTwoDatabases = async (tables: string) => {
    const _A = new Database("./tests/db_A.db", { int64: true });
    const _B = new Database("./tests/db_B.db", { int64: true });

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
    const _A = new Database("./tests/db_A.db", { int64: true });
    const _B = new Database("./tests/db_B.db", { int64: true });
    const _S = new Database("./tests/db_S.db", { int64: true });

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

export const setupTwoNonFinalizedDatabases = async (tables: string) => {
    const _A = new Database("./tests/db_A.db", { int64: true });
    const _B = new Database("./tests/db_B.db", { int64: true });

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

    return [A, B];
}

const dropAllTables = async (db: SqliteDB) => {
    const tables = await db.select<{ name: string }[]>(`SELECT name FROM sqlite_master WHERE type='table' ORDER BY name`, []);
    for (const table of tables) {
        await db.exec(`DROP TABLE IF EXISTS "${table.name}"`, []);
    }
}