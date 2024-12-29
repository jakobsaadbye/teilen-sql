// Copyright 2024 Roy T. Hashimoto. All Rights Reserved.

import SQLiteESMFactory from './vendor/wa-sqlite/dist/wa-sqlite.mjs';
import { OPFSCoopSyncVFS as MyVFS } from './vendor/wa-sqlite/src/examples/OPFSCoopSyncVFS.js';

import * as SQLite from './vendor/wa-sqlite/src/sqlite-api.js';

const searchParams = new URLSearchParams(location.search);

Promise.resolve().then(async () => {
    const dbName = searchParams.get('dbName') ?? 'main';

    // Set up communications with the main thread.
    const messagePort = await new Promise(resolve => {
        addEventListener('message', function handler(event) {
            if (event.data === 'messagePort') {
                resolve(event.ports[0]);
                removeEventListener('message', handler);
            }
        });
    });

    // Initialize SQLite.
    const module = await SQLiteESMFactory();
    const sqlite3 = SQLite.Factory(module);

    // Register a custom file system.
    const vfs = await MyVFS.create('vfs', module);
    sqlite3.vfs_register(vfs, true);

    // Open the database.
    const db = await sqlite3.open_v2(dbName);

    // Signal that database is ready
    messagePort.postMessage('dbReady');

    const ctx = {
        sqlite3,
        db,
        messagePort
    };

    // Handle simple database changes
    const handleChange = (updateType, dbName, tblName, rowid) => {
        let type;
        switch (updateType) {
            case 9: type = 'delete'; break;
            case 18: type = 'insert'; break;
            case 23: type = 'update'; break;
        }
        const change = { updateType: type, dbName, tableName: tblName, rowid };
        messagePort.postMessage({ type: 'change', change });
    }
    sqlite3.update_hook(db, handleChange);

    // Handle messages
    messagePort.addEventListener('message', async event => {
        const { type } = event.data;
        switch (type) {
            case 'dbClose': return await handleDbClose(ctx, event);
            case 'exec': return await handleExec(ctx, event);
            case 'select': return await handleSelect(ctx, event);
            default:
                console.error(`Received unknown message with type '${type}'`, event);
        }
    });

    // Signal that we're ready.
    messagePort.start();
});

const handleDbClose = async (ctx, event) => {
    try {
        ctx.sqlite3.close(ctx.db);
        ctx.messagePort.postMessage({ id: event.data.id });
    } catch (error) {
        ctx.messagePort.postMessage({ id: event.data.id, error });
    }
}

const handleSelect = async (ctx, event) => {
    const { sqlite3, db } = ctx;
    const { id, sql, params } = event.data;

    try {
        const results = [];
        for await (const stmt of sqlite3.statements(db, sql)) {
            bind_parameters(sqlite3, stmt, params);
            const columns = sqlite3.column_names(stmt);
            while (await sqlite3.step(stmt) === SQLite.SQLITE_ROW) {
                const row = sqlite3.row(stmt);
                let result = {};
                for (let i = 0; i < columns.length; i++) {
                    result[columns[i]] = row[i];
                }
                results.push(result);
            }
        }

        ctx.messagePort.postMessage({ id, results });
    } catch (error) {
        ctx.messagePort.postMessage({ id, error });
    }
}

const handleExec = async (ctx, event) => {
    const { sqlite3, db } = ctx;
    const { id, sql, params } = event.data;

    try {
        for await (const stmt of sqlite3.statements(db, sql)) {
            bind_parameters(sqlite3, stmt, params);
            while (await sqlite3.step(stmt) === SQLite.SQLITE_ROW) {
                continue
            }
        }

        ctx.messagePort.postMessage({ id });
    } catch (error) {
        ctx.messagePort.postMessage({ id, error });
    }
}

const bind_parameters = (sqlite3, stmt, params) => {
    const expectedParamCount = sqlite3.bind_parameter_count(stmt);
    if (expectedParamCount != params.length) {
        throw new Error(`Mismatch in number of parameters. Got ${params.length}, expected ${expectedParamCount}`)
    }
    for (let i = 0; i < params.length; i++) {
        sqlite3.bind(stmt, i + 1, params[i]);
    }
}