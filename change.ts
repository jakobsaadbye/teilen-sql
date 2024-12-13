import { SqliteDB } from "./sqlitedb.ts"

// NOTE: Move away from timestamps and instead use something like Hybrid Logical Clocks instead. https://jaredforsyth.com/posts/hybrid-logical-clocks/

export type Client = {
    site_id: string
    last_pulled_at: string
    last_pushed_at: string
};

export type CrrFracIndex = {
    tbl_name: string
    pk: string
    parent_col_id: string
    parent_id: string
    after_id: string
};

export type CrrColumn = {
    tbl_name: string
    col_id: string
    type: 'lww' | 'fractional_index'
    parent_col_id: string // set if fractional_index otherwise its null
};

export type OpType = 'insert' | 'update' | 'delete'
export type Change = {
    id: string
    type: OpType
    tbl_name: string
    col_id: string | null, // null when type is delete
    pk: string,
    value: any | null // null when type is delete
    site_id: string
    created_at: number
    applied_at: number
    seq: number
};

export const applyChanges = async (db: SqliteDB, changes: Change[]) => {
    const currentChanges = await db.select<Change[]>(`SELECT * FROM "crr_changes"`, []);
    let currentUpdates = currentChanges.filter(c => c.type === 'update');

    const stmts = [];

    const inserts = groupById(changes.filter(c => c.type === 'insert'));
    for (const insert of inserts) {
        const tblName = insert[0].tbl_name;
        const cols = insert.map(i => i.col_id);

        const touchesFracIdx = db.crrColumns[tblName].findIndex(col => col.type === 'fractional_index' && cols.includes(col.col_id)) !== -1
        if (touchesFracIdx) {
            // 1. Get all other rows that are in group with the now inserted row

            console.log("Insert contains fractional index column");
        }

        const values = insert.map(i => i.value);
        const stmt = `
            INSERT INTO "${tblName}" (${cols.join(",")})
            VALUES (${values.map(v => `'${v}'`).join(",")})
        `;
        stmts.push(stmt);
    }

    const updates = groupById(changes.filter(c => c.type === 'update'));
    currentUpdates = currentUpdates.sort((a, b) => b.created_at - a.created_at);
    for (const update of updates) {
        const changesToApply: Change[] = [];
        const tblName = update[0].tbl_name;
        const pk = update[0].pk;

        // Check if we should resurect a deleted row
        const priorDelete = await db.first<Change>(`SELECT * FROM "crr_changes" WHERE type = 'delete' AND tbl_name = ? AND pk = ?`, [tblName, pk]);
        if (priorDelete !== undefined) {
            console.log("Resurrecting row!");
            await resurrectRow(db, tblName, pk);
        }

        for (const change of update) {
            const colType = db.crrColumns[change.tbl_name].find(col => col.col_id === change.col_id)?.type ?? null;
            assert(colType !== null);

            switch (colType) {
                case 'fractional_index': {
                    console.log("Change detected to a fractional index column!");
                    break;
                }
                case 'lww': {
                    const thisChangeWon = lwwWins(change, currentUpdates);
                    if (thisChangeWon) {
                        changesToApply.push(change)
                    }
                }
            }
        }
        if (changesToApply.length === 0) continue;

        const stmt = `
            UPDATE "${tblName}"
            SET ${changesToApply.map(c => `${c.col_id} = '${c.value}'`)}
            WHERE ${decodePkToWhereClause(db, tblName, pk)}
        `
        stmts.push(stmt);
    }

    // NOTE: Deletions to a row is based on the Add-Wins semantics, meaning
    //       any updates to a row results in the row being kept alive.
    //       Deletes only happen if no changes was made to the row since the last time
    //       the two peers synced state.
    const deletes = changes.filter(c => c.type === 'delete');
    const rowsToDelete: { [tblName: string]: string[] } = {};
    const peers = await lastSyncWithPeers(db, deletes.map(c => c.site_id));
    for (const del of deletes) {
        const lastSync = peers[del.site_id] !== undefined ? peers[del.site_id] : 0
        const changesSinceDelete = await db.select<Change[]>(`SELECT * FROM "crr_changes" WHERE tbl_name = ? AND pk = ? AND applied_at > ?`, [del.tbl_name, del.pk, lastSync]);

        if (changesSinceDelete.length === 0) {
            if (rowsToDelete[del.tbl_name] === undefined) {
                rowsToDelete[del.tbl_name] = [del.pk]
            } else {
                rowsToDelete[del.tbl_name].push(del.pk);
            }
            continue;
        }

        // NOTE: If we both have deleted the row, do we take the delete from the peer or ourselves?
        const iDeletedTheRow = changesSinceDelete.findIndex(c => c.type === 'delete');
        if (iDeletedTheRow) {
            // We both deleted the row, so we don't care about it. Maybe???
            continue;
        } else {
            // Changes were made to the row, so no delete
        }
    }
    for (const [tblName, pks] of Object.entries(rowsToDelete)) {
        const condition = pks.map(pk => decodePkToWhereClause(db, tblName, pk)).join(' OR ');
        const stmt = `
            DELETE FROM "${tblName}"
            WHERE ${condition}
        `
        stmts.push(stmt);
    }

    for (const stmt of stmts) {
        const err = await db.exec(stmt, []);
        if (err !== undefined) {
            console.error(`Error running ${stmt}`, err);
        }
    }

    return await saveChanges(db, changes);
}

export const saveFractionalIndexRows = async (db: SqliteDB, changes: Change[]): Promise<Change[]> => {
    if (changes.length === 0) return [];

    const inserts = groupById(changes.filter(c => c.type === 'insert'));
    if (inserts.length === 0) return [];

    const values = [];
    for (const insert of inserts) {
        const tblName = insert[0].tbl_name;
        const pk = insert[0].pk;
        const fiCol = db.crrColumns[tblName].find(col => col.tbl_name === tblName && col.type === 'fractional_index');
        if (fiCol === undefined) continue;

        const parentColId = fiCol.parent_col_id;
        const parentId = insert.find(c => c.col_id === fiCol.parent_col_id)?.value ?? null
        const afterId = insert.find(c => c.col_id === fiCol.col_id)?.value ?? null
        assert(parentColId && parentId && afterId !== null);

        // Patch up the positions
        {
            let assignedAfterId = "";
            const items = await db.select<CrrFracIndex[]>(`SELECT * FROM "crr_frac_index" WHERE tbl_name = ? AND parent_id = ? ORDER BY position ASC`, [tblName, parentId]);
            if (items.length === 0) {
                assignedAfterId = "0"
            } else {
                if (afterId === "-1") { // Prepend
                    // Update the head to be after this id
                } else if (afterId === "1") { // Append
                    // Update the tail to 
                } else {

                }
            }
        }

        values.push(`('${tblName}', '${pk}', '${parentColId}', '${parentId}', '${afterId}', '0')`);
    }
    if (values.length === 0) return [];

    const err = await db.exec(`
        INSERT INTO "crr_frac_index" (tbl_name, pk, parent_col_id, parent_id, after_id, position)
        VALUES ${values.join(',\n')}
    `, []);
    if (err) {
        console.error(err);
        return [];
    }

    return []; // @Incomplete
}

const resurrectRow = async (db: SqliteDB, tblName: string, pk: string) => {
    const changes = await db.select<Change[]>(`SELECT * FROM "crr_changes" WHERE tbl_name = ? AND pk = ?`, [tblName, pk]);
    assert(changes.length > 0);

    const inserts = changes.filter(c => c.type === 'insert');
    assert(inserts.length > 0);
    const insertedCols = inserts.map(i => i.col_id);
    const insertedValues = inserts.map(i => i.value);
    let err = await db.exec(`
        INSERT INTO "${tblName}" (${insertedCols.join(',')})
        VALUES ${sqlArray(insertedValues)}
    `, []);
    if (err) console.error(err);

    // NOTE: This assumes LWW on the column meaning we just care about the latest value
    //       and NOT all previous values.
    const updates = changes.filter(c => c.type === 'update');
    if (updates.length === 0) return;
    const latestUpdated: { [colId: string]: any } = {};
    const updatePerColumn = Object.groupBy(updates, ({ col_id }) => col_id as string);
    for (const [colId, updts] of Object.entries(updatePerColumn)) {
        const latestValue = updts!.sort((a, b) => b.created_at - a.created_at)[0].value;
        latestUpdated[colId] = latestValue;
    }
    err = await db.exec(`
        UPDATE "${tblName}"
        SET ${Object.entries(latestUpdated).map(([colId, value]) => `${colId}='${value}'`).join(',')}
        WHERE ${decodePkToWhereClause(db, tblName, pk)}
    `, []);
    if (err) console.error(err);
}

const lastSyncWithPeers = async (db: SqliteDB, siteIds: string[]) => {
    if (siteIds.length === 0) return {};
    const peers = await db.select<{ site_id: string, max_applied_at: number }[]>(`SELECT site_id, MAX(applied_at) as max_applied_at FROM "crr_changes" WHERE site_id IN ${sqlArray(siteIds)} GROUP BY site_id`, []);
    let group: { [site_id: string]: number } = {};
    for (const p of peers) {
        group[p.site_id] = p.max_applied_at;
    }
    return group;
}

export const saveChanges = async (db: SqliteDB, changes: Change[]) => {
    if (changes.length === 0) return;
    const applied_at = new Date().getTime();

    const values = changes.map(c => `('${c.id}', '${c.type}', '${c.tbl_name}', '${c.col_id}', '${c.pk}', '${c.value}', '${c.site_id}', '${c.created_at}', '${applied_at}', '${c.seq}')\n`);
    const sql = `
        INSERT INTO "crr_changes" (id, type, tbl_name, col_id, pk, value, site_id, created_at, applied_at, seq)
        VALUES ${values};
    `;

    return await db.exec(sql, []);
}

export const compactChanges = async (db: SqliteDB) => {
    const client = await db.first<Client>(`SELECT * FROM "crr_client" WHERE site_id = ?`, [db.siteId]);
    if (!client) return;

    const nonPushedChanges = await db.select<Change[]>(`SELECT * FROM "crr_changes" WHERE site_id = ? AND created_at > ?`, [db.siteId, client.last_pushed_at]);

    const groupByRow = (changes: Change[]) => {
        const groups: { tblName: string, colId: string, pk: string, changes: Change[] }[] = [];
        for (const c of changes) {
            const groupIdx = groups.findIndex(g => g.tblName === c.tbl_name && g.colId === c.col_id && g.pk === c.pk);
            if (groupIdx === -1) {
                groups.push({ tblName: c.tbl_name, colId: c.col_id, pk: c.pk, changes: [c] });
                continue;
            }
            groups[groupIdx].changes.push(c);
        }
        return groups;
    }

    const toDelete = new Set<string>();

    // Updates:
    //  - The most recent update to a column in a row is preserved, all others are to be removed
    const updates = nonPushedChanges.filter(c => c.type === 'update');
    const updatesGrouped = groupByRow(updates);
    for (const ug of updatesGrouped) {
        if (ug.changes.length <= 1) continue
        const outdated = ug.changes.sort((a, b) => b.created_at - a.created_at).splice(1);
        for (const c of outdated) {
            toDelete.add(c.id);
        }
    }

    // Inserts / Deletes:
    //  - A delete to a row r removes all the non-pushed changes to r
    const deletes = nonPushedChanges.filter(c => c.type === 'delete');
    for (const del of deletes) {
        let affected = nonPushedChanges.filter(c => c.tbl_name === del.tbl_name && c.pk === del.pk);
        const keepDelete = affected.findIndex(c => c.type === 'insert') === -1;
        if (keepDelete) {
            affected = affected.filter(c => c.type !== 'delete');
        }
        for (const a of affected) {
            toDelete.add(a.id);
        }
    }

    await db.exec(`DELETE FROM "crr_changes" WHERE id IN (${[...toDelete].map(id => `'${id}'`).join(',')})`, []);
}

const lwwWins = (update: Change, currentUpdates: Change[]): boolean => {
    if (currentUpdates.length === 0) return true;
    const latestCurrentUpdate = currentUpdates.find(u => {
        if (u.tbl_name === update.tbl_name && u.col_id === update.col_id && u.pk === update.pk) {
            return u;
        }
    });
    if (latestCurrentUpdate === undefined) return true;

    const timeCurrentLatestChange = latestCurrentUpdate.created_at;
    const timeThisChange = update.created_at;
    if (timeCurrentLatestChange > timeThisChange) {
        return false
    } else if (timeCurrentLatestChange === timeThisChange) {
        if (update.value > latestCurrentUpdate.value) {
            return true;
        } else {
            return false;
        }
    } else {
        return true;
    }
}

const groupById = (changes: Change[]): Change[][] => {
    const unquieIds = new Set(changes.map(i => i.id));
    const groups = [];
    for (const id of unquieIds) {
        const group = changes.filter(c => c.id === id).sort((a, b) => a.seq - b.seq);
        groups.push(group);
    }
    return groups;
}

export const diff = (db: SqliteDB, before: any[], after: any[], tblName: string, opType: OpType): Change[] => {
    switch (opType) {
        case "insert": {
            assert(before.length <= after.length);

            // TODO: This is probably THEE dumbeest way to check for changes in an insert. Only @Temporary @Cleanup
            const changes: Change[] = [];
            const keys = after.length > 0 ? Object.keys(after[0]) : [];
            const created_at = (new Date()).getTime();

            if (after.length > before.length) {
                for (let i = before.length; i < after.length; i++) { // Only take the ones that got added in the reverse
                    const pk = pkValueOfRow(db, tblName, after[i]);
                    let seq = 0;
                    const id = crypto.randomUUID();
                    for (const key of keys) {
                        const valAfter = after[i][key];
                        changes.push({ id, type: opType, tbl_name: tblName, col_id: key, pk, value: valAfter, site_id: db.siteId, created_at, applied_at: 0, seq });
                        seq += 1;
                    }
                }
            } else if (after.length === before.length) { // Must have been because of an ON CONFLICT UPDATE
                return diffUpdate(db, before, after, tblName, 'update');
            }

            return changes;
        }
        case "delete": {
            if (before.length === after.length) {
                return [];
            }
            assert(before.length > after.length);

            const changes: Change[] = [];
            const created_at = (new Date()).getTime();
            for (let i = before.length - 1; i >= after.length; i--) {
                const pk = pkValueOfRow(db, tblName, before[i]);
                let seq = 0;
                const id = crypto.randomUUID();
                changes.push({ id, type: opType, tbl_name: tblName, col_id: null, pk, value: null, site_id: db.siteId, created_at, applied_at: 0, seq });
                seq += 1;
            }

            return changes;
        }
        case "update": {
            return diffUpdate(db, before, after, tblName, opType);
        }
    }
}

const diffUpdate = (db: SqliteDB, before: any[], after: any[], tblName: string, opType: OpType): Change[] => {
    const changes: Change[] = [];
    const keys = before.length > 0 ? Object.keys(before[0]) : [];
    const created_at = (new Date()).getTime();
    for (let i = 0; i < before.length; i++) {
        let seq = 0;
        const id = crypto.randomUUID();
        for (const key of keys) {
            const valBefore = before[i][key];
            const valAfter = after[i][key];
            if (valBefore !== valAfter) {
                const pk = pkValueOfRow(db, tblName, before[i]);
                changes.push({ id, type: opType, tbl_name: tblName, col_id: key, pk, value: valAfter, site_id: db.siteId, created_at, applied_at: 0, seq });
                seq += 1;
            }
        }
    }

    return changes;
}

const decodePkToWhereClause = (db: SqliteDB, tblName: string, pk: string) => {
    return "(" + decodePk(db, tblName, pk).map(([colId, value]) => `${colId} = '${value}'`).join(' AND ') + ")";
}

const decodePk = (db: SqliteDB, tblName: string, pk: string): [colId: string, value: any][] => {
    const pkCols = db.pks[tblName];
    assert(pkCols.length > 0);
    const values = pk.split('|');
    assert(pkCols.length === values.length);
    return pkCols.map((colId, i) => [colId, values[i]]);
}

const pkValueOfRow = (db: SqliteDB, tblName: string, row: any) => {
    const pkCols = db.pks[tblName];
    assert(pkCols.length > 0);
    return Object.entries(row).filter(([colId, _]) => pkCols.includes(colId)).map(([_, value]) => value).join('|');
}

export const convertToSelectStmt = (sql: string) => {
    const s = sql.trim().split(' ');
    switch (s[0].toLowerCase()) {
        case "insert": {
            s.shift();
            s.shift();
            const tableName = s[0];
            return [`SELECT * FROM ${tableName}`, null, tableName.replaceAll(`"`, ''), 'insert'];
        }
        case "update": {
            s.shift();
            const tableName = s[0];
            const whereIndex = s.findIndex(tok => tok.toLowerCase() === "where");
            if (whereIndex === -1) {
                return [null, "Missign WHERE clause in UPDATE statement"];
            }
            const condition = s.slice(whereIndex + 1).join(' ');
            return [`SELECT * FROM ${tableName} WHERE ${condition}`, null, tableName.replaceAll(`"`, ''), 'update']
        }
        case "delete": {
            s.shift();
            s.shift();
            const tableName = s[0];
            s.shift();
            s.shift();
            const condition = s.join(' ');
            return [`SELECT * FROM ${tableName} WHERE ${condition}`, null, tableName.replaceAll(`"`, ''), 'delete'];
        }
        default:
            return [null, `Unknown start of sql statement in convertToSelectStmt(). Starts with ${s[0]}`];
    }
}

const sqlArray = (a: any[]) => {
    return `(${a.map(v => `'${v}'`).join(',')})`;
}

const assert = (b: boolean) => {
    if (!b) {
        throw new Error("Assertion failed!");
    }
}

const panic = () => {
    throw new Error("Code path got reached which shouldn't have gotten reached!");
}