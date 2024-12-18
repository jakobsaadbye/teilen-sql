import { assert } from "jsr:@std/assert@0.217/assert";
import { fracMid } from "./frac.ts";
import { SqliteDB } from "./sqlitedb.ts"

// TODO: Move away from timestamps and instead use something like Hybrid Logical Clocks instead. https://jaredforsyth.com/posts/hybrid-logical-clocks/

export type Client = {
    site_id: string
    last_pulled_at: string
    last_pushed_at: string
};

export type CrrColumn = {
    tbl_name: string
    col_id: string
    type: 'lww' | 'fractional_index'
    parent_col_id: string // set if fractional_index otherwise empty string
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
    const stmts = [];

    const inserts = groupById(changes.filter(c => c.type === 'insert'));
    for (const insert of inserts) {
        const tblName = insert[0].tbl_name;
        const cols = insert.map(i => i.col_id);
        const values = insert.map(i => i.value);
        const stmt = `
            INSERT INTO "${tblName}" (${cols.join(",")})
            VALUES (${values.map(v => `'${v}'`).join(",")})
        `;
        stmts.push(stmt);
    }

    // @Speed - Is it really nesasary to select all changes here? I think
    //          we can just select from the min. timestamp of the incomming changes?
    const currentUpdates = await db.select<Change[]>(`SELECT * FROM "crr_changes" WHERE type = 'update' ORDER BY created_at DESC`, []);
    const updates = groupById(changes.filter(c => c.type === 'update'));
    for (const update of updates) {
        const changesToApply: Change[] = [];
        const tblName = update[0].tbl_name;
        const id = update[0].id;
        const pk = update[0].pk;

        // Check if we should resurect a deleted row
        const priorDelete = await db.first<Change>(`SELECT * FROM "crr_changes" WHERE type = 'delete' AND tbl_name = ? AND pk = ?`, [tblName, pk]);
        if (priorDelete !== undefined) {
            console.log("Resurrecting row!");
            await resurrectRow(db, tblName, pk);
        }

        const findPreviousChangeToSameColumn = (colId: string) => {
            return currentUpdates.find(change => change.id === id && change.col_id === colId);
        }

        for (const change of update) {
            const colType = db.crrColumns[change.tbl_name].find(col => col.col_id === change.col_id)?.type ?? 'lww';
            switch (colType) {
                case 'fractional_index': {
                    const prevChange = findPreviousChangeToSameColumn(change.col_id as string);
                    if (isLastWriter(change, prevChange)) {
                        changesToApply.push(change);
                    }
                    break;
                }
                case 'lww': {
                    const prevChange = findPreviousChangeToSameColumn(change.col_id as string);
                    if (isLastWriter(change, prevChange)) {
                        changesToApply.push(change);
                    }
                    break;
                }
            }
        }
        if (changesToApply.length === 0) continue;

        const stmt = `
            UPDATE "${tblName}"
            SET ${changesToApply.map(c => `${c.col_id} = '${c.value}'`)}
            WHERE ${pkEqual(db, tblName, pk)}
        `
        stmts.push(stmt);
    }

    // NOTE: Deletions to a row is based on the Add-Wins semantics, meaning
    //       any updates to a row results in the row being kept alive.
    //       Deletes only happen if no changes was made to the row since the
    //       peers last synced changes
    const deletes = changes.filter(c => c.type === 'delete');
    const rowsToDelete: { [tblName: string]: string[] } = {};
    const peers = await lastSyncWithPeers(db, deletes.map(c => c.site_id));
    for (const del of deletes) {
        const lastSync = peers[del.site_id] !== undefined ? peers[del.site_id] : 0
        const changesSinceDelete = await db.select<Change[]>(`SELECT * FROM "crr_changes" WHERE tbl_name = ? AND pk = ? AND applied_at > ?`, [del.tbl_name, del.pk, lastSync]);
        if (changesSinceDelete.length === 0) {
            if (rowsToDelete[del.tbl_name] === undefined) rowsToDelete[del.tbl_name] = [del.pk]
            else rowsToDelete[del.tbl_name].push(del.pk);
        }
    }
    for (const [tblName, pks] of Object.entries(rowsToDelete)) {
        const condition = pks.map(pk => pkEqual(db, tblName, pk)).join(' OR ');
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

    const err = await saveChanges(db, changes);
    if (err) {
        console.error(err);
        return err;
    }

    // Patch any fractional index columns that might have collided. 
    // NOTE(**important**): Must run after all the changes have been applied 
    // so that all rows are known about
    await fixAnyFractionalIndexCollisions(db, groupById(changes));
}

const fixAnyFractionalIndexCollisions = async (db: SqliteDB, changes: Change[][]) => {
    const fiChanges = changes.filter(changeSet => {
        const tblName = changeSet[0].tbl_name;
        const fiCols = db.crrColumns[tblName].filter(col => col.type === 'fractional_index');
        if (fiCols.length === 0) return false;
        return changeSet.find(change => fiCols.find(col => col.col_id === change.col_id) !== undefined) !== undefined;
    });
    if (fiChanges.length === 0) return [];

    // Extract the lists that are affected in each table
    type List = {
        parentColId: string
        parentId: string
        posColId: string
    };
    const tables: { [tblName: string]: { [parentId: string]: List } } = {};
    for (const changeSet of fiChanges) {
        const pk = changeSet[0].pk;
        const tblName = changeSet[0].tbl_name;
        const fiCol = db.crrColumns[tblName].find(col => col.type === 'fractional_index'); // NOTE: This assumes only one fractional index column
        assert(fiCol !== undefined);

        const posColId = fiCol.col_id;
        const parentColId = fiCol.parent_col_id;
        let parentId = changeSet.find(change => change.col_id === parentColId)?.value ?? -1;
        if (parentId === -1) {
            // parentId is missing because only the position changed in the same list.
            // We need to find the parentId of the updated row.
            assert(changeSet[0].type === 'update');

            // See if we already have the parentId for this list
            if (tables[tblName] !== undefined && tables[tblName][parentId] !== undefined) {
                continue;
            } else {
                const row = await db.first<any>(`SELECT * FROM "${tblName}" WHERE ${pkEqual(db, tblName, pk)}`, []);
                if (row === undefined) {
                    console.log(`Row is missing with pk '${pk}'`);
                    continue;
                }
                parentId = row[parentColId];
            }
        }

        if (tables[tblName] !== undefined) tables[tblName][parentId] = { parentColId, parentId, posColId };
        else tables[tblName] = { [parentId]: { parentColId, parentId, posColId } };
    }

    const patchStmts = [];
    for (const [tblName, lists] of Object.entries(tables)) {
        for (const list of Object.values(lists)) {
            const parentColId = list.parentColId;
            const parentId = list.parentId;
            const posColId = list.posColId;

            // Group each item in a list as (item, lastChange)
            const items = await db.select<any[]>(`SELECT * FROM "${tblName}" WHERE ${parentColId} = '${parentId}' ORDER BY ${posColId} ASC`, []);
            if (items.length === 0 || items.length === 1) return;

            const itemPks = items.map(item => pkEncodingOfRow(db, tblName, item));
            const lastChanges = await db.select<Change[]>(`SELECT * FROM "crr_changes" WHERE pk IN ${sqlArray(itemPks)} AND col_id = '${posColId}' ORDER BY created_at DESC`, []);

            const comparePk = (item: any, pkB: string): boolean => {
                const pkA = pkEncodingOfRow(db, tblName, item);
                return pkA === pkB;
            }

            type Pair = [idx: number, item: any, change: Change]
            const pairs: Pair[] = [];
            for (let i = 0; i < items.length; i++) {
                const item = items[i];
                const posChange = lastChanges.find(change => comparePk(item, change.pk));
                if (posChange === undefined) {
                    console.error(`Failed to get last change of item"`, item);
                }
                pairs.push([i, item, posChange as Change]);
            }

            const positionGroups = Object.groupBy(pairs, (([, item, _]) => item[posColId]));
            for (const [pos, group] of Object.entries(positionGroups)) {
                if (group!.length > 1) {
                    console.log(`Detected collision in table '${tblName}', ${list.parentColId} '${list.parentId}' on position '${pos}'`);

                    // Collision on a position!
                    // Resolve by last-writer-wins. The last writer, gets to be below the other. 
                    // NOTE: Maybe it should be an option when upgrading to a fractional index, to choose weather the last writer gets below or above
                    const sorted = group!.toSorted(([, , changeA], [, , changeB]) => isLastWriter(changeA, changeB) ? +1 : -1);

                    // First item in the sorted collisions will just stay put as the anchor point for the rest of the collisions to go after.
                    const [, head] = sorted[0];
                    const nextIdx = 1 + Math.max(...sorted.map(([idx,]) => idx));

                    const anchorA = head[posColId] as string;
                    let anchorB = "";
                    if (nextIdx === items.length) {
                        // Last collided item is also the end of the list
                        anchorB = "]"
                    } else {
                        anchorB = items[nextIdx][posColId];
                    }

                    for (let j = sorted.length - 1; j > 0; j--) {
                        if (j === sorted.length - 1) {
                            let [, tail] = sorted[j];
                            const position = fracMid(anchorA, anchorB);
                            tail[posColId] = position;
                        } else {
                            let [, item] = sorted[j];
                            let [, prev] = sorted[j + 1];
                            const position = fracMid(anchorA, prev[posColId]);
                            item[posColId] = position
                        }
                    }

                    for (let i = 0; i < sorted.length; i++) {
                        const [idx, item] = sorted[i];
                        const stmt = `UPDATE "${tblName}" SET ${posColId} = '${item[posColId]}' WHERE ${pkEqual(db, tblName, itemPks[idx])}`;
                        patchStmts.push(stmt);
                    }
                }
            }
        }
    }

    for (const stmt of patchStmts) {
        const err = await db.exec(stmt, []);
        if (err !== undefined) {
            console.error(`Error running ${stmt}`, err);
        }
    }
}

/**
 * @returns True if a is the last writer
 */
const isLastWriter = (a: Change | undefined, b: Change | undefined) => {
    if (a === undefined) return false;
    if (b === undefined) return true;

    if (a.created_at > b.created_at) return true;
    if (a.created_at < b.created_at) return false;

    if (a.value > b.value) return true;
    if (a.value < b.value) return false;
    return true;
}

export const saveFractionalIndexCols = async (db: SqliteDB, changes: Change[]) => {
    if (changes.length === 0) return;

    const changeType = changes[0].type;
    if (changeType === 'delete') return;

    const tblName = changes[0].tbl_name;
    const pk = changes[0].pk;

    const changedCols = changes.map(c => c.col_id);
    const fiCols = db.crrColumns[tblName].filter(col => col.tbl_name === tblName && col.type === 'fractional_index');
    const fiCol = fiCols.find(col => changedCols.includes(col.col_id));
    if (fiCol === undefined) return; // Table doesn't have a fractional index column

    const fiChangeIdx = changes.findIndex(c => c.col_id === fiCol.col_id);
    if (fiChangeIdx === -1) return;

    const positionColName = fiCol.col_id;
    const parentColId = fiCol.parent_col_id;
    let afterId = changes.find(c => c.col_id === fiCol.col_id)?.value ?? null
    assert(afterId !== null);
    if (typeof (afterId) === 'number') afterId = afterId.toString();

    let parentChanged = false;
    let parentId = changes.find(c => c.col_id === fiCol.parent_col_id)?.value ?? null // null if only an update to the same list. We need to grab the parentId of the row that changed in order to get the other items with the same parent
    if (parentId !== null) parentChanged = true;
    if (parentId === null && changeType === 'update') {
        const item = await db.first<any>(`SELECT * FROM "${tblName}" WHERE ${pkEqual(db, tblName, pk)}`, []);
        assert(item !== undefined);
        parentId = item[parentColId];
    }

    const position = await getFracIdxPosition(db, tblName, parentId, parentChanged, parentColId, positionColName, pk, afterId);
    if (position === "-1" || typeof (position) === "number") console.error("Position value is corrupted", position);

    const stmt = `UPDATE "${tblName}" SET ${positionColName} = '${position}' WHERE ${pkEqual(db, tblName, pk)};`;
    const err = await db.exec(stmt, [], { notify: false });
    if (err) {
        console.error(err);
        return;
    }

    changes[fiChangeIdx].value = position;
}

const getFracIdxPosition = async (db: SqliteDB, tblName: string, parentId: string, parentChanged: boolean, parentColId: string, positionColId: string, pk: string, afterId: string): Promise<string> => {
    const pci = positionColId;

    const comparePk = (item: any, pkB: string): boolean => {
        const pkA = pkEncodingOfRow(db, tblName, item);
        return pkA === pkB;
    }

    // @Speed - Only select the pk, position and parent column, no need to select all the fields
    let items = [];
    if (parentChanged) {
        items = await db.select<any[]>(`SELECT * FROM "${tblName}" WHERE ${parentColId} = ? AND ${pkNotEqual(db, tblName, pk)} ORDER BY ${positionColId} ASC`, [parentId]);
    } else {
        items = await db.select<any[]>(`SELECT * FROM "${tblName}" WHERE ${parentColId} = ? ORDER BY ${positionColId} ASC`, [parentId]);
    }
    if (items.length === 0) return fracMid("[", "]");


    // @Hack - Inject the previous position of the changed item into itself, as the previous position is lost on save.
    if (!parentChanged) {
        let prevChange = await db.first<Change>(`SELECT * FROM "crr_changes" WHERE tbl_name = ? AND pk = ? AND col_id = ? ORDER BY created_at DESC`, [tblName, pk, positionColId]);
        if (prevChange !== undefined) {
            const changedItemIdx = items.findIndex(item => comparePk(item, pk));
            assert(changedItemIdx !== -1);
            if (typeof (prevChange.value) === 'number') prevChange.value = prevChange.value.toString();
            items[changedItemIdx][positionColId] = prevChange.value;
            items.sort((a, b) => a[pci] < b[pci] ? -1 : +1);
        }
    }

    if (afterId === "-1") { // Prepend
        if (comparePk(items[0], pk)) return items[0][pci] // Placing head after itself
        return fracMid("[", items[0][pci]);
    }
    else if (afterId === "1") { // Append
        if (comparePk(items[items.length - 1], pk)) return items[items.length - 1][pci] // Placing last item after itself
        return fracMid(items[items.length - 1][pci], "]");
    }
    else { // Insert after item id
        if (afterId === pk) { // Placing after ourselves
            const thisItem = items.find(item => comparePk(item, pk));
            assert(thisItem !== undefined);
            return thisItem![pci];
        }

        const afterIdx = items.findIndex(item => comparePk(item, afterId));
        const afterItem = items.find(item => comparePk(item, afterId));
        assert(afterIdx !== -1 && afterItem !== undefined);

        if (afterIdx === items.length - 1) { // Append last item
            return fracMid(items[items.length - 1][pci], "]");
        } else { // In-between
            const itemA = items[afterIdx + 0];
            const itemB = items[afterIdx + 1];

            if (comparePk(itemB, pk)) return itemB[pci]; // Placing above ourselves

            return fracMid(itemA[pci], itemB[pci]);
        }
    }
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
        WHERE ${pkEqual(db, tblName, pk)}
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

    return await db.exec(sql, [], { notify: false });
}

export const compactChanges = async (db: SqliteDB) => {
    const client = await db.first<Client>(`SELECT * FROM "crr_client" WHERE site_id = ?`, [db.siteId]);
    if (!client) return;

    const nonPushedChanges = await db.select<Change[]>(`SELECT * FROM "crr_changes" WHERE site_id = ? AND created_at > ?`, [db.siteId, client.last_pushed_at]);

    const groupByRow = (changes: Change[]) => {
        const groups: { tblName: string, colId: string | null, pk: string, changes: Change[] }[] = [];
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

    await db.exec(`DELETE FROM "crr_changes" WHERE id IN (${[...toDelete].map(id => `'${id}'`).join(',')})`, [], { notify: false });
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
                    const pk = pkEncodingOfRow(db, tblName, after[i]);
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
                const pk = pkEncodingOfRow(db, tblName, before[i]);
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
                const pk = pkEncodingOfRow(db, tblName, before[i]);
                changes.push({ id, type: opType, tbl_name: tblName, col_id: key, pk, value: valAfter, site_id: db.siteId, created_at, applied_at: 0, seq });
                seq += 1;
            }
        }
    }

    return changes;
}

const pkEqual = (db: SqliteDB, tblName: string, pk: string) => {
    return "(" + decodePk(db, tblName, pk).map(([colId, value]) => `${colId} = '${value}'`).join(' AND ') + ")";
}

const pkNotEqual = (db: SqliteDB, tblName: string, pk: string) => {
    return "(" + decodePk(db, tblName, pk).map(([colId, value]) => `${colId} != '${value}'`).join(' AND ') + ")";
}

const decodePk = (db: SqliteDB, tblName: string, pk: string): [colId: string, value: any][] => {
    const pkCols = db.pks[tblName];
    assert(pkCols.length > 0);
    const values = pk.split('|');
    assert(pkCols.length === values.length);
    return pkCols.map((colId, i) => [colId, values[i]]);
}

const pkEncodingOfRow = (db: SqliteDB, tblName: string, row: any) => {
    const pkCols = db.pks[tblName];
    assert(pkCols.length > 0);
    return Object.entries(row).filter(([colId, _]) => pkCols.includes(colId)).map(([_, value]) => value).join('|');
}

export const sqlAsSelectStmt = (sql: string) => {
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

export const sqlArray = (a: any[]) => {
    return `(${a.map(v => `'${v}'`).join(',')})`;
}

export const sqlExplainExec = (sql: string) : string => {
    const s = sql.trim().split(' ');
    switch (s[0].toLowerCase()) {
        case "insert": {
            s.shift();
            s.shift();
            const tableName = s[0];
            return tableName.replaceAll(`"`, '');
        }
        case "update": {
            s.shift();
            const tableName = s[0];
            return tableName.replaceAll(`"`, '');
        }
        case "delete": {
            s.shift();
            s.shift();
            const tableName = s[0];
            return tableName.replaceAll(`"`, '');
        }
        default: {
            return "";
        }
    }
}

export const sqlExplainQuery = async (db: SqliteDB, sql: string): Promise<string[]> => {
    const s = sql.trim().split(' ');
    switch (s[0].toLowerCase()) {
        case "pragma": {
            s.shift();
            if (s[0].includes("table_info")) {
                const split = s[0].split("(");
                if (split.length > 1) {
                    const tblName = split[1].replaceAll("'", "").replaceAll(")", "");
                    return [tblName]
                }
            }
            return [];
        }
        case "select": {
            const rows = await db.select<{ detail: string }[]>(`EXPLAIN QUERY PLAN ${sql}`, []);
            if (rows.length === 0) return [];
            const tblNames = [];
            for (const row of rows) {
                if (row.detail.includes("SCAN")) {
                    const tblName = row.detail.split(" ")[1];
                    tblNames.push(tblName)
                }
            }
            return tblNames;
        }
        default: {
            // console.log(`In sqlAffectedTable(). Couldn't infer table name from '${sql}'`);
            return [];
        }
    }

}