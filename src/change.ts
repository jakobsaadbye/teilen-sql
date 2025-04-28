import { assert, pkEncodingOfRow, sqlPlaceholders, unique, insertRows, sqlPlaceholdersMulti, pkEqual, pkNotEqual } from "./utils.ts";
import { fracMid } from "./frac.ts";
import { SqliteDB, TemporaryData } from "./sqlitedb.ts"
import { decodeHlc, encodeHlc, newHlc, receiveHlc } from "./hlc.ts";

export type Client = {
    site_id: string
    last_pulled_at: bigint
    last_pushed_at: bigint
    is_me: boolean
};

export type CrrColumn = {
    tbl_name: string
    col_id: string
    type: 'lww' | 'fractional_index'
    fk: string | null               // format is 'table|col_id'
    fk_on_delete: null | 'CASCADE' | 'RESTRICT' | "NO ACTION"
    parent_col_id: string | null    // set if column type is fractional_index
    manual_conflict: boolean
    replicate: boolean
};

export type OpType = 'insert' | 'update' | 'delete'

export type Change = {
    type: OpType
    tbl_name: string
    col_id: string,     // 'tombstone' when type is delete
    pk: string,         // primary-key of the changed row. If primary-key is composed of multiple columns, the format is "col_1|col_2|...|col_n"
    value: any          // when type is delete, value is either 1 (deleted) or 0 (not deleted)
    site_id: string     // client ID that made the change
    created_at: string  // when this change was created (stored as a hybrid-logical-clock)
    applied_at: number
    version: string
    document: string
};

export type FkRelation = {
    childTblName: string
    childColId: string
    tblName: string
    colId: string
    pk: string
};

export const applyChanges = async (db: SqliteDB, changes: Change[]) => {
    if (changes.length === 0) return [];

    await db.exec(`BEGIN EXCLUSIVE TRANSACTION;`, []);
    await db.exec(`UPDATE "crr_temp" SET time_travelling = 1`, []);

    const changeSets = getChangeSets(changes);

    const touchedTables = unique(changeSets.map(changeSet => changeSet[0].tbl_name));

    // :ModifyForeignKeyInserts
    const fkColsPerTable: { [table: string]: CrrColumn[] } = {};
    for (const table of touchedTables) {
        fkColsPerTable[table] = db.crrColumns[table]?.filter(col => col.fk) ?? [];
    };

    const appliedChanges = [] as Change[];
    for (const changeSet of changeSets) {
        const type = changeSet[0].type;
        switch (type) {
            case 'insert': {
                const insert = changeSet;

                await saveChanges(db, insert);
                appliedChanges.push(...insert);

                const insertRow = true // await doResurrection(db, insert); // nocheckin

                if (insertRow) {
                    const tblName = insert[0].tbl_name;
                    const cols = insert.map(i => i.col_id);
                    const values = insert.map(i => i.value);

                    const stmt = `
                        INSERT OR IGNORE INTO "${tblName}" (${cols.join(",")})
                        VALUES (${sqlPlaceholders(cols)})
                    `;

                    await db.execOrThrow(stmt, values);
                }
            } break;
            case 'update': {
                const updateSet = changeSet;

                const tblName = updateSet[0].tbl_name;
                const pk = updateSet[0].pk;

                const currentChanges = await db.select<Change[]>(`SELECT * FROM "crr_changes" WHERE pk = ? ORDER BY created_at DESC`, [pk]);
                const fkCols = fkColsPerTable[tblName];

                const updatesToApply: Change[] = [];
                for (const update of updateSet) {
                    const prevChange = currentChanges.find(change => change.col_id === update.col_id);
                    if (isLastWriter(update, prevChange)) {
                        updatesToApply.push(update);

                        // :ModifyForeignKeyInserts
                        if (fkCols.length > 0) {
                            const fkCol = fkCols.find(col => col.col_id === update.col_id);
                            if (fkCol) {
                                await db.execOrThrow(`UPDATE "crr_changes" SET value = ? WHERE type = 'insert' AND tbl_name = ? AND pk = ? AND col_id = ?`, [update.value, update.tbl_name, pk, fkCol.col_id]);
                            }
                        }
                    }
                }
                if (updatesToApply.length === 0) continue;

                await saveChanges(db, updatesToApply);
                appliedChanges.push(...updatesToApply);

                const applyChangesToRow = true // await doResurrection(db, updateSet); // nocheckin
                if (applyChangesToRow) {
                    const values = updatesToApply.map(c => c.value);
                    const stmt = `
                        UPDATE "${tblName}"
                        SET ${updatesToApply.map(c => `${c.col_id} = ?`).join(",")}
                        WHERE ${pkEqual(db, tblName, pk)}
                    `;
                    await db.execOrThrow(stmt, values);
                } else {
                    // Update is probably cancelled because of a dead parent
                    // Check if we are moving into a dead parent, if so, we need to delete ourselves here
                    const fkCols = fkColsPerTable[tblName];
                    const fkColNames = fkCols.map(col => col.col_id);
                    if (fkCols.length === 0) continue;

                    const fkUpdates = updateSet.filter(change => fkColNames.includes(change.col_id as string));
                    if (fkUpdates.length === 0) continue;

                    let movedIntoDeadParent = false;
                    for (const fkUpdate of fkUpdates) {
                        const fkCol = fkCols.find(col => col.col_id === fkUpdate.col_id);
                        assert(fkCol);

                        const parentTblName = fkCol.fk!.split("|")[0];
                        const parentPk = fkUpdate.value;

                        const activeParentDelete = await db.first<Change>(`
                            SELECT * FROM "crr_changes"
                            WHERE tbl_name = ? AND pk = ? AND type = 'delete' AND value = 1
                        `, [parentTblName, parentPk]);

                        if (activeParentDelete) {
                            movedIntoDeadParent = true;
                            break;
                        }
                    }

                    if (movedIntoDeadParent) {
                        await db.execOrThrow(`DELETE FROM "${tblName}" WHERE ${pkEqual(db, tblName, pk)}`, []);
                    }
                }
            } break;
            case "delete": {

                // @Speed @LowhangingFruit - We really want to look into caching the way we look up if any changes has been
                // made to child rows as this is right now sloooooow.

                const del = changeSet[0];

                const newChangesToThisRow = await db.select<Change[]>(`
                    SELECT * FROM "crr_changes" 
                    WHERE type != 'delete' AND site_id != ? AND tbl_name = ? AND pk = ? AND created_at >= ?
                    ORDER BY created_at DESC
                `, [del.site_id, del.tbl_name, del.pk, del.created_at]);

                let newChildChanges: Change[] = [];
                if (newChangesToThisRow.length === 0) {
                    const root = { tblName: del.tbl_name, pk: del.pk };
                    const queue = [root];
                    search: while (queue.length > 0) {
                        const parent = queue.pop() as { tblName: string, pk: string };

                        const childRelations = childFkRelations(db, parent.tblName, parent.pk);

                        for (const rel of childRelations) {
                            // @Speed - Make a dedicated query to select only child changes that are newer than the delete instead of getChildChanges()
                            // which grabs all of them into memory first.
                            const childChanges = await getChildChanges(db, rel);
                            const childPks = childChanges.map(change => change.pk);

                            const newChanges = childChanges.filter(c => {
                                if (c.type !== 'delete' && c.site_id !== del.site_id && c.created_at >= del.created_at) {
                                    return true;
                                }
                                return false;
                            });

                            if (newChanges.length > 0) {
                                // We found a new child change. Don't delete the row!
                                // console.log(`Stopped a delete from occuring because a new change was found to child table '${rel.childTblName}'`, newChanges);
                                newChildChanges = newChanges;
                                break search;
                            }

                            for (const childPk of childPks) {
                                const grandChildRelations = childFkRelations(db, rel.childTblName, childPk);
                                queue.push(...grandChildRelations);
                            }
                        }
                    }
                }

                const allNewChanges = [...newChangesToThisRow, ...newChildChanges];
                if (true || allNewChanges.length === 0) { // nocheckin
                    await db.execOrThrow(`DELETE FROM "${del.tbl_name}" WHERE ${pkEqual(db, del.tbl_name, del.pk)}`, []);
                } else {
                    // We ignore the delete.
                    // We make a 'counter' change that acts as a 'new' change
                    // that can get pushed to other clients so that they will reflect that the delete got cancelled.
                    // Its simply a re-play of the newest change so it won't have any effect.
                    // NOTE: Should this be re-playing all the new changes???
                    del.value = 0; // Mark delete as cancelled
                }

                await saveChanges(db, changeSet);
                appliedChanges.push(...changeSet);
            } break;
        }
    }

    // Maybe update our hybrid-logical-clock
    const theirClock = decodeHlc(changeSets[changeSets.length - 1][0].created_at);  // Because changeSets are sorted in time, the last one is their greatest clock value which we match against
    let ourClock;
    const temp = await db.first<TemporaryData>(`SELECT * FROM "crr_temp"`, []);
    if (!temp) {
        ourClock = newHlc();
    } else {
        ourClock = decodeHlc(temp.clock);
    }
    const newClock = receiveHlc(ourClock, theirClock);
    const newClockEncoded = encodeHlc(newClock);
    await db.exec(`INSERT OR REPLACE INTO "crr_temp" (clock)`, [newClockEncoded]);


    // Patch any fractional index columns that might have collided. 
    // NOTE(*important*): Must run after all the changes have been applied 
    // so that all rows are known about
    await fixFractionalIndexCollisions(db, changeSets);

    await db.exec(`UPDATE "crr_temp" SET time_travelling = 0`, []);
    await db.exec(`COMMIT;`, []);

    return appliedChanges;
}

export const getChildChanges = async (db: SqliteDB, fkRelation: FkRelation) => {
    const rel = fkRelation;

    const childFkChanges = await db.select<Change[]>(`
        SELECT * FROM "crr_changes" 
        WHERE tbl_name = ? AND col_id = ? AND value = ?
    `, [rel.childTblName, rel.childColId, rel.pk]);
    if (childFkChanges.length === 0) return [];

    const childPks = childFkChanges.map(change => change.pk);

    const childChanges = await db.select<Change[]>(`
        SELECT * FROM "crr_changes" 
        WHERE tbl_name = ? AND pk IN (${sqlPlaceholders(childPks)})
        ORDER BY created_at DESC
    `, [rel.childTblName, ...childPks]);

    return childChanges;
}

// const updateLastPulledAtFromPeers = async (db: SqliteDB, changes: Change[]) => {
//     const changesPerSite = Object.groupBy(changes, (change) => change.site_id);
//     for (const [siteId, changes] of Object.entries(changesPerSite)) {
//         changes!.sort((a, b) => b.applied_at - a.applied_at);
//         const maxAppliedAt = changes![changes!.length - 1].applied_at;

//         const err = await db.exec(`
//             INSERT INTO "crr_clients" (site_id, last_pulled_at, is_me)
//             VALUES (?, ?, false)
//             ON CONFLICT DO UPDATE SET
//                 last_pulled_at = EXCLUDED.last_pulled_at,
//                 is_me          = false
//         `, [siteId, maxAppliedAt]);
//         if (err) return err;
//     }
// }

export const childFkRelations = (db: SqliteDB, parentTblName: string, parentPk: string) => {
    const relations = [];
    for (const [tblName, cols] of Object.entries(db.crrColumns)) {
        const fkCols = cols.filter(col => col.fk !== null && col.fk_on_delete === 'CASCADE' && col.fk.split("|")[0] === parentTblName);
        if (fkCols.length > 0) {
            for (const col of fkCols) {
                relations.push({
                    childTblName: tblName,
                    childColId: col.col_id,
                    tblName: parentTblName,
                    colId: col.fk!.split("|")[1],
                    pk: parentPk
                } as FkRelation);
            }
        }
    }
    return relations;
};

const doResurrection = async (db: SqliteDB, changeSet: Change[]) => {
    assert(changeSet.length > 0 && changeSet[0].type !== 'delete');
    const tblName = changeSet[0].tbl_name;
    const pk = changeSet[0].pk;

    const parentFkRelations = async (tblName: string, pk: string): Promise<[rels: FkRelation[], ok: boolean]> => {
        const crrCols = db.crrColumns[tblName];
        const fkCols = crrCols.filter(col => col.fk !== null && col.fk_on_delete === 'CASCADE');
        if (fkCols.length === 0) return [[], true];

        const fkRelations: FkRelation[] = [];
        for (const rel of fkCols) {
            const [parentTblName, parentColId] = rel.fk!.split("|");

            const childParentChange = await db.first<Change>(`
                SELECT * FROM "crr_changes"
                WHERE tbl_name = ? AND pk = ? AND col_id = ?
            `, [tblName, pk, rel.col_id]);
            if (childParentChange === undefined) {
                // :ModifyForeignKeyInserts
                // @Refactor @Hack - Becuase of the way we update the foreign-key columns, it might happen that
                // we haven't inserted the parent yet, but we know that the update will come together but later, so here 
                // we just ignore the change insert change and resurrect it upon updating.
                // This illustrates how it can happen currently:
                // col1 created,
                // todo created <- col1
                // col2 created
                // todo updated <- col2 (original insert also updates to reflect col2)
                return [[], false];
            }

            // If the incomming change is a change of parent, we need to use that parent relation instead of the old one
            const parentChange = changeSet.find(change => change.type === 'update' && change.tbl_name === tblName && change.col_id === rel.col_id);
            if (parentChange) {
                if (isLastWriter(parentChange, childParentChange)) {
                    childParentChange.value = parentChange.value;
                }
            }

            fkRelations.push({ childTblName: tblName, childColId: rel.col_id as string, tblName: parentTblName, colId: parentColId, pk: childParentChange.value });
        }

        return [fkRelations, true];
    }

    // Progressively, move up the tree of possibly deleted parents to find
    // the root cause of a deletion. We only care about the parents
    // that have an ON DELETE CASCADE relation, as we don't capture those
    // in the change history as deletes, so we need to figure out all the re-inserts
    // that we need to do, in-order to invert the cascading delete operation.
    //
    // NOTE: We might also do something like 
    // Synql (https://inria.hal.science/hal-03999168/document), let the ON DELETE on a foreign-key decide what happens.
    // ON DELETE RESTRICT, would resurrect the entire graph of relations (although, i think sqlite would already block the delete comming through so maybe no need to do something at all), 
    // ON DELETE CASCADE, would be to let remove win always.
    const timeIncChange = changeSet[0].created_at;

    let root: { tblName: string, pk: string } = { tblName, pk };
    const myself: FkRelation = { childTblName: tblName, childColId: "", tblName, colId: "", pk };
    const [parents, ok] = await parentFkRelations(myself.tblName, myself.pk);
    if (!ok) return false;

    const queue = [myself, ...parents];
    while (queue.length !== 0) {
        const parent = queue.pop() as FkRelation;

        const parentRow = await db.first<any>(`SELECT * FROM "${parent.tblName}" WHERE ${pkEqual(db, parent.tblName, parent.pk)}`, []);
        if (parentRow === undefined) {
            const parentDelete = await db.first<Change>(`
                SELECT * FROM "crr_changes" 
                WHERE type = 'delete' AND tbl_name = ? AND pk = ? AND value = 1
                ORDER BY created_at DESC
            `, [parent.tblName, parent.pk]);

            if (parentDelete) {
                if (parentDelete.created_at > timeIncChange) {
                    // The parent delete was made after the latest change, so we ignore the change
                    return false;
                } else {
                    // This change supercedes the parent delete, so we cancel the delete on it. The parent will be resurrected in the next phase
                    const pd = parentDelete;
                    await db.execOrThrow(`UPDATE "crr_changes" SET value = 0 WHERE type = 'delete' AND tbl_name = ? AND pk = ?`, [pd.tbl_name, pd.pk]);
                }
            }

            // The deletion of this row happended through an ON DELETE CASCADE, keep searching for the root of the cascade.
            root = { tblName: parent.tblName, pk: parent.pk };

            const [parentRels, ok] = await parentFkRelations(parent.tblName, parent.pk);
            if (!ok) return false;
            if (Object.keys(parentRels).length === 0) break; // Parent doesn't have any fk relations

            queue.push(...parentRels);
        } else {
            const parentDelete = await db.first<Change>(`
                SELECT * FROM "crr_changes" 
                WHERE type = 'delete' AND tbl_name = ? AND pk = ? AND value = 1
                ORDER BY created_at DESC
            `, [parent.tblName, parent.pk]);

            if (parentDelete) {
                console.error(`Found a parent row that should have been deleted but is not`, parentRow, parentDelete);
            }
        }
    }

    // Next phase:
    // Insert the resurrected parents, and any of its child rows that also should be resurrected.
    // @Speed - We should deduplicate rows that have already been resurrected to not do massive inserts
    // with no effect!
    const parentQueue = [root];
    while (parentQueue.length !== 0) {
        const parent = parentQueue.pop() as { tblName: string, pk: string };

        // console.log(`Parent ('${parent.tblName}', ${parent.pk})`);

        if (changeSet[0].type === 'insert' && tblName === parent.tblName && pk === parent.pk) {
            // This is a newly inserted row. There can't possibly be any children to resurrect, but there might
            // be parents that should be resurrected.
            continue;
        }

        // Reconstruct the parent if it is deleted!
        let parentRow = await db.first<any>(`SELECT * FROM "${parent.tblName}" WHERE ${pkEqual(db, parent.tblName, parent.pk)}`, []);
        if (parentRow === undefined) {
            parentRow = await reconstructRowFromHistory(db, parent.tblName, parent.pk);
            if (parentRow === undefined || Object.keys(parentRow).length === 0) {
                console.error(`Failed to reconstruct parent row (${parent.tblName}, ${parent.pk})`);
                return false;
            }

            if (tblName === parent.tblName && pk === parent.pk) {
                // We are trying to resurrect the currently changing row. Before resurrecting
                // we layer in the incomming changes to make sure that we don't resurrect the row
                // while the parent is missing.
                const currentChanges = await db.select<Change[]>(`SELECT * FROM "crr_changes" WHERE tbl_name = ? AND pk = ? ORDER BY created_at DESC`, [tblName, pk]);
                for (const c of changeSet) {
                    const latestChangeToCell = currentChanges.find(change => change.col_id === c.col_id);
                    if (isLastWriter(c, latestChangeToCell)) {
                        parentRow[c.col_id as string] = c.value;
                    }
                }
            }

            const err = await insertRows(db, parent.tblName, [parentRow]);
            if (err) {
                console.error(err);
                return false;
            }

            // console.log(`Resurrected parent: ('${parent.tblName}', ${parentRow["title"]})`);
        }

        const childRelations = childFkRelations(db, parent.tblName, parent.pk);
        for (const rel of childRelations) {

            // Check if the children themselves have a deletion on them that is winning and therefore should not be resurrected
            const childTblName = rel.childTblName;
            const childChanges = await getChildChanges(db, rel);
            const activeDeletions = childChanges.filter(change => change.type === 'delete' && change.value === 1);
            const ignorePks: string[] = unique(activeDeletions.map(del => del.pk));
            const childPks = childChanges.filter(change => !ignorePks.includes(change.pk)).map(change => change.pk);

            // Resurrect the non-deleted children
            const childRows = [];
            for (const childPk of childPks) {
                const child = await reconstructRowFromHistory(db, childTblName, childPk);
                if (child === undefined || Object.keys(child).length === 0) {
                    console.error(`Failed to reconstruct child row (${childTblName}, ${childPk})`);
                    return false;
                }
                childRows.push(child);
                // console.log(`Resurrected child: ('${rel.childTblName}', '${child["title"]}')`);
            }
            const err = await insertRows(db, childTblName, childRows);
            if (err) {
                console.error(err);
                return false;
            }

            // Each child gets to be the parent in the next iteration of resurrection.
            // We grab all of the child of child foreign relationships and add that to the queue of
            // relations to visit.
            for (let i = 0; i < childRows.length; i++) {
                const childPk = childPks[i];
                const grandChildRelations = childFkRelations(db, childTblName, childPk);
                parentQueue.push(...grandChildRelations);
            }
        }
    }

    return true;
}

export const reconstructRowFromHistory = async (db: SqliteDB, tblName: string, pk: string): Promise<any> => {
    const latestChanges = await db.select<Change[]>(`
        SELECT * FROM "crr_changes" 
        WHERE type != 'delete' AND tbl_name = ? AND pk = ? 
        ORDER BY created_at DESC
    `, [tblName, pk]);
    if (latestChanges.length === 0) return;

    const cols = db.crrColumns[tblName].map(col => col.col_id);
    const constructed: any = {};
    for (const key of cols) {
        const col = latestChanges.find(c => c.col_id === key);
        constructed[key] = col !== undefined ? col.value : null;
    }
    return constructed;
}

const fixFractionalIndexCollisions = async (db: SqliteDB, changes: Change[][]) => {
    const fiChanges = changes.filter(changeSet => {
        const tblName = changeSet[0].tbl_name;
        const fiCols = db.crrColumns[tblName]?.filter(col => col.type === 'fractional_index') ?? [];
        if (fiCols.length === 0) return false;
        return changeSet.find(change => fiCols.find(col => col.col_id === change.col_id) !== undefined) !== undefined;
    });
    if (fiChanges.length === 0) return [];

    // Extract the lists containing children with fractional index columns that are affected in each table
    type List = {
        parentColId: string
        parentId: string
        posColId: string
    };

    const tables: { [tblName: string]: { [parentId: string]: List } } = {};
    for (const changeSet of fiChanges) {
        const pk = changeSet[0].pk;
        const tblName = changeSet[0].tbl_name;
        const fiCol = db.crrColumns[tblName].find(col => col.type === 'fractional_index'); // @Improvement: This assumes only one fractional index column in the table
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
                    // The changes both contained an update and a delete to this row. Since the row is not here
                    // it means that the delete took precedence over the update, so we just ignore it.
                    continue;
                }
                parentId = row[parentColId];
            }
        }

        if (tables[tblName] !== undefined) tables[tblName][parentId] = { parentColId, parentId, posColId };
        else tables[tblName] = { [parentId]: { parentColId, parentId, posColId } };
    }

    // Try search for collisions on the same positions
    for (const [tblName, lists] of Object.entries(tables)) {
        for (const list of Object.values(lists)) {
            const parentColId = list.parentColId;
            const parentId = list.parentId;
            const posColId = list.posColId;

            const items = await db.select<any[]>(`SELECT * FROM "${tblName}" WHERE ${parentColId} = '${parentId}' ORDER BY ${posColId} ASC`, []);
            if (items.length === 0 || items.length === 1) continue; // Don't put a return here if you want to succeed in the software industry ... jsaad - 29 jan. 2025

            const itemPks = items.map(item => pkEncodingOfRow(db, tblName, item));
            const lastChanges = await db.select<Change[]>(`SELECT * FROM "crr_changes" WHERE pk IN (${sqlPlaceholders(itemPks)}) AND col_id = ? ORDER BY created_at DESC`, [...itemPks, posColId]);

            const comparePk = (item: any, pkB: string): boolean => {
                const pkA = pkEncodingOfRow(db, tblName, item);
                return pkA === pkB;
            }

            type Pair = [idx: number, item: any, change: Change];
            const pairs: Pair[] = [];
            for (let i = 0; i < items.length; i++) {
                const item = items[i];
                const posChange = lastChanges.find(change => comparePk(item, change.pk));
                if (posChange === undefined) {
                    console.error(`Failed to get last change of item"`, item);
                }
                pairs.push([i, item, posChange as Change]);
            }

            const positionGroups = Object.entries(Object.groupBy(pairs, (([, item, _]) => item[posColId])));
            for (const [pos, group] of positionGroups) {
                if (group!.length > 1) {
                    // console.log(`Detected collision in table '${tblName}', ${list.parentColId} '${list.parentId}' on position '${pos}'`);

                    // Collision on a position!
                    // Resolve by last-writer-wins. The last writer, gets to be after the other. 
                    // NOTE: Maybe it should be an option when upgrading to a fractional index, to choose weather the last writer gets below or above?
                    const sorted = group!.toSorted(([, , changeA], [, , changeB]) => isLastWriter(changeA, changeB) ? +1 : -1);

                    // First item in the sorted collisions will just stay put as the anchor point for the rest of the collisions to go after.
                    const [, head] = sorted[0];
                    const nextIdx = 1 + Math.max(...sorted.map(([idx,]) => idx));

                    const anchorA = head[posColId] as string;
                    let anchorB = "";
                    if (nextIdx === items.length) {
                        // Case when last collided item is at the end of the list
                        anchorB = "]"
                    } else {
                        anchorB = items[nextIdx][posColId];
                    }

                    for (let j = sorted.length - 1; j > 0; j--) {
                        if (j === sorted.length - 1) {
                            const [, tail] = sorted[j];
                            const position = fracMid(anchorA, anchorB);
                            tail[posColId] = position;
                        } else {
                            const [, item] = sorted[j];
                            const [, prev] = sorted[j + 1];
                            const position = fracMid(anchorA, prev[posColId]);
                            item[posColId] = position;
                        }
                    }

                    const positions = sorted.map(([, item]) => item[posColId]);
                    const uniquePositions = unique(positions);
                    assert(uniquePositions.length === positions.length, `**Bug**, produced non unique positions ${positions}`);

                    for (let i = 0; i < sorted.length; i++) {
                        const [idx, item] = sorted[i];
                        const pk = itemPks[idx];
                        const posValue = item[posColId];

                        // Update the position of the item in the array such that new position is accounted for in the next iteration!
                        items[idx][posColId] = posValue;

                        // Update the actual row's position
                        let err = await db.exec(`UPDATE "${tblName}" SET ${posColId} = ? WHERE ${pkEqual(db, tblName, pk)}`, [posValue]);
                        if (err) {
                            console.error(`Failed to update fractional index position in ${tblName} after collision`, err);
                            continue;
                        }

                        // err = await db.exec(`UPDATE "crr_changes" SET value = ? WHERE tbl_name = ? AND pk = ? AND col_id = ?`, [posValue, tblName, pk, posColId]);
                        // if (err) {
                        //     console.error(`Failed to update fractional index position in crr changes after collision`, err);
                        //     continue;
                        // }
                    }
                }
            }

            const itemPositions: string[] = items.map(item => item[posColId]);
            if (unique(itemPositions).length !== itemPositions.length) {
                console.error(`**Bug** Item positions were not unique after fixup`, items);
            }
        }
    }
}

/**
 * @returns True if a is the last writer
 */
export const isLastWriter = (a: Change | undefined, b: Change | undefined) => {
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
    if (fiCols.length === 0) return; // Table doesn't have a fractional index column
    const fiCol = fiCols.find(col => changedCols.includes(col.col_id));
    if (fiCol === undefined) return; // No change to a fraction index column

    const fiChangeIdx = changes.findIndex(c => c.col_id === fiCol.col_id);
    if (fiChangeIdx === -1) return;

    const positionColName = fiCol.col_id;
    const parentColId = fiCol.parent_col_id;
    let afterId = changes.find(c => c.col_id === fiCol.col_id)?.value ?? null
    assert(afterId !== null);
    if (typeof afterId === "number") afterId = afterId.toString();

    let parentChanged = false;
    let parentId = changes.find(c => c.col_id === fiCol.parent_col_id)?.value ?? null // null if only an update to the same list. We need to grab the parentId of the row that changed in order to get the other items with the same parent
    if (parentId !== null) parentChanged = true;
    if (parentId === null && changeType === 'update') {
        const item = await db.first<any>(`SELECT * FROM "${tblName}" WHERE ${pkEqual(db, tblName, pk)}`, []);
        assert(item !== undefined);
        parentId = item[parentColId];
    }
    if (changeType === "insert") parentChanged = false;
    if (typeof parentId === "number") parentId = parentId.toString();

    const position = await getFracIdxPosition(db, tblName, parentId, parentChanged, parentColId, positionColName, pk, afterId);
    if (position === "|prepend" || position === "|append") console.error("Position value is corrupted", position);

    const stmt = `UPDATE "${tblName}" SET ${positionColName} = '${position}' WHERE ${pkEqual(db, tblName, pk)};`;
    const err = await db.exec(stmt, [], { notify: false });
    if (err) {
        console.error(err);
        return;
    }

    // We need to update the position on the previous insert aswell as we keep two rows for the position!
    await db.execOrThrow(`UPDATE "crr_changes" SET value = ? WHERE tbl_name = ? AND pk = ? AND col_id = ?`, [position, tblName, pk, fiCol.col_id])

    changes[fiChangeIdx].value = position;
}

const getFracIdxPosition = async (db: SqliteDB, tblName: string, parentId: string, parentChanged: boolean, parentColId: string, positionColId: string, pk: string, afterId: string): Promise<string> => {
    const pcid = positionColId;

    const comparePk = (item: any, pkB: string): boolean => {
        const pkA = pkEncodingOfRow(db, tblName, item);
        return pkA === pkB;
    }

    let items = [];
    if (parentChanged) {
        items = await db.select<any[]>(`SELECT * FROM "${tblName}" WHERE ${parentColId} = ? AND ${pkNotEqual(db, tblName, pk)} ORDER BY ${positionColId} ASC`, [parentId]);
    } else {
        items = await db.select<any[]>(`SELECT * FROM "${tblName}" WHERE ${parentColId} = ? ORDER BY ${positionColId} ASC`, [parentId]);
    }
    if (items.length === 0 || items.length === 1) return fracMid("[", "]");

    // @Hack - Inject the previous position of the changed item into itself, as the previous position is lost on save.
    if (!parentChanged) {
        const prevChange = await db.first<Change>(`SELECT * FROM "crr_changes" WHERE tbl_name = ? AND pk = ? AND col_id = ? ORDER BY created_at DESC`, [tblName, pk, positionColId]);
        if (prevChange !== undefined) {
            const changedItemIdx = items.findIndex(item => comparePk(item, pk));
            assert(changedItemIdx !== -1);
            if (typeof (prevChange.value) === 'number') prevChange.value = prevChange.value.toString();
            items[changedItemIdx][positionColId] = prevChange.value;
            items.sort((a, b) => a[pcid] < b[pcid] ? -1 : +1);
        }
    }

    if (afterId === "|prepend") {
        const first = items[0];
        const firstPos = first[pcid];
        if (comparePk(first, pk)) return firstPos; // Placing head after itself
        return fracMid("[", firstPos);
    }
    else if (afterId === "|append") {
        const last = items[items.length - 2];
        const lastPos = last[pcid];
        if (comparePk(last, pk)) return lastPos; // Placing last item after itself
        return fracMid(lastPos, "]");
    }
    else { // Insert after item id
        if (afterId === pk) { // Placing after ourselves
            const thisItem = items.find(item => comparePk(item, pk));
            assert(thisItem !== undefined);
            return thisItem![pcid];
        }

        const afterIdx = items.findIndex(item => comparePk(item, afterId));
        const afterItem = items.find(item => comparePk(item, afterId));
        assert(afterIdx !== -1 && afterItem !== undefined);

        if (afterIdx === items.length - 1) { // Append last item
            return fracMid(items[items.length - 1][pcid], "]");
        } else { // In-between
            const itemA = items[afterIdx + 0];
            const itemB = items[afterIdx + 1];

            if (comparePk(itemB, pk)) return itemB[pcid]; // Placing above ourselves

            return fracMid(itemA[pcid], itemB[pcid]);
        }
    }
}

export const saveChanges = async (db: SqliteDB, changes: Change[]) => {
    if (changes.length === 0) return;
    const appliedAt = new Date().getTime();

    const values = changes.reduce((vals, c) => {
        vals.push(c.type, c.tbl_name, c.col_id, c.pk, c.value, c.site_id, c.created_at, appliedAt, c.version, c.document);
        return vals;
    }, [] as any[])

    const sql = `
        INSERT INTO "crr_changes" (type, tbl_name, col_id, pk, value, site_id, created_at, applied_at, version, document)
        VALUES ${sqlPlaceholdersMulti(changes)}
        ON CONFLICT DO UPDATE SET
            value      = EXCLUDED.value,
            site_id    = EXCLUDED.site_id,
            created_at = EXCLUDED.created_at,
            applied_at = EXCLUDED.applied_at
    `;

    await db.execOrThrow(sql, values, { notify: false });
}

const getRelatedChangesFromChanges = (db: SqliteDB, changes: Change[], rootTblName: string, rootPk: string) => {
    const fkRelations = childFkRelations(db, rootTblName, rootPk);
    if (fkRelations.length === 0) return { [rootTblName]: [rootPk] };

    const changesPerTable = Object.groupBy(changes, (change) => change.tbl_name);

    const related: { [tblName: string]: string[] } = { [rootTblName]: [rootPk] };
    const queue = [...fkRelations];
    while (queue.length > 0) {
        const rel = queue.pop() as FkRelation;

        const childChanges = changesPerTable[rel.childTblName];
        if (!childChanges || childChanges.length === 0) continue;

        const childFkChanges = childChanges.filter(change => change.col_id === rel.childColId && change.value === rel.pk);
        if (childFkChanges.length === 0) continue;

        const childPks = childFkChanges.map(change => change.pk);
        if (related[rel.childTblName]) related[rel.childTblName].push(...childPks);
        else related[rel.childTblName] = [...childPks];

        for (const childPk of childPks) {
            const grandChildRelations = childFkRelations(db, rel.childTblName, childPk);
            queue.push(...grandChildRelations);
        }
    }

    return related;
}

// TODO: This function should be updated, as it is now called in batches of changes rather than individual
//       changesets which it assumes.
export const compactChanges = async (db: SqliteDB, changeSet: Change[]) => {
    if (changeSet.length === 0) return [];

    const tblName = changeSet[0].tbl_name;
    const pk = changeSet[0].pk;
    const changeType = changeSet[0].type;

    switch (changeType) {
        case "insert": return; // Nothing to compact
        case "update": return; // Nothing to compact. Update overrides previous update in saveChanges()
        case "delete": {
            const client = await db.first<Client>(`SELECT * FROM "crr_clients" WHERE is_me = 1`, []);
            if (!client) return;

            const unsyncedChanges = await db.select<Change[]>(`SELECT * FROM "crr_changes" WHERE site_id = ? AND applied_at > ? AND version = 0`, [db.siteId, client.last_pushed_at]);
            if (unsyncedChanges.length === 0) return;

            const rootContainsInsert = unsyncedChanges.find(change => change.type === 'insert' && change.tbl_name === tblName && change.pk === pk);
            if (rootContainsInsert) {
                // We can remove the entire history tree of related changes as they were never synced to any peers
                const relatedChanges = getRelatedChangesFromChanges(db, unsyncedChanges, tblName, pk);
                for (const [tblName, pks] of Object.entries(relatedChanges)) {
                    const err = await db.exec(`DELETE FROM "crr_changes" WHERE tbl_name = ? AND pk IN (${sqlPlaceholders(pks)})`, [tblName, ...pks]);
                    if (err) console.error(err);
                }
                return;
            }
        }
    }
}

export const detachChangeGenerationTriggers = async (db: SqliteDB) => {
    const crrColumns = db.crrColumns;
    for (const [tblName, columns] of Object.entries(crrColumns)) {
        await db.execOrThrow(`DROP TRIGGER IF EXISTS _${tblName}_insert`, []);
        await db.execOrThrow(`DROP TRIGGER IF EXISTS _${tblName}_update`, []);
        await db.execOrThrow(`DROP TRIGGER IF EXISTS _${tblName}_delete`, []);
    }
}

export const attachChangeGenerationTriggers = async (db: SqliteDB) => {
    const crrColumns = db.crrColumns;
    for (const [tblName, columns] of Object.entries(crrColumns)) {

        const getPk = (type: "insert" | "update" | "delete") => {
            const pkCols = db.pks[tblName];
            if (pkCols.length === 0) {
                return "NEW.rowid";
            }
            const prefix = type === "delete" ? "OLD" : "NEW";
            let pk = `${prefix}.${pkCols[0]}`;
            if (pkCols.length > 1) {
                const pkValue = pkCols.map(colId => `${prefix}.${colId}`).join(",'|',");
                pk = `concat(${pkValue})`;
            }
            return pk;
        }

        const columnInsert = (col: CrrColumn) => {
            if (!col.replicate) return "";
            return `
                INSERT OR IGNORE INTO crr_changes(type, tbl_name, col_id, pk, value, site_id, created_at, applied_at, version, document)
                    SELECT 'insert', '${tblName}', '${col.col_id}', ${getPk("insert")}, NEW.${col.col_id}, '${db.siteId}',
                            (SELECT clock FROM crr_temp LIMIT 1), 
                            (SELECT clock FROM crr_temp LIMIT 1),
                            '0',
                            (SELECT document FROM crr_temp LIMIT 1)
                    WHERE EXISTS (SELECT 1 FROM crr_temp WHERE time_travelling = 0 LIMIT 1)
                ;
            `;
        }

        const columnUpdate = (col: CrrColumn) => {
            if (!col.replicate) return "";
            return `
                INSERT INTO crr_changes(type, tbl_name, col_id, pk, value, site_id, created_at, applied_at, version, document)
                    SELECT 'update', '${tblName}', '${col.col_id}', ${getPk("update")}, NEW.${col.col_id}, '${db.siteId}',
                        (SELECT clock FROM crr_temp LIMIT 1), 
                        (SELECT clock FROM crr_temp LIMIT 1),
                        '0',
                        (SELECT document FROM crr_temp LIMIT 1)
                    WHERE OLD.${col.col_id} != NEW.${col.col_id} AND
                    EXISTS (SELECT 1 FROM crr_temp WHERE time_travelling = 0 LIMIT 1)
                ON CONFLICT (type, tbl_name, col_id, pk, version) DO UPDATE SET
                    value = EXCLUDED.value, 
                    site_id = EXCLUDED.site_id, 
                    created_at = EXCLUDED.created_at, 
                    applied_at = EXCLUDED.applied_at
                ;
            `;
        }

        const columnInserts = columns.map(col => columnInsert(col)).join("\n");
        const columnUpdates = columns.map(col => columnUpdate(col)).join("\n");

        const insertTrigger = `
            CREATE TRIGGER IF NOT EXISTS _${tblName}_insert
            AFTER INSERT ON ${tblName}
            FOR EACH ROW
            BEGIN
                ${columnInserts}
            END;
        `;

        const updateTrigger = `
            CREATE TRIGGER IF NOT EXISTS _${tblName}_update
            AFTER UPDATE ON ${tblName}
            FOR EACH ROW
            BEGIN
                ${columnUpdates}
            END;
        `

        const deleteTrigger = `
            CREATE TRIGGER IF NOT EXISTS _${tblName}_delete
            AFTER DELETE ON ${tblName}
            FOR EACH ROW
            BEGIN
                INSERT OR IGNORE INTO crr_changes(type, tbl_name, col_id, pk, value, site_id, created_at, applied_at, version, document)
                SELECT 'delete', '${tblName}', 'tombstone', ${getPk("delete")}, 1, '${db.siteId}',
                        (SELECT clock FROM crr_temp LIMIT 1), 
                        (SELECT clock FROM crr_temp LIMIT 1),
                        '0',
                        (SELECT document FROM crr_temp LIMIT 1)
                WHERE EXISTS (SELECT 1 FROM crr_temp WHERE time_travelling = 0 LIMIT 1)
                ;
            END;
        `;

        await db.exec(insertTrigger, []);
        await db.exec(updateTrigger, []);
        await db.exec(deleteTrigger, []);
    }

}

export const getCurrentChangeCount = async (db: SqliteDB) => {
    const client = await db.first<Client>(`SELECT * FROM "crr_clients" WHERE site_id = $1`, [db.siteId]);
    if (!client) return -1;
    const lastPushedAt = client.last_pushed_at;

    const rows = await db.select<Change[]>(`SELECT * FROM "crr_changes" WHERE applied_at > $1 AND site_id = $2`, [lastPushedAt, db.siteId]);
    const changeSets = getChangeSets(rows);
    return changeSets.length;
}

export const getChangeSets = (changes: Change[]): Change[][] => {
    if (changes.length === 0) return [];

    // Split the changes into groups of primary key and change type
    const groups: Change[][] = [];
    const pkGroups = Object.groupBy(changes, (change) => change.pk);
    for (const [pk, changes] of Object.entries(pkGroups)) {
        const typeGroups = Object.groupBy(changes as Change[], (change) => change.type);
        for (const [type, changes] of Object.entries(typeGroups)) {
            groups.push(changes);
        }
    }

    // Sort changes in ascending order
    groups.sort((a, b) => {
        if (a[0].created_at < b[0].created_at) return -1;
        else if (a[0].created_at > b[0].created_at) return +1;
        else return 0;
    });

    return groups;
}

