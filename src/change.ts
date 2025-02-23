import { assert, pkEncodingOfRow, sqlPlaceholders } from "./utils.ts";
import { fracMid } from "./frac.ts";
import { SqliteDB } from "./sqlitedb.ts"

// TODO: 
//  - Move away from timestamps and instead use something like Hybrid Logical Clocks instead. https://jaredforsyth.com/posts/hybrid-logical-clocks/
//  - Record all the tables that got changed in applyChanges and only then notify table changes


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
    fk: string | null // format is 'table|col_id'
    fk_on_delete: 'CASCADE' | 'RESTRICT'
    delete_wins_after: bigint
    parent_col_id: string // set if fractional_index otherwise empty string
};

export type OpType = 'insert' | 'update' | 'delete'

export type Change = {
    type: OpType
    tbl_name: string
    col_id: string | null, // null when type is delete
    pk: string,
    value: any | null // when type is delete, value is either 0 - active delete, 1 - cancelled delete
    site_id: string
    created_at: number
    applied_at: number
};

export type FkRelation = {
    childTblName: string
    childColId: string
    tblName: string
    colId: string
    pk: string
};

export const applyChanges = async (db: SqliteDB, changes: Change[]) => {
    await db.exec(`BEGIN EXCLUSIVE TRANSACTION;`, []);

    const changeSets = getChangeSets(changes);

    const touchedTables = unique(changeSets.map(changeSet => changeSet[0].tbl_name));

    // :ModifyForeignKeyInserts
    const fkColsPerTable: {[table: string] : CrrColumn[]} = {}; 
    for (const table of touchedTables) {
        fkColsPerTable[table] = db.crrColumns[table].filter(col => col.fk);
    };

    for (const changeSet of changeSets) {
        const type = changeSet[0].type;
        switch (type) {
            case 'insert': {
                const insert = changeSet;

                await saveChanges(db, insert);
            
                const insertRow = await doResurrection(db, insert);

                if (insertRow) {
                    const tblName = insert[0].tbl_name;
                    const cols = insert.map(i => i.col_id);
                    const values = insert.map(i => i.value);
                    const stmt = `
                        INSERT OR IGNORE INTO "${tblName}" (${cols.join(",")})
                        VALUES (${cols.map(_ => '?').join(",")})
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

                const changesToApply: Change[] = [];
                for (const update of updateSet) {
                    const prevChange = currentChanges.find(change => change.col_id === update.col_id);
                    if (isLastWriter(update, prevChange)) {
                        changesToApply.push(update);

                        // :ModifyForeignKeyInserts
                        if (fkCols.length > 0) {
                            const fkCol = fkCols.find(col => col.col_id === update.col_id);
                            if (fkCol) {
                                await db.execOrThrow(`UPDATE "crr_changes" SET value = ? WHERE type = 'insert' AND tbl_name = ? AND pk = ? AND col_id = ?`, [update.value, update.tbl_name, pk, fkCol.col_id]);
                            }
                        }
                    }
                }
                if (changesToApply.length === 0) continue;

                await saveChanges(db, changesToApply);

                const applyChangesToRow = await doResurrection(db, updateSet);
                if (applyChangesToRow) {
                    const values = changesToApply.map(c => c.value);
                    const stmt = `
                        UPDATE "${tblName}"
                        SET ${changesToApply.map(c => `${c.col_id} = ?`).join(",")}
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
                        const parentPk      = fkUpdate.value;

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
                // made to child rows.

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
                if (allNewChanges.length === 0) {
                    await db.execOrThrow(`DELETE FROM "${del.tbl_name}" WHERE ${pkEqual(db, del.tbl_name, del.pk)}`, []);
                } else {
                    // We ignore the delete.
                    // We make a 'counter' change that acts as a 'new' change
                    // that can get pushed to other clients so that they will reflect that the delete got cancelled.
                    // Its simply a re-play of the newest change so it won't have any effect.
                    // NOTE: Should this be re-playing all the new changes???
                    del.value = 0; // Mark delete as cancelled
                }

                saveChanges(db, changeSet);
            } break;
        }
    }

    // Patch any fractional index columns that might have collided. 
    // NOTE(*important*): Must run after all the changes have been applied 
    // so that all rows are known about
    await fixFractionalIndexCollisions(db, changeSets, false);

    // @Investigate: Is this necessary???
    await updateLastPulledAtFromPeers(db, changes);

    await db.exec(`COMMIT;`, []);
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

const updateLastPulledAtFromPeers = async (db: SqliteDB, changes: Change[]) => {
    const changesPerSite = Object.groupBy(changes, (change) => change.site_id);
    for (const [siteId, changes] of Object.entries(changesPerSite)) {
        changes!.sort((a, b) => b.applied_at - a.applied_at);
        const maxAppliedAt = changes![changes!.length - 1].applied_at;

        const err = await db.exec(`
            INSERT INTO "crr_clients" (site_id, last_pulled_at, is_me)
            VALUES (?, ?, false)
            ON CONFLICT DO UPDATE SET
                last_pulled_at = EXCLUDED.last_pulled_at,
                is_me          = false
        `, [siteId, maxAppliedAt]);
        if (err) return err;
    }
}

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
    const myself: FkRelation = {childTblName: tblName, childColId: "", tblName, colId: "", pk };
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

const unique = (arr: any[]) => {
    return [...new Set(arr)];
}

const insertRows = async (db: SqliteDB, tblName: string, rows: any[]) => {
    if (rows.length === 0) return;

    const cols = Object.keys(rows[0]);
    const valueSets = rows.map(row => Object.values(row));
    const allVals = valueSets.reduce((allVals, vals) => [...allVals, ...vals], []);
    const err = await db.exec(`
        INSERT INTO "${tblName}" (${cols.join(', ')})
        VALUES ${valueSets.map(vals => `(${sqlPlaceholders(vals)})`).join(",")}
        ON CONFLICT DO NOTHING
    `, allVals);
    if (err) return err;
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

const fixFractionalIndexCollisions = async (db: SqliteDB, changes: Change[][], sendByError: boolean) => {
    const fiChanges = changes.filter(changeSet => {
        const tblName = changeSet[0].tbl_name;
        const fiCols = db.crrColumns[tblName].filter(col => col.type === 'fractional_index');
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

    // @Cleanup
    if (sendByError) {
        console.log(`fixFractionalIndexCollisions(), Tables: `, tables);
    }

    // Try search for collisions on the same positions
    for (const [tblName, lists] of Object.entries(tables)) {
        for (const list of Object.values(lists)) {
            const parentColId = list.parentColId;
            const parentId = list.parentId;
            const posColId = list.posColId;

            const items = await db.select<any[]>(`SELECT * FROM "${tblName}" WHERE ${parentColId} = '${parentId}' ORDER BY ${posColId} ASC`, []);
            if (items.length === 0 || items.length === 1) continue; // Don't put a return here if you want to succeed in the software industry ... jsaad - 29 jan. 2025

            // @Cleanup
            if (sendByError) {
                console.log(`Items in '${tblName}'`, items);
            }

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
    if (fiCols.length === 0) return; // Table doesn't have a fractional index column
    const fiCol = fiCols.find(col => changedCols.includes(col.col_id));
    if (fiCol === undefined) return; // No change to a fraction index column

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
    
    // We need to update the position on the previous insert aswell as we keep two rows for the position!
    await db.execOrThrow(`UPDATE "crr_changes" SET value = ? WHERE tbl_name = ? AND pk = ? AND col_id = ?`, [position, tblName, pk, fiCol.col_id])

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
        const prevChange = await db.first<Change>(`SELECT * FROM "crr_changes" WHERE tbl_name = ? AND pk = ? AND col_id = ? ORDER BY created_at DESC`, [tblName, pk, positionColId]);
        if (prevChange !== undefined) {
            const changedItemIdx = items.findIndex(item => comparePk(item, pk));
            assert(changedItemIdx !== -1);
            if (typeof (prevChange.value) === 'number') prevChange.value = prevChange.value.toString();
            items[changedItemIdx][positionColId] = prevChange.value;
            items.sort((a, b) => a[pci] < b[pci] ? -1 : +1);
        }
    }

    if (afterId === "-1") { // Prepend
        if (comparePk(items[0], pk)) return items[0][pci]; // Placing head after itself
        return fracMid("[", items[0][pci]);
    }
    else if (afterId === "1") { // Append
        if (comparePk(items[items.length - 1], pk)) return items[items.length - 1][pci]; // Placing last item after itself
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

export const saveChanges = async (db: SqliteDB, changes: Change[]) => {
    if (changes.length === 0) return;
    const appliedAt = new Date().getTime();

    const valueSets = changes.map(c => [c.type, c.tbl_name, c.col_id, c.pk, c.value, c.site_id, c.created_at, appliedAt]);
    const values = valueSets.reduce((acc, vals) => [...acc, ...vals], []);
    const sql = `
        INSERT INTO "crr_changes" (type, tbl_name, col_id, pk, value, site_id, created_at, applied_at)
        VALUES ${valueSets.map(vals => `(${sqlPlaceholders(vals)})`).join(',')}
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
    if (fkRelations.length === 0) return {[rootTblName] : [rootPk]};

    const changesPerTable = Object.groupBy(changes, (change) => change.tbl_name);

    const related: {[tblName: string] : string[]} = {[rootTblName] : [rootPk]};
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

export const compactChanges = async (db: SqliteDB, changeSet: Change[]) => {
    if (changeSet.length === 0) return [];

    const tblName = changeSet[0].tbl_name;
    const pk = changeSet[0].pk;
    const changeType = changeSet[0].type;

    switch (changeType) {
        case "insert": return; // Nothing to compact
        case "update": return; // Nothing to compact. Update overrides previous update in saveChanges()
        case "delete": {
            const client = await db.first<Client>(`SELECT * FROM "crr_clients" WHERE site_id = ?`, [db.siteId]);
            if (!client) return;

            const unsyncedChanges = await db.select<Change[]>(`SELECT * FROM "crr_changes" WHERE site_id = ? AND applied_at > ?`, [db.siteId, client.last_pushed_at]);
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

            // @Improvement - The following outcommented code was an attempt to reduce the amount of updates send by removing updates
            // that were never synced. Although it lead to several problems and so is scraped in favor of @Correctness.
            //
            //
            // No insert. We mark all related unsynced changes as 'old' by setting the 'created_at' field to a negative number,
            // so that they are overridden by others changes. We don't delete them, as we might have to recreate the rows
            // at a later point if any new inserts are made.
            // const relatedChanges = getRelatedChangesFromChanges(db, unsyncedChanges, tblName, pk);
            // for (const [tblName, pks] of Object.entries(relatedChanges)) {
            //     const err = await db.exec(`
            //         UPDATE "crr_changes" 
            //         SET created_at = -1
            //         WHERE type != 'delete' AND tbl_name = ? AND site_id = ? AND applied_at > ? AND pk IN (${sqlPlaceholders(pks)})
            //     `, [tblName, db.siteId, client.last_pushed_at, ...pks]);
            //     if (err) console.error(err);
            // }
        }
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

const getChangeSets = (changes: Change[]): Change[][] => {
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
    groups.sort((a, b) => a[0].created_at - b[0].created_at);

    return groups;
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