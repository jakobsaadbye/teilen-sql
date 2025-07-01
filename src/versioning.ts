import { applyChanges, Change, generateUniqueId, insertRows, SqliteDB, isLastWriter, sqlPlaceholdersNxM, saveChanges, pkEqual } from "../index.ts";
import { getCommitGraph } from "./graph.ts";
import { createTimestamp } from "./hlc.ts";
import { applySnapshot, DocumentSnapshot } from "./snapshot.ts";
import { assert, deleteRows, flatten } from "./utils.ts";

export type Document = {
    id: string
    head: string | null
    last_pulled_at: number  // @Deprecate: This should not be used anymore. Instead use the info on the last_pulled_commit
    last_pushed_at: number
    last_pushed_commit: string | null
    last_pulled_commit: string | null
}

export type Commit = {
    id: string
    document: string
    parent: string | null   // null when commit is the first (root) in the document
    message: string
    author: string
    created_at: number
    applied_at: number
}

type CellConflict = [our: Change, their: Change];

export type RowConflict<T> = {
    document: string    // Document that the conflict happened in
    tbl_name: string    // The conflicting table
    pk: string          // Encoded primary-key of the row
    columns: string[]   // Columns that were conflicting
    their: T            // Their version of the row
}

export type ConflictChoice = "all-our" | "all-their" | [col: string, "our" | "their"][]

export type PushRequest = {
    siteId: string
    document: Document
    commits: Commit[]
    changes: Change[][]
}

export type PullRequest = {
    siteId: string
    documents: Document[]
}

type PushResponseStatus = PushResponse["status"];
export type PushResponse = {
    documentId: string
    status: "ok" | "needs-pull" | "request-contained-no-commits" | "request-malformed"
    code: number
    message?: string
    appliedAt: number
}

export type PullResult = {
    documentId: string
    merge?: Commit                  // If the pull produced a merge, the resulting merge commit will be set
    conflicts: RowConflict<any>[]   // Conflicts that resulted from a merge on manual conflict columns
    appliedChanges: Change[]        // The changes that got applied to the database during the pull
    commonAncestor?: Commit,        // The commit from which both branches diverged
    concurrentChanges: {            // The divergent changes from your and their branch. Useful if you want to construct a snapshot of how their document looked like before the pull
        our: Change[]
        their: Change[]
    }
}

export const createDocument = async (db: SqliteDB, id: string, head: string | null) => {
    const doc: Document = {
        id: id,
        head: head,
        last_pulled_at: 0,
        last_pushed_at: 0,
        last_pulled_commit: null,
        last_pushed_commit: null
    };

    await saveDocument(db, doc);

    return doc;
}

export const saveDocument = async (db: SqliteDB, doc: Document) => {
    await db.execOrThrow(`
        INSERT INTO "crr_documents" (id, head, last_pulled_at, last_pushed_at, last_pushed_commit, last_pulled_commit)
        VALUES ${sqlPlaceholdersNxM(6, 1)}
        ON CONFLICT DO UPDATE SET
            id = EXCLUDED.id,
            head = EXCLUDED.head,
            last_pulled_at = EXCLUDED.last_pulled_at,
            last_pushed_at = EXCLUDED.last_pushed_at,
            last_pushed_commit = EXCLUDED.last_pushed_commit,
            last_pulled_commit = EXCLUDED.last_pulled_commit
    `, [doc.id, doc.head, doc.last_pulled_at, doc.last_pushed_at, doc.last_pushed_commit, doc.last_pulled_commit]);
}

export const commit = async (db: SqliteDB, message: string, documentId: string): Promise<Commit | undefined> => {
    return await db.tx(async () => {

    const commit: Commit = {
        id: generateUniqueId(),
        document: "",       // Set later down
        parent: null,       // Set later down
        message: message,
        author: db.siteId,
        created_at: (new Date).getTime(),
        applied_at: 0       // Set in saveCommits
    }

    // Prevent committing if there is any conflicts present in the document
    const conflicts = await db.select<RowConflict<any>[]>(`SELECT * FROM "crr_conflicts" WHERE document = ?`, [documentId]);
    if (conflicts.length > 0) {
        // @TODO: We should probably return some kind of error here so that the application can respond to this
        console.log(`Trying to commit while there is still conflicts in the document. All conflicts needs to be resolved before comitting`);
        return;
    }

    // Update all uncommitted changes to have this commit as their version
    const uncommittedChangeCount = (await db.first<{ count: number }>(`SELECT COUNT(*) as count FROM "crr_changes" WHERE version='0' AND document = ?`, [documentId]))?.count ?? 0;
    if (uncommittedChangeCount === 0) {
        return;
    }
    await db.execOrThrow(`UPDATE "crr_changes" SET version = ? WHERE version='0' AND document = ?`, [commit.id, documentId]);


    let doc = await db.first<Document>(`SELECT * FROM "crr_documents" WHERE id = ?`, [documentId]);
    if (!doc) {
        // First commit of the document
        doc = await createDocument(db, documentId, commit.id);
        commit.parent = null;
    } else {
        commit.parent = doc.head;
    }

    commit.document = doc.id;

    await saveCommits(db, [commit]);

    // Advance head of document
    doc.head = commit.id;
    await saveDocument(db, doc);

    return commit;
    
    });
}

export const checkout = async (db: SqliteDB, targetID: string) => {
    const target = await db.first<Commit>(`SELECT * FROM "crr_commits" WHERE id = ?`, [targetID]);
    if (!target) {
        console.error(`Trying to checkout non-existing commit '${targetID}'`);
        return;
    }

    const head = await getHead(db, target.document);
    if (!head) {
        console.error(`No HEAD was found for document '${target.document}'`);
        return;
    }

    if (target.id === head.id) {
        console.log(`Ignoring checkout of HEAD`);
        return;
    }

    await setTimetravelling(db, true);

    const G = await getCommitGraph(db, target.document);
    if (!G) return;

    if (G.isAncestor(target, head)) {
        await dropDocument(db, target.document);
        const ancestorsOfTarget = G.ancestors(target.id);
        const changes = flatten(await getChangesForCommits(db, ancestorsOfTarget));
        await fastApplyChanges(db, changes);
    } else {
        // The HEAD must currenly be detached. Instead of rebuilding the state from scratch, start from current HEAD 
        // and apply the commits in-between on-top
        const ancestorsOfTarget = G.ancestors(target.id);
        const ancestorsOfHead = G.ancestors(head.id);

        // Apply the ancestors of the target commit minus those already applied from the head and ...down?
        const ancestorsDiff = ancestorsOfTarget.filter(a => {
            const alreadyApplied = ancestorsOfHead.find(b => a.id === b.id) !== undefined;
            if (alreadyApplied) return false;
            else return true;
        });

        const changes = flatten(await getChangesForCommits(db, ancestorsDiff));

        const tip = G.tip();
        if (tip && target.id === tip.id) {
            // Apply "stashed" changes ontop. We are now no longer in detached mode!
            const cgs = await db.select<Change[]>(`SELECT * FROM "crr_changes" WHERE version = '0'`, []);
            changes.push(...cgs);
        }

        await fastApplyChanges(db, changes);
    }

    await db.exec(`UPDATE "crr_documents" SET head = ? WHERE id = ?`, [target.id, target.document]);
    await setTimetravelling(db, false);
}

export const setTimetravelling = async (db: SqliteDB, value: boolean) => {
    await db.exec(`UPDATE "crr_temp" SET time_travelling = ?`, [value ? 1 : 0]);
}

export const discardChanges = async (db: SqliteDB, documentId: string) => {

    // Reapply all changes from history except the uncomitted changes
    // @Speed - We could probably be smarter about this. Something along the lines of reapply the inverse of the changes to discard.
    // One challenge with reverting state from these snapshots, is that we potentially have to look back to the very first commit
    // when figuring out what the previous snapshopt looked like
    const changes = await db.select<Change[]>(`SELECT * FROM "crr_changes" WHERE document = ? AND version != '0' ORDER BY created_at ASC`, [documentId]);

    await setTimetravelling(db, true);
    const root = new DocumentSnapshot(documentId);
    const lastSnapshot = root.applyChanges(changes);
    await applySnapshot(db, lastSnapshot);
    await setTimetravelling(db, false);

    // Remove uncommitted changes
    await db.execOrThrow(`DELETE FROM "crr_changes" WHERE version = '0' AND document = ?`, [documentId]);
}



const fastApplyChanges = async (db: SqliteDB, changes: Change[]) => {
    const sorted = changes.toSorted((a, b) => a < b ? -1 : +1);

    const changesPerTable = Object.groupBy(sorted, (c => c.tbl_name));
    for (const [table, tableChanges] of Object.entries(changesPerTable)) {
        const changesPerRow = Object.groupBy(tableChanges!, (c => c.pk));

        // Recreate all the rows for this table
        const pkCols = db.pks[table];
        const rows = [];
        rowCreation: for (const [pk, rowChanges] of Object.entries(changesPerRow)) {
            const row = {} as any;

            // Insert primary-key columns
            const pks = pk.split('|');
            if (pkCols.length !== pks.length) {
                console.error(`Mismatch in number of primary-keys found in change and what was expected for table '${table}'. Primary-keys in table: ${pkCols}, primary-keys in change: ${pks}`);
                continue;
            }
            for (let i = 0; i < pkCols.length; i++) {
                const pkCol = pkCols[i];
                const pkValue = pks[i];
                row[pkCol] = pkValue;
            }

            // Insert updated values
            for (const change of rowChanges!) {
                if (change.type === "delete" && change.value % 2 === 1) {
                    continue rowCreation;
                }
                row[change.col_id] = change.value; // Recreation of the row assummes that the changes are in order such that an update takes precedence over a past insert
            }

            rows.push(row);

            // @Speed @TODO - We are doing single row inserts, only because
            //        we might not have the same amount of values and keys in each row which 
            //        insertRows assumes
            await insertRows(db, table, [row]);
        }

        // await insertRows(db, table, rows);
    }
}

export const preparePushCommits = async (db: SqliteDB, documentId = "main") => {
    let doc = await db.first<Document>(`SELECT * FROM "crr_documents" WHERE id = ?`, [documentId]);
    if (!doc) {
        doc = await createDocument(db, documentId, null);
    }

    const lastPushedCommit = await db.first<Commit>(`SELECT * FROM "crr_commits" WHERE id = (SELECT last_pushed_commit FROM "crr_documents" WHERE id = ?)`, [doc.id]);

    const nonPushedCommits = await db.select<Commit[]>(`SELECT * FROM "crr_commits" WHERE created_at > ? AND author = ? AND document = ? ORDER BY created_at ASC`, [lastPushedCommit?.created_at ?? 0, db.siteId, documentId]);
    const nonPushedChanges = await getChangesForCommits(db, nonPushedCommits);

    const data: PushRequest = {
        siteId: db.siteId,
        document: doc!,
        commits: nonPushedCommits,
        changes: nonPushedChanges
    }

    return data;
}

export const preparePullCommits = async (db: SqliteDB, documentId = "main"): Promise<PullRequest> => {
    const documents = await db.select<Document[]>(`SELECT * FROM "crr_documents"`, []);
    return {
        siteId: db.siteId,
        documents
    };
}

export const receivePushCommits = async (db: SqliteDB, their: PushRequest): Promise<PushResponse> => {
    const docId = their.document.id;

    const Response = (status: PushResponseStatus, code: number, message?: string, appliedAt = -1): PushResponse => {
        return {
            documentId: docId,
            status,
            code,
            message,
            appliedAt,
        };
    }

    let our = await db.first<Document>(`SELECT * FROM "crr_documents" WHERE id = ?`, [docId]);
    if (!our) {
        // Create the document as we haven't seen it before
        our = await createDocument(db, docId, null);
    };

    // Check if pusher has all the latest commits
    if (our.head !== their.document.last_pulled_commit) {
        return Response("needs-pull", 200, "Client needs pull");
    }

    if (their.commits.length === 0) {
        return Response("request-contained-no-commits", 200, "No commits to be pushed");
    }

    if (their.changes.length !== their.commits.length) {
        return Response("request-malformed", 400, "Mismatch in the number of changesets and commits");
    }

    // Apply each commit on-top
    for (let i = 0; i < their.commits.length; i++) {
        const changes = their.changes[i];
        await saveChanges(db, changes);
        // await applyChanges(db, changes);
    }

    const savedAt = await saveCommits(db, their.commits);

    // Update our HEAD
    await db.exec(`UPDATE "crr_documents" SET head = ? WHERE id = ?`, [their.commits[their.commits.length - 1].id, docId]);

    return Response("ok", 200, "", savedAt);
}

type PullResponseStatus = PullResponse["status"];
export type PullResponse = {
    status: "ok",
    code: number,
    packets: PullPacket[]
}

type PullPacket = {
    documentId: string
    commits: Commit[]
    changes: Change[][]
}


export const receivePullCommits = async (db: SqliteDB, pull: PullRequest): Promise<PullResponse> => {

    const ourDocuments = await db.select<Document[]>(`SELECT * FROM "crr_documents"`, []);
    const theirDocuments = pull.documents;

    const packets: PullPacket[] = [];
    for (const their of theirDocuments) {
        const docId = their.id;

        // Do we have the document?
        const our = ourDocuments.find(doc => doc.id === docId);
        if (!our) {
            // Nope @NOTE: Should we create the document???
            packets.push({ documentId: docId, commits: [], changes: [] });
            continue;
        }

        // Have they pulled before?
        if (!their.last_pulled_commit) {
            // Nope, send them all commits
            const commits = await db.select<Commit[]>(`SELECT * FROM "crr_commits" WHERE document = ? ORDER BY created_at ASC`, [docId]);
            const changes = await getChangesForCommits(db, commits);
            packets.push({ documentId: docId, commits, changes });
            continue;
        }

        // Give back the unseen commits (the difference in commits between our head and their last pulled commit)
        const G = await db.getCommitGraph(docId);
        if (!G) continue;

        const diff = G.diff(our.head!, their.last_pulled_commit);

        const unseenCommits = diff;
        const unseenChanges = await getChangesForCommits(db, unseenCommits);

        console.log(`Their last seen commit = ${their.last_pulled_commit}`);
        for (const c of unseenCommits) {
            console.log(`Sending: ${c.message}`);
        }

        packets.push({ documentId: docId, commits: unseenCommits, changes: unseenChanges });
    }

    // Include documents that they have not seen
    const unseenDocuments: Document[] = [];
    for (const ourDoc of ourDocuments) {
        const exists = theirDocuments.find(doc => doc.id === ourDoc.id);
        if (!exists) {
            unseenDocuments.push(ourDoc);
        }
    }
    for (const doc of unseenDocuments) {
        const commits = await db.select<Commit[]>(`SELECT * FROM "crr_commits" WHERE document = ? ORDER BY created_at ASC`, [doc.id]);
        const changes = await getChangesForCommits(db, commits);
        packets.push({ documentId: doc.id, commits, changes });
    }

    return {
        status: "ok",
        code: 200,
        packets: packets
    }
}

export const getChangesForCommits = async (db: SqliteDB, commits: Commit[]) => {
    const changes: Change[][] = [];
    for (const commit of commits) {
        const cgs = await db.select<Change[]>(`SELECT * FROM "crr_changes" WHERE version = ?`, [commit.id]);
        changes.push(cgs);
    }

    return changes;
}

export const getHead = async (db: SqliteDB, documentId = "main") => {
    return db.first<Commit>(`SELECT * FROM "crr_commits" WHERE id = (SELECT head FROM "crr_documents" WHERE id = ?)`, [documentId]);
}

export const isMerge = (commit: Commit) => {
    return commit.parent !== null && commit.parent.split("|").length === 2;
}

export const applyPull = async (db: SqliteDB, pull: PullResponse): Promise<PullResult[]> => {
    const packets = pull.packets;

    const results: PullResult[] = [];
    for (const packet of packets) {
        const result = await applyPullPacket(db, packet);
        if (result) {
            results.push(result);
        }
    }

    return results;
}

const applyPullPacket = async (db: SqliteDB, their: PullPacket): Promise<PullResult | undefined> => {
    const theirCommits = their.commits;
    let theirChanges = their.changes.reduce((result, changes) => { result.push(...changes); return result }, [] as Change[]);

    const docId = their.documentId;

    // Create the document if we don't have it
    let doc = await db.first<Document>(`SELECT * FROM "crr_documents" WHERE id = ?`, [docId]);
    if (!doc) {
        doc = await createDocument(db, docId, null);
    }

    if (theirCommits.length === 0 || theirChanges.length === 0) return;

    // Scenarios for pull
    //    1. I have decendent commits
    //       Merge the divergent changes and create a new merge commit
    //
    //    2. I have no decendent commits
    //       Apply their commits and fast-forward
    //
    let ourCommits: Commit[] = [];

    // Check if they have already incorporated our commits through a merge.
    // in that case, we can simply accept their changes.
    let theyHaveIncorporatedOurChanges = false;
    const ourHead = await getHead(db, docId);
    if (ourHead) {
        for (const commit of theirCommits) {
            if (isMerge(commit)) {
                const [aId, bId] = commit.parent!.split("|") as [string, string];
                if (ourHead.id === aId || ourHead.id === bId) {
                    // They have !! Just apply their commits ontop of ours without a new merge
                    ourCommits = [];
                    theyHaveIncorporatedOurChanges = true;
                }
            }
        }
    }

    // 1. Is this the first pull ever on this document?
    // 2. 

    // Find a common ancestor commit from which we diverged
    let commonAncestor: Commit | undefined;
    if (!theyHaveIncorporatedOurChanges) {
        const commonAncestorId = theirCommits[0].parent;
        if (commonAncestorId === null) {
            // We have no common ancestor other than the root. All our commits to this document are thus divergent and needs to be merged
            const allCommits = await db.select<Commit[]>(`SELECT * FROM "crr_commits" WHERE document = ? AND created_at > 0 ORDER BY created_at ASC`, [docId]);
            ourCommits = allCommits;
        } else {
            // We do have a common ancestor. The commits we have made after the common ancestor are divergent commits that needs to be merged
            commonAncestor = await db.first<Commit>(`SELECT * FROM "crr_commits" WHERE document = ? AND id = ?`, [docId, commonAncestorId]);
            assert(commonAncestor);
            ourCommits = await db.select<Commit[]>(`SELECT * FROM "crr_commits" WHERE document = ? AND created_at > ? ORDER BY created_at ASC`, [docId, commonAncestor.created_at]);
        }
    }

    const ourCommitChanges = flatten(await getChangesForCommits(db, ourCommits));
    const ourWorkingSet = await db.getUncommittedChanges(docId);
    let ourChanges = [...ourCommitChanges, ...ourWorkingSet];

    // "Deduplicate" both our changes and their changes to not care about different versions of
    // the same cell changes. We only care about the latest one of those
    ourChanges = deduplicateChanges(ourChanges);
    theirChanges = deduplicateChanges(theirChanges);


    const collides = (their: Change, ours: Change[]) => {
        return ours.find(our => {
            return (
                our.pk === their.pk &&
                our.tbl_name === their.tbl_name &&
                our.col_id === their.col_id
            );
        })
    }

    const isManualConflictColumn = (db: SqliteDB, table: string, column: string) => {
        return db.crrColumns[table].find(col => col.col_id === column && col.manual_conflict === 1) !== undefined;
    }

    if (ourChanges.length > 0) {
        // Merge their changes with ours.
        //
        // Merging happens through column-wise last-writer wins (auto-resolve), unless its a manual-conflict column.
        // In that case we add those to the conflicts table to be resolved by the user.

        const accepted: Change[] = [];
        const merged: Change[] = [];
        const potentialManualConflicts: CellConflict[] = [];
        let i = 0;
        for (; i < theirChanges.length; i++) {
            const theirChange = theirChanges[i];

            const collided = collides(theirChange, ourChanges);
            if (collided) {
                const ourChange = collided;

                const autoResolve = !isManualConflictColumn(db, ourChange.tbl_name, ourChange.col_id);
                if (autoResolve) {
                    if (isLastWriter(theirChange, ourChange)) {
                        accepted.push(theirChange);
                    }
                } else {
                    potentialManualConflicts.push([ourChange, theirChange]);
                }
            } else {
                // Should we still check for lww if the values are the same? Right now we just pick
                // their change to add to merge.
                accepted.push(theirChange);
            }
            
            merged.push(theirChange);
        }

        // Check if the potential cell collisons on manual conflict columns actually differ
        // from a common base point. This table describes how a cell collision
        // gets "resolved"
        //
        //  Base    Our     Their      Conflict     Resolve
        //   0       1        1           No          LWW 
        //   0       1        0           No          Our 
        //   0       0        1           No          Their 
        //   0       1        2           Yes         Manual 
        const rowConflicts: RowConflict<any>[] = [];
        if (potentialManualConflicts.length > 0) {

            // @Speed - This is very slow to do. If we should even do this at all,
            // then atleast just recreate the rows that are actually conflicting, instead
            // of right now, recreating their entire document, just to pull out a few rows.
            const baseSnapshot = await db.getDocumentSnapshot(commonAncestor);
            const theirSnapshot = baseSnapshot.applyChanges(theirChanges);

            for (const [our, their] of potentialManualConflicts) {
                const docId = our.document;
                const table = our.tbl_name;
                const pk = our.pk;
                const col = our.col_id;

                const baseValue = baseSnapshot.getRow<any>(table, pk)[col];
                const ourValue = our.value;
                const theirValue = their.value;

                if (ourValue === theirValue) {
                    if (isLastWriter(our, their))
                        accepted.push(our);
                    else accepted.push(their);
                }
                else if (baseValue !== ourValue && baseValue === theirValue) accepted.push(our);
                else if (baseValue === ourValue && baseValue !== theirValue) accepted.push(their);
                else if (baseValue !== ourValue && baseValue !== theirValue) {
                    const conflict = rowConflicts.find(c => c.pk === pk && c.tbl_name === table && !c.columns.includes(col));
                    if (!conflict) {
                        rowConflicts.push({
                            document: docId,
                            tbl_name: table,
                            pk: pk,
                            columns: [col],
                            their: theirSnapshot.getRow(table, pk)
                        });
                    } else {
                        conflict.columns.push(col);
                    }
                }
                else {
                    console.error(`**BUG**: Unhandled case (base, our, their)`, baseValue, ourValue, theirValue);
                }
            }

            await saveRowConflicts(db, rowConflicts);
        }


        // Any leftovers of our changes gets pushed onto the merged changes
        if (i < ourChanges.length) {
            const leftover = ourChanges.slice(i, -1);
            merged.push(...leftover);
        }

        // Create the merged commit
        const theirLastCommit = theirCommits[theirCommits.length - 1];
        const ourLastCommit = ourCommits[ourCommits.length - 1];

        const merge: Commit = {
            id: generateUniqueId(),
            document: docId,
            parent: `${theirLastCommit.id}|${ourLastCommit.id}`,
            message: `Merge of '${theirLastCommit.message.slice(0, 10)}' and '${ourLastCommit.message.slice(0, 10)}'`,
            author: db.siteId,
            created_at: (new Date).getTime(),
            applied_at: 0 // Set later in saveCommits
        }

        // Assign the merged changes to this commit
        for (const change of accepted) {
            change.version = merge.id;
        }

        // Apply all the accepted changes
        const appliedChanges = await applyChanges(db, accepted);

        // Save all of their changes, also those that are not accepted
        await saveChanges(db, theirChanges);

        // Save commits and merged commit
        await saveCommits(db, [merge, ...theirCommits]);

        // Update the document metadata
        const now = (new Date).getTime();
        await db.execOrThrow(`UPDATE "crr_documents" SET head = ?, last_pulled_commit = ?, last_pulled_at = ? WHERE id = ?`, [merge.id, theirLastCommit.id, now, docId]);

        return {
            documentId: docId,
            merge,
            conflicts: rowConflicts,
            appliedChanges,
            commonAncestor: commonAncestor,
            concurrentChanges: {
                our: ourChanges,
                their: theirChanges
            }
        };
    }
    else {
        const appliedChanges = await applyChanges(db, theirChanges);
        await saveChanges(db, theirChanges);
        await saveCommits(db, theirCommits);

        const theirLastCommit = theirCommits[theirCommits.length - 1];

        // Update the document metadata
        const now = (new Date).getTime();
        await db.execOrThrow(`UPDATE "crr_documents" SET head = ?, last_pulled_commit = ?, last_pulled_at = ? WHERE id = ?`, [theirLastCommit.id, theirLastCommit.id, now, docId]);

        return {
            documentId: docId,
            conflicts: [],
            appliedChanges,
            concurrentChanges: {
                our: ourChanges, // empty
                their: theirChanges,
            }
        };
    }
}

export const deduplicateChanges = (changes: Change[]) => {
    const result: Change[] = [];

    for (const a of changes) {
        const existingIndex = result.findIndex(b => {
            return (
                a.pk === b.pk &&
                a.tbl_name === b.tbl_name &&
                a.col_id === b.col_id
            )
        });
        if (existingIndex === -1) {
            result.push(a);
        } else {
            const b = result[existingIndex];
            if (a.created_at > b.created_at) {
                result[existingIndex] = a;
            }
        }
    }

    return result;
}

/** Resolve a single conflict */
export const resolveConflict = async (db: SqliteDB, table: string, pk: string, documentId = "main", choice: ConflictChoice) => {

    // @NOTE:
    // Resolving a conflict, equates to duplicating the chosen changes and make them part of the
    // current merge commit that was stopped due to manual conflicts.
    // We prevent clients from pushing any commit until the conflicts are resolved and made part of 
    // the merge commit for this reason. Duplicating the change with a new timestamp ensures that other clients
    // will automatically accept the chosen change as the merge indicates that the pushing clients have atleast seen
    // the receiving clients change.

    const conflictRow = await db.first<RowConflict<any>>(`SELECT * FROM "crr_conflicts" WHERE document = ? AND tbl_name = ? AND pk = ?`, [documentId, table, pk]);
    if (!conflictRow) {
        console.warn(`Conflict does not exist`);
        return;
    }
    const conflict = deserializeConflict<any>(conflictRow);

    const merge = await getHead(db, documentId);
    if (!merge || !isMerge(merge)) {
        throw new Error("**BUG** Expected that the current HEAD is pointing at the current merge");
    }

    // Compare their row against how the row currently looks
    const currentRow = await db.first<any>(`SELECT * FROM "${table}" WHERE ${pkEqual(db, table, pk)}`, []);

    if (!currentRow) {
        // @TODO: Handle cases with deletes
        throw new Error("NOT IMPLEMENTED");
    }

    // Generate duplicate cell changes with a new timestamp
    let columnsToResolve: [col: string, winner: "our" | "their"][] = [];
    if (choice === "all-their") {
        columnsToResolve = Object.keys(conflict.their).map(col => [col, "their"]);
    } else if (choice === "all-our") {
        columnsToResolve = Object.keys(currentRow).map(col => [col, "our"]);
    } else {
        columnsToResolve = choice;
    }

    const winningChanges: Change[] = [];
    const now = await createTimestamp(db);
    for (const [column, winner] of columnsToResolve) {
        const change: Change = {
            type: "update",     // @TODO: I guess we can technically also conflict with deletions?
            tbl_name: table,
            col_id: column,
            pk: pk,
            value: winner === "our" ? currentRow[column] : conflict.their[column],
            site_id: db.siteId,
            created_at: now,
            applied_at: 0,      // Set in applyChanges
            version: merge.id,
            document: documentId,
        }
        winningChanges.push(change);
    }
    await applyChanges(db, winningChanges);


    const conflictingColumns = conflict.columns;
    const newConflictingColumns = conflictingColumns.filter(col => columnsToResolve.find(([x, _]) => col === x) === undefined);
    if (newConflictingColumns.length === 0) {
        // All conflicts are resolved on this row
        await db.exec(`DELETE FROM "crr_conflicts" WHERE document = ? AND tbl_name = ? AND pk = ?`, [documentId, table, pk]);
    } else {
        // Update the list of conflicting columns to the ones still not resolved
        const serializedColumns = JSON.stringify(newConflictingColumns);
        await db.exec(`UPDATE "crr_conflicts" SET columns = ? WHERE document = ? AND tbl_name = ? AND pk = ?`, [serializedColumns, documentId, table, pk]);
    }
}

const deserializeConflict = <T>(conflict: RowConflict<string>): RowConflict<T> => {
    return {
        ...conflict,
        columns: JSON.parse((conflict.columns as unknown as string)),
        their: JSON.parse(conflict.their),
    }
}

/** Gets conflicts for a specific table and document */
export const getConflicts = async <T>(db: SqliteDB, table: string, documentId = "main") => {
    const conflicts = await db.select<RowConflict<string>[]>(`SELECT * FROM "crr_conflicts" WHERE document = ? AND tbl_name = ?`, [documentId, table]);

    const result = conflicts.map(deserializeConflict<T>);
    return result;
}


type RowChange = {
    pk: string
    columns: string[]   // inserted = Empty, updated = The updated columns, deleted = Empty  
    row: any
}

type TableDiff = {
    tblName: string
    inserted: RowChange[]
    updated: RowChange[]
    deleted: RowChange[]
}

// @WIP @TODO @Cleanup - Needs fixing or be removed???
export const getDiff = async (db: SqliteDB, a: Commit, b: Commit) => {

}


/** Gets the non-pushed commit count for a given document */
export const getPushCount = async (db: SqliteDB, documentId = "main") => {
    const doc = await db.first<Document>(`SELECT * FROM "crr_documents" WHERE id = ?`, [documentId]);
    if (!doc) {
        console.error(`Document '${documentId}' not found. Make sure to pass a documentId to getPushCount()`);
        return 0;
    }

    if (!doc.last_pushed_commit) {
        const unpushedCommits = await db.select<Commit[]>(`SELECT * FROM "crr_commits" WHERE document = ? AND author = ?`, [doc.id, db.siteId]);
        return unpushedCommits.length;
    } else {
        const unpushedCommits = await db.select<Commit[]>(`
            SELECT * FROM "crr_commits" WHERE document = ? AND author = ? AND created_at > (SELECT created_at FROM "crr_commits" WHERE document = ? AND id = ?)
        `, [doc.id, db.siteId, doc.id, doc.last_pushed_commit ?? 0]);
        return unpushedCommits.length;
    }
}

const saveRowConflicts = async (db: SqliteDB, conflicts: RowConflict<any>[]) => {
    if (conflicts.length === 0) return;

    const columns = Object.keys(conflicts[0]);

    const serialize = (c: RowConflict<any>) => {
        return [
            c.document,
            c.tbl_name,
            c.pk,
            JSON.stringify(c.columns),
            JSON.stringify(c.their),
        ];
    }

    const values = flatten(conflicts.map(serialize));

    await db.execOrThrow(`
        INSERT OR IGNORE INTO "crr_conflicts" (${columns.join(',')})
        VALUES ${sqlPlaceholdersNxM(columns.length, conflicts.length)}
    `, values);
}

/**
 * @returns The time at which the commits are saved
 */
const saveCommits = async (db: SqliteDB, commits: Commit[]) => {
    const now = (new Date).getTime();

    if (commits.length === 0) return now;

    // Set the commits applied_at field
    for (const commit of commits) {
        commit.applied_at = now;
    }

    const columns = Object.keys(commits[0]);
    const values = commits.map(commit => columns.map(col => commit[col])).reduce((result, vals) => { result.push(...vals); return result }, [] as any[]);

    const updateStr = columns
        .filter(col => col !== "id") // Exlucde pks
        .map(col => `${col} = EXCLUDED.${col}`)
        .join(',\n\t\t\t');

    await db.execOrThrow(`
        INSERT INTO "crr_commits" (${columns.join(',')})
        VALUES ${sqlPlaceholdersNxM(columns.length, commits.length)}
        ON CONFLICT DO UPDATE SET
            ${updateStr}
    `, values);

    return now;
}

export const dropDocument = async (db: SqliteDB, documentId: string) => {
    const docRows = await db.select<{ tbl_name: string, pk: string }[]>(`SELECT tbl_name, pk FROM "crr_changes" WHERE document = ? GROUP BY pk`, [documentId]);

    const rowsPerTable = Object.groupBy(docRows, (row) => row.tbl_name);
    for (const [tblName, rows] of Object.entries(rowsPerTable)) {
        const pks = rows!.map(row => row.pk);
        await deleteRows(db, tblName, pks);
    }
}