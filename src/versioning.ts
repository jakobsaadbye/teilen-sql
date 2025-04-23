import { applyChanges, Change, generateUniqueId, insertRows, SqliteDB, isLastWriter, sqlPlaceholdersNxM, pksEqual, pkEncodingOfRow, saveChanges } from "../index.ts";
import { assert, deleteRows, flatten } from "./utils.ts";

export type Document = {
    id: string
    head: string | null
    last_pulled_at: number
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
    columns: string[]   // Columns that are conflicting
    base: T             // The row as it was before diverging
    our: T              // Your version of the row
    their: T            // Their version of the row
}

export type ConflictChoice = "our" | "their";

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
    merge?: Commit              // If the pull produced a merge, this field will be set
    conflicts: RowConflict<any>[]
    manualConflicts: RowConflict<any>[]
    appliedChanges: Change[]    // The changes that got applied to the database during the pull
}

const createDocument = async (db: SqliteDB, id: string, head: string | null) => {
    const doc: Document = {
        id: id,
        head: head,
        last_pulled_at: 0,
        last_pulled_commit: null,
        last_pushed_commit: null
    };

    await db.execOrThrow(`INSERT INTO "crr_documents" (id, head) VALUES (?, ?)`, [doc.id, doc.head]);

    return doc;
}

export const commit = async (db: SqliteDB, message: string, documentId: string): Promise<Commit | undefined> => {

    const commit: Commit = {
        id: generateUniqueId(),
        document: "",       // Set later down
        parent: null,       // Set later down
        message: message,
        author: db.siteId,
        created_at: (new Date).getTime(),
        applied_at: 0       // Set in saveCommits
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
    await db.execOrThrow(`UPDATE "crr_documents" SET head = ? WHERE id = ?`, [commit.id, doc.id]);

    return commit;
}

export const checkout = async (db: SqliteDB, commitId: string) => {
    const commit = await db.first<Commit>(`SELECT * FROM "crr_commits" WHERE id = ?`, [commitId]);
    if (!commit) {
        console.error(`Trying to checkout non-existing commit '${commitId}'`);
        return;
    }

    const head = await db.first<Commit>(`
        SELECT * FROM "crr_commits" 
        WHERE id = (SELECT head FROM "crr_documents" WHERE id = ?)
    `, [commit.document]);
    if (!head) {
        console.error(`No HEAD was found for document '${commit.document}'`);
        return;
    }

    if (commit.id === head.id) {
        console.log(`Ignoring checkout of HEAD`);
        return;
    }

    await db.exec(`UPDATE "crr_temp" SET time_travelling = 1`, []);
    if (commit.created_at < head.created_at) {
        await dropDocument(db, commit.document);
        const backwardCommits = await db.select<Commit[]>(`SELECT * FROM "crr_commits" WHERE created_at <= ? AND document = ?`, [commit.created_at, commit.document]);
        for (const c of backwardCommits) {
            const changes = await db.select<Change[]>(`SELECT * FROM "crr_changes" WHERE version = ?`, [c.id]);
            await fastApplyChanges(db, changes);
        }
    } else {
        // The HEAD must currenly be detached. Here we can simply apply the changes in-between ontop
        const forwardCommits = await db.select<Commit[]>(`SELECT * FROM "crr_commits" WHERE created_at > ? AND created_at <= ? AND document = ?`, [head.created_at, commit.created_at, commit.document]);

        const changes: Change[] = [];
        for (const c of forwardCommits) {
            const cgs = await db.select<Change[]>(`SELECT * FROM "crr_changes" WHERE version = ?`, [c.id]);
            changes.push(...cgs);
        }

        const lastCommit = await db.first<Commit>(`SELECT *, MAX(created_at) FROM "crr_commits" `, []);
        if (commit.id === lastCommit!.id) {
            // Apply "stashed" changes ontop. We are now no longer in detached mode!
            const cgs = await db.select<Change[]>(`SELECT * FROM "crr_changes" WHERE version = '0'`, []);
            changes.push(...cgs);
        }

        await fastApplyChanges(db, changes);
    }


    await db.exec(`UPDATE "crr_documents" SET head = ? WHERE id = ?`, [commit.id, commit.document]);
    await db.exec(`UPDATE "crr_temp" SET time_travelling = 0`, []);
}

export const revert = async (db: SqliteDB) => {
    // @TODO
}



type CommitGraph = {
    head: CommitNode
    roots: CommitNode[]
    nodes: CommitNode[]
}

type CommitNode = {
    commit: Commit
    parents: CommitNode[]
    children: CommitNode[]
}

const newCommitNode = (commit: Commit, parents: CommitNode[], children: CommitNode[]): CommitNode => {
    return {
        commit,
        parents,
        children,
    }
}

export const getCommitGraph = async (db: SqliteDB, documentId = "main") => {
    const head = await getHead(db, documentId);
    if (!head) return;

    const commits = await db.select<Commit[]>(`SELECT * FROM "crr_commits" WHERE document = ? ORDER BY created_at`, [documentId]);

    // Stitch the graph together by following the parent relations
    const nodes: CommitNode[] = [];
    const roots: CommitNode[] = [];

    // Attach each commit to a node
    for (const commit of commits) {
        const node = newCommitNode(commit, [], []);
        nodes.push(node);
    }

    let headNode;
    for (const node of nodes) {
        const commit = node.commit;

        if (commit.id === head.id) {
            headNode = node;
        }

        const parentId = commit.parent;
        if (!parentId) {
            node.parents = [];
            roots.push(node);
            continue;
        }

        if (isMerge(commit)) {
            // Commit will have two parents
            assert(commit.parent);
            const [parentAId, parentBId] = commit.parent.split("|");
            assert(parentAId && parentBId);

            const parentA = nodes.find(node => node.commit.id === parentAId);
            const parentB = nodes.find(node => node.commit.id === parentBId);
            assert(parentA && parentB);

            parentA.children.push(node);
            parentB.children.push(node);
            node.parents = [parentA, parentB];
        } else {
            const parent = nodes.find(node => node.commit.id === parentId);
            assert(parent);

            parent.children.push(node);
            node.parents = [parent];
        }
    }

    assert(roots.length > 0);
    assert(headNode);

    const G: CommitGraph = {
        head: headNode,
        roots,
        nodes,
    };

    return G;
}

export const printCommitGraph = (G: CommitGraph) => {

    const root = G.roots[0];
    let node = G.head;
    let timelines: (CommitNode | null)[] = [node];

    const generateBars = () => {
        const bars = [];
        for (let i = 0; i < timelines.length; i++) bars.push("| ");
        return bars.join('');
    }

    const getNext = (): [CommitNode, number] => {
        // Pick the commit with the highest timestamp
        let maxCreatedAt = -Infinity, maxNode = null, maxIndex = -1;
        for (let i = 0; i < timelines.length; i++) {
            const node = timelines[i];
            if (node && node.commit.created_at > maxCreatedAt) {
                maxNode = node;
                maxIndex = i;
                maxCreatedAt = node.commit.created_at;
            }
        }
        assert(maxNode && maxIndex !== -1);
        return [maxNode, maxIndex];
    }

    const printCommitLine = (node: CommitNode, branch: number) => {
        let symbols = "";
        for (let i = 0; i < timelines.length; i++) {
            if (i === branch) {
                symbols += "o  ";
            } else {
                symbols += "|  ";
            }
        }

        const line = `${symbols} ${node.commit.message}`;
        console.log(line);
    }

    const printJoin = (into: number, from: number) => {
        let symbols = "";
        for (let i = 0; i < timelines.length + 1; i++) {
            if (i === from) {
                if (from > into) {
                    symbols += "/  ";
                } else {
                    symbols += "\\  ";
                }
            } else {
                symbols += "| ";
            }
        }
        const line = `${symbols}`;
        console.log(line);
    }

    const printIntermediateLine = () => {
        let symbols = "";
        for (let i = 0; i < timelines.length; i++) {
            symbols += "|  ";
        }
        const line = `${symbols}`;
        console.log(line);
    }

    let joinBranch: number | null = null;

    while (true) {
        const [node, branch] = getNext();

        if (node.parents.length === 2) {
            // Merge
            printCommitLine(node, branch);
            console.log(`| \\`);

            // Split the branches
            timelines.push(node.parents[1]);
            timelines[branch] = node.parents[0];

        } else if (node.parents.length === 1) {
            // Normal
            printCommitLine(node, branch);
            printIntermediateLine();

            const parent = node.parents[0];
            timelines[branch] = parent;

            if (parent.children.length === 2) {
                // We've hit a common ancestor. Proceed on the other branch or join the branches;
                timelines[branch] = null;

                // Join the branches?
                let joinTimelines = true;
                for (const node of timelines) {
                    if (node) joinTimelines = false;
                }
                if (joinTimelines) {
                    joinBranch = timelines.length - 1;
                    timelines.pop();
                    timelines[branch] = parent;
                }
            }


        } else {
            // Root
            if (joinBranch) {
                printJoin(branch, joinBranch);
            }
            printCommitLine(node, branch);
            break;
        }
    }
}

export const discardChanges = async (db: SqliteDB, documentId: string) => {
    await db.exec(`UPDATE "crr_temp" SET time_travelling = 1`, []);
    await dropDocument(db, documentId);

    // Remove uncommitted changes
    await db.exec(`DELETE FROM "crr_changes" WHERE version = '0' AND document = ?`, [documentId]);

    // Reapply all changes from history
    // @Speed - This probably needs to be reconsidered ...
    //          One way we could speed this up would be to group each set of changes
    //          into their respective tables, turning foreign-keys off, then doing bulk inserts
    //          of rows
    const changes = await db.select<Change[]>(`SELECT * FROM "crr_changes" WHERE document = ? ORDER BY created_at ASC`, [documentId]);

    await fastApplyChanges(db, changes);
    await db.exec(`UPDATE "crr_temp" SET time_travelling = 0`, []);
}

const fastApplyChanges = async (db: SqliteDB, changes: Change[]) => {
    const changesPerTable = Object.groupBy(changes, (c => c.tbl_name));
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
                if (change.type === "delete" && change.value === 1) {
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
        // @Cleanup - This probably should be an error??
        doc = { id: documentId, head: null } as Document;
        await db.exec(`INSERT INTO "crr_documents" (id, head) VALUES (?, ?)`, [doc!.id, null]);
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
        await applyChanges(db, changes);
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
    for (const theirDoc of theirDocuments) {
        const docId = theirDoc.id;

        // Do we have the document?
        const our = ourDocuments.find(doc => doc.id === docId);
        if (!our) {
            // Nope @NOTE: Should we create the document???
            packets.push({ documentId: docId, commits: [], changes: [] });
            continue;
        }

        // Have they pulled before?
        if (!theirDoc.last_pulled_commit) {
            // Nope, send them all commits
            const commits = await db.select<Commit[]>(`SELECT * FROM "crr_commits" WHERE document = ? ORDER BY created_at ASC`, [docId]);
            const changes = await getChangesForCommits(db, commits);
            packets.push({ documentId: docId, commits, changes });
            continue;
        }

        // Give the commits they have not seen (those with applied_at > time of last pull)
        const unseenCommits = await db.select<Commit[]>(`SELECT * FROM "crr_commits" WHERE document = ? AND applied_at > ? ORDER BY created_at ASC`, [docId, theirDoc.last_pulled_at]);
        const unseenChanges = await getChangesForCommits(db, unseenCommits);

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

const getChangesForCommits = async (db: SqliteDB, commits: Commit[]) => {
    const changes: Change[][] = [];
    for (const commit of commits) {
        const cgs = await db.select<Change[]>(`SELECT * FROM "crr_changes" WHERE version = ?`, [commit.id]);
        changes.push(cgs);
    }

    return changes;
}

const getHead = async (db: SqliteDB, documentId = "main") => {
    return db.first<Commit>(`SELECT * FROM "crr_commits" WHERE id = (SELECT head FROM "crr_documents" WHERE id = ?)`, [documentId]);
}

const isMerge = (commit: Commit) => {
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
    const theirChanges = their.changes.reduce((result, changes) => { result.push(...changes); return result }, [] as Change[]);

    const docId = their.documentId;

    // Create the document if we don't have it
    const doc = await db.first<Document>(`SELECT * FROM "crr_documents" WHERE id = ?`, [docId]);
    if (!doc) {
        await db.exec(`INSERT INTO "crr_documents" (id, head) VALUES (?, ?)`, [docId, null]);
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

    // Check if they have already incorporated our commits through a merge ...
    // @NOTE: Incorporated here means that the pushing client had knowledge of our
    // commit and have merged our changes
    let incorporated = false;
    const ourHead = await getHead(db, docId);
    if (ourHead) {
        for (const commit of theirCommits) {
            if (isMerge(commit)) {
                const [aId, bId] = commit.parent!.split("|") as [string, string];
                if (ourHead.id === aId || ourHead.id === bId) {
                    // They have !! Just apply their commits ontop of ours without a new merge
                    ourCommits = [];
                    incorporated = true;
                }
            }
        }
    }

    let commonAncestor: Commit | undefined;
    if (!incorporated) {
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


    if (ourCommits.length > 0) {
        // The two branches have diverged
        assert(commonAncestor || docId === "main"); // Its only possible to not have a common ancestor here if the entire database is versioned a.k.a docId = "main"

        const ourChanges = flatten(await getChangesForCommits(db, ourCommits));

        const collides = (their: Change, ours: Change[]) => {
            return ours.find(our => {
                return (
                    our.pk === their.pk &&
                    our.tbl_name === their.tbl_name &&
                    our.col_id === their.col_id &&
                    our.value !== their.value
                );
            })
        }

        // Merge their changes with ours.
        //
        // Merging happens through last-writer wins, unless the column
        // is marked with 'manual_conflict = 1' in that case the conflict needs to be resolved by the user through 'db.resolveConflict()'.
        const crrColumns = db.crrColumns;
        const accepted: Change[] = [];
        const merged: Change[] = [];
        const cellConflicts: CellConflict[] = [];
        const manualConflicts: CellConflict[] = [];
        let i = 0;
        for (; i < theirChanges.length; i++) {
            const theirChange = theirChanges[i];

            const collided = collides(theirChange, ourChanges);
            if (collided) {
                const ourChange = collided;
                
                cellConflicts.push([ourChange, theirChange]);

                const autoResolve = crrColumns[ourChange.tbl_name].find(col => col.col_id === ourChange.col_id && !col.manual_conflict) !== undefined;
                if (autoResolve) {
                    if (isLastWriter(theirChange, ourChange)) {
                        accepted.push(theirChange);
                    }
                } else {
                    manualConflicts.push([ourChange, theirChange]);
                }
            } else {
                accepted.push(theirChange);
            }
            merged.push(theirChange);
        }

        // Any leftovers of our changes gets pushed onto the merged changes
        if (i < ourChanges.length) {
            const leftover = ourChanges.slice(i, -1);
            merged.push(...leftover);
        }

        // Populate the conflicting cells with their full rows
        const rowConflicts = await getRowConflictsFromCellConflicts(db, cellConflicts, commonAncestor, docId);
        const manualRowConflicts = await getRowConflictsFromCellConflicts(db, manualConflicts, commonAncestor, docId);
        await saveRowConflicts(db, manualRowConflicts);

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
            manualConflicts: manualRowConflicts,
            appliedChanges
        };
    } else {
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
            manualConflicts: [],
            appliedChanges
        };
    }
}

const getRowConflictsFromCellConflicts = async (db: SqliteDB, cellConflicts: CellConflict[], commonAncestor: Commit, documentId: string) => {
    if (cellConflicts.length === 0) return [];

    const conflictingChanges: { [tblName: string]: { [pk: string]: [our: Change, their: Change][] } } = {};
    const cc = conflictingChanges;
    for (const [our, their] of cellConflicts) {
        const tblName = our.tbl_name;
        const pk = our.pk;
        if (cc[tblName]) {
            cc[tblName][pk].push([our, their]);
        } else {
            cc[tblName] = { [pk]: [[our, their]] };
        }
    }

    const pksPerTable: { [tblName: string]: string[] } = {};
    for (const [tblName, rows] of Object.entries(conflictingChanges)) {
        pksPerTable[tblName] = Object.keys(rows);
    }

    const rowsPerTable: { [tblName: string]: any[] } = {};
    for (const [tblName, pks] of Object.entries(pksPerTable)) {
        rowsPerTable[tblName] = await db.select<any[]>(`SELECT * FROM "${tblName}" WHERE ${pksEqual(db, tblName, pks)}`, []);
    }

    const conflictingRows: RowConflict<any>[] = [];

    const baseDocument = await getDocumentSnapshot(db, commonAncestor);
    for (const [tblName, rows] of Object.entries(rowsPerTable)) {
        for (const row of rows) {
            const pk = pkEncodingOfRow(db, tblName, row);

            const changes = conflictingChanges[tblName][pk];

            if (!baseDocument[tblName]) {
                // @TODO: Filter out tables that shouldn't be replicated in the syncer so they never propogate to any server
                console.warn(`Table '${tblName}' is not part of a document`);
                continue;
            }

            // Construct 3 versions of the row (base, ours, theirs)
            const base = baseDocument[tblName][pk];
            const our = { ...base };
            const their = { ...base };
            const columns: string[] = [];
            for (const [ourChange, theirChange] of changes) {
                const col = ourChange.col_id;
                our[col] = ourChange.value;
                their[col] = theirChange.value;
                columns.push(col);
            }

            const conflict: RowConflict<any> = {
                document: documentId,
                tbl_name: tblName,
                pk,
                columns,
                base,
                our,
                their
            };

            conflictingRows.push(conflict);
        }
    }

    return conflictingRows;
}

/** Resolve a single conflict */
export const resolveConflict = async (db: SqliteDB, table: string, pk: string, documentId = "main", choice: ConflictChoice) => {

    // @NOTE:
    // Resolving a conflict, equates to duplicating the chosen changes and make them part of the
    // current merge commit that was stopped due to manual conflicts.
    // We prevent clients from pushing any commit until the conflicts are resolved and made part of 
    // the merge commit for this reason. Duplicating the change with a new timestamp ensures that other clients
    // will automatically accept the chosen change as the merge indicates than the pushing clients have atleast seen
    // the receiving clients change.
    
    const conflictRow = await db.first<RowConflict<any>>(`SELECT * FROM "crr_conflicts" WHERE document = ? AND tbl_name = ? AND pk = ?`, [documentId, table, pk]);
    if (!conflictRow) {
        throw new Error("Conflict was not found");
    }

    const conflict = deserializeConflict<any>(conflictRow);
    
    const merge = await getHead(db, documentId);
    if (!merge || !isMerge(merge)) {
        throw new Error("**BUG** Expected that the current HEAD is pointing at the current merge");
    }
    
    // Generate duplicate cell changes with a new timestamp
    const winningRow = choice === "our" ? conflict.our : conflict.their;
    const winningChanges: Change[] = [];

    const now = (new Date).getTime();   // @TODO: Needs to be made into a HLC
    for (const column of conflict.columns) {
        const change: Change = {
            type: "update",     // @TODO: I guess we can technically also conflict with deletions/insertions???
            tbl_name: table,
            col_id: column,
            pk: pk,
            value: winningRow[column],
            site_id: db.siteId,
            created_at: now,
            applied_at: 0,      // Set in saveChanges
            version: merge.id,
            document: documentId,
        }
        winningChanges.push(change);
    }

    await applyChanges(db, winningChanges);
    await db.exec(`DELETE FROM "crr_conflicts" WHERE document = ? AND tbl_name = ? AND pk = ?`, [documentId, table, pk]);
}

const deserializeConflict = <T>(conflict: RowConflict<string>) : RowConflict<T> => {
    return {
        ...conflict,
        columns: JSON.parse((conflict.columns as unknown as string)),
        base: JSON.parse(conflict.base),
        our: JSON.parse(conflict.our),
        their: JSON.parse(conflict.their),
    }
}

/** Gets conflicts for a specific table and document */
export const getConflicts = async <T>(db: SqliteDB, table: string, documentId = "main") => {
    const conflicts = await db.select<RowConflict<string>[]>(`SELECT * FROM "crr_conflicts" WHERE document = ? AND tbl_name = ?`, [documentId, table]);

    const result = conflicts.map(deserializeConflict<T>);
    return result;
}

/** Gets a snapshot of a document at a certain commit */
export const getDocumentSnapshot = async (db: SqliteDB, commit: Commit) => {
    // Get all changes up to (including) this commit
    const pastCommits = await db.select<Commit[]>(`SELECT * FROM "crr_commits" WHERE document = ? AND created_at <= ?`, [commit.document, commit.created_at]);
    const pastChanges = flatten(await getChangesForCommits(db, pastCommits));

    const document: { [tblName: string]: { [pk: string]: { [col: string]: any } } } = {};
    for (const change of pastChanges) {
        const tblName = change.tbl_name;
        const pk = change.pk;
        const col = change.col_id;

        if (change.type === "delete") {
            delete document[tblName][pk];
            continue;
        }

        if (document[tblName]) {
            if (document[tblName][pk]) {
                document[tblName][pk][col] = change.value;
            } else {
                document[tblName][pk] = { [col]: change.value };
            }
        } else {
            document[tblName] = { [pk]: { [col]: change.value } };
        }

    }

    return document;
}

/** Gets the non-pushed commit count for a given document */
export const getPushCount = async (db: SqliteDB, documentId = "main") => {
    const doc = await db.first<Document>(`SELECT * FROM "crr_documents" WHERE id = ?`, [documentId]);
    if (!doc) {
        console.error(`Document '${documentId}' not found. Make sure to pass a documentId to getPushCount()`);
        return 0;
    }

    const unpushedCommits = await db.select<Commit[]>(`
        SELECT * FROM "crr_commits" WHERE document = ? AND author = ? AND created_at > (SELECT created_at FROM "crr_commits" WHERE document = ? AND id = ?)
    `, [doc.id, db.siteId, doc.id, doc.last_pushed_commit ?? 0]);
    
    return unpushedCommits.length;
}

const saveRowConflicts = async (db: SqliteDB, conflicts: RowConflict<any>[]) => {
    if (conflicts.length === 0) return;

    const columns = Object.keys(conflicts[0]);

    // Serialize the conflicts
    // @Space - Currently we just store the three different versions of the row (base, our, their) as a json string,
    // as that makes it easy to serialize / deserialize, buttt, we are paying an extra cost in that we are duplicating information and the rows might be big.
    // In most cases this should not be a problem, as the conflict rows are only temporary until they are resolved, but for long outstanding commits with a lot 
    // of conflicts and documents, we might consider reconstructing the row conflicts in a more clever way from the changelog ...   - jsaad 20. April 2025
    const serialize = (c: RowConflict<any>) => {
        return [
            c.document,
            c.tbl_name,
            c.pk,
            JSON.stringify(c.columns),
            JSON.stringify(c.base),
            JSON.stringify(c.our),
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

const dropDocument = async (db: SqliteDB, documentId: string) => {
    const docRows = await db.select<{ tbl_name: string, pk: string }[]>(`SELECT tbl_name, pk FROM "crr_changes" WHERE document = ? GROUP BY pk`, [documentId]);

    const rowsPerTable = Object.groupBy(docRows, (row) => row.tbl_name);
    for (const [tblName, rows] of Object.entries(rowsPerTable)) {
        const pks = rows!.map(row => row.pk);
        await deleteRows(db, tblName, pks);
    }
}