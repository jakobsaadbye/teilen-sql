import { applyChanges, Change, generateUniqueId, insertRows, SqliteDB, isLastWriter, sqlPlaceholdersNxM } from "../index.ts";
import { assert, deleteRows } from "./utils.ts";

export type Document = {
    id: string
    head: string | null
    last_pushed_commit: string | null
    last_pulled_commit: string | null
}

export type Commit = {
    id: string
    document: string
    parent: string | null
    message: string
    author: string
    created_at: number
}

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
}

type PullResult = {
    documentId: string
    merge?: Commit
    conflicts: [Change, Change][]
    appliedChanges: Change[]
}

export const commit = async (db: SqliteDB, message: string, documentId: string): Promise<Commit | undefined> => {

    const id = generateUniqueId();
    const me = db.siteId;
    const now = (new Date).getTime();

    // Update all uncommitted changes to have this commit as their version
    const uncommittedChangeCount = (await db.first<{ count: number }>(`SELECT COUNT(*) as count FROM "crr_changes" WHERE version='0' AND document = ?`, [documentId]))?.count ?? 0;
    if (uncommittedChangeCount === 0) {
        return;
    }
    await db.execOrThrow(`UPDATE "crr_changes" SET version = ? WHERE version='0' AND document = ?`, [id, documentId]);

    let doc = await db.first<Document>(`SELECT * FROM "crr_documents" WHERE id = ?`, [documentId]);
    if (!doc) {
        doc = { id: documentId, head: id, last_pushed_commit: null, last_pulled_commit: null };
        await db.execOrThrow(`INSERT INTO "crr_documents" (id, head) VALUES (?, ?)`, [doc.id, doc.head]);
    }

    const parent = doc.head;

    await db.execOrThrow(`INSERT INTO "crr_commits" (id, document, parent, message, author, created_at) 
        VALUES (?, ?, ?, ?, ?, ?)
    `, [id, documentId, parent, message, me, now]);

    await db.execOrThrow(`UPDATE "crr_documents" SET head = ? WHERE id = ?`, [id, doc.id]);

    return { id, document: doc.id, parent, message, author: me, created_at: now };
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

    const changes: Change[][] = [];
    const commits: Commit[] = [];
    for (const commit of nonPushedCommits) {
        const cgs = await db.select<Change[]>(`SELECT * FROM "crr_changes" WHERE version = ? ORDER BY created_at ASC`, [commit.id]);
        commits.push(commit);
        changes.push(cgs);
    }

    const data: PushRequest = {
        siteId: db.siteId,
        document: doc!,
        commits,
        changes
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

    const Response = (status: PushResponseStatus, code: number, message?: string): PushResponse => {
        return {
            documentId: docId,
            status,
            code,
            message,
        };
    }

    let our = await db.first<Document>(`SELECT * FROM "crr_documents" WHERE id = ?`, [docId]);
    if (!our) {
        // Create the document as we haven't seen it before
        our = { id: docId, head: null } as Document;
        await db.exec(`INSERT INTO "crr_documents" (id, head) VALUES (?, ?)`, [docId, null]);
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

    // Save commits
    await insertRows(db, "crr_commits", their.commits);

    // Update our HEAD
    await db.exec(`UPDATE "crr_documents" SET head = ? WHERE id = ?`, [their.commits[their.commits.length - 1].id, docId]);

    return Response("ok", 200);
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

        // Give the commits they have not seen
        const ancestor = await db.first<Commit>(`SELECT * FROM "crr_commits" WHERE document = ? AND id = ?`, [docId, theirDoc.last_pulled_commit]);
        if (!ancestor) {
            packets.push({ documentId: docId, commits: [], changes: [] });
            continue;
        }

        const commits = await db.select<Commit[]>(`SELECT * FROM "crr_commits" WHERE created_at > ? AND document = ? ORDER BY created_at ASC`, [ancestor.created_at, docId]);
        const changes = await getChangesForCommits(db, commits);

        packets.push({ documentId: docId, commits, changes });
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

export const applyPull = async (db: SqliteDB, packets: PullPacket[]): Promise<PullResult[]> => {
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
    //       Apply their commits on-top
    //
    let commonAncestor;
    const commonAncestorId = theirCommits[0].parent;
    if (commonAncestorId === null) {
        commonAncestor = await db.first<Commit>(`SELECT * FROM "crr_commits" WHERE document = ? AND parent IS NULL`, [docId]);
    } else {
        commonAncestor = await db.first<Commit>(`SELECT * FROM "crr_commits" WHERE document = ? AND id = ?`, [docId, commonAncestorId]);
    }

    // We might not have a common ancestor commit if its a new document
    let ourCommits: Commit[];
    if (!commonAncestor) {
        ourCommits = [];
    } else {
        ourCommits = await db.select<Commit[]>(`SELECT * FROM "crr_commits" WHERE document = ? AND created_at > ?  ORDER BY created_at ASC`, [docId, commonAncestor.created_at]);
    }

    if (ourCommits.length > 0) {
        // The two branches have diverged
        const ourChanges: Change[] = [];
        for (const commit of ourCommits) {
            const changes = await db.select<Change[]>(`SELECT * FROM "crr_changes" WHERE version = ? ORDER BY created_at ASC`, [commit.id]);
            ourChanges.push(...changes);
        }

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

        // Merge their changes with ours
        const accepted: Change[] = [];
        const merged: Change[] = [];
        const conflicts: [Change, Change][] = [];
        let i = 0;
        for (; i < theirChanges.length; i++) {
            const theirChange = theirChanges[i];

            const collided = collides(theirChange, ourChanges);
            if (collided) {
                // Resolve conflicts using LWW
                const ourChange = collided;
                if (isLastWriter(theirChange, ourChange)) {
                    accepted.push(theirChange);
                    merged.push(theirChange);
                } else {
                    merged.push(theirChange);
                }
                conflicts.push([ourChange, theirChange])
            } else {
                accepted.push(theirChange);
                merged.push(theirChange);
            }
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
            message: `Merge of '${theirLastCommit.id}' and '${ourLastCommit.id}'`,
            author: db.siteId,
            created_at: (new Date).getTime(),
        }

        // Assign the merged changes to this commit
        for (const change of accepted) {
            change.version = merge.id;
        }

        // Apply all the accepted changes
        const appliedChanges = await applyChanges(db, accepted);

        // Save commits and merged commit
        await saveCommits(db, [merge, ...theirCommits]);

        // Update the last pulled commit and the HEAD
        await db.execOrThrow(`UPDATE "crr_documents" SET head = ?, last_pulled_commit = ? WHERE id = ?`, [merge.id, theirLastCommit.id, docId]);

        return {
            documentId: docId,
            merge,
            conflicts,
            appliedChanges
        };
    } else {
        const appliedChanges = await applyChanges(db, theirChanges);
        await saveCommits(db, theirCommits);

        const theirLastCommit = theirCommits[theirCommits.length - 1];

        await db.execOrThrow(`UPDATE "crr_documents" SET head = ?, last_pulled_commit = ? WHERE id = ?`, [theirLastCommit.id, theirLastCommit.id, docId]);

        return {
            documentId: docId,
            conflicts: [],
            appliedChanges
        };
    }
}

const saveCommits = async (db: SqliteDB, commits: Commit[]) => {
    if (commits.length === 0) return;

    const columns = Object.keys(commits[0]);
    const values = commits.map(commit => columns.map(col => commit[col])).reduce((result, vals) => { result.push(...vals); return result }, [] as any[]);

    const updateStr = columns
        .filter(col => col !== "id") // Exlucde pks
        .map(col => `${col} = EXCLUDED.${col}`)
        .join(',\n\t\t\t');

    const stmt = `
        INSERT INTO "crr_commits" (
            id,
            document,
            parent,
            message,
            author,
            created_at
        )
        VALUES ${sqlPlaceholdersNxM(columns.length, commits.length)}
        ON CONFLICT DO UPDATE SET
            ${updateStr}
    `;
    console.log(values);
    await db.execOrThrow(stmt, values)
}

const dropDocument = async (db: SqliteDB, documentId: string) => {
    const docRows = await db.select<{ tbl_name: string, pk: string }[]>(`SELECT tbl_name, pk FROM "crr_changes" WHERE document = ? GROUP BY pk`, [documentId]);

    const rowsPerTable = Object.groupBy(docRows, (row) => row.tbl_name);
    for (const [tblName, rows] of Object.entries(rowsPerTable)) {
        const pks = rows!.map(row => row.pk);
        await deleteRows(db, tblName, pks);
    }
}