import { Client, applyChanges, Change, generateUniqueId, insertRows, SqliteDB, isLastWriter } from "../index.ts";
import { assert } from "@/src/utils.ts";

export type Commit = {
    id: string
    parent: string | null
    message: string
    author: string
    created_at: number
}

export type PushData = {
    lastPulledCommit: string | null
    lastPushedCommit: string | null
    commits: Commit[]
    changes: Change[][]
}

type PushResponse = {
    status: "ok" | "needs-pull" | "boring" | "malformed"
    hint?: string
}

export type PullData = {
    lastPushedCommit: string | null
    lastPulledCommit: string | null
}

export type Pull = {
    commits: Commit[]
    changes: Change[][]
}

export const commit = async (db: SqliteDB, message: string): Promise<Commit | undefined> => {

    const id = generateUniqueId();
    const me = db.siteId;
    const now = (new Date).getTime();

    // Update all uncommitted changes to have this commit as their version
    const uncommittedChangeCount = (await db.first<{count: number}>(`SELECT COUNT(*) as count FROM "crr_changes" WHERE version='0'`, []))?.count ?? 0;
    if (uncommittedChangeCount === 0) {
        return;
    }
    await db.execOrThrow(`UPDATE "crr_changes" SET version = ? WHERE version='0'`, [id]);

    const client = await db.first<Client>(`SELECT * FROM "crr_clients" WHERE is_me = 1`, []);
    const parent = client?.head ?? null;

    await db.execOrThrow(`INSERT INTO "crr_commits" (id, parent, message, author, created_at) 
        VALUES (?, ?, ?, ?, ?)
    `, [id, parent, message, me, now]);

    await db.execOrThrow(`UPDATE "crr_clients" SET head=? WHERE is_me = 1`, [id]);

    return { id, parent, message, author: me, created_at: now };
}

export const checkout = async (db: SqliteDB, commitId: string) => {
    const commit = await db.first<Commit>(`SELECT * FROM "crr_commits" WHERE id = ?`, [commitId]);
    if (!commit) {
        console.error(`Trying to checkout non-existing commit '${commitId}'`);
        return;
    }

    const head = await db.first<Commit>(`
        SELECT * FROM "crr_commits" 
        WHERE id = (SELECT head FROM "crr_clients" WHERE is_me = 1)
    `, []);
    if (!head) {
        console.error(`No HEAD was found. Make sure that the 'head' column in crr_clients has a value`);
        return;
    }

    if (commit.id === head.id) {
        console.log(`Ignoring checkout of HEAD`);
        return;
    }

    await db.exec(`UPDATE "crr_clients" SET time_travelling = 1 WHERE is_me = 1`, []);
    if (commit.created_at < head.created_at) {

        await dropVersionedTables(db);
        const backwordCommits = await db.select<Commit[]>(`SELECT * FROM "crr_commits" WHERE created_at <= ?`, [commit.created_at]);
        for (const c of backwordCommits) {
            const changes = await db.select<Change[]>(`SELECT * FROM "crr_changes" WHERE version = ?`, [c.id]);
            await fastApplyChanges(db, changes);
        }
    } else {
        // The HEAD must currenly be detached. Here we can simply apply the changes in-between ontop
        const forwardCommits = await db.select<Commit[]>(`SELECT * FROM "crr_commits" WHERE created_at > ? AND created_at <= ?`, [head.created_at, commit.created_at]);
        for (const c of forwardCommits) {
            const changes = await db.select<Change[]>(`SELECT * FROM "crr_changes" WHERE version = ?`, [c.id]);
            await fastApplyChanges(db, changes);
        }
    }
    await db.exec(`UPDATE "crr_clients" SET time_travelling = 0 WHERE is_me = 1`, []);
    await db.exec(`UPDATE "crr_clients" SET head = ? WHERE is_me = 1`, [commit.id]);
}

export const revert = async (db: SqliteDB) => {
    // TODO
}

export const discardChanges = async (db: SqliteDB) => {
    await db.exec(`UPDATE "crr_clients" SET time_travelling = 1 WHERE is_me = 1`, []);

    await dropVersionedTables(db);

    // Remove uncommitted changes
    await db.exec(`DELETE FROM "crr_changes" WHERE version = 0`, []);

    // Reapply all changes from history
    // @Speed - This probably needs to be reconsidered ...
    //          One way we could speed this up would be to group each set of changes
    //          into their respective tables, turning foreign-keys off, then doing bulk inserts
    //          of rows
    const changes = await db.select<Change[]>(`SELECT * FROM "crr_changes" ORDER BY created_at ASC`, []);

    await fastApplyChanges(db, changes);

    await db.exec(`UPDATE "crr_clients" SET time_travelling = 0 WHERE is_me = 1`, []);
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

            // TODO - We are doing single row inserts, only because
            //        we might now have the same amount of values and keys in each row which 
            //        insertRows assumes
            await insertRows(db, table, [row]);
        }

        // await insertRows(db, table, rows);
    }
}

export const preparePush = async (db: SqliteDB) => {
    const client = await db.first<Client>(`SELECT * FROM "crr_clients" WHERE is_me = 1`, []);
    if (!client) throw new Error("Database is missing a site_id");

    const lastPushedCommit = await db.first<Commit>(`SELECT * FROM "crr_commits" WHERE id = (SELECT last_pushed_commit FROM "crr_clients" WHERE is_me = 1)`, []);
    const nonPushedCommits = await db.select<Commit[]>(`SELECT * FROM "crr_commits" WHERE created_at > ? AND author = ? ORDER BY created_at ASC`, [lastPushedCommit?.created_at ?? 0, client.site_id]);

    const changes = [];
    const commits = [];
    for (const commit of nonPushedCommits) {
        const cgs = await db.select<Change[]>(`SELECT * FROM "crr_changes" WHERE version = ? ORDER BY created_at ASC`, [commit.id]);
        commits.push(commit);
        changes.push(cgs);
    }

    const data: PushData = {
        lastPulledCommit: client.last_pulled_commit,
        lastPushedCommit: client.last_pushed_commit,
        commits,
        changes
    }

    return data;
}

export const preparePull = async (db: SqliteDB): Promise<PullData> => {
    const client = await db.first<Client>(`SELECT * FROM "crr_clients" WHERE is_me = 1`, []);
    if (!client) throw new Error("Database is missing a site_id");

    return {
        lastPushedCommit: client.last_pushed_commit,
        lastPulledCommit: client.last_pulled_commit
    };
}

export const receivePush = async (db: SqliteDB, their: PushData): Promise<PushResponse> => {
    const our = await db.first<Client>(`SELECT * FROM "crr_clients" WHERE is_me = 1`, []);
    if (!our) throw new Error("Database is missing a site_id");

    // Check if pusher has all the latest commits
    if (our.head !== their.lastPushedCommit && our.head !== their.lastPulledCommit) {
        return { status: "needs-pull" };
    }

    if (their.commits.length === 0) {
        return { status: "boring", hint: "Nothing to be pushed" };
    }

    if (their.changes.length !== their.commits.length) {
        return { status: "malformed", hint: "Mismatch in the number of changesets and commits" };
    }

    // Apply each commit on-top
    for (let i = 0; i < their.commits.length; i++) {
        const changes = their.changes[i];
        await applyChanges(db, changes);
    }

    // Save commits
    await insertRows(db, "crr_commits", their.commits);

    // Update our HEAD
    await db.exec(`UPDATE "crr_clients" SET head = ? WHERE is_me = 1`, [their.commits[their.commits.length - 1].id]);

    return { status: "ok" };
}

export const receivePushResponse = async (db: SqliteDB, remote: SqliteDB, response: PushResponse) => {
    switch (response.status) {
        case "ok": {
            const now = (new Date).getTime();
            await db.exec(`UPDATE "crr_clients" SET last_pushed_at = ?, last_pushed_commit = (SELECT head FROM "crr_clients" WHERE is_me = 1) WHERE is_me = 1`, [now]);
            break;
        }
        case "needs-pull": {
            // Here we just auto pull down any changes
            const pullData = await db.preparePull();
            const pullResponse = await remote.receivePull(pullData);
            
            await applyPull(db, pullResponse);

            const secondPush = await db.preparePush();
            
            const secondResponse = await remote.receivePush(secondPush);
            if (secondResponse.status !== "ok" && secondResponse.status !== "boring" ) {
                console.error(`Remote is still ahead after a pull and push`, secondResponse);
            }
            break;
        }
        case "malformed": {
            console.error(`Push was malformed`);
            break;
        }
        case "boring": {
            console.log(`Nothing got pushed`)
            break;
        }
    }
}

export const receivePull = async (db: SqliteDB, their: PullData): Promise<Pull> => {
    const our = await db.first<Client>(`SELECT * FROM "crr_clients" WHERE is_me = 1`, []);
    if (!our) throw new Error("Database is missing a site_id");

    let commits = [] as Commit[];
    const ancestor = await db.first<Commit>(`SELECT * FROM "crr_commits" WHERE id = ?`, [their.lastPulledCommit]);
    if (!ancestor) {
        // They have never pulled, send them all the commits
        commits = await db.select(`SELECT * FROM "crr_commits" ORDER BY created_at ASC`, []);
    } else {
        // Send the commits after the point at which they last pulled
        commits = await db.select(`SELECT * FROM "crr_commits" WHERE created_at > ? ORDER BY created_at ASC`, [ancestor.created_at]);
    }

    const changes: Change[][] = [];
    for (const commit of commits) {
        const cgs = await db.select<Change[]>(`SELECT * FROM "crr_changes" WHERE version = ?`, [commit.id]);
        changes.push(cgs);
    }

    return {
        commits,
        changes
    }
}

export const applyPull = async (db: SqliteDB, pull: Pull) => {
    if (pull.commits.length === 0) return;

    const theirCommits = pull.commits;
    const theirChanges = pull.changes.reduce((result, changes) => { result.push(...changes); return result }, [] as Change[]);

    const commonAncestor = theirCommits[0];

    // Scenarios for pull
    //    1. I have no decendent commits
    //       Apply their commits on-top
    //
    //    2. I have decendent commits
    //       Merge the divergent changes and create new merge commit
    const ourCommits = await db.select<Commit[]>(`SELECT * FROM "crr_commits" WHERE created_at > ? ORDER BY created_at ASC`, [commonAncestor.created_at]);
    if (ourCommits.length > 0) {
        // The two branches have diverged
        const ourChanges = [];
        for (const commit of ourCommits) {
            const changes = await db.select<Change[]>(`SELECT * FROM "crr_changes" WHERE version = ? ORDER BY created_at ASC`, [commit.id]);
            ourChanges.push(...changes);
        }

        const collides = (c2: Change, ours: Change[]) => {
            return ours.find(c1 => {
                return (
                    c1.pk === c2.pk &&
                    c1.tbl_name === c2.tbl_name &&
                    c1.col_id === c2.col_id &&
                    c1.value !== c2.value
                );
            })
        }

        // Merge their changes with ours
        const acceptedChanges = [];
        const merged = [];
        const collisions = [] as [Change, Change][];
        let i = 0;
        for (; i < theirChanges.length; i++) {
            const theirChange = theirChanges[i];

            const collided = collides(theirChange, ourChanges);
            if (collided) {
                // Resolve conflicts using LWW
                if (isLastWriter(theirChange, collided)) {
                    acceptedChanges.push(theirChange);
                    merged.push(theirChange);
                } else {
                    merged.push(theirChange);
                }
                collisions.push([theirChange, collided])
            } else {
                acceptedChanges.push(theirChange);
                merged.push(theirChange);
            }
        }
        if (collisions.length > 0) {
            // console.log(collisions);
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
            parent: `${theirLastCommit.id}|${ourLastCommit.id}`,
            message: `Merge of '${theirLastCommit.id}' and '${ourLastCommit.id}'`,
            author: db.siteId,
            created_at: (new Date).getTime()
        }

        // Assign the merged changes to this commit
        for (const change of acceptedChanges) {
            change.version = merge.id;
        }

        // Apply all the accepted changes
        await applyChanges(db, acceptedChanges);

        // Save commits and merged commit
        await insertRows(db, "crr_commits", [merge, ...theirCommits]);

        // Update the last pulled commit and the HEAD
        await db.execOrThrow(`UPDATE "crr_clients" SET head = ?, last_pulled_commit = ? WHERE is_me = 1`, [merge.id, theirLastCommit.id]);

        return merge;
    } else {
        await applyChanges(db, theirChanges);
        await insertRows(db, "crr_commits", theirCommits);

        const theirLastCommit = theirCommits[theirCommits.length - 1];

        await db.execOrThrow(`UPDATE "crr_clients" SET head = ?, last_pulled_commit = ? WHERE is_me = 1`, [theirLastCommit.id, theirLastCommit.id]);
    }
}


const dropVersionedTables = async (db: SqliteDB) => {
    const versionedTables = Object.keys(db.crrColumns);
    for (const table of versionedTables) {
        await db.exec(`DELETE FROM "${table}" WHERE 1`, []);
    }
}