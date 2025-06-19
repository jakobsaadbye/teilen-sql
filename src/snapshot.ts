import { SqliteDB } from "./sqlitedb.ts";
import { Change, isLastWriter } from "./change.ts";
import { getCommitGraph } from "./graph.ts";
import { flatten, insertRows } from "./utils.ts";
import { Commit, dropDocument, getChangesForCommits } from "./versioning.ts";

type SnapshotData = {
    [tblName: string]: {
        [pk: string]: {
            [col: string]: Change
        }
    }
}

/** Gets a snapshot of a document at a certain commit */
export const getDocumentSnapshot = async (db: SqliteDB, commit: Commit): Promise<DocumentSnapshot> => {
    const G = await getCommitGraph(db, commit.document);
    if (!G) return new DocumentSnapshot(commit.document);

    const pastCommits = G.ancestors(commit.id);
    const pastChanges = flatten(await getChangesForCommits(db, pastCommits));

    const root = new DocumentSnapshot(commit.document);
    const snapshot = root.applyChanges(pastChanges);
    return snapshot;
}

/** Does a naive apply of the entire snapshot against the database, dropping the previous snapshot */
export const applySnapshot = async (db: SqliteDB, snapshot: DocumentSnapshot) => {
    await dropDocument(db, snapshot.documentId);

    const tables = snapshot.tables();
    for (const table of tables) {
        const rows = snapshot.getRows<any[]>(table);
        await insertRows(db, table, rows);
    }
}


export class DocumentSnapshot {
    documentId: string;
    data: SnapshotData = {};

    constructor(documentId: string) {
        this.data = {};
        this.documentId = documentId;
    }

    applyChanges(changes: Change[]) {
        if (changes.length === 0) return this;

        const result = this.copy();

        const sortedChanges = changes.toSorted();

        for (const c of sortedChanges) {
            const table = c.tbl_name;
            const pk = c.pk;
            const col = c.col_id;

            if (result.data[table]) {
                if (result.data[table][pk]) {
                    if (result.data[table][pk][col]) {
                        const prevChange = result.data[table][pk][col];
                        if (isLastWriter(c, prevChange)) {
                            result.data[table][pk][col] = c;
                        }
                    } else {
                        result.data[table][pk][col] = c;
                    }
                } else {
                    result.data[table][pk] = { [c.col_id]: c };
                }
            } else {
                result.data[table] = { [pk]: { [c.col_id]: c } };
            }
        }

        return result;
    }

    getRow<T>(table: string, pk: string): T | undefined {
        if (this.data[table]) {
            if (this.data[table][pk]) {
                const row = {} as T;

                // A row with an active tombstone returns undefined
                let deleted = false;
                for (const [col, change] of Object.entries(this.data[table][pk])) {
                    if (change.type === "delete" && change.value % 2 === 1) {
                        deleted = true;
                        break;
                    } else {
                        row[col] = change.value;
                    }
                }
                if (deleted) {
                    return undefined;
                } else {
                    return row;
                }
            }
        }
    }

    getRows<T>(table: string) {
        const rows: T[] = [];
        if (this.data[table]) {
            const pks = Object.keys(this.data[table]);
            for (const pk of pks) {
                const row = this.getRow<T>(table, pk);
                if (row) {
                    rows.push(row);
                }
            }
        }

        return rows;
    }

    tables(): string[] {
        return Object.keys(this.data)
    }

    copy(): DocumentSnapshot {
        const dataCopy: SnapshotData = {};
        for (const [table, rows] of Object.entries(this.data)) {
            dataCopy[table] = {};
            for (const [pk, cols] of Object.entries(rows)) {
                dataCopy[table][pk] = {};
                for (const [col, change] of Object.entries(cols)) {
                    dataCopy[table][pk][col] = { ...change };
                }
            }
        }

        const copy = new DocumentSnapshot(this.documentId);
        copy.data = dataCopy;
        return copy;
    }
}