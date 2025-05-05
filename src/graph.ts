import { Commit, isMerge } from "./versioning.ts";
import { SqliteDB } from "./sqlitedb.ts";
import { assert, intersect } from "./utils.ts";

export type CommitNode = {
    commit: Commit
    parents: CommitNode[]
    children: CommitNode[]
}

export class CommitGraph {
    head: CommitNode
    root: CommitNode
    nodes: CommitNode[]

    constructor(head: CommitNode, root: CommitNode, nodes: CommitNode[]) {
        this.head = head;
        this.root = root;
        this.nodes = nodes;
    }

    /** Returns the ancestor commits (including given) from the given commit sorted from oldest to newest */
    ancestors(commit: Commit) : Commit[] {
        const A = new Map<string, Commit>();

        const start = this.findNode(commit.id);
        if (!start) {
            return [];
        }

        const queue = [start];
        while (queue.length > 0) {
            const node = queue.pop()!;
            A.set(node.commit.id, node.commit);
            queue.push(...node.parents);
        }

        return A.values().toArray().toSorted((a, b) => a.created_at - b.created_at);
    }

    /** Returns all the commits that decends from the given commit sorted from oldest to newest */
    decendants(commit: Commit) : Commit[] {
        const D = new Map<string, Commit>();

        const start = this.findNode(commit.id);
        if (!start) {
            return [];
        }

        const queue = [start];
        while (queue.length > 0) {
            const node = queue.pop()!;
            D.set(node.commit.id, node.commit);
            queue.push(...node.children);
        }

        return D.values().toArray().toSorted((a, b) => a.created_at - b.created_at);
    }

    /** Returns weather commit a is an ancestor of b  */
    isAncestor(a: Commit, b: Commit) {
        const ancestorsOfB = this.ancestors(b);
        return ancestorsOfB.find(commit => commit.id === a.id) !== undefined;
    }

    /** Returns the latest commit in the graph (one with no decendants) */
    tip() {
        const iceberg = this.decendants(this.root.commit);
        if (iceberg.length === 0) return undefined;
        return iceberg[iceberg.length - 1];
    }

    print() {
        printCommitGraph(this);
    }

    private findNode(commitID: string) {
        return this.nodes.find(node => node.commit.id === commitID);
    }

}


export const getCommitGraph = async (db: SqliteDB, documentId = "main") => {
    const head = await db.getHead(documentId);
    if (!head) return;

    const commits = await db.select<Commit[]>(`SELECT * FROM "crr_commits" WHERE document = ? ORDER BY created_at`, [documentId]);

    // Stitch the graph together by following the parent relations
    const nodes: CommitNode[] = [];
    const roots: CommitNode[] = []; // @NOTE: There might be multiple roots, if working on a shared 'main' document

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

    // If we have multiple roots, form a new root commit that is the parent of the multiple roots
    let root = roots[0];
    if (roots.length > 1) {
        const trueRootCommit: Commit = {
            id: "root",
            document: head.document,
            parent: null,
            message: "An inserted root to have only 1 root",
            author: "teilen-sql",
            created_at: 0,
            applied_at: 0,
        }

        const trueRoot = newCommitNode(trueRootCommit, [], roots);

        // Link the multiple roots to the true root
        for (const root of roots) {
            root.parents = [trueRoot];
        }

        root = trueRoot;
    }

    const G = new CommitGraph(headNode, root, nodes);

    return G;
}

const findCommit = (G: CommitGraph, commitID: string) => {
    return G.nodes.find(node => node.commit.id === commitID);
}

// export const getDescendants = (G: CommitGraph, commitID: string) => {
//     const descendants: CommitNode[] = [];

//     const commit = findCommit(G, commitID);
//     if (!commit) {
//         return [];
//     }
    
// }

// /** Gets parent commits up until and including the given commit following the authors commits on any merges/branches */
// export const getAncestors = (G: CommitGraph, commit: Commit) => {

//     const pickOurChild = (children: CommitNode[]) => {
//         if (children.length === 1) return children[0];
//         return children.find(child => child.commit.author === commit.author);
//     }

//     // On branches (children > 1), we follow the children of the commit author
//     if (G.root.commit.id === commit.id) return [G.root];

//     const immediateParents: CommitNode[] = [G.root];

//     let node = G.root;
//     while (node.children.length > 0) {
//         const children = node.children;

//         const child = pickOurChild(children);
//         if (!child) {
//             // This should ideally not happen, but lets not assert it to not break client applications
//             console.error(`**Corrupt**: Missing child in commit graph. Unable to follow commits to the head of the document`);
//             break;
//         }

//         immediateParents.push(child);
//         if (child.commit.id === commit.id) {
//             break;
//         } else {
//             node = child;
//         }
//     }

//     return immediateParents;
// }

const getCommonAncestor = (G: CommitGraph, a: Commit, b: Commit) => {

    const aAncestors = getAncestors(G, a);
    const bAncestors = getAncestors(G, b);

    // Do an intersection to find all common ancestors
    const commonAncestors: CommitNode[] = intersect(aAncestors, bAncestors);
    assert(commonAncestors.length > 0);

    // The lowest common ancestor (common ancestor) will be the last one
    return commonAncestors[commonAncestors.length - 1];
}

export const printCommitGraph = (G: CommitGraph) => {

    const node = G.head;
    const timelines: (CommitNode | null)[] = [node];

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

const newCommitNode = (commit: Commit, parents: CommitNode[], children: CommitNode[]): CommitNode => {
    return {
        commit,
        parents,
        children,
    }
}