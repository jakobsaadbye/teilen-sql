import { applyChanges, Change, Client } from "./change.ts";
import { SqliteDB } from "./sqlitedb.ts";
import { unique, assert } from "./utils.ts";
import { applyPull, Commit, Document, PullResponse, PushResponse } from "./versioning.ts";

type SyncEventType = "change";

export type SyncEvent = {
    type: SyncEventType,
    data: Change[]
}

type Listener = {
    type: SyncEventType,
    callback: (event: SyncEvent) => void
}

type Options = {
    pullEndpoint: string
    pushEndpoint: string

    /** Commit endpoints only need to be defined if mode is git-style */
    commitPushEndpoint?: string 
    commitPullEndpoint?: string
}

export class Syncer {
    db: SqliteDB
    options: Options
    socket?: WebSocket
    listeners: Listener[]

    constructor(db: SqliteDB, options: Options) {
        this.db = db;
        this.options = options;
        this.listeners = [];
    }

    async pullChangesHttp() {
        const client = await this.db.first<Client>(`SELECT * FROM "crr_clients" WHERE site_id = ?`, [this.db.siteId]);
        if (!client) return;
        
        const url = new URL(this.options.pullEndpoint);
        url.searchParams.append("lastPulledAt", '' + client.last_pulled_at);
        url.searchParams.append("siteId", client.site_id);

        try {
            const res = await fetch(url.toString());
            if (res.ok) {
                const data = await res.json();

                const { changes, pulledAt } = data;

                const appliedChanges = await applyChanges(this.db, changes);
                
                await this.db.execOrThrow(`UPDATE "crr_clients" SET last_pulled_at = ? WHERE site_id = ?`, [pulledAt, this.db.siteId]);

                return appliedChanges;
            } else {
                const data = await res.json();
                console.error(`Failed to pull changes. Is the server running?`, data.error);
            }
        } catch (e) {
            await this.db.exec(`ROLLBACK`, []);
            console.error(`Failed to pull changes`, e);
        }
    }

    async pushChangesHttp() {
        const client = await this.db.first<Client>(`SELECT * FROM "crr_clients" WHERE site_id = ?`, [this.db.siteId]);
        if (!client) return;

        const lastPushedAt = client.last_pushed_at;
        const changes = await this.db.select<Change[]>(`SELECT * FROM "crr_changes" WHERE applied_at > ? AND site_id = ? ORDER BY created_at ASC`, [lastPushedAt, this.db.siteId]);

        console.log(`Pushed ${changes.length} changes`);

        if (changes.length === 0) return;

        const data = JSON.stringify({
            changes
        });

        try {
            const res = await fetch(this.options.pushEndpoint, {
                method: 'POST',
                headers: new Headers({
                    "Content-Type": "application/json"
                }),
                body: data
            });

            if (res.ok) {
                const err = await this.db.exec(`UPDATE "crr_clients" SET last_pushed_at = ? WHERE site_id = ?`, [new Date().getTime(), this.db.siteId]);
                if (err !== undefined) console.error(err);
            } else {
                const data = await res.json();
                console.error(`Failed to push changes`, res.status, data);
            }
        } catch (e) {
            console.error(`Failed to push changes`, e);
        }
    }

    async pushChangesWs(ws: WebSocket) {
        const client = await this.db.first<Client>(`SELECT * FROM "crr_clients" WHERE site_id = ?`, [this.db.siteId]);
        if (!client) return;

        const lastPushedAt = client.last_pushed_at;
        const changes = await this.db.select<Change[]>(`SELECT * FROM "crr_changes" WHERE applied_at > ? AND site_id = ? ORDER BY created_at ASC`, [lastPushedAt, this.db.siteId]);
        const commits = await this.db.select<Commit[]>(`SELECT * FROM "crr_commits" WHERE created_at > ? AND author = ? ORDER BY created_at ASC`, [lastPushedAt, this.db.siteId]);

        console.log(`Pushing ${changes.length} changes`);
        if (changes.length === 0) return;

        const msg = JSON.stringify({
            type: "push-changes",
            data: {
                commits,
                changes
            },
        });

        ws.send(msg);
        return;
    }

    async pushCommits(db: SqliteDB, documentId = 'main') {
        if (!this.options.commitPushEndpoint) {
            console.warn(`No commit push-endpoint was specified in the syncer options. Set 'commitPushEndpoint' as an option to push version controlled changes`);
            return;
        }

        const pushRequest = await db.preparePushCommits(documentId);
        if (pushRequest.commits.length === 0) return;

        const data = JSON.stringify(pushRequest);

        try {
            const response = await fetch(this.options.commitPushEndpoint, {
                method: 'PUT',
                headers: new Headers({
                    "Content-Type": "application/json"
                }),
                body: data
            });

            if (!response.ok) {
                if (response.status === 404) {
                    return;
                } else {
                    const err = await response.json();
                    console.error("Failed to push commits. Error from server: ", err);
                    return;
                }
            }

            const push = await response.json() as PushResponse;
            switch (push.status) {
            case "ok": {
                await db.exec(`
                    UPDATE "crr_documents" SET 
                        last_pulled_at = ?,
                        last_pushed_commit = head, 
                        last_pulled_commit = head 
                    WHERE id = ?
                `, [push.appliedAt, push.documentId]);
                return;
            }
            case "needs-pull": {
                console.log(push.message);
                return push;
            }
            case "request-contained-no-commits": {
                console.error(`Message from server:`, push.message);
                return push;
            }
            case "request-malformed":
                console.error(`Message from server:`, push.message);
                return push;
            }
        } catch (error) {
            console.error("Failed to push commits", error);
            return;
        }
    }

    async pullCommits(db: SqliteDB, documentId = "main") {
        if (!this.options.commitPullEndpoint) {
            console.warn(`No commit pull-endpoint was specified in the syncer options. Set 'commitPullEndpoint' as an option to pull version controlled changes`);
            return [];
        }

        const url = new URL(this.options.commitPullEndpoint);
        
        const requestData = await db.preparePullCommits();
        const data = JSON.stringify(requestData);
        
        try {
            const response = await fetch(url, {
                method: 'PUT',
                headers: new Headers({
                    "Content-Type": "application/json"
                }),
                body: data
            });

            if (!response.ok) {
                if (response.status === 404) {
                    return [];
                } else {
                    const err = await response.json();
                    console.error("Failed to pull commits. Error from server: ", err);
                    return [];
                }
            }

            const pull = await response.json() as PullResponse;
            switch (pull.status) {
            case "ok": {
                return await applyPull(db, pull);
            }
            }
        } catch (error) {
            console.error("Failed to pull commits", error);
            return [];
        }
    }

    removeEventListener(listener: Listener) {
        this.listeners = [];
        // @Fix - the listener should be removed properly like is done below.
        //        i think we ran into an issue of binding this before this function
        //        got called, so it wasn't being removed???
        // const index = this.listeners.findIndex(x => x.type === listener.type);
        // delete this.listeners[index];
    }

    addEventListener(type: SyncEventType, callback: (event: SyncEvent) => void) {
        this.listeners.push({ type, callback });
    }

    private notify(event: SyncEvent) {
        if (event.type === "change" && event.data.length === 0) return;
        
        for (const listener of this.listeners) {
            if (listener.type === event.type) {
                listener.callback(event);
            }
        }
    }

    async handleWebSocketMessage(ws: WebSocket, ev: MessageEvent) {
        const msg = JSON.parse(ev.data);

        switch (msg.type) {
            case "push-changes-ok": {
                const err = await this.db.exec(`UPDATE "crr_clients" SET last_pushed_at = ? WHERE site_id = ?`, [new Date().getTime(), this.db.siteId]);
                if (err !== undefined) console.error(err);
                return;
            }
            case "push-changes-fail": {
                const err = msg.data;
                console.log(`Failed to push changes.`, err);
                return;
            }
            case "pull-hint": {
                const client = await this.db.first<Client>(`SELECT * FROM "crr_clients" WHERE site_id = ?`, [this.db.siteId]);
                if (!client) return;

                const msg = JSON.stringify({
                    type: "pull-changes",
                    data: {
                        lastPulledAt: client.last_pulled_at
                    }
                })
                
                ws.send(msg);
                return;
            }
            case "pull-changes-ok": {
                const changes = msg.data.changes as Change[];
                const pulledAt = msg.data.pulledAt as number;
                
                try {
                    const appliedChanges = await applyChanges(this.db, changes);

                    await this.db.execOrThrow(`UPDATE "crr_clients" SET last_pulled_at = ? WHERE site_id = ?`, [pulledAt, this.db.siteId]);

                    this.notify({ type: "change", data: appliedChanges });

                    const touchedTables = unique(appliedChanges.map(change => change.tbl_name));
                    for (const table of touchedTables) {
                        this.db.channelTableChange.postMessage(table);
                    }
                } catch (e) {
                    console.log(`Failed to apply pulled changes.`, e.message);
                }

                return;
            }
            case "pull-changes-fail": {
                const err = msg.data;
                console.error(`Failed to pull changes from the server`, err);
                return;
            }
            default: {
                console.warn(`Recieved unknown message '${msg.type}'`);
            }
        }
    }
}