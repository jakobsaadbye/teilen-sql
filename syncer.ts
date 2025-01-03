import { applyChanges, Change, Client } from "./change.ts";
import { SqliteDB } from "./sqlitedb.ts";

export class Syncer {
    #db: SqliteDB
    #endpoint: string

    constructor(db: SqliteDB, endpoint: string) {
        this.#db = db;
        this.#endpoint = endpoint;
    }

    async pullChanges() {
        const client = await this.#db.first<Client>(`SELECT * FROM "crr_clients" WHERE site_id = ?`, [this.#db.siteId]);
        if (!client) return;

        const url = new URL(this.#endpoint);
        url.searchParams.append("lastPulledAt", client.last_pulled_at);
        url.searchParams.append("siteId", client.site_id);

        try {
            const res = await fetch(url.toString());
            if (res.ok) {
                const data = await res.json();
                
                const { changes, pulledAt } = data;

                let err = await applyChanges(this.#db, changes);
                if (err) return console.error(err);
                
                err = await this.#db.exec(`UPDATE "crr_clients" SET last_pulled_at = ? WHERE site_id = ?`, [pulledAt, this.#db.siteId]);
                if (err !== undefined) console.error(err);

                console.log(`Pulled ${changes.length} changes`);
            } else {
                const data = await res.json();
                console.error(`Failed to pull changes`, data.error);
            }
        } catch (e) {
            console.error(`Failed to pull changes`, e);
        }
    }

    async pushChanges() {
        const client = await this.#db.first<Client>(`SELECT * FROM "crr_clients" WHERE site_id = ?`, [this.#db.siteId]);
        if (!client) return;

        const lastPushedAt = client.last_pushed_at;
        const changes = await this.#db.select<Change[]>(`SELECT * FROM "crr_changes" WHERE applied_at > ? AND site_id = ?`, [lastPushedAt, this.#db.siteId]);

        console.log(`Changes to push`, changes);

        if (changes.length === 0) return console.log(`Nothing to push`);

        const payload = {
            changes,
        };

        try {
            const res = await fetch(this.#endpoint, {
                method: 'POST',
                headers: new Headers({
                    "Content-Type": "application/json"
                }),
                body: JSON.stringify(payload)
            });

            if (res.ok) {
                const err = await this.#db.exec(`UPDATE "crr_clients" SET last_pushed_at = ? WHERE site_id = ?`, [new Date().getTime(), this.#db.siteId]);
                if (err !== undefined) console.error(err);
            } else {
                console.error(`Failed to push changes`, res.status);
            }
        } catch (e) {
            console.error(`Failed to push changes`, e);
        }
    }
}