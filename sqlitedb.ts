import { applyChanges, Change, compactChanges, convertToSelectStmt, diff, OpType, saveChanges } from "./change.ts";
import { data } from "./data.ts";

type MessageType = 'dbClose' | 'exec' | 'select' | 'change';

export class SqliteDB {
    #debug = false;
    #mp: MessagePort
    #channelTableChange: BroadcastChannel
    siteId = "";
    pks: { [tblName: string]: string[] } = {};

    constructor(mp: MessagePort) {
        this.#mp = mp;

        // Broadcast channel to notify dependent queries to re-run
        this.#channelTableChange = new BroadcastChannel("table_change");

        // Broadcast any events handled by the worker to any of the functions below waiting for a result
        const bc = new BroadcastChannel("message_bus");
        this.#mp.addEventListener('message', async (event) => {
            bc.postMessage(event.data);
        });

        // Listen for database updates
        const otherChannel = new BroadcastChannel("message_bus");
        otherChannel.addEventListener('message', (e) => {
            if (e.data.type === 'change') {
                this.onChange(e.data.change);
            }
        });

        // Get or assign a unique site_id
        this.select(`SELECT * FROM "crr_client"`, [])
            .then(rows => {
                if (rows.length > 0) {
                    this.siteId = rows[0].site_id;
                } else {
                    const id = crypto.randomUUID();
                    this.exec(`INSERT INTO "crr_client" (site_id) VALUES ($1)`, [id])
                        .then(() => {
                            console.log("Assigned a new site_id");
                            this.siteId = id;
                        })
                        .catch(e => console.error("Failed to insert a new site_id", e));
                }
            })
            .catch(e => console.error("Failed to get site_id", e));

        // Extract the primary keys of tables to be used in changes
        this.extractPks();
    }

    onChange(change: {
        updateType: 'delete' | 'insert' | 'update',
        dbName: string | null,
        tableName: string | null,
        rowid: bigint
    }) {
        // this.#channelTableChange.postMessage(change);
        // console.log(change);
    }

    async close(): Promise<Error> {
        const data = await this.send('dbClose');
        if (!data.error) {
            console.log(`Closed database connection ...`);
        }
        this.#channelTableChange.close();
        return data.error;
    }

    async exec(sql: string, params: any[]) {
        if (this.#debug) console.info(sql);
        const data = await this.send('exec', { sql, params });
        this.#channelTableChange.postMessage(sql);
        return data.error as Error;
    }

    async execWithChanges(sql: string, params: any[]) {
        let [stmt, err, tableName, opType] = convertToSelectStmt(sql);
        if (err !== null) {
            console.error(err);
            return [];
        }

        // NOTE: Getting the state before and after the statement has fired like is done here is a pretty slow way of knowning what changed.
        //       The sqlite update_hook() can give the info that a row was changed, but not what got changed. The ideal 
        //       preupdate_hook() https://sqlite.org/c3ref/preupdate_count.html can give what is about to change, but is not part of this wasm build, 
        //       but that would be the way to go i think.   jsaad 10 Dec. 2024
        const pms = opType === 'insert' ? [] : params; // Strip any parameters to match the reduced select query
        const before = await this.select<any[]>(stmt as string, pms);
        await this.exec(sql, params);
        const after = await this.select<any[]>(stmt as string, pms);

        const changes = diff(this, before, after, tableName as string, opType as OpType);
        // console.log(changes);

        err = await saveChanges(this, changes);
        if (err !== undefined) return err;

        await compactChanges(this);
    }

    async first<T>(sql: string, params: any[]) {
        const { data: results, error } = await this.selectWithError<T>(sql, params);
        if (error) throw new Error(error);
        if (results.length === 0) {
            return undefined;
        }
        return results[0] as T;
    }

    async select<T>(sql: string, params: any[]) {
        const { data: results, error } = await this.selectWithError(sql, params);
        if (error) throw new Error(error);
        return results as T;
    }

    async selectWithError<T>(sql: string, params: any[]) {
        if (this.#debug) console.info(sql);

        const { results, error } = await this.send('select', { sql, params });
        return { data: results as T, error };
    }

    private async extractPks() {
        const pks: { [tblName: string]: string[] } = {};
        const tables = await this.select<{ name: string }[]>(`SELECT name FROM sqlite_master WHERE type='table' ORDER BY name`, []);
        for (const table of tables) {
            const columns = await this.select(`PRAGMA table_info('${table.name}')`, []);
            pks[table.name] = columns.filter(c => c.pk !== 0).map(c => c.name);
        }
        this.pks = pks;
    }

    private async send(type: MessageType, payload?: any): Promise<any> {
        const id = crypto.randomUUID();
        const bc = new BroadcastChannel('message_bus');
        this.#mp.postMessage({ type: type, id, ...payload });
        const data = await new Promise(resolve => {
            bc.addEventListener('message', (event) => {
                if (event.data.id === id) {
                    // Only resolve the promise once we see a reply from the message that we posted!
                    resolve(event.data);
                }
            });
        });
        bc.close();
        return data;
    }
}

export const createDb = async (workerScriptPath: string): Promise<SqliteDB> => {
    // Setup up a message channel to communicate with worker thread
    const { port1, port2 } = new MessageChannel();
    port1.start();

    const workerURL = new URL(workerScriptPath, import.meta.url);
    const w = new Worker(workerURL, { type: 'module' });
    w.postMessage('messagePort', [port2]);

    // Wait for database to be ready
    const ok = await new Promise(resolve => {
        port1.addEventListener('message', event => {
            if (event.data === 'dbReady') {
                resolve(true);
            } else {
                resolve(false)
            }
        }, { once: true });
    });

    if (!ok) {
        console.log(`Failed to receive ready event`);
        throw new Error("Failed to create the database");
    }

    console.log(`Succesfully connected to database ...`);
    return new SqliteDB(port1);
}



