import { Database } from "jsr:@db/sqlite@0.12";
import { SqliteDB, SqliteDBWrapper, applyChanges, createDocument, insertCrrTablesStmt } from "@jakobsaadbye/teilen-sql"
import { tables } from "../common/tables.ts";
import { Application, Context, Router } from "@oak/oak";
import { oakCors } from "https://deno.land/x/cors/mod.ts";
import logger from "https://deno.land/x/oak_logger/mod.ts";
import { handleStartWebSocketConnection } from "./ws.ts";


const db = new Database("example.db", { int64: true });
const wDb = new SqliteDBWrapper(db) as unknown as SqliteDB;
await wDb.exec(insertCrrTablesStmt, []);
await wDb.exec(tables, []);
await wDb.upgradeAllTablesToCrr();
await wDb.finalize();


const PORT = 3000;

const app = new Application();
const router = new Router();

router.post("/push-changes", async (ctx: Context) => {
    const { documentId, changes } = await ctx.request.body.json();
    try {
        const doc = await wDb.getDocument(documentId);
        if (!doc) {
            await createDocument(wDb, documentId, null);
        }
        await applyChanges(wDb, changes);

        ctx.response.status = 200;
    } catch (e) {
        await wDb.exec(`ROLLBACK`, []);
        console.error(e);
        ctx.response.status = 400;
        ctx.response.body = { error: e.message };
    }
});

router.get("/pull-changes", async (ctx: Context) => {
    const params = ctx.request.url.searchParams;
    const lastPulledAt = params.get("lastPulledAt");
    const siteId = params.get("siteId");
    if (lastPulledAt === undefined || siteId === undefined) {
        ctx.response.status = 400;
        ctx.response.body = { error: "Invalid query parameters. Need 'lastPulledAt' & 'siteId'" };
        return;
    }

    try {
        const now = new Date().getTime();
        const rows = await wDb.select(`SELECT * FROM "crr_changes" WHERE site_id != ? AND applied_at > ? ORDER BY created_at ASC`, [siteId, lastPulledAt]);

        ctx.response.status = 200;
        ctx.response.body = { changes: rows, pulledAt: now };
        return;
    } catch (e) {
        console.error(e);
        ctx.response.status = 400;
        ctx.response.body = { error: e.message };
    }
});

router.get("/start-web-socket", (ctx) => handleStartWebSocketConnection(ctx, wDb));

app.use(logger.logger);
app.use(logger.responseTime);
app.use(oakCors());
app.use(router.routes());
app.use(router.allowedMethods());

console.log("Listening at http://localhost:" + PORT);
await app.listen({ port: PORT });