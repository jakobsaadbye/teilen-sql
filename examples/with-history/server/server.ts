import { Database } from "jsr:@db/sqlite@0.12";
import { PullRequest, PushRequest, SqliteDB, SqliteDBWrapper, applyChanges, createDocument, insertCrrTablesStmt } from "@jakobsaadbye/teilen-sql"
import { tables } from "../common/tables.ts";
import { Application, Context, Router } from "@oak/oak";
import { oakCors } from "https://deno.land/x/cors/mod.ts";
import logger from "https://deno.land/x/oak_logger/mod.ts";


const db = new Database("example.db", { int64: true });
const wDb = new SqliteDBWrapper(db) as unknown as SqliteDB;
await wDb.exec(insertCrrTablesStmt, []);
await wDb.exec(tables, []);
await wDb.upgradeAllTablesToCrr();
await wDb.finalize();


const PORT = 3001;

const app = new Application();
const router = new Router();

router.put("/push-changes", async (ctx: Context) => {
    const push = await ctx.request.body.json() as PushRequest;

	try {
		const result = await wDb.receivePushCommits(push);
		ctx.response.status = result.code;
		ctx.response.body = result;
	} catch (error) {
		console.error(error);
		ctx.response.status = 500;
        ctx.response.body = { error: error.message };
	}
});

router.put("/pull-changes", async (ctx: Context) => {
    const pull = await ctx.request.body.json() as PullRequest;
	
	try {
		const result = await wDb.receivePullCommits(pull);
		ctx.response.status = result.code;
		ctx.response.body = result;
	} catch (error) {
		console.error(error);
		ctx.response.status = 500;
        ctx.response.body = { error: error.message };
	}
});

app.use(logger.logger);
app.use(logger.responseTime);
app.use(oakCors());
app.use(router.routes());
app.use(router.allowedMethods());

console.log("Listening at http://localhost:" + PORT);
await app.listen({ port: PORT });