// @deno-types="npm:@types/express@4"
import express, { NextFunction, Request, Response } from "npm:express@4.18.2";
import bodyParser from "npm:body-parser";
import cors from "npm:cors"

import { SqliteDBWrapper } from "../../../dbwrapper.ts"
import { applyChanges } from "../../../change.ts"

import { Database } from "jsr:@db/sqlite@0.12";
import { SqliteDB } from "../../../sqlitedb.ts";

const PORT = 3000;

const db = new Database("lello.db", { int64: true }); // int64 here is important for the timestamps, defaults to false, sigh ...
const sql = Deno.readTextFileSync("tables.sql");
db.exec(sql);

const wDb = new SqliteDBWrapper(db) as unknown as SqliteDB;
await wDb.upgradeAllTablesToCrr();
await wDb.upgradeColumnToFractionalIndex("columns", "position", "board_id");
await wDb.upgradeColumnToFractionalIndex("todos", "position", "column_id");
await wDb.finalizeUpgrades();


const reqLogger = function (req: Request, _res: Response, next: NextFunction) {
  console.info(`${req.method} "${req.path}"`);
  next();
};

const app = express();
app.use(cors());
app.use(reqLogger);
app.use(bodyParser.urlencoded({ extended: false }));
app.use(bodyParser.json({ limit: "1mb" }));

app.get("/", (req: Request, res: Response) => {
  res.send("Ping Pong");
});

// Push-endpoint
app.post("/changes", async (req: Request, res: Response) => {
  const { changes } = req.body;
  try {
    const err = await applyChanges(wDb, changes);
    if (err) {
      console.error(err);
      res.status(400);
      res.send(err);
    } else {
      res.sendStatus(200);
    }
  } catch (e) {
    console.error(e);
    res.status(400);
    res.send(e);
  }
});

// Pull-endpoint
app.get("/changes", (req: Request, res: Response) => {
  const { lastPulledAt, siteId } = req.query;
  if (lastPulledAt === undefined || siteId === undefined) {
    res.status(400).send(`Invalid query parameters. Need 'lastPulledAt' & 'siteId'`);
    return;
  }

  try {
    const now = new Date().getTime();
    const rows = db.prepare(`SELECT * FROM "crr_changes" WHERE site_id != ? AND applied_at >= ?`).all(siteId, lastPulledAt);

    res.status(200);
    res.send({ changes: rows, pulledAt: now });
  } catch (e) {
    console.error(e);
    res.status(400);
    res.send(e);
  }
});

app.listen(PORT, () => {
  console.log(`Listening on port: ${PORT}`);
});