import React from "react";
import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import './index.css'
import App from './App.tsx'

import { createDb } from "@teilen-sql"
import { SqliteContext } from "@teilen-sql-react"
import { tables } from "./db/tables.ts";

const db = await createDb('main');
await db.exec(tables, []);

await db.upgradeTableToCrr("boards");
await db.upgradeTableToCrr("todos");
await db.upgradeTableToCrr("columns");
await db.upgradeColumnToFractionalIndex("todos", "position", "column_id");
await db.upgradeColumnToFractionalIndex("columns", "position", "board_id");
await db.finalizeUpgrades();

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <SqliteContext.Provider value={db}>
      <App />
    </SqliteContext.Provider>
  </StrictMode>,
)
