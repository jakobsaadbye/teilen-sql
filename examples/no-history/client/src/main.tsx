import './index.css'
// @deno-types="@types/react"
import { StrictMode } from 'react'
// @deno-types="@types/react-dom/client"
import { createRoot } from 'react-dom/client'
import App from './App.tsx'
import { createDb, Syncer } from "@jakobsaadbye/teilen-sql"
import { SqliteContext, Inspector, SyncContext } from "@jakobsaadbye/teilen-sql/react"
import { tables } from "../../common/tables.ts"

// Schema setup
//////////////////
const db = await createDb("example.db");
await db.exec(tables, []);
await db.upgradeTableToCrr("todos");
await db.finalize();

// Network setup
//////////////////
const serverAddr = "127.0.0.1:3000";

const syncer = new Syncer(db, {
  pushEndpoint: `http://${serverAddr}/push-changes`,
  pullEndpoint: `http://${serverAddr}/pull-changes`,
  wsEndpoint:   `ws://${serverAddr}/start-web-socket`,
});

createRoot(document.getElementById('root') as HTMLElement).render(
  <StrictMode>
    <SqliteContext.Provider value={db}>
      <Inspector>
        <SyncContext.Provider value={syncer}>
          <App />
        </SyncContext.Provider>
      </Inspector>
    </SqliteContext.Provider>
  </StrictMode>,
)
