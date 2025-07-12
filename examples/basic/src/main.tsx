import './index.css'
import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import { App } from './App.tsx'

import { createDb } from "@jakobsaadbye/teilen-sql"
import { SqliteContext, Inspector } from "@jakobsaadbye/teilen-sql/react"
import { commonTables } from './tables.ts'

const db = await createDb(":memory:");

await db.exec(commonTables, []);
await db.finalize();


createRoot(document.getElementById('root') as HTMLElement).render(
  <StrictMode>
    <SqliteContext.Provider value={db}>
      <Inspector>
        <App />
      </Inspector>
    </SqliteContext.Provider>
  </StrictMode>,
)
