import './index.css'
// @deno-types="@types/react"
import { StrictMode } from 'react'
// @deno-types="@types/react-dom/client"
import { createRoot } from 'react-dom/client'
import App from './App.tsx'

import { createDb } from "@jakobsaadbye/teilen-sql"
import { SqliteContext, Inspector } from "@jakobsaadbye/teilen-sql/react"

const db = await createDb("example.db");

createRoot(document.getElementById('root') as HTMLElement).render(
  <StrictMode>
    <SqliteContext.Provider value={db}>
      <Inspector>
        <App />
      </Inspector>
    </SqliteContext.Provider>
  </StrictMode>,
)
