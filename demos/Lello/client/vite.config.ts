import { defineConfig } from 'vite'
import svgr from "vite-plugin-svgr";
import react from '@vitejs/plugin-react'
import deno from '@deno/vite-plugin'
import { fileURLToPath, URL } from "node:url";
import topLevelAwait from "vite-plugin-top-level-await";
import path from 'node:path';

// https://vite.dev/config/
export default defineConfig({
  plugins: [
    deno(),
    react(),
    svgr(),
    topLevelAwait({
      promiseExportName: "__tla",
      promiseImportName: i => `__tla_${i}`
    }),
  ],
  resolve: {
    alias: [
      { find: '@teilen-sql', replacement: path.resolve(__dirname + "/../../../index.ts")},
      { find: '@teilen-sql-react', replacement: path.resolve(__dirname + "/../../../src/react/index.ts")},
      { find: '@', replacement: fileURLToPath(new URL('./src', import.meta.url)) },
    ],
  },
  server: {
    fs: {
      allow: [
        path.resolve(__dirname),
        path.resolve(__dirname + "/../../../"), // @Temporary - This will not be needed if teilen-sql was made into an npm package such that it could live in this project
        path.resolve(__dirname + "/../../../src/react/index.ts"), // @Temporary - This will not be needed if teilen-sql was made into an npm package such that it could live in this project
      ]
    }
  }
})
