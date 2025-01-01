// vite.config.ts
import { defineConfig } from "file:///Users/jsaad/Lello/client/node_modules/vite/dist/node/index.js";
import svgr from "file:///Users/jsaad/Lello/client/node_modules/vite-plugin-svgr/dist/index.js";
import react from "file:///Users/jsaad/Lello/client/node_modules/@vitejs/plugin-react/dist/index.mjs";
import deno from "file:///Users/jsaad/Lello/client/node_modules/.deno/@deno+vite-plugin@1.0.2/node_modules/@deno/vite-plugin/dist/index.js";
import { fileURLToPath, URL } from "node:url";
import topLevelAwait from "file:///Users/jsaad/Lello/client/node_modules/vite-plugin-top-level-await/exports/import.mjs";
import path from "node:path";
var __vite_injected_original_dirname = "/Users/jsaad/Lello/client";
var __vite_injected_original_import_meta_url = "file:///Users/jsaad/Lello/client/vite.config.ts";
var vite_config_default = defineConfig({
  plugins: [
    deno(),
    react(),
    svgr(),
    topLevelAwait({
      promiseExportName: "__tla",
      promiseImportName: (i) => `__tla_${i}`
    })
  ],
  resolve: {
    alias: [
      { find: "@", replacement: fileURLToPath(new URL("./src", __vite_injected_original_import_meta_url)) }
    ]
  },
  server: {
    fs: {
      allow: [
        path.resolve(__vite_injected_original_dirname),
        path.resolve(__vite_injected_original_dirname + "/../teilen-sql")
        // @Temporary - This will not be need if teilen-sql was made into an npm package such that it could live in this project
      ]
    }
  }
});
export {
  vite_config_default as default
};
//# sourceMappingURL=data:application/json;base64,ewogICJ2ZXJzaW9uIjogMywKICAic291cmNlcyI6IFsidml0ZS5jb25maWcudHMiXSwKICAic291cmNlc0NvbnRlbnQiOiBbImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCIvVXNlcnMvanNhYWQvTGVsbG8vY2xpZW50XCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ZpbGVuYW1lID0gXCIvVXNlcnMvanNhYWQvTGVsbG8vY2xpZW50L3ZpdGUuY29uZmlnLnRzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ltcG9ydF9tZXRhX3VybCA9IFwiZmlsZTovLy9Vc2Vycy9qc2FhZC9MZWxsby9jbGllbnQvdml0ZS5jb25maWcudHNcIjtpbXBvcnQgeyBkZWZpbmVDb25maWcgfSBmcm9tICd2aXRlJ1xuaW1wb3J0IHN2Z3IgZnJvbSBcInZpdGUtcGx1Z2luLXN2Z3JcIjtcbmltcG9ydCByZWFjdCBmcm9tICdAdml0ZWpzL3BsdWdpbi1yZWFjdCdcbmltcG9ydCBkZW5vIGZyb20gJ0BkZW5vL3ZpdGUtcGx1Z2luJ1xuaW1wb3J0IHsgZmlsZVVSTFRvUGF0aCwgVVJMIH0gZnJvbSBcIm5vZGU6dXJsXCI7XG5pbXBvcnQgdG9wTGV2ZWxBd2FpdCBmcm9tIFwidml0ZS1wbHVnaW4tdG9wLWxldmVsLWF3YWl0XCI7XG5pbXBvcnQgcGF0aCBmcm9tICdub2RlOnBhdGgnO1xuXG4vLyBodHRwczovL3ZpdGUuZGV2L2NvbmZpZy9cbmV4cG9ydCBkZWZhdWx0IGRlZmluZUNvbmZpZyh7XG4gIHBsdWdpbnM6IFtcbiAgICBkZW5vKCksXG4gICAgcmVhY3QoKSxcbiAgICBzdmdyKCksXG4gICAgdG9wTGV2ZWxBd2FpdCh7XG4gICAgICBwcm9taXNlRXhwb3J0TmFtZTogXCJfX3RsYVwiLFxuICAgICAgcHJvbWlzZUltcG9ydE5hbWU6IGkgPT4gYF9fdGxhXyR7aX1gXG4gICAgfSksXG4gIF0sXG4gIHJlc29sdmU6IHtcbiAgICBhbGlhczogW1xuICAgICAgeyBmaW5kOiAnQCcsIHJlcGxhY2VtZW50OiBmaWxlVVJMVG9QYXRoKG5ldyBVUkwoJy4vc3JjJywgaW1wb3J0Lm1ldGEudXJsKSkgfSxcbiAgICBdLFxuICB9LFxuICBzZXJ2ZXI6IHtcbiAgICBmczoge1xuICAgICAgYWxsb3c6IFtcbiAgICAgICAgcGF0aC5yZXNvbHZlKF9fZGlybmFtZSksXG4gICAgICAgIHBhdGgucmVzb2x2ZShfX2Rpcm5hbWUgKyBcIi8uLi90ZWlsZW4tc3FsXCIpLCAvLyBAVGVtcG9yYXJ5IC0gVGhpcyB3aWxsIG5vdCBiZSBuZWVkIGlmIHRlaWxlbi1zcWwgd2FzIG1hZGUgaW50byBhbiBucG0gcGFja2FnZSBzdWNoIHRoYXQgaXQgY291bGQgbGl2ZSBpbiB0aGlzIHByb2plY3RcbiAgICAgIF1cbiAgICB9XG4gIH1cbn0pXG4iXSwKICAibWFwcGluZ3MiOiAiO0FBQTZQLFNBQVMsb0JBQW9CO0FBQzFSLE9BQU8sVUFBVTtBQUNqQixPQUFPLFdBQVc7QUFDbEIsT0FBTyxVQUFVO0FBQ2pCLFNBQVMsZUFBZSxXQUFXO0FBQ25DLE9BQU8sbUJBQW1CO0FBQzFCLE9BQU8sVUFBVTtBQU5qQixJQUFNLG1DQUFtQztBQUFpSCxJQUFNLDJDQUEyQztBQVMzTSxJQUFPLHNCQUFRLGFBQWE7QUFBQSxFQUMxQixTQUFTO0FBQUEsSUFDUCxLQUFLO0FBQUEsSUFDTCxNQUFNO0FBQUEsSUFDTixLQUFLO0FBQUEsSUFDTCxjQUFjO0FBQUEsTUFDWixtQkFBbUI7QUFBQSxNQUNuQixtQkFBbUIsT0FBSyxTQUFTLENBQUM7QUFBQSxJQUNwQyxDQUFDO0FBQUEsRUFDSDtBQUFBLEVBQ0EsU0FBUztBQUFBLElBQ1AsT0FBTztBQUFBLE1BQ0wsRUFBRSxNQUFNLEtBQUssYUFBYSxjQUFjLElBQUksSUFBSSxTQUFTLHdDQUFlLENBQUMsRUFBRTtBQUFBLElBQzdFO0FBQUEsRUFDRjtBQUFBLEVBQ0EsUUFBUTtBQUFBLElBQ04sSUFBSTtBQUFBLE1BQ0YsT0FBTztBQUFBLFFBQ0wsS0FBSyxRQUFRLGdDQUFTO0FBQUEsUUFDdEIsS0FBSyxRQUFRLG1DQUFZLGdCQUFnQjtBQUFBO0FBQUEsTUFDM0M7QUFBQSxJQUNGO0FBQUEsRUFDRjtBQUNGLENBQUM7IiwKICAibmFtZXMiOiBbXQp9Cg==
