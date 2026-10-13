import { fileURLToPath } from "node:url";
import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";

const page = (path: string) => fileURLToPath(new URL(path, import.meta.url));

export default defineConfig({
  plugins: [react(), tailwindcss()],
  // The parity section reads the differential harness's coverage baseline
  // straight from ../scripts, so the number on the page is the harness's own.
  server: { fs: { allow: [".."] } },
  // Two static pages: the site, and /dashboard (where the retired hosted demo
  // links now land). Each builds to its own index.html in dist/.
  build: {
    rolldownOptions: {
      input: { main: page("index.html"), dashboard: page("dashboard/index.html") },
    },
  },
});
