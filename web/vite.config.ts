import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";

export default defineConfig({
  plugins: [react(), tailwindcss()],
  // The parity section reads the differential harness's coverage baseline
  // straight from ../scripts, so the number on the page is the harness's own.
  server: { fs: { allow: [".."] } },
});
