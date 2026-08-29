import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import path from "path";

const legacy = path.resolve(__dirname, "../../LEGACY/client/packages");

export default defineConfig({
  plugins: [react()],
  server: { port: 5173, host: true },
  resolve: {
    alias: {
      "@instantdb/react": path.join(legacy, "react/dist/esm/index.js"),
      "@instantdb/react-common": path.join(legacy, "react-common/dist/esm/index.js"),
      "@instantdb/core": path.join(legacy, "core/dist/esm/index.js"),
      "@instantdb/version": path.join(legacy, "version/dist/esm/index.js"),
    },
  },
});
