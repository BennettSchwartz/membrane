import { defineConfig } from "vitest/config";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const packageRoot = dirname(fileURLToPath(import.meta.url));

export default defineConfig({
  resolve: {
    alias: {
      "@bennettschwartz/membrane": resolve(packageRoot, "../typescript/dist/index.js"),
    },
  },
  test: {
    include: ["test/**/*.test.ts"],
  },
});
