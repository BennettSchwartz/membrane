import { defineConfig } from "tsup";

export default defineConfig({
  clean: true,
  dts: true,
  entry: ["src/index.ts"],
  format: ["esm", "cjs"],
  platform: "node",
  shims: true,
  sourcemap: true,
  target: "node20"
});
