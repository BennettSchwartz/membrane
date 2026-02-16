#!/usr/bin/env node

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const source = path.resolve(__dirname, "../../../api/proto/membrane/v1/membrane.proto");
const destination = path.resolve(__dirname, "../proto/membrane/v1/membrane.proto");

const sourceData = fs.readFileSync(source, "utf8");
const destinationData = fs.readFileSync(destination, "utf8");

if (sourceData !== destinationData) {
  console.error("Proto copy is stale. Run: npm run sync:proto");
  process.exit(1);
}

console.log("Proto copy is in sync.");
