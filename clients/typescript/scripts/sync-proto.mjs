#!/usr/bin/env node

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const source = path.resolve(__dirname, "../../../api/proto/membrane/v1/membrane.proto");
const destination = path.resolve(__dirname, "../proto/membrane/v1/membrane.proto");

fs.mkdirSync(path.dirname(destination), { recursive: true });
fs.copyFileSync(source, destination);

console.log(`Synced proto:\n  from: ${source}\n    to: ${destination}`);
