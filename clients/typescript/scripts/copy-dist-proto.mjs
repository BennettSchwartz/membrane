#!/usr/bin/env node

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const sourceDir = path.resolve(__dirname, "../proto");
const destinationDir = path.resolve(__dirname, "../dist/proto");

fs.mkdirSync(destinationDir, { recursive: true });
fs.cpSync(sourceDir, destinationDir, { recursive: true });

console.log(`Copied proto assets:\n  from: ${sourceDir}\n    to: ${destinationDir}`);
