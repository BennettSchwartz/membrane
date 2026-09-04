import { spawn, type ChildProcess } from "node:child_process";
import fs from "node:fs";
import net from "node:net";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { MembraneClient, Sensitivity } from "@bennettschwartz/membrane";

import { HARNESS_SCOPE } from "./scenario.js";

const READY_TIMEOUT_MS = 60_000;
const BUILD_TIMEOUT_MS = 180_000;

const thisDir = path.dirname(fileURLToPath(import.meta.url));
export const repoRoot = path.resolve(thisDir, "../../..");

export interface MembraneRuntime {
  addr: string;
  apiKey?: string;
  postgresDSN?: string;
  close(): Promise<void>;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function runCommand(command: string, args: string[], options: { cwd: string; env?: NodeJS.ProcessEnv; timeoutMs: number }): Promise<void> {
  return new Promise<void>((resolve, reject) => {
    let output = "";
    const child = spawn(command, args, {
      cwd: options.cwd,
      env: options.env,
      stdio: ["ignore", "pipe", "pipe"]
    });

    const timer = setTimeout(() => {
      child.kill("SIGTERM");
      reject(new Error(`${command} ${args.join(" ")} timed out after ${options.timeoutMs}ms\n${output}`));
    }, options.timeoutMs);

    child.stdout?.on("data", (chunk: Buffer) => {
      output += chunk.toString("utf8");
    });
    child.stderr?.on("data", (chunk: Buffer) => {
      output += chunk.toString("utf8");
    });
    child.on("error", (err) => {
      clearTimeout(timer);
      reject(err);
    });
    child.on("exit", (code) => {
      clearTimeout(timer);
      if (code === 0) {
        resolve();
        return;
      }
      reject(new Error(`${command} ${args.join(" ")} exited with code ${code}\n${output}`));
    });
  });
}

async function getFreePort(): Promise<number> {
  return await new Promise<number>((resolve, reject) => {
    const server = net.createServer();
    server.listen(0, "127.0.0.1", () => {
      const address = server.address();
      if (!address || typeof address === "string") {
        server.close();
        reject(new Error("failed to allocate an ephemeral port"));
        return;
      }
      const { port } = address;
      server.close((err) => {
        if (err) {
          reject(err);
          return;
        }
        resolve(port);
      });
    });
    server.on("error", reject);
  });
}

async function waitForReady(addr: string, apiKey: string | undefined, daemon: ChildProcess | undefined, getLogs: () => string): Promise<void> {
  const deadline = Date.now() + READY_TIMEOUT_MS;
  let lastError: unknown;

  while (Date.now() < deadline) {
    const client = new MembraneClient(addr, { apiKey, timeoutMs: 1_000 });
    try {
      await client.getMetrics();
      client.close();
      return;
    } catch (err) {
      lastError = err;
      client.close();
      if (daemon?.exitCode !== null && daemon?.exitCode !== undefined) {
        break;
      }
      await sleep(250);
    }
  }

  throw new Error(`membraned did not become ready at ${addr}: ${String(lastError)}\nLogs:\n${getLogs()}`);
}

export async function startOrConnectMembrane(options: {
  scopes?: string[];
  maxSensitivity?: Sensitivity;
} = {}): Promise<MembraneRuntime> {
  const externalAddr = process.env.MEMBRANE_ADDR;
  const apiKey = process.env.MEMBRANE_API_KEY || "agent-harness-secret";

  if (externalAddr) {
    await waitForReady(externalAddr, process.env.MEMBRANE_API_KEY, undefined, () => "");
    return {
      addr: externalAddr,
      apiKey: process.env.MEMBRANE_API_KEY,
      async close() {
        // Caller owns external daemon lifecycle.
      }
    };
  }

  const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "membrane-agent-harness-"));
  const port = await getFreePort();
  const addr = `127.0.0.1:${port}`;
  const postgresDSN = process.env.MEMBRANE_TEST_POSTGRES_DSN || process.env.MEMBRANE_POSTGRES_DSN;
  if (!postgresDSN) {
    fs.rmSync(tempDir, { recursive: true, force: true });
    throw new Error("MEMBRANE_POSTGRES_DSN is required to auto-start membraned; set MEMBRANE_ADDR to use an existing daemon");
  }
  const daemonBinary = path.join(tempDir, process.platform === "win32" ? "membraned.exe" : "membraned");
  const configPath = path.join(tempDir, "membrane.json");
  const scopes = options.scopes ?? [HARNESS_SCOPE];
  const maxSensitivity = options.maxSensitivity ?? Sensitivity.LOW;
  fs.writeFileSync(configPath, JSON.stringify({
    read_scopes: scopes,
    write_scopes: scopes,
    read_max_sensitivity: maxSensitivity,
    write_max_sensitivity: maxSensitivity
  }));

  await runCommand("go", ["build", "-o", daemonBinary, "./cmd/membraned"], {
    cwd: repoRoot,
    env: process.env,
    timeoutMs: BUILD_TIMEOUT_MS
  });

  let daemonLogs = "";
  const daemon = spawn(daemonBinary, ["-config", configPath, "-addr", addr], {
    cwd: repoRoot,
    env: {
      ...process.env,
      MEMBRANE_POSTGRES_DSN: postgresDSN,
      MEMBRANE_API_KEY: apiKey
    },
    stdio: ["ignore", "pipe", "pipe"]
  });

  daemon.stdout?.on("data", (chunk: Buffer) => {
    daemonLogs += chunk.toString("utf8");
  });
  daemon.stderr?.on("data", (chunk: Buffer) => {
    daemonLogs += chunk.toString("utf8");
  });
  daemon.on("error", (err: Error) => {
    daemonLogs += `${err.stack ?? err.message}\n`;
  });

  await waitForReady(addr, apiKey, daemon, () => daemonLogs);

  return {
    addr,
    apiKey,
    postgresDSN,
    async close() {
      if (daemon.exitCode === null) {
        daemon.kill("SIGTERM");
      }
      await new Promise<void>((resolve) => {
        daemon.once("exit", () => resolve());
        setTimeout(() => resolve(), 5_000);
      });
      fs.rmSync(tempDir, { recursive: true, force: true });
    }
  };
}
