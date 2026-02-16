import { spawn, type ChildProcess } from "node:child_process";
import fs from "node:fs";
import net from "node:net";
import os from "node:os";
import path from "node:path";

import { MembraneClient, Sensitivity } from "../src/index";

const API_KEY = "ts-integration-secret";

let daemon: ChildProcess | undefined;
let daemonAddr = "";
let daemonLogs = "";

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function getFreePort(): Promise<number> {
  return await new Promise<number>((resolve, reject) => {
    const server = net.createServer();
    server.listen(0, "127.0.0.1", () => {
      const address = server.address();
      if (!address || typeof address === "string") {
        server.close();
        reject(new Error("Failed to allocate ephemeral port"));
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

async function waitForDaemonReady(addr: string): Promise<void> {
  for (let attempt = 0; attempt < 80; attempt += 1) {
    const client = new MembraneClient(addr, { apiKey: API_KEY, timeoutMs: 1_000 });
    try {
      await client.getMetrics();
      client.close();
      return;
    } catch {
      client.close();
      if (daemon?.exitCode !== null && daemon?.exitCode !== undefined) {
        break;
      }
      await sleep(250);
    }
  }

  throw new Error(`membraned did not become ready. Logs:\n${daemonLogs}`);
}

beforeAll(async () => {
  const port = await getFreePort();
  daemonAddr = `127.0.0.1:${port}`;

  const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "membrane-ts-client-"));
  const dbPath = path.join(tempDir, "membrane.db");

  daemon = spawn("go", ["run", "./cmd/membraned", "-addr", daemonAddr, "-db", dbPath], {
    cwd: path.resolve(__dirname, "../../.."),
    env: {
      ...process.env,
      MEMBRANE_API_KEY: API_KEY
    },
    stdio: ["ignore", "pipe", "pipe"]
  });

  const processRef = daemon;
  if (!processRef.stdout || !processRef.stderr) {
    throw new Error("Failed to capture membraned process output streams");
  }

  processRef.stdout.on("data", (chunk: Buffer) => {
    daemonLogs += chunk.toString("utf8");
  });
  processRef.stderr.on("data", (chunk: Buffer) => {
    daemonLogs += chunk.toString("utf8");
  });

  await waitForDaemonReady(daemonAddr);
});

afterAll(async () => {
  if (!daemon) {
    return;
  }

  if (daemon.exitCode === null) {
    daemon.kill("SIGTERM");
  }

  await new Promise<void>((resolve) => {
    daemon?.once("exit", () => resolve());
    setTimeout(() => resolve(), 5_000);
  });
});

describe("MembraneClient integration", () => {
  it("returns unauthenticated without API key", async () => {
    const client = new MembraneClient(daemonAddr, { timeoutMs: 2_000 });
    try {
      await client.getMetrics();
      throw new Error("expected unauthenticated error");
    } catch (err) {
      expect(String(err)).toContain("UNAUTHENTICATED");
    } finally {
      client.close();
    }
  });

  it("supports ingest, retrieve, reinforce, penalize, and metrics", async () => {
    const client = new MembraneClient(daemonAddr, { apiKey: API_KEY, timeoutMs: 2_000 });
    try {
      const ingested = await client.ingestEvent("user_input", "session-1", {
        summary: "User asked for deployment help",
        tags: ["integration", "typescript"]
      });

      expect(ingested.id.length).toBeGreaterThan(0);

      const byId = await client.retrieveById(ingested.id, {
        trust: {
          max_sensitivity: Sensitivity.MEDIUM,
          authenticated: true,
          actor_id: "ts-test",
          scopes: []
        }
      });
      expect(byId.id).toBe(ingested.id);

      const results = await client.retrieve("deployment help", {
        trust: {
          max_sensitivity: Sensitivity.MEDIUM,
          authenticated: true,
          actor_id: "ts-test",
          scopes: []
        },
        limit: 20
      });
      expect(results.some((record) => record.id === ingested.id)).toBe(true);

      await client.reinforce(ingested.id, "ts-test", "validated in integration test");
      await client.penalize(ingested.id, 0.05, "ts-test", "exercise penalize path");

      const metrics = await client.getMetrics();
      expect(typeof metrics.total_records).toBe("number");

      await expect(client.retract(ingested.id, "ts-test", "episodic should fail")).rejects.toBeDefined();
    } finally {
      client.close();
    }
  });
});
