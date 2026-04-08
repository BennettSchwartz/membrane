import { spawn, type ChildProcess } from "node:child_process";
import fs from "node:fs";
import net from "node:net";
import os from "node:os";
import path from "node:path";

import { MembraneClient, MembraneError, Sensitivity } from "../src/index";

const API_KEY = "ts-integration-secret";
const BUILD_TIMEOUT_MS = 180_000;
const READY_TIMEOUT_MS = 60_000;

let daemon: ChildProcess | undefined;
let daemonAddr = "";
let daemonLogs = "";
let tempDir = "";

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function runCommand(
  command: string,
  args: string[],
  options: { cwd: string; env?: NodeJS.ProcessEnv; timeoutMs: number }
): Promise<void> {
  await new Promise<void>((resolve, reject) => {
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

async function waitForDaemonReady(addr: string, timeoutMs: number): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  let lastError: unknown;

  while (Date.now() < deadline) {
    const client = new MembraneClient(addr, { apiKey: API_KEY, timeoutMs: 1_000 });
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

  throw new Error(`membraned did not become ready at ${addr}: ${String(lastError)}\nLogs:\n${daemonLogs}`);
}

beforeAll(async () => {
  const repoRoot = path.resolve(__dirname, "../../..");
  const port = await getFreePort();
  daemonAddr = `127.0.0.1:${port}`;

  tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "membrane-ts-client-"));
  const dbPath = path.join(tempDir, "membrane.db");
  const daemonBinary = path.join(tempDir, process.platform === "win32" ? "membraned.exe" : "membraned");

  await runCommand("go", ["build", "-o", daemonBinary, "./cmd/membraned"], {
    cwd: repoRoot,
    env: process.env,
    timeoutMs: BUILD_TIMEOUT_MS
  });

  daemon = spawn(daemonBinary, ["-addr", daemonAddr, "-db", dbPath], {
    cwd: repoRoot,
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
  processRef.on("error", (err: Error) => {
    daemonLogs += `${err.stack ?? err.message}\n`;
  });

  await waitForDaemonReady(daemonAddr, READY_TIMEOUT_MS);
}, BUILD_TIMEOUT_MS + READY_TIMEOUT_MS);

afterAll(async () => {
  if (!daemon) {
    if (tempDir) {
      fs.rmSync(tempDir, { recursive: true, force: true });
    }
    return;
  }

  if (daemon.exitCode === null) {
    daemon.kill("SIGTERM");
  }

  await new Promise<void>((resolve) => {
    daemon?.once("exit", () => resolve());
    setTimeout(() => resolve(), 5_000);
  });

  if (tempDir) {
    fs.rmSync(tempDir, { recursive: true, force: true });
  }
}, 10_000);

describe("MembraneClient integration", () => {
  it("returns unauthenticated without API key", async () => {
    const client = new MembraneClient(daemonAddr, { timeoutMs: 2_000 });
    try {
      await client.getMetrics();
      throw new Error("expected unauthenticated error");
    } catch (err) {
      expect(err).toBeInstanceOf(MembraneError);
      expect((err as MembraneError).codeName).toBe("UNAUTHENTICATED");
    } finally {
      client.close();
    }
  });

  it("supports capture, graph retrieval, reinforce, penalize, and metrics", async () => {
    const client = new MembraneClient(daemonAddr, { apiKey: API_KEY, timeoutMs: 2_000 });
    try {
      const captured = await client.captureMemory(
        {
          text: "Membrane should remember the Orchid deploy target for the staging cluster.",
          project: "Orchid"
        },
        {
          reasonToRemember: "Deployment jargon should be recoverable later",
          summary: "Remember Orchid as the staging deploy target",
          tags: ["integration", "typescript", "orchid"],
          scope: "ts-integration"
        }
      );

      expect(captured.primary_record.id.length).toBeGreaterThan(0);
      expect(captured.created_records.some((record) => record.type === "entity")).toBe(true);

      const ingested = captured.primary_record;

      const secondCapture = await client.captureMemory(
        {
          text: "Use Orchid for rollout verification before production deploys.",
          project: "Orchid"
        },
        {
          reasonToRemember: "Repeated niche term should converge on the same entity",
          tags: ["integration", "orchid"],
          scope: "ts-integration"
        }
      );

      const secondEntity = secondCapture.created_records.find((record) => record.type === "entity");
      const firstEntity = captured.created_records.find((record) => record.type === "entity");
      expect(firstEntity?.id).toBeDefined();
      expect(secondEntity).toBeUndefined();
      expect(
        secondCapture.edges.find((edge) => edge.predicate === "mentions_entity" && edge.source_id === secondCapture.primary_record.id)
          ?.target_id
      ).toBe(firstEntity?.id);

      const byId = await client.retrieveById(ingested.id, {
        trust: {
          max_sensitivity: Sensitivity.MEDIUM,
          authenticated: true,
          actor_id: "ts-test",
          scopes: []
        }
      });
      expect(byId.id).toBe(ingested.id);

      const graph = await client.retrieveGraph("Orchid deploy target", {
        trust: {
          max_sensitivity: Sensitivity.MEDIUM,
          authenticated: true,
          actor_id: "ts-test",
          scopes: []
        },
        rootLimit: 10,
        nodeLimit: 20,
        edgeLimit: 20,
        maxHops: 1
      });
      expect(graph.root_ids.length).toBeGreaterThan(0);
      expect(graph.nodes.some((node) => node.record.id === ingested.id)).toBe(true);
      expect(graph.nodes.some((node) => node.record.type === "entity")).toBe(true);
      expect(graph.edges.some((edge) => edge.predicate === "mentions_entity")).toBe(true);

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
