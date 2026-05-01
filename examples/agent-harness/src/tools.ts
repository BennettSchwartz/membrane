import {
  MemoryType,
  Sensitivity,
  SourceKind,
  TaskState,
  type GraphNode,
  type MemoryRecord,
  type RetrieveGraphResult
} from "@bennettschwartz/membrane";
import { MembraneClient } from "@bennettschwartz/membrane";

import {
  GRAPH_MEMORY_TYPES,
  HARNESS_ACTOR,
  HARNESS_SCOPE,
  PROJECT_NAME,
  SERVICE_NAME,
  captureFact,
  compactRecord,
  outcomeSummary,
  trustContext,
  uniqueStrings
} from "./scenario.js";

export interface ToolTraceEntry {
  name: string;
  args: Record<string, unknown>;
  result: unknown;
  forced: boolean;
  recordIds: string[];
  recordRefs: RecordRef[];
  durationMs: number;
}

export interface ToolExecution {
  content: string;
  value: unknown;
  recordIds: string[];
  recordRefs: RecordRef[];
}

export interface RecordRef {
  id: string;
  type: string;
}

type ToolDefinition = {
  type: "function";
  function: {
    name: string;
    description: string;
    parameters: Record<string, unknown>;
  };
};

function asObject(value: unknown): Record<string, unknown> {
  if (value && typeof value === "object" && !Array.isArray(value)) {
    return value as Record<string, unknown>;
  }
  return {};
}

function asString(value: unknown, fallback = ""): string {
  return typeof value === "string" && value.trim() ? value.trim() : fallback;
}

function asStringArray(value: unknown): string[] {
  return Array.isArray(value) ? value.map((item) => String(item)).filter(Boolean) : [];
}

function parseArgs(raw: string | undefined): Record<string, unknown> {
  if (!raw) {
    return {};
  }
  try {
    return asObject(JSON.parse(raw));
  } catch {
    return {};
  }
}

function summarizeGraph(graph: RetrieveGraphResult): Record<string, unknown> {
  const nodes = graph.nodes.map((node: GraphNode) => ({
    root: node.root,
    hop: node.hop,
    ...compactRecord(node.record)
  }));

  return {
    root_ids: graph.root_ids,
    nodes,
    edges: graph.edges,
    selection: graph.selection
  };
}

function recordIdsFromGraph(graph: RetrieveGraphResult): string[] {
  return graph.nodes.map((node) => node.record.id);
}

function recordIdsFromRecords(records: MemoryRecord[]): string[] {
  return records.map((record) => record.id);
}

function recordRefsFromGraph(graph: RetrieveGraphResult): RecordRef[] {
  return graph.nodes.map((node) => ({ id: node.record.id, type: String(node.record.type) }));
}

function recordRefsFromRecords(records: MemoryRecord[]): RecordRef[] {
  return records.map((record) => ({ id: record.id, type: String(record.type) }));
}

export class MembraneToolRuntime {
  readonly trace: ToolTraceEntry[] = [];

  constructor(private readonly client: MembraneClient) {}

  definitions(): ToolDefinition[] {
    return [
      {
        type: "function",
        function: {
          name: "membrane_retrieve_graph",
          description: "Retrieve relevant Membrane records as a bounded entity-connected graph.",
          parameters: {
            type: "object",
            properties: {
              task_descriptor: { type: "string" },
              memory_types: {
                type: "array",
                items: { type: "string", enum: [...GRAPH_MEMORY_TYPES] }
              },
              root_limit: { type: "number" },
              node_limit: { type: "number" },
              edge_limit: { type: "number" },
              max_hops: { type: "number" }
            },
            required: ["task_descriptor"]
          }
        }
      },
      {
        type: "function",
        function: {
          name: "membrane_capture_fact",
          description: "Teach Membrane a durable semantic fact.",
          parameters: {
            type: "object",
            properties: {
              subject: { type: "string" },
              predicate: { type: "string" },
              object: {},
              reason: { type: "string" },
              tags: { type: "array", items: { type: "string" } }
            },
            required: ["subject", "predicate", "object"]
          }
        }
      },
      {
        type: "function",
        function: {
          name: "membrane_capture_working_state",
          description: "Store the current agent task state in Membrane working memory.",
          parameters: {
            type: "object",
            properties: {
              thread_id: { type: "string" },
              state: { type: "string" },
              context_summary: { type: "string" },
              next_actions: { type: "array", items: { type: "string" } },
              open_questions: { type: "array", items: { type: "string" } },
              tags: { type: "array", items: { type: "string" } }
            },
            required: ["thread_id", "context_summary"]
          }
        }
      },
      {
        type: "function",
        function: {
          name: "membrane_capture_episode",
          description: "Store an agent turn, tool result, or execution observation as episodic memory.",
          parameters: {
            type: "object",
            properties: {
              summary: { type: "string" },
              result: {},
              tags: { type: "array", items: { type: "string" } }
            },
            required: ["summary"]
          }
        }
      },
      {
        type: "function",
        function: {
          name: "membrane_retrieve_by_id",
          description: "Fetch one exact Membrane record by ID.",
          parameters: {
            type: "object",
            properties: {
              id: { type: "string" }
            },
            required: ["id"]
          }
        }
      }
    ];
  }

  async execute(name: string, args: Record<string, unknown>, forced = false): Promise<ToolExecution> {
    const startedAt = Date.now();
    let execution: ToolExecution;

    switch (name) {
      case "membrane_retrieve_graph":
        execution = await this.retrieveGraph(args);
        break;
      case "membrane_capture_fact":
        execution = await this.captureFact(args);
        break;
      case "membrane_capture_working_state":
        execution = await this.captureWorkingState(args);
        break;
      case "membrane_capture_episode":
        execution = await this.captureEpisode(args);
        break;
      case "membrane_retrieve_by_id":
        execution = await this.retrieveById(args);
        break;
      default:
        execution = {
          content: JSON.stringify({ error: `unknown tool: ${name}` }),
          value: { error: `unknown tool: ${name}` },
          recordIds: [],
          recordRefs: []
        };
        break;
    }

    this.trace.push({
      name,
      args,
      result: execution.value,
      forced,
      recordIds: execution.recordIds,
      recordRefs: execution.recordRefs,
      durationMs: Date.now() - startedAt
    });

    return execution;
  }

  async executeToolCall(toolCall: { function?: { name?: string; arguments?: string } }): Promise<ToolExecution> {
    const name = toolCall.function?.name ?? "";
    const args = parseArgs(toolCall.function?.arguments);
    return await this.execute(name, args, false);
  }

  hasCapture(): boolean {
    return this.trace.some((entry) => entry.name.startsWith("membrane_capture_"));
  }

  private async retrieveGraph(args: Record<string, unknown>): Promise<ToolExecution> {
    const requestedTypes = asStringArray(args.memory_types);
    const memoryTypes = requestedTypes.length > 0 ? requestedTypes : [...GRAPH_MEMORY_TYPES];

    const graph = await this.client.retrieveGraph(asString(args.task_descriptor, "Project Orion auth-service incident"), {
      trust: trustContext("agent-harness-tool"),
      memoryTypes,
      rootLimit: Number(args.root_limit ?? 16),
      nodeLimit: Number(args.node_limit ?? 64),
      edgeLimit: Number(args.edge_limit ?? 160),
      maxHops: Number(args.max_hops ?? 2)
    });

    const value = summarizeGraph(graph);
    return {
      content: JSON.stringify(value),
      value,
      recordIds: recordIdsFromGraph(graph),
      recordRefs: recordRefsFromGraph(graph)
    };
  }

  private async captureFactTool(args: Record<string, unknown>): Promise<{ records: MemoryRecord[]; value: Record<string, unknown> }> {
    const subject = asString(args.subject, SERVICE_NAME);
    const predicate = asString(args.predicate, "learned_fact");
    const object = args.object ?? asString(args.value, "unknown");
    const tags = uniqueStrings(["agent-harness", "agent-taught", ...asStringArray(args.tags)]);

    const fact = await captureFact(this.client, {
      subject,
      predicate,
      object,
      reason: asString(args.reason, `${subject} ${predicate}`),
      tags
    });

    const records = [fact.capture.primary_record, ...fact.capture.created_records];
    return {
      records,
      value: {
        primary_record_id: fact.capture.primary_record.id,
        semantic_record_id: fact.semantic.id,
        created_records: records.map(compactRecord),
        edges: fact.capture.edges
      }
    };
  }

  private async captureFact(args: Record<string, unknown>): Promise<ToolExecution> {
    const captured = await this.captureFactTool(args);
    return {
      content: JSON.stringify(captured.value),
      value: captured.value,
      recordIds: recordIdsFromRecords(captured.records),
      recordRefs: recordRefsFromRecords(captured.records)
    };
  }

  private async captureWorkingState(args: Record<string, unknown>): Promise<ToolExecution> {
    const capture = await this.client.captureMemory(
      {
        thread_id: asString(args.thread_id, "agent-harness-thread"),
        state: asString(args.state, TaskState.EXECUTING),
        context_summary: asString(args.context_summary, "Agent is using Membrane graph memory."),
        next_actions: asStringArray(args.next_actions),
        open_questions: asStringArray(args.open_questions),
        project: PROJECT_NAME,
        subject: SERVICE_NAME
      },
      {
        source: HARNESS_ACTOR,
        sourceKind: SourceKind.WORKING_STATE,
        reasonToRemember: "Agent working-state update",
        summary: asString(args.context_summary, "Agent working-state update"),
        tags: uniqueStrings(["agent-harness", "agent-working", ...asStringArray(args.tags)]),
        scope: HARNESS_SCOPE,
        sensitivity: Sensitivity.LOW
      }
    );

    const records = [capture.primary_record, ...capture.created_records];
    const value = {
      primary_record_id: capture.primary_record.id,
      created_records: records.map(compactRecord),
      edges: capture.edges
    };

    return {
      content: JSON.stringify(value),
      value,
      recordIds: recordIdsFromRecords(records),
      recordRefs: recordRefsFromRecords(records)
    };
  }

  private async captureEpisode(args: Record<string, unknown>): Promise<ToolExecution> {
    const capture = await this.client.captureMemory(
      {
        ref: `agent-turn-${Date.now()}`,
        project: PROJECT_NAME,
        subject: SERVICE_NAME,
        tool_name: "membrane-agent-harness",
        result: args.result ?? outcomeSummary("captured", { summary: args.summary })
      },
      {
        source: HARNESS_ACTOR,
        sourceKind: SourceKind.AGENT_TURN,
        reasonToRemember: asString(args.summary, "Agent turn summary"),
        summary: asString(args.summary, "Agent turn summary"),
        tags: uniqueStrings(["agent-harness", "agent-episode", ...asStringArray(args.tags)]),
        scope: HARNESS_SCOPE,
        sensitivity: Sensitivity.LOW
      }
    );

    const records = [capture.primary_record, ...capture.created_records];
    const value = {
      primary_record_id: capture.primary_record.id,
      created_records: records.map(compactRecord),
      edges: capture.edges
    };

    return {
      content: JSON.stringify(value),
      value,
      recordIds: recordIdsFromRecords(records),
      recordRefs: recordRefsFromRecords(records)
    };
  }

  private async retrieveById(args: Record<string, unknown>): Promise<ToolExecution> {
    const record = await this.client.retrieveById(asString(args.id), {
      trust: trustContext("agent-harness-tool")
    });

    const value = compactRecord(record);
    return {
      content: JSON.stringify(value),
      value,
      recordIds: [record.id],
      recordRefs: [{ id: record.id, type: String(record.type) }]
    };
  }
}

export function hasFiveMemoryTypes(records: MemoryRecord[]): boolean {
  const seen = new Set(records.map((record) => record.type));
  return [
    MemoryType.EPISODIC,
    MemoryType.WORKING,
    MemoryType.SEMANTIC,
    MemoryType.COMPETENCE,
    MemoryType.PLAN_GRAPH
  ].every((type) => seen.has(type));
}
