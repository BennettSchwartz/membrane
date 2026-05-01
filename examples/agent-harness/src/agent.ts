import OpenAI from "openai";

import { MembraneToolRuntime, type ToolTraceEntry } from "./tools.js";

export interface LlmCallTrace {
  phase: string;
  durationMs: number;
  toolCalls: number;
  finishReason?: string | null;
}

export interface AgentTurnResult {
  answer: string;
  trace: ToolTraceEntry[];
  llmCalls: LlmCallTrace[];
  durationMs: number;
}

function appendTraceFooter(answer: string, trace: ToolTraceEntry[]): string {
  const refs: Array<{ id: string; type: string }> = [];
  const seen = new Set<string>();

  for (const entry of trace) {
    for (const ref of entry.recordRefs) {
      if (seen.has(ref.id)) {
        continue;
      }
      seen.add(ref.id);
      refs.push(ref);
      if (refs.length >= 12) {
        break;
      }
    }
    if (refs.length >= 12) {
      break;
    }
  }

  const cited = refs.map((ref) => `${ref.id} (${ref.type})`).join(", ");
  const toolSummary = trace.map((entry) => `${entry.name}:${entry.durationMs}ms`).join(", ");
  return `${answer.trim()}\n\nMembrane trace: ${trace.length} tool calls. Cited records: ${
    cited || "none"
  }. Tool timings: ${toolSummary}.`;
}

export async function runAgentTurn(input: {
  llm: OpenAI;
  model: string;
  runtime: MembraneToolRuntime;
  task: string;
  maxToolRounds?: number;
  maxTokens?: number;
}): Promise<AgentTurnResult> {
  const { llm, model, runtime, task } = input;
  const startedAt = Date.now();
  const llmCalls: LlmCallTrace[] = [];
  const maxToolRounds = input.maxToolRounds ?? 8;
  const maxTokens = input.maxTokens ?? 800;

  const messages: Array<Record<string, unknown>> = [
    {
      role: "system",
      content:
        "You are an agent using Membrane as long-term memory. " +
        "Membrane tools are optional, but you should use them when the user asks you to remember facts, update active state, record observations, retrieve exact records, or ground an answer in prior memory. " +
        "When a user asks you to remember something, store it as a durable memory before relying on it. " +
        "When a user asks for an exact record by ID, retrieve that record before answering. " +
        "Final answers must cite relevant memory IDs and mention which memory layers informed the answer. " +
        "Keep final answers concise and operational; avoid restating full tool payloads."
    },
    {
      role: "user",
      content: task
    }
  ];

  let answer = "";

  for (let round = 0; round < maxToolRounds; round += 1) {
    const callStartedAt = Date.now();
    const completion = await llm.chat.completions.create({
      model,
      temperature: 0.2,
      max_tokens: maxTokens,
      parallel_tool_calls: true,
      messages: messages as never,
      tools: runtime.definitions() as never,
      tool_choice: "auto"
    });

    const message = completion.choices[0]?.message;
    llmCalls.push({
      phase: `tool_round_${round + 1}`,
      durationMs: Date.now() - callStartedAt,
      toolCalls: message?.tool_calls?.length ?? 0,
      finishReason: completion.choices[0]?.finish_reason
    });

    if (!message) {
      throw new Error("LLM returned no message");
    }

    const toolCalls = message.tool_calls ?? [];
    if (toolCalls.length > 0) {
      messages.push(message as unknown as Record<string, unknown>);
      for (const toolCall of toolCalls) {
        const functionToolCall = toolCall as { id: string; function: { name: string; arguments?: string } };
        const execution = await runtime.executeToolCall(functionToolCall);
        messages.push({
          role: "tool",
          tool_call_id: functionToolCall.id,
          name: functionToolCall.function.name,
          content: execution.content
        });
      }
      continue;
    }

    answer = typeof message.content === "string" ? message.content : "";
    break;
  }

  if (!answer) {
    const callStartedAt = Date.now();
    const completion = await llm.chat.completions.create({
      model,
      temperature: 0.2,
      max_tokens: maxTokens,
      messages: [
        ...messages,
        {
          role: "user",
          content: "Now provide the final answer using the Membrane context and cite memory IDs."
        }
      ] as never,
      tools: runtime.definitions() as never,
      tool_choice: "none"
    });
    llmCalls.push({
      phase: "final_answer",
      durationMs: Date.now() - callStartedAt,
      toolCalls: completion.choices[0]?.message?.tool_calls?.length ?? 0,
      finishReason: completion.choices[0]?.finish_reason
    });
    answer = completion.choices[0]?.message?.content ?? "";
  }

  answer = appendTraceFooter(answer, runtime.trace);

  await runtime.execute(
    "membrane_capture_episode",
    {
      summary: `Agent answered task and used ${runtime.trace.length} Membrane tool calls`,
      result: {
        task,
        answer,
        trace: runtime.trace.map((entry) => ({
          name: entry.name,
          forced: entry.forced,
          recordIds: entry.recordIds,
          recordRefs: entry.recordRefs,
          durationMs: entry.durationMs
        })),
        llmCalls
      },
      tags: ["turn-summary"]
    },
    true
  );

  return {
    answer,
    trace: [...runtime.trace],
    llmCalls,
    durationMs: Date.now() - startedAt
  };
}
