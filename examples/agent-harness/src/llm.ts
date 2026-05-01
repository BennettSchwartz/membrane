import OpenAI from "openai";

export interface LlmConfig {
  client: OpenAI;
  model: string;
  timeoutMs: number;
  maxTokens: number;
}

export function hasLlmCredentials(): boolean {
  return Boolean(process.env.AGENT_HARNESS_API_KEY || process.env.OPENROUTER_API_KEY || process.env.OPENAI_API_KEY);
}

export function createLlmClient(): LlmConfig {
  const openRouterKey = process.env.OPENROUTER_API_KEY;
  const openAiKey = process.env.OPENAI_API_KEY;
  const explicitKey = process.env.AGENT_HARNESS_API_KEY;
  const apiKey = explicitKey || openRouterKey || openAiKey;

  if (!apiKey) {
    throw new Error("Set OPENROUTER_API_KEY, OPENAI_API_KEY, or AGENT_HARNESS_API_KEY to run the live LLM harness.");
  }

  const baseURL = process.env.AGENT_HARNESS_BASE_URL ?? (openRouterKey ? "https://openrouter.ai/api/v1" : undefined);
  const timeoutMs = positiveIntEnv("AGENT_HARNESS_REQUEST_TIMEOUT_MS", 60_000);
  const maxRetries = positiveIntEnv("AGENT_HARNESS_MAX_RETRIES", 1);
  const maxTokens = positiveIntEnv("AGENT_HARNESS_MAX_TOKENS", 1_200);
  const model =
    process.env.AGENT_HARNESS_MODEL ??
    process.env.OPENROUTER_MODEL ??
    process.env.OPENAI_MODEL ??
    (openRouterKey ? "openai/gpt-5.5" : "gpt-5.5");

  const client = new OpenAI({
    apiKey,
    baseURL,
    timeout: timeoutMs,
    maxRetries,
    defaultHeaders: openRouterKey
      ? {
          "HTTP-Referer": "https://github.com/BennettSchwartz/membrane",
          "X-OpenRouter-Title": "Membrane Agent Harness"
        }
      : undefined
  });

  return { client, model, timeoutMs, maxTokens };
}

function positiveIntEnv(name: string, fallback: number): number {
  const raw = process.env[name];
  if (!raw) {
    return fallback;
  }
  const parsed = Number.parseInt(raw, 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}
