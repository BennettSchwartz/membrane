package ingestion

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/BennettSchwartz/membrane/pkg/schema"
)

const interpreterPrompt = "You are a memory-ingest interpretation system. Given a candidate memory capture, return a JSON object with keys: status, summary, proposed_type, topical_labels, mentions, relation_candidates, reference_candidates, extraction_confidence. Use only the allowed memory types: episodic, working, semantic, competence, plan_graph, entity. Each mention must use keys: surface, entity_kind, canonical_entity_id, confidence, aliases. Each relation candidate must use keys: predicate, target_record_id, target_entity_id, confidence, resolved. Each reference candidate must use keys: ref, target_record_id, target_entity_id, confidence, resolved. If something is unknown, leave it empty. Return only JSON."

// HTTPInterpreter calls an OpenAI-compatible endpoint for ingest interpretation.
type HTTPInterpreter struct {
	endpoint string
	model    string
	apiKey   string
	http     *http.Client
}

// NewHTTPInterpreter creates a new ingest interpreter.
func NewHTTPInterpreter(endpoint, model, apiKey string) *HTTPInterpreter {
	return &HTTPInterpreter{
		endpoint: endpoint,
		model:    model,
		apiKey:   apiKey,
		http: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// Interpret extracts ingest interpretation metadata for a capture request.
func (c *HTTPInterpreter) Interpret(ctx context.Context, req InterpretRequest) (*schema.Interpretation, error) {
	body := map[string]any{
		"model": c.model,
		"messages": []map[string]string{
			{"role": "system", "content": interpreterPrompt},
			{"role": "user", "content": mustJSON(map[string]any{
				"source":             req.Source,
				"source_kind":        req.SourceKind,
				"content":            req.Content,
				"context":            req.Context,
				"reason_to_remember": req.ReasonToRemember,
				"proposed_type":      req.ProposedType,
				"summary":            req.Summary,
				"tags":               req.Tags,
				"scope":              req.Scope,
				"timestamp":          req.Timestamp.UTC().Format(time.RFC3339Nano),
			})},
		},
		"temperature": 0,
	}

	data, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("marshal interpreter request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("new interpreter request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	if c.apiKey != "" {
		httpReq.Header.Set("Authorization", "Bearer "+c.apiKey)
	}

	resp, err := c.http.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("interpreter request failed: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, fmt.Errorf("read interpreter response: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("interpreter request failed with %d: %s", resp.StatusCode, strings.TrimSpace(string(respBody)))
	}

	var parsed struct {
		Choices []struct {
			Message struct {
				Content any `json:"content"`
			} `json:"message"`
		} `json:"choices"`
	}
	if err := json.Unmarshal(respBody, &parsed); err != nil {
		return nil, fmt.Errorf("parse interpreter response: %w", err)
	}
	if len(parsed.Choices) == 0 {
		return &schema.Interpretation{}, nil
	}

	content := normalizeLLMContent(parsed.Choices[0].Message.Content)
	content = stripMarkdownFences(content)
	if content == "" {
		return &schema.Interpretation{}, nil
	}

	var interpretation schema.Interpretation
	if err := json.Unmarshal([]byte(content), &interpretation); err != nil {
		return nil, fmt.Errorf("parse interpreter output: %w", err)
	}
	return &interpretation, nil
}

func mustJSON(value any) string {
	data, _ := json.Marshal(value)
	return string(data)
}

func normalizeLLMContent(content any) string {
	switch v := content.(type) {
	case string:
		return strings.TrimSpace(v)
	case []any:
		var b strings.Builder
		for _, part := range v {
			m, ok := part.(map[string]any)
			if !ok {
				continue
			}
			if partType, _ := m["type"].(string); partType != "" && partType != "text" {
				continue
			}
			if text, ok := m["text"].(string); ok {
				if b.Len() > 0 {
					b.WriteByte('\n')
				}
				b.WriteString(text)
			}
		}
		return strings.TrimSpace(b.String())
	default:
		return ""
	}
}

func stripMarkdownFences(s string) string {
	trimmed := strings.TrimSpace(s)
	if !strings.HasPrefix(trimmed, "```") {
		return trimmed
	}
	lines := strings.Split(trimmed, "\n")
	if len(lines) == 0 {
		return trimmed
	}
	lines = lines[1:]
	if len(lines) > 0 && strings.TrimSpace(lines[len(lines)-1]) == "```" {
		lines = lines[:len(lines)-1]
	}
	return strings.TrimSpace(strings.Join(lines, "\n"))
}
