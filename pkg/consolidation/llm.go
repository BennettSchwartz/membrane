package consolidation

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

const semanticExtractorPrompt = "You are a fact extraction system. Given a description of an event or experience, extract structured facts as a JSON array of objects with exactly three keys: \"subject\", \"predicate\", and \"object\". Extract only facts that would be useful to remember across future sessions - persistent preferences, learned capabilities, environment facts, and recurring patterns. Do not extract transient state or one-off observations. If there are no memorable facts, return an empty JSON array. Return only the JSON array with no preamble, explanation, or markdown formatting."

// Triple is a single extracted semantic fact.
type Triple struct {
	Subject   string `json:"subject"`
	Predicate string `json:"predicate"`
	Object    string `json:"object"`
}

// LLMClient extracts semantic triples from episodic content.
type LLMClient interface {
	Extract(ctx context.Context, content string) ([]Triple, error)
}

// HTTPLLMClient calls an OpenAI-compatible chat completions endpoint.
type HTTPLLMClient struct {
	endpoint string
	model    string
	apiKey   string
	http     *http.Client
}

// NewHTTPLLMClient creates a new HTTP LLM client.
func NewHTTPLLMClient(endpoint, model, apiKey string) *HTTPLLMClient {
	return &HTTPLLMClient{
		endpoint: endpoint,
		model:    model,
		apiKey:   apiKey,
		http: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// Extract sends episodic content to the LLM and parses a JSON array of triples.
func (c *HTTPLLMClient) Extract(ctx context.Context, content string) ([]Triple, error) {
	body := map[string]any{
		"model": c.model,
		"messages": []map[string]string{
			{
				"role":    "system",
				"content": semanticExtractorPrompt,
			},
			{
				"role":    "user",
				"content": content,
			},
		},
		"temperature": 0,
	}

	data, _ := json.Marshal(body)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("new llm request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if c.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.apiKey)
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("llm request failed: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, fmt.Errorf("read llm response: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("llm request failed with %d: %s", resp.StatusCode, strings.TrimSpace(string(respBody)))
	}

	var parsed struct {
		Choices []struct {
			Message struct {
				Content any `json:"content"`
			} `json:"message"`
		} `json:"choices"`
	}
	if err := json.Unmarshal(respBody, &parsed); err != nil {
		return nil, fmt.Errorf("parse llm response: %w", err)
	}
	if len(parsed.Choices) == 0 {
		return []Triple{}, nil
	}

	contentText := normalizeLLMContent(parsed.Choices[0].Message.Content)
	contentText = stripMarkdownFences(contentText)
	if contentText == "" {
		return []Triple{}, nil
	}

	var triples []Triple
	if err := json.Unmarshal([]byte(contentText), &triples); err != nil {
		return nil, fmt.Errorf("parse llm triples: %w", err)
	}
	if triples == nil {
		return []Triple{}, nil
	}
	return triples, nil
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
	lines = lines[1:]
	if len(lines) > 0 && strings.TrimSpace(lines[len(lines)-1]) == "```" {
		lines = lines[:len(lines)-1]
	}
	return strings.TrimSpace(strings.Join(lines, "\n"))
}
