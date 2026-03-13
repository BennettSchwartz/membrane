package embedding

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

// Client generates embeddings for text.
type Client interface {
	Embed(ctx context.Context, text string) ([]float32, error)
}

// HTTPClient calls an OpenAI-compatible embeddings endpoint.
type HTTPClient struct {
	endpoint   string
	model      string
	apiKey     string
	dimensions int
	http       *http.Client
}

// NewHTTPClient creates a new HTTP embedding client.
func NewHTTPClient(endpoint, model, apiKey string, dimensions int) *HTTPClient {
	return &HTTPClient{
		endpoint:   endpoint,
		model:      model,
		apiKey:     apiKey,
		dimensions: dimensions,
		http: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

// Embed generates an embedding for text.
func (c *HTTPClient) Embed(ctx context.Context, text string) ([]float32, error) {
	body := map[string]any{
		"model": c.model,
		"input": text,
	}
	if c.dimensions > 0 {
		body["dimensions"] = c.dimensions
	}
	data, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("marshal embedding request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("new embedding request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if c.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.apiKey)
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("embedding request failed: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, fmt.Errorf("read embedding response: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("embedding request failed with %d: %s", resp.StatusCode, strings.TrimSpace(string(respBody)))
	}

	var parsed struct {
		Data []struct {
			Embedding []float32 `json:"embedding"`
		} `json:"data"`
	}
	if err := json.Unmarshal(respBody, &parsed); err != nil {
		return nil, fmt.Errorf("parse embedding response: %w", err)
	}
	if len(parsed.Data) == 0 || len(parsed.Data[0].Embedding) == 0 {
		return nil, fmt.Errorf("embedding response did not include an embedding")
	}
	return parsed.Data[0].Embedding, nil
}
