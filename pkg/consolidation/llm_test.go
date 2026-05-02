package consolidation

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

type errorReadCloser struct{}

func (errorReadCloser) Read([]byte) (int, error) {
	return 0, errors.New("read failed")
}

func (errorReadCloser) Close() error {
	return nil
}

func TestHTTPLLMClientExtractParsesFencedTriples(t *testing.T) {
	var requestBody map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("method = %s, want POST", r.Method)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer token" {
			t.Fatalf("authorization = %q, want bearer token", got)
		}
		if err := json.NewDecoder(r.Body).Decode(&requestBody); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"` +
			"```json\\n" +
			`[{\"subject\":\"Orchid\",\"predicate\":\"uses\",\"object\":\"Borealis\"}]` +
			"\\n```" +
			`"}}]}`))
	}))
	defer server.Close()

	client := NewHTTPLLMClient(server.URL, "model-a", "token")
	got, err := client.Extract(context.Background(), "Orchid uses Borealis")
	if err != nil {
		t.Fatalf("Extract: %v", err)
	}

	want := []Triple{{Subject: "Orchid", Predicate: "uses", Object: "Borealis"}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("triples = %#v, want %#v", got, want)
	}
	if requestBody["model"] != "model-a" {
		t.Fatalf("model = %v, want model-a", requestBody["model"])
	}
	messages := requestBody["messages"].([]any)
	user := messages[1].(map[string]any)
	if user["content"] != "Orchid uses Borealis" {
		t.Fatalf("user content = %v, want source content", user["content"])
	}
}

func TestHTTPLLMClientExtractParsesMultipartContent(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"choices":[{"message":{"content":[{"type":"image","text":"ignore me"},{"type":"text","text":"[{\"subject\":\"CLI\",\"predicate\":\"runs\",\"object\":\"tests\"}]"}]}}]}`))
	}))
	defer server.Close()

	got, err := NewHTTPLLMClient(server.URL, "model", "").Extract(context.Background(), "content")
	if err != nil {
		t.Fatalf("Extract: %v", err)
	}
	want := []Triple{{Subject: "CLI", Predicate: "runs", Object: "tests"}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("triples = %#v, want %#v", got, want)
	}
}

func TestHTTPLLMClientEmptyResponses(t *testing.T) {
	for _, body := range []string{
		`{"choices":[]}`,
		`{"choices":[{"message":{"content":"   "}}]}`,
		`{"choices":[{"message":{"content":"null"}}]}`,
	} {
		t.Run(body, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				_, _ = w.Write([]byte(body))
			}))
			defer server.Close()

			got, err := NewHTTPLLMClient(server.URL, "model", "").Extract(context.Background(), "content")
			if err != nil {
				t.Fatalf("Extract: %v", err)
			}
			if len(got) != 0 {
				t.Fatalf("triples = %#v, want empty", got)
			}
		})
	}

	t.Run("request failed", func(t *testing.T) {
		client := NewHTTPLLMClient("http://example.test", "model", "")
		client.http = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			return nil, errors.New("transport failed")
		})}
		_, err := client.Extract(context.Background(), "content")
		if err == nil || !strings.Contains(err.Error(), "llm request failed") {
			t.Fatalf("error = %v, want llm request failed", err)
		}
	})

	t.Run("read failed", func(t *testing.T) {
		client := NewHTTPLLMClient("http://example.test", "model", "")
		client.http = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       errorReadCloser{},
				Header:     make(http.Header),
			}, nil
		})}
		_, err := client.Extract(context.Background(), "content")
		if err == nil || !strings.Contains(err.Error(), "read llm response") {
			t.Fatalf("error = %v, want read llm response", err)
		}
	})
}

func TestNormalizeLLMContentAndFenceVariants(t *testing.T) {
	if got := normalizeLLMContent("  text  "); got != "text" {
		t.Fatalf("normalize string = %q, want text", got)
	}
	got := normalizeLLMContent([]any{
		"ignore",
		map[string]any{"type": "image", "text": "skip"},
		map[string]any{"type": "text", "text": "one"},
		map[string]any{"text": "two"},
		map[string]any{"type": 123, "text": "three"},
		map[string]any{"type": "text"},
	})
	if got != "one\ntwo\nthree" {
		t.Fatalf("normalize multipart = %q, want joined text parts", got)
	}
	if got := normalizeLLMContent(42); got != "" {
		t.Fatalf("normalize unsupported = %q, want empty", got)
	}
	if got := stripMarkdownFences("```json\n[{\"subject\":\"a\",\"predicate\":\"b\",\"object\":\"c\"}]"); got != "[{\"subject\":\"a\",\"predicate\":\"b\",\"object\":\"c\"}]" {
		t.Fatalf("strip unclosed fence = %q, want JSON body", got)
	}
	if got := stripMarkdownFences("plain"); got != "plain" {
		t.Fatalf("strip plain = %q, want plain", got)
	}
}

func TestHTTPLLMClientErrors(t *testing.T) {
	t.Run("invalid endpoint", func(t *testing.T) {
		_, err := NewHTTPLLMClient("http://[::1", "model", "").Extract(context.Background(), "content")
		if err == nil || !strings.Contains(err.Error(), "new llm request") {
			t.Fatalf("error = %v, want request creation error", err)
		}
	})

	for _, tc := range []struct {
		name   string
		status int
		body   string
		want   string
	}{
		{name: "non-2xx", status: http.StatusBadGateway, body: "upstream down", want: "llm request failed with 502: upstream down"},
		{name: "invalid response", status: http.StatusOK, body: "{", want: "parse llm response"},
		{name: "invalid triples", status: http.StatusOK, body: `{"choices":[{"message":{"content":"not-json"}}]}`, want: "parse llm triples"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer server.Close()

			_, err := NewHTTPLLMClient(server.URL, "model", "").Extract(context.Background(), "content")
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v, want containing %q", err, tc.want)
			}
		})
	}
}
