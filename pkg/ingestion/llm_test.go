package ingestion

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/BennettSchwartz/membrane/pkg/schema"
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

func TestHTTPInterpreterInterpretParsesFencedJSON(t *testing.T) {
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
			`{\"status\":\"tentative\",\"summary\":\"Remember Orchid\",\"proposed_type\":\"semantic\",\"topical_labels\":[\"deploy\"],\"mentions\":[{\"surface\":\"Orchid\",\"entity_kind\":\"project\",\"confidence\":0.9,\"aliases\":[\"orchid\"]}],\"extraction_confidence\":0.8}` +
			"\\n```" +
			`"}}]}`))
	}))
	defer server.Close()

	interpreter := NewHTTPInterpreter(server.URL, "model-a", "token")
	got, err := interpreter.Interpret(context.Background(), InterpretRequest{
		Source:           "tester",
		SourceKind:       "event",
		Content:          map[string]any{"text": "Remember Orchid"},
		Context:          map[string]any{"thread_id": "thread-1"},
		ReasonToRemember: "deployment context",
		ProposedType:     schema.MemoryTypeSemantic,
		Summary:          "Remember Orchid",
		Tags:             []string{"deploy"},
		Scope:            "project:alpha",
		Timestamp:        time.Date(2026, 5, 1, 10, 0, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("Interpret: %v", err)
	}

	if got.Status != schema.InterpretationStatusTentative || got.ProposedType != schema.MemoryTypeSemantic {
		t.Fatalf("interpretation = %+v, want tentative semantic", got)
	}
	if len(got.Mentions) != 1 || got.Mentions[0].Surface != "Orchid" || got.Mentions[0].EntityKind != schema.EntityKindProject {
		t.Fatalf("mentions = %+v, want Orchid project mention", got.Mentions)
	}
	if requestBody["model"] != "model-a" {
		t.Fatalf("model = %v, want model-a", requestBody["model"])
	}
	messages := requestBody["messages"].([]any)
	user := messages[1].(map[string]any)
	if !strings.Contains(user["content"].(string), `"reason_to_remember":"deployment context"`) {
		t.Fatalf("user message content = %s, want serialized interpret request", user["content"])
	}
}

func TestHTTPInterpreterResolveParsesMultipartContent(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"choices":[{"message":{"content":[{"type":"image","text":"ignore me"},{"type":"text","text":" {\"status\":\"resolved\",\"mentions\":[{\"surface\":\"Orchid\",\"canonical_entity_id\":\"entity-1\",\"resolved\":true}]} "} ]}}]}`))
	}))
	defer server.Close()

	interpreter := NewHTTPInterpreter(server.URL, "model-a", "")
	got, err := interpreter.Resolve(context.Background(), ResolveRequest{
		Capture:        InterpretRequest{Source: "tester", SourceKind: "event", Timestamp: time.Now()},
		Interpretation: &schema.Interpretation{Status: schema.InterpretationStatusTentative},
		Candidates: []*schema.MemoryRecord{
			{ID: "entity-1", Type: schema.MemoryTypeEntity},
		},
	})
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if got.Status != schema.InterpretationStatusResolved {
		t.Fatalf("Status = %q, want resolved", got.Status)
	}
	if len(got.Mentions) != 1 || got.Mentions[0].CanonicalEntityID != "entity-1" {
		t.Fatalf("Mentions = %+v, want resolved entity", got.Mentions)
	}
}

func TestHTTPInterpreterEmptyResponses(t *testing.T) {
	for _, body := range []string{
		`{"choices":[]}`,
		`{"choices":[{"message":{"content":"   "}}]}`,
	} {
		t.Run(body, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				_, _ = w.Write([]byte(body))
			}))
			defer server.Close()

			got, err := NewHTTPInterpreter(server.URL, "model", "").Interpret(context.Background(), InterpretRequest{Timestamp: time.Now()})
			if err != nil {
				t.Fatalf("Interpret: %v", err)
			}
			if got == nil || !reflect.DeepEqual(*got, schema.Interpretation{}) {
				t.Fatalf("Interpretation = %+v, want empty interpretation", got)
			}
		})
	}
}

func TestHTTPInterpreterErrors(t *testing.T) {
	t.Run("invalid endpoint", func(t *testing.T) {
		_, err := NewHTTPInterpreter("http://[::1", "model", "").Interpret(context.Background(), InterpretRequest{Timestamp: time.Now()})
		if err == nil || !strings.Contains(err.Error(), "new interpreter request") {
			t.Fatalf("error = %v, want request creation error", err)
		}
	})

	for _, tc := range []struct {
		name   string
		status int
		body   string
		want   string
	}{
		{name: "non-2xx", status: http.StatusBadGateway, body: "upstream down", want: "interpreter request failed with 502: upstream down"},
		{name: "invalid response", status: http.StatusOK, body: "{", want: "parse interpreter response"},
		{name: "invalid output", status: http.StatusOK, body: `{"choices":[{"message":{"content":"not-json"}}]}`, want: "parse interpreter output"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer server.Close()

			_, err := NewHTTPInterpreter(server.URL, "model", "").Interpret(context.Background(), InterpretRequest{Timestamp: time.Now()})
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v, want containing %q", err, tc.want)
			}
		})
	}

	t.Run("request failed", func(t *testing.T) {
		interpreter := NewHTTPInterpreter("http://example.test", "model", "")
		interpreter.http = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			return nil, errors.New("transport failed")
		})}
		_, err := interpreter.Interpret(context.Background(), InterpretRequest{Timestamp: time.Now()})
		if err == nil || !strings.Contains(err.Error(), "interpreter request failed") {
			t.Fatalf("error = %v, want interpreter request failed", err)
		}
	})

	t.Run("read failed", func(t *testing.T) {
		interpreter := NewHTTPInterpreter("http://example.test", "model", "")
		interpreter.http = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       errorReadCloser{},
				Header:     make(http.Header),
			}, nil
		})}
		_, err := interpreter.Interpret(context.Background(), InterpretRequest{Timestamp: time.Now()})
		if err == nil || !strings.Contains(err.Error(), "read interpreter response") {
			t.Fatalf("error = %v, want read interpreter response", err)
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
	if got := stripMarkdownFences("```json\n{\"status\":\"resolved\"}"); got != "{\"status\":\"resolved\"}" {
		t.Fatalf("strip unclosed fence = %q, want JSON body", got)
	}
	if got := stripMarkdownFences("plain"); got != "plain" {
		t.Fatalf("strip plain = %q, want plain", got)
	}
}
