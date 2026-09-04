package grpc

import (
	"context"
	"os"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"

	pb "github.com/BennettSchwartz/membrane/api/grpc/gen/membranev1"
	"github.com/BennettSchwartz/membrane/internal/testutil"
	"github.com/BennettSchwartz/membrane/pkg/membrane"
	"github.com/BennettSchwartz/membrane/pkg/schema"
)

func TestHandlerCaptureDefaultMatchesAuthorizationAndStorage(t *testing.T) {
	dsn := os.Getenv("MEMBRANE_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("MEMBRANE_TEST_POSTGRES_DSN is required")
	}
	for _, label := range []schema.Sensitivity{schema.SensitivityHigh, schema.SensitivityPublic} {
		t.Run(string(label), func(t *testing.T) {
			testutil.ResetPostgresDatabase(t, dsn)
			cfg := membrane.DefaultConfig()
			cfg.PostgresDSN = dsn
			cfg.DefaultSensitivity = string(label)
			cfg.WriteMaxSensitivity = string(label)
			cfg.ReadMaxSensitivity = string(schema.SensitivityLow)
			m, err := membrane.New(cfg)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = m.Stop() })
			policy := newAccessPolicy(cfg)
			h := &Handler{membrane: m, access: policy}
			ctx := withAccessPrincipal(context.Background(), policy, true)
			content, err := structpb.NewValue(map[string]any{"subject": "Orchid", "predicate": "uses", "object": "PostgreSQL"})
			if err != nil {
				t.Fatal(err)
			}
			resp, err := h.CaptureMemory(ctx, &pb.CaptureMemoryRequest{Source: "producer", SourceKind: "observation", Scope: "default", Content: content})
			if err != nil {
				t.Fatal(err)
			}
			for _, rec := range append(resp.CreatedRecords, resp.PrimaryRecord) {
				if rec.Sensitivity != string(label) {
					t.Fatalf("response label = %s, want %s", rec.Sensitivity, label)
				}
				metadata, err := m.GetAuthorizationMetadata(ctx, []string{rec.Id})
				if err != nil || len(metadata) != 1 || metadata[0].Sensitivity != label {
					t.Fatalf("stored policy = %+v, err=%v", metadata, err)
				}
				_, err = h.RetrieveByID(ctx, &pb.RetrieveByIDRequest{Id: rec.Id, Trust: &pb.TrustContext{MaxSensitivity: "low", Scopes: []string{"default"}}})
				if label == schema.SensitivityHigh {
					if status.Code(err) != codes.NotFound {
						t.Fatalf("low reader error = %v, want opaque NotFound", err)
					}
				} else if err != nil {
					t.Fatalf("public read: %v", err)
				}
			}
		})
	}
}
