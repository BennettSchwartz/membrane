package grpc

import (
	"context"
	"reflect"
	"testing"

	pb "github.com/BennettSchwartz/membrane/api/grpc/gen/membranev1"
	"github.com/BennettSchwartz/membrane/pkg/schema"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestHandlerMetricsTrustUsesServerReadPolicy(t *testing.T) {
	policy := &accessPolicy{
		readMax:    schema.SensitivityLow,
		readScopes: []string{"project:alpha"},
	}
	handler := &Handler{access: policy}
	ctx := withAccessPrincipal(context.Background(), policy, true)

	trust, err := handler.metricsReadTrust(ctx)
	if err != nil {
		t.Fatalf("metricsReadTrust: %v", err)
	}
	if trust == nil || trust.MaxSensitivity != schema.SensitivityLow || !reflect.DeepEqual(trust.Scopes, []string{"project:alpha"}) {
		t.Fatalf("metrics trust = %+v, want server low/project:alpha policy", trust)
	}
	if !trust.Authenticated || trust.ActorID != "grpc" {
		t.Fatalf("metrics principal = %+v, want authenticated grpc principal", trust)
	}
}

func TestHandlerGetMetricsAppliesServerReadPolicy(t *testing.T) {
	ctx := context.Background()
	handler := newHandlerTest(t)
	captureFactThroughHandler(t, ctx, handler)
	allowedSnapshot, err := handler.membrane.GetMetrics(ctx)
	if err != nil {
		t.Fatalf("baseline GetMetrics: %v", err)
	}

	for _, request := range []*pb.CaptureMemoryRequest{
		{
			Source: "tester", SourceKind: "event", Content: structpb.NewStringValue("wrong scope"),
			Scope: "project:beta", Sensitivity: string(schema.SensitivityLow),
		},
		{
			Source: "tester", SourceKind: "event", Content: structpb.NewStringValue("too sensitive"),
			Scope: "project:alpha", Sensitivity: string(schema.SensitivityHigh),
		},
	} {
		if _, err := handler.CaptureMemory(ctx, request); err != nil {
			t.Fatalf("CaptureMemory fixture: %v", err)
		}
	}
	handler.access = &accessPolicy{
		readMax:            schema.SensitivityLow,
		readScopes:         []string{"project:alpha"},
		writeMax:           schema.SensitivityLow,
		writeScopes:        []string{"project:alpha"},
		defaultSensitivity: schema.SensitivityLow,
	}

	response, err := handler.GetMetrics(ctx, &pb.GetMetricsRequest{})
	if err != nil {
		t.Fatalf("GetMetrics: %v", err)
	}
	value, ok := response.Snapshot.AsInterface().(map[string]any)
	if !ok {
		t.Fatalf("metrics snapshot = %#v, want object", response.Snapshot.AsInterface())
	}
	if got := int(value["total_records"].(float64)); got != allowedSnapshot.TotalRecords {
		t.Fatalf("policy-filtered total_records = %d, want %d alpha/low records", got, allowedSnapshot.TotalRecords)
	}
}
