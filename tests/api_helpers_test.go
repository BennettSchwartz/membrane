package tests_test

import (
	"context"
	"fmt"

	"github.com/BennettSchwartz/membrane/pkg/ingestion"
	"github.com/BennettSchwartz/membrane/pkg/membrane"
	"github.com/BennettSchwartz/membrane/pkg/retrieval"
	"github.com/BennettSchwartz/membrane/pkg/schema"
)

func captureEventRecord(ctx context.Context, m *membrane.Membrane, req ingestion.IngestEventRequest) (*schema.MemoryRecord, error) {
	resp, err := m.CaptureMemory(ctx, ingestion.CaptureMemoryRequest{
		Source:           req.Source,
		SourceKind:       "event",
		Content:          map[string]any{"ref": req.Ref, "text": req.Summary},
		ReasonToRemember: req.Summary,
		Summary:          req.Summary,
		Tags:             req.Tags,
		Scope:            req.Scope,
		Sensitivity:      req.Sensitivity,
		Timestamp:        req.Timestamp,
	})
	if err != nil {
		return nil, err
	}
	return resp.PrimaryRecord, nil
}

func captureToolOutputRecord(ctx context.Context, m *membrane.Membrane, req ingestion.IngestToolOutputRequest) (*schema.MemoryRecord, error) {
	resp, err := m.CaptureMemory(ctx, ingestion.CaptureMemoryRequest{
		Source:     req.Source,
		SourceKind: "tool_output",
		Content: map[string]any{
			"tool_name":  req.ToolName,
			"args":       req.Args,
			"result":     req.Result,
			"depends_on": req.DependsOn,
		},
		ReasonToRemember: fmt.Sprintf("Tool output from %s", req.ToolName),
		Tags:             req.Tags,
		Scope:            req.Scope,
		Sensitivity:      req.Sensitivity,
		Timestamp:        req.Timestamp,
	})
	if err != nil {
		return nil, err
	}
	return resp.PrimaryRecord, nil
}

func captureObservationRecord(ctx context.Context, m *membrane.Membrane, req ingestion.IngestObservationRequest) (*schema.MemoryRecord, error) {
	resp, err := m.CaptureMemory(ctx, ingestion.CaptureMemoryRequest{
		Source:     req.Source,
		SourceKind: "observation",
		Content: map[string]any{
			"subject":   req.Subject,
			"predicate": req.Predicate,
			"object":    req.Object,
		},
		ReasonToRemember: fmt.Sprintf("%s %s", req.Subject, req.Predicate),
		Summary:          fmt.Sprintf("%s %s", req.Subject, req.Predicate),
		Tags:             req.Tags,
		Scope:            req.Scope,
		Sensitivity:      req.Sensitivity,
		Timestamp:        req.Timestamp,
	})
	if err != nil {
		return nil, err
	}
	for _, rec := range resp.CreatedRecords {
		if rec != nil && rec.Type == schema.MemoryTypeSemantic {
			return rec, nil
		}
	}
	return nil, fmt.Errorf("capture observation did not produce semantic record")
}

func captureWorkingStateRecord(ctx context.Context, m *membrane.Membrane, req ingestion.IngestWorkingStateRequest) (*schema.MemoryRecord, error) {
	resp, err := m.CaptureMemory(ctx, ingestion.CaptureMemoryRequest{
		Source:     req.Source,
		SourceKind: "working_state",
		Content: map[string]any{
			"thread_id":          req.ThreadID,
			"state":              req.State,
			"next_actions":       req.NextActions,
			"open_questions":     req.OpenQuestions,
			"context_summary":    req.ContextSummary,
			"active_constraints": req.ActiveConstraints,
		},
		ReasonToRemember: req.ContextSummary,
		Summary:          req.ContextSummary,
		Tags:             req.Tags,
		Scope:            req.Scope,
		Sensitivity:      req.Sensitivity,
		Timestamp:        req.Timestamp,
	})
	if err != nil {
		return nil, err
	}
	return resp.PrimaryRecord, nil
}

func recordOutcome(ctx context.Context, m *membrane.Membrane, req ingestion.IngestOutcomeRequest) (*schema.MemoryRecord, error) {
	return m.RecordOutcome(ctx, req)
}

func retrieveRecords(ctx context.Context, m *membrane.Membrane, req *retrieval.RetrieveRequest) (*retrieval.RetrieveResponse, error) {
	if req == nil {
		req = &retrieval.RetrieveRequest{}
	}
	rootLimit := req.Limit
	if rootLimit <= 0 {
		rootLimit = 10000
	}
	graphResp, err := m.RetrieveGraph(ctx, &retrieval.RetrieveGraphRequest{
		TaskDescriptor: req.TaskDescriptor,
		Trust:          req.Trust,
		MemoryTypes:    req.MemoryTypes,
		MinSalience:    req.MinSalience,
		RootLimit:      rootLimit,
		NodeLimit:      rootLimit,
		EdgeLimit:      0,
		MaxHops:        0,
	})
	if err != nil {
		return nil, err
	}
	records := make([]*schema.MemoryRecord, 0, len(graphResp.Nodes))
	for _, node := range graphResp.Nodes {
		if !node.Root || node.Record == nil {
			continue
		}
		records = append(records, node.Record)
	}
	return &retrieval.RetrieveResponse{
		Records:   records,
		Selection: graphResp.Selection,
	}, nil
}
