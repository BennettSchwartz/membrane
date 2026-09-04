package ingestion

import (
	"strconv"

	"github.com/BennettSchwartz/membrane/pkg/schema"
)

const supportedSourceKinds = "event, tool_output, observation, working_state, agent_turn"
const supportedCaptureMemoryTypes = "episodic, working, semantic, competence, plan_graph, entity"

// IsValidSourceKind reports whether sourceKind is a known capture source shape.
// Empty is accepted because CaptureMemory treats it as the default event shape.
func IsValidSourceKind(sourceKind string) bool {
	switch sourceKind {
	case "", "event", "tool_output", "observation", "working_state", "agent_turn":
		return true
	default:
		return false
	}
}

func validateCaptureSourceKind(sourceKind string) error {
	if IsValidSourceKind(sourceKind) {
		return nil
	}
	return &invalidSourceKindError{sourceKind: sourceKind}
}

func validateCaptureProposedType(proposedType schema.MemoryType) error {
	if proposedType == "" || schema.IsValidMemoryType(proposedType) {
		return nil
	}
	return &invalidProposedTypeError{proposedType: proposedType}
}

type invalidSourceKindError struct {
	sourceKind string
}

func (e *invalidSourceKindError) Error() string {
	return "ingestion: invalid source_kind " + strconv.Quote(e.sourceKind) + " (must be one of: " + supportedSourceKinds + ")"
}

type invalidProposedTypeError struct {
	proposedType schema.MemoryType
}

func (e *invalidProposedTypeError) Error() string {
	return "ingestion: invalid proposed_type " + strconv.Quote(string(e.proposedType)) + " (must be one of: " + supportedCaptureMemoryTypes + ")"
}
