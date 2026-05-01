package ingestion

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/BennettSchwartz/membrane/pkg/schema"
	"github.com/BennettSchwartz/membrane/pkg/storage"
)

const (
	captureCandidateLimit      = 20
	captureCandidateSearchPool = 80
)

// CaptureMemoryRequest is the richer ingest request for graph-aware memory capture.
type CaptureMemoryRequest struct {
	Source           string
	SourceKind       string
	Content          any
	Context          any
	ReasonToRemember string
	ProposedType     schema.MemoryType
	Summary          string
	Tags             []string
	Scope            string
	Sensitivity      schema.Sensitivity
	Timestamp        time.Time
}

// CaptureMemoryResponse contains the source record plus any created linked records.
type CaptureMemoryResponse struct {
	PrimaryRecord  *schema.MemoryRecord
	CreatedRecords []*schema.MemoryRecord
	Edges          []schema.GraphEdge
}

// Interpreter uses an ingest-side LLM to extract structured interpretation metadata.
type Interpreter interface {
	Interpret(ctx context.Context, req InterpretRequest) (*schema.Interpretation, error)
}

// CandidateResolver resolves interpretation candidates against a bounded set of
// existing records after the first-pass interpretation step.
type CandidateResolver interface {
	Resolve(ctx context.Context, req ResolveRequest) (*schema.Interpretation, error)
}

// InterpretRequest is the input given to the ingest-side interpreter.
type InterpretRequest struct {
	Source           string
	SourceKind       string
	Content          any
	Context          any
	ReasonToRemember string
	ProposedType     schema.MemoryType
	Summary          string
	Tags             []string
	Scope            string
	Timestamp        time.Time
}

// ResolveRequest contains the original capture plus bounded candidates for a
// second-pass resolution step.
type ResolveRequest struct {
	Capture        InterpretRequest
	Interpretation *schema.Interpretation
	Candidates     []*schema.MemoryRecord
}

// CaptureMemory persists a source record, optionally interprets it, resolves
// mentions/references, and creates entity/link records on demand.
func (s *Service) CaptureMemory(ctx context.Context, req CaptureMemoryRequest) (*CaptureMemoryResponse, error) {
	if req.Sensitivity == "" {
		req.Sensitivity = schema.SensitivityLow
	}
	ts := req.Timestamp
	if ts.IsZero() {
		ts = time.Now().UTC()
	}

	interpretation, candidates, err := s.prepareCaptureResolution(ctx, req, ts)
	if err != nil {
		return nil, err
	}

	var resp *CaptureMemoryResponse
	err = storage.WithTransaction(ctx, s.store, func(tx storage.Transaction) error {
		txSvc := *s
		txSvc.store = &captureTransactionStore{
			Transaction: tx,
			lookup:      entityLookup(s.store),
		}
		var err error
		resp, err = txSvc.captureMemory(ctx, req, ts, interpretation, candidates)
		return err
	})
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (s *Service) prepareCaptureResolution(ctx context.Context, req CaptureMemoryRequest, ts time.Time) (*schema.Interpretation, []*schema.MemoryRecord, error) {
	interpretReq := captureInterpretRequest(req, ts)
	interpretation := buildFallbackInterpretation(req)
	if s.interpreter != nil {
		interpreted, err := s.interpreter.Interpret(ctx, interpretReq)
		if err == nil && interpreted != nil {
			interpretation = mergeInterpretations(interpretation, interpreted)
		}
	}

	candidates, err := s.fetchCaptureCandidates(ctx, req, interpretation)
	if err != nil {
		return nil, nil, fmt.Errorf("ingestion: fetch candidates: %w", err)
	}
	if resolver, ok := s.interpreter.(CandidateResolver); ok {
		resolved, err := resolver.Resolve(ctx, ResolveRequest{
			Capture:        interpretReq,
			Interpretation: interpretation,
			Candidates:     candidates,
		})
		if err == nil && resolved != nil {
			interpretation = mergeInterpretations(interpretation, resolved)
		}
	}
	return interpretation, candidates, nil
}

func captureInterpretRequest(req CaptureMemoryRequest, ts time.Time) InterpretRequest {
	return InterpretRequest{
		Source:           req.Source,
		SourceKind:       req.SourceKind,
		Content:          req.Content,
		Context:          req.Context,
		ReasonToRemember: req.ReasonToRemember,
		ProposedType:     req.ProposedType,
		Summary:          req.Summary,
		Tags:             req.Tags,
		Scope:            req.Scope,
		Timestamp:        ts,
	}
}

func (s *Service) captureMemory(ctx context.Context, req CaptureMemoryRequest, ts time.Time, interpretation *schema.Interpretation, candidates []*schema.MemoryRecord) (*CaptureMemoryResponse, error) {
	sourceRecord, err := s.createPrimaryRecord(ctx, req, ts)
	if err != nil {
		return nil, err
	}
	if interpretation == nil {
		interpretation = buildFallbackInterpretation(req)
	}
	candidates = append([]*schema.MemoryRecord(nil), candidates...)

	sourceRecord.Interpretation = interpretation
	if err := s.store.Update(ctx, sourceRecord); err != nil {
		return nil, fmt.Errorf("ingestion: update primary interpretation: %w", err)
	}
	candidates = append(candidates, sourceRecord)

	createdRecords := make([]*schema.MemoryRecord, 0)
	edges := make([]schema.GraphEdge, 0)

	for idx := range sourceRecord.Interpretation.Mentions {
		mention := &sourceRecord.Interpretation.Mentions[idx]
		entity, created, entityEdges, err := s.resolveMention(ctx, sourceRecord, mention, req.Scope, req.Source, req.Sensitivity, ts, candidates)
		if err != nil {
			return nil, err
		}
		if entity == nil {
			continue
		}
		mention.CanonicalEntityID = entity.ID
		if mention.Confidence == 0 {
			mention.Confidence = 1.0
		}
		if created != nil {
			createdRecords = append(createdRecords, created)
			candidates = append(candidates, created)
		}
		edges = append(edges, entityEdges...)
		sourceRecord.Relations = append(sourceRecord.Relations, schema.Relation{
			Predicate: edgePredicate(entityEdges, sourceRecord.ID),
			TargetID:  entity.ID,
			Weight:    1.0,
			CreatedAt: ts,
		})
	}

	for idx := range sourceRecord.Interpretation.ReferenceCandidates {
		ref := &sourceRecord.Interpretation.ReferenceCandidates[idx]
		target, ok := s.resolveReferenceCandidate(ctx, ref, candidates)
		if ok && target != nil && target.ID == sourceRecord.ID {
			ref.TargetRecordID = sourceRecord.ID
			ref.Resolved = true
			continue
		}
		if !ok || target == nil || target.ID == sourceRecord.ID {
			ref.Resolved = false
			ref.TargetRecordID = ""
			ref.TargetEntityID = ""
			continue
		}
		if target.Type == schema.MemoryTypeEntity {
			ref.TargetEntityID = target.ID
			ref.TargetRecordID = ""
		} else {
			ref.TargetRecordID = target.ID
		}
		ref.Resolved = true
		if ref.Confidence == 0 {
			ref.Confidence = 1.0
		}
		edgeA := schema.GraphEdge{
			SourceID:  sourceRecord.ID,
			Predicate: "references_record",
			TargetID:  target.ID,
			Weight:    1.0,
			CreatedAt: ts,
		}
		edgeB := schema.GraphEdge{
			SourceID:  target.ID,
			Predicate: "referenced_by",
			TargetID:  sourceRecord.ID,
			Weight:    1.0,
			CreatedAt: ts,
		}
		if err := s.writeEdge(ctx, edgeA); err != nil {
			return nil, err
		}
		if err := s.writeEdge(ctx, edgeB); err != nil {
			return nil, err
		}
		edges = append(edges, edgeA, edgeB)
		sourceRecord.Relations = append(sourceRecord.Relations, schema.Relation{
			Predicate: edgeA.Predicate,
			TargetID:  edgeA.TargetID,
			Weight:    edgeA.Weight,
			CreatedAt: edgeA.CreatedAt,
		})
	}
	for idx := range sourceRecord.Interpretation.RelationCandidates {
		rel := &sourceRecord.Interpretation.RelationCandidates[idx]
		relationEdges, err := s.materializeRelationCandidate(ctx, sourceRecord, rel, ts, candidates)
		if err != nil {
			return nil, err
		}
		edges = append(edges, relationEdges...)
		for _, edge := range relationEdges {
			if edge.SourceID != sourceRecord.ID {
				continue
			}
			sourceRecord.Relations = append(sourceRecord.Relations, schema.Relation{
				Predicate: edge.Predicate,
				TargetID:  edge.TargetID,
				Weight:    edge.Weight,
				CreatedAt: edge.CreatedAt,
			})
		}
	}

	if semanticRecord, semanticEdges, err := s.maybeCreateSemanticRecord(ctx, sourceRecord, req, ts); err != nil {
		return nil, err
	} else if semanticRecord != nil {
		createdRecords = append(createdRecords, semanticRecord)
		edges = append(edges, semanticEdges...)
		for _, edge := range semanticEdges {
			if edge.SourceID != sourceRecord.ID {
				continue
			}
			sourceRecord.Relations = append(sourceRecord.Relations, schema.Relation{
				Predicate: edge.Predicate,
				TargetID:  edge.TargetID,
				Weight:    edge.Weight,
				CreatedAt: edge.CreatedAt,
			})
		}
	}

	sourceRecord.Interpretation.Status = finalInterpretationStatus(sourceRecord.Interpretation)
	if err := s.store.Update(ctx, sourceRecord); err != nil {
		return nil, fmt.Errorf("ingestion: finalize interpretation: %w", err)
	}

	return &CaptureMemoryResponse{
		PrimaryRecord:  sourceRecord,
		CreatedRecords: createdRecords,
		Edges:          edges,
	}, nil
}

type captureTransactionStore struct {
	storage.Transaction
	lookup storage.EntityLookup
}

func entityLookup(store storage.Store) storage.EntityLookup {
	lookup, _ := store.(storage.EntityLookup)
	return lookup
}

func (s *captureTransactionStore) Begin(context.Context) (storage.Transaction, error) {
	return nil, fmt.Errorf("ingestion: nested capture transaction")
}

func (s *captureTransactionStore) Close() error {
	return nil
}

func (s *captureTransactionStore) FindEntitiesByTerm(ctx context.Context, term, scope string, limit int) ([]*schema.MemoryRecord, error) {
	if s.lookup == nil {
		return nil, nil
	}
	return s.lookup.FindEntitiesByTerm(ctx, term, scope, limit)
}

func (s *captureTransactionStore) FindEntityByIdentifier(ctx context.Context, namespace, value, scope string) (*schema.MemoryRecord, error) {
	if s.lookup == nil {
		return nil, storage.ErrNotFound
	}
	return s.lookup.FindEntityByIdentifier(ctx, namespace, value, scope)
}

func (s *Service) createPrimaryRecord(ctx context.Context, req CaptureMemoryRequest, ts time.Time) (*schema.MemoryRecord, error) {
	switch req.SourceKind {
	case "working_state":
		content := asObject(req.Content)
		state := schema.TaskState(fmt.Sprint(content["state"]))
		if !schema.IsValidTaskState(state) {
			state = schema.TaskStateExecuting
		}
		rec, err := s.IngestWorkingState(ctx, IngestWorkingStateRequest{
			Source:            req.Source,
			ThreadID:          stringValue(content, "thread_id", "thread"),
			State:             state,
			NextActions:       stringList(content["next_actions"]),
			OpenQuestions:     stringList(content["open_questions"]),
			ContextSummary:    firstNonEmpty(req.Summary, req.ReasonToRemember, stringValue(content, "context_summary", "summary")),
			ActiveConstraints: constraintList(content["active_constraints"]),
			Timestamp:         ts,
			Tags:              req.Tags,
			Scope:             req.Scope,
			Sensitivity:       req.Sensitivity,
		})
		if err != nil {
			return nil, fmt.Errorf("ingestion: capture working_state: %w", err)
		}
		return rec, nil
	default:
		content := asObject(req.Content)
		switch req.SourceKind {
		case "tool_output":
			rec, err := s.IngestToolOutput(ctx, IngestToolOutputRequest{
				Source:      req.Source,
				ToolName:    stringValue(content, "tool_name", "tool"),
				Args:        asObject(content["args"]),
				Result:      content["result"],
				DependsOn:   stringList(content["depends_on"]),
				Timestamp:   ts,
				Tags:        req.Tags,
				Scope:       req.Scope,
				Sensitivity: req.Sensitivity,
			})
			if err != nil {
				return nil, fmt.Errorf("ingestion: capture tool_output: %w", err)
			}
			attachCaptureContext(rec, req)
			if err := s.store.Update(ctx, rec); err != nil {
				return nil, fmt.Errorf("ingestion: update tool_output capture context: %w", err)
			}
			return rec, nil
		default:
			ref := firstNonEmpty(stringValue(content, "ref", "id"), uuid.New().String())
			rec, err := s.IngestEvent(ctx, IngestEventRequest{
				Source:      req.Source,
				EventKind:   firstNonEmpty(req.SourceKind, "event"),
				Ref:         ref,
				Summary:     firstNonEmpty(req.Summary, req.ReasonToRemember, summarizeContent(req.Content)),
				Timestamp:   ts,
				Tags:        req.Tags,
				Scope:       req.Scope,
				Sensitivity: req.Sensitivity,
			})
			if err != nil {
				return nil, fmt.Errorf("ingestion: capture event: %w", err)
			}
			attachCaptureContext(rec, req)
			if err := s.store.Update(ctx, rec); err != nil {
				return nil, fmt.Errorf("ingestion: update event capture context: %w", err)
			}
			return rec, nil
		}
	}
}

func attachCaptureContext(rec *schema.MemoryRecord, req CaptureMemoryRequest) {
	ep, ok := rec.Payload.(*schema.EpisodicPayload)
	if !ok {
		return
	}
	if ep.Environment == nil {
		ep.Environment = &schema.EnvironmentSnapshot{}
	}
	if ep.Environment.Context == nil {
		ep.Environment.Context = map[string]any{}
	}
	ep.Environment.Context["capture_content"] = req.Content
	ep.Environment.Context["capture_context"] = req.Context
	ep.Environment.Context["reason_to_remember"] = req.ReasonToRemember
	rec.Payload = ep
}

func buildFallbackInterpretation(req CaptureMemoryRequest) *schema.Interpretation {
	interpretation := &schema.Interpretation{
		Status:               schema.InterpretationStatusTentative,
		Summary:              firstNonEmpty(req.Summary, req.ReasonToRemember, summarizeContent(req.Content)),
		ProposedType:         req.ProposedType,
		TopicalLabels:        uniqueStrings(req.Tags),
		ExtractionConfidence: 0.25,
	}
	interpretation.Mentions = append(interpretation.Mentions, inferMentionsFromContent(req.Content)...)
	interpretation.ReferenceCandidates = append(interpretation.ReferenceCandidates, inferReferenceCandidates(req.Content, req.Context)...)
	if interpretation.ProposedType == "" {
		interpretation.ProposedType = inferProposedType(req.SourceKind)
	}
	return interpretation
}

func mergeInterpretations(base, override *schema.Interpretation) *schema.Interpretation {
	if base == nil {
		return override
	}
	if override == nil {
		return base
	}
	result := *base
	if override.Status != "" {
		result.Status = override.Status
	}
	if override.Summary != "" {
		result.Summary = override.Summary
	}
	if override.ProposedType != "" {
		result.ProposedType = override.ProposedType
	}
	if len(override.TopicalLabels) > 0 {
		result.TopicalLabels = uniqueStrings(append(result.TopicalLabels, override.TopicalLabels...))
	}
	if len(override.Mentions) > 0 {
		result.Mentions = override.Mentions
	}
	if len(override.RelationCandidates) > 0 {
		result.RelationCandidates = override.RelationCandidates
	}
	if len(override.ReferenceCandidates) > 0 {
		result.ReferenceCandidates = override.ReferenceCandidates
	}
	if override.ExtractionConfidence > 0 {
		result.ExtractionConfidence = override.ExtractionConfidence
	}
	return &result
}

func (s *Service) fetchCaptureCandidates(ctx context.Context, req CaptureMemoryRequest, interpretation *schema.Interpretation) ([]*schema.MemoryRecord, error) {
	records, err := s.store.List(ctx, storage.ListOptions{Scope: req.Scope, Limit: captureCandidateSearchPool})
	if err != nil {
		return nil, err
	}
	global, err := s.store.List(ctx, storage.ListOptions{Scope: "", Limit: captureCandidateSearchPool})
	if err != nil {
		return nil, err
	}
	records = append(records, global...)
	seen := make(map[string]struct{}, len(records))
	type scoredCandidate struct {
		record *schema.MemoryRecord
		score  float64
	}
	scored := make([]scoredCandidate, 0, len(records))
	for _, rec := range records {
		if rec == nil {
			continue
		}
		if _, ok := seen[rec.ID]; ok {
			continue
		}
		seen[rec.ID] = struct{}{}
		scored = append(scored, scoredCandidate{
			record: rec,
			score:  captureCandidateScore(rec, req, interpretation),
		})
	}
	sort.Slice(scored, func(i, j int) bool {
		if scored[i].score == scored[j].score {
			return scored[i].record.CreatedAt.After(scored[j].record.CreatedAt)
		}
		return scored[i].score > scored[j].score
	})
	filtered := make([]*schema.MemoryRecord, 0, min(len(scored), captureCandidateLimit))
	for _, item := range scored {
		filtered = append(filtered, item.record)
		if len(filtered) >= captureCandidateLimit {
			break
		}
	}
	return filtered, nil
}

func (s *Service) resolveMention(ctx context.Context, source *schema.MemoryRecord, mention *schema.Mention, scope, actor string, sensitivity schema.Sensitivity, ts time.Time, candidates []*schema.MemoryRecord) (*schema.MemoryRecord, *schema.MemoryRecord, []schema.GraphEdge, error) {
	entity := s.resolveMentionEntity(ctx, mention, scope, candidates)
	var created *schema.MemoryRecord
	if entity == nil {
		entity = buildEntityRecord(mention, scope, actor, sensitivity, ts)
		if err := s.store.Create(ctx, entity); err != nil {
			return nil, nil, nil, fmt.Errorf("ingestion: create entity: %w", err)
		}
		created = entity
	}

	edgeA := schema.GraphEdge{
		SourceID:  source.ID,
		Predicate: "mentions_entity",
		TargetID:  entity.ID,
		Weight:    1.0,
		CreatedAt: ts,
	}
	edgeB := schema.GraphEdge{
		SourceID:  entity.ID,
		Predicate: "mentioned_in",
		TargetID:  source.ID,
		Weight:    1.0,
		CreatedAt: ts,
	}
	if err := s.writeEdge(ctx, edgeA); err != nil {
		return nil, nil, nil, err
	}
	if err := s.writeEdge(ctx, edgeB); err != nil {
		return nil, nil, nil, err
	}
	return entity, created, []schema.GraphEdge{edgeA, edgeB}, nil
}

func (s *Service) resolveMentionEntity(ctx context.Context, mention *schema.Mention, scope string, candidates []*schema.MemoryRecord) *schema.MemoryRecord {
	if mention == nil {
		return nil
	}
	if mention.CanonicalEntityID != "" {
		if rec := findRecordByID(mention.CanonicalEntityID, candidates); rec != nil && rec.Type == schema.MemoryTypeEntity {
			return rec
		}
		if rec, err := s.store.Get(ctx, mention.CanonicalEntityID); err == nil && rec != nil && rec.Type == schema.MemoryTypeEntity {
			return rec
		}
	}
	if lookup, ok := s.store.(storage.EntityLookup); ok {
		for _, term := range uniqueStrings(append([]string{mention.Surface}, mention.Aliases...)) {
			matches, err := lookup.FindEntitiesByTerm(ctx, term, scope, 1)
			if err == nil && len(matches) > 0 && matches[0].Type == schema.MemoryTypeEntity {
				return matches[0]
			}
		}
	}
	return findMatchingEntity(mention, candidates)
}

func (s *Service) writeEdge(ctx context.Context, edge schema.GraphEdge) error {
	return s.store.AddRelation(ctx, edge.SourceID, schema.Relation{
		Predicate: edge.Predicate,
		TargetID:  edge.TargetID,
		Weight:    edge.Weight,
		CreatedAt: edge.CreatedAt,
	})
}

func edgePredicate(edges []schema.GraphEdge, sourceID string) string {
	for _, edge := range edges {
		if edge.SourceID == sourceID {
			return edge.Predicate
		}
	}
	return ""
}

func buildEntityRecord(mention *schema.Mention, scope, actor string, sensitivity schema.Sensitivity, ts time.Time) *schema.MemoryRecord {
	canonicalName := firstNonEmpty(mention.Surface, "entity")
	primaryType := entityTypeFromMentionKind(mention.EntityKind)
	if sensitivity == "" {
		sensitivity = schema.SensitivityLow
	}
	rec := schema.NewMemoryRecord(uuid.New().String(), schema.MemoryTypeEntity, sensitivity, &schema.EntityPayload{
		Kind:          "entity",
		CanonicalName: canonicalName,
		PrimaryType:   primaryType,
		Types:         []string{primaryType},
		Aliases:       entityAliases(uniqueStrings(append([]string{canonicalName}, mention.Aliases...))),
		Identifiers: []schema.EntityIdentifier{{
			Namespace: schema.EntityIdentifierNamespaceSelf,
			Value:     uuid.NewString(),
		}},
		Summary: canonicalName,
	})
	rec.Scope = scope
	rec.CreatedAt = ts
	rec.UpdatedAt = ts
	rec.Provenance.CreatedBy = actor
	rec.AuditLog = []schema.AuditEntry{{
		Action:    schema.AuditActionCreate,
		Actor:     actor,
		Timestamp: ts,
		Rationale: "Created canonical entity during capture resolution",
	}}
	return rec
}

func entityAliases(values []string) []schema.EntityAlias {
	aliases := make([]schema.EntityAlias, 0, len(values))
	for _, value := range values {
		if strings.TrimSpace(value) == "" {
			continue
		}
		aliases = append(aliases, schema.EntityAlias{Value: strings.TrimSpace(value)})
	}
	return aliases
}

func entityTypeFromMentionKind(kind schema.EntityKind) string {
	switch kind {
	case schema.EntityKindPerson:
		return schema.EntityTypePerson
	case schema.EntityKindTool:
		return schema.EntityTypeTool
	case schema.EntityKindProject:
		return schema.EntityTypeProject
	case schema.EntityKindFile:
		return schema.EntityTypeFile
	case schema.EntityKindConcept:
		return schema.EntityTypeConcept
	default:
		return schema.EntityTypeOther
	}
}

func findMatchingEntity(mention *schema.Mention, candidates []*schema.MemoryRecord) *schema.MemoryRecord {
	queryTerms := uniqueStrings(append([]string{mention.Surface}, mention.Aliases...))
	for _, rec := range candidates {
		entity, ok := rec.Payload.(*schema.EntityPayload)
		if !ok {
			continue
		}
		for _, term := range queryTerms {
			if term == "" {
				continue
			}
			if strings.EqualFold(entity.CanonicalName, term) {
				return rec
			}
			for _, alias := range entity.Aliases {
				if strings.EqualFold(alias.Value, term) {
					return rec
				}
			}
		}
	}
	return nil
}

func (s *Service) resolveReferenceCandidate(ctx context.Context, candidate *schema.ReferenceCandidate, candidates []*schema.MemoryRecord) (*schema.MemoryRecord, bool) {
	if candidate == nil {
		return nil, false
	}
	if candidate.TargetRecordID != "" {
		if rec := findRecordByID(candidate.TargetRecordID, candidates); rec != nil {
			return rec, true
		}
		if rec, err := s.store.Get(ctx, candidate.TargetRecordID); err == nil {
			return rec, true
		}
	}
	if candidate.TargetEntityID != "" {
		if rec := findRecordByID(candidate.TargetEntityID, candidates); rec != nil {
			return rec, true
		}
		if rec, err := s.store.Get(ctx, candidate.TargetEntityID); err == nil {
			return rec, true
		}
	}
	for _, rec := range candidates {
		if rec == nil {
			continue
		}
		if rec.ID == candidate.Ref || recordContainsReference(rec, candidate.Ref) {
			return rec, true
		}
	}
	return nil, false
}

func (s *Service) materializeRelationCandidate(ctx context.Context, source *schema.MemoryRecord, candidate *schema.RelationCandidate, ts time.Time, candidates []*schema.MemoryRecord) ([]schema.GraphEdge, error) {
	target, ok := s.resolveRelationCandidateTarget(ctx, candidate, candidates)
	if !ok || target == nil || source == nil || target.ID == source.ID {
		if candidate != nil {
			candidate.Resolved = false
			candidate.TargetRecordID = ""
			candidate.TargetEntityID = ""
		}
		return nil, nil
	}
	predicate := normalizeRelationPredicate(candidate.Predicate)
	if predicate == "" {
		return nil, nil
	}
	if target.Type == schema.MemoryTypeEntity {
		candidate.TargetEntityID = target.ID
		candidate.TargetRecordID = ""
	} else {
		candidate.TargetRecordID = target.ID
	}
	candidate.Resolved = true
	if candidate.Confidence == 0 {
		candidate.Confidence = 1.0
	}
	candidate.Confidence = clamp01(candidate.Confidence)
	edgeA := schema.GraphEdge{
		SourceID:  source.ID,
		Predicate: predicate,
		TargetID:  target.ID,
		Weight:    candidate.Confidence,
		CreatedAt: ts,
	}
	edgeB := schema.GraphEdge{
		SourceID:  target.ID,
		Predicate: inverseRelationPredicate(predicate),
		TargetID:  source.ID,
		Weight:    candidate.Confidence,
		CreatedAt: ts,
	}
	if err := s.writeEdge(ctx, edgeA); err != nil {
		return nil, err
	}
	if err := s.writeEdge(ctx, edgeB); err != nil {
		return nil, err
	}
	return []schema.GraphEdge{edgeA, edgeB}, nil
}

func (s *Service) resolveRelationCandidateTarget(ctx context.Context, candidate *schema.RelationCandidate, candidates []*schema.MemoryRecord) (*schema.MemoryRecord, bool) {
	if candidate == nil {
		return nil, false
	}
	if candidate.TargetRecordID != "" {
		if rec := findRecordByID(candidate.TargetRecordID, candidates); rec != nil {
			return rec, true
		}
		if rec, err := s.store.Get(ctx, candidate.TargetRecordID); err == nil {
			return rec, true
		}
	}
	if candidate.TargetEntityID != "" {
		if rec := findRecordByID(candidate.TargetEntityID, candidates); rec != nil {
			return rec, true
		}
		if rec, err := s.store.Get(ctx, candidate.TargetEntityID); err == nil {
			return rec, true
		}
	}
	return nil, false
}

func (s *Service) maybeCreateSemanticRecord(ctx context.Context, sourceRecord *schema.MemoryRecord, req CaptureMemoryRequest, ts time.Time) (*schema.MemoryRecord, []schema.GraphEdge, error) {
	subject, predicate, object, ok := extractSemanticFact(req.Content)
	if !ok {
		return nil, nil, nil
	}
	if req.SourceKind != "observation" && sourceRecord.Interpretation != nil &&
		sourceRecord.Interpretation.ProposedType != schema.MemoryTypeSemantic &&
		sourceRecord.Interpretation.ExtractionConfidence < 0.5 {
		return nil, nil, nil
	}

	rec := schema.NewMemoryRecord(uuid.New().String(), schema.MemoryTypeSemantic, req.Sensitivity, &schema.SemanticPayload{
		Kind:      "semantic",
		Subject:   canonicalizeSemanticSide(subject, sourceRecord.Interpretation),
		Predicate: predicate,
		Object:    canonicalizeSemanticObject(object, sourceRecord.Interpretation),
		Validity:  schema.Validity{Mode: schema.ValidityModeGlobal},
		Evidence: []schema.ProvenanceRef{{
			SourceType: string(sourceRecord.Type),
			SourceID:   sourceRecord.ID,
			Timestamp:  ts,
		}},
	})
	rec.Scope = req.Scope
	rec.Tags = uniqueStrings(append(uniqueStrings(req.Tags), interpretationLabels(sourceRecord.Interpretation)...))
	rec.CreatedAt = ts
	rec.UpdatedAt = ts
	rec.Confidence = maxFloat(sourceRecord.Confidence, interpretationConfidence(sourceRecord.Interpretation, 0.7))
	rec.Salience = maxFloat(sourceRecord.Salience, 0.9)
	rec.Interpretation = deriveSemanticInterpretation(sourceRecord.Interpretation, rec)
	rec.Provenance.CreatedBy = req.Source
	rec.AuditLog = []schema.AuditEntry{{
		Action:    schema.AuditActionCreate,
		Actor:     req.Source,
		Timestamp: ts,
		Rationale: "Derived semantic record during capture interpretation",
	}}
	if err := s.store.Create(ctx, rec); err != nil {
		return nil, nil, fmt.Errorf("ingestion: create semantic record: %w", err)
	}

	edgeA := schema.GraphEdge{
		SourceID:  sourceRecord.ID,
		Predicate: "derived_semantic",
		TargetID:  rec.ID,
		Weight:    1.0,
		CreatedAt: ts,
	}
	edgeB := schema.GraphEdge{
		SourceID:  rec.ID,
		Predicate: "derived_from",
		TargetID:  sourceRecord.ID,
		Weight:    1.0,
		CreatedAt: ts,
	}
	if err := s.writeEdge(ctx, edgeA); err != nil {
		return nil, nil, err
	}
	if err := s.writeEdge(ctx, edgeB); err != nil {
		return nil, nil, err
	}
	rec.Relations = append(rec.Relations, schema.Relation{
		Predicate: edgeB.Predicate,
		TargetID:  edgeB.TargetID,
		Weight:    edgeB.Weight,
		CreatedAt: edgeB.CreatedAt,
	})
	edges := []schema.GraphEdge{edgeA, edgeB}
	entityEdges, err := s.linkRecordToCanonicalEntities(ctx, rec, ts)
	if err != nil {
		return nil, nil, err
	}
	edges = append(edges, entityEdges...)
	return rec, edges, nil
}

func inferMentionsFromContent(content any) []schema.Mention {
	obj := asObject(content)
	candidates := make([]schema.Mention, 0)
	for key, value := range obj {
		if !strings.Contains(key, "entity") && key != "subject" && key != "tool_name" && key != "project" && key != "file" {
			continue
		}
		text := strings.TrimSpace(fmt.Sprint(value))
		if text == "" {
			continue
		}
		candidates = append(candidates, schema.Mention{
			Surface:    text,
			EntityKind: inferEntityKind(key),
			Confidence: 0.5,
		})
	}
	return uniqueMentions(candidates)
}

func inferReferenceCandidates(content any, context any) []schema.ReferenceCandidate {
	refs := make([]schema.ReferenceCandidate, 0)
	for _, source := range []any{content, context} {
		obj := asObject(source)
		for key, value := range obj {
			if !strings.Contains(key, "ref") && !strings.Contains(key, "record_id") && key != "id" {
				continue
			}
			text := strings.TrimSpace(fmt.Sprint(value))
			if text == "" {
				continue
			}
			refs = append(refs, schema.ReferenceCandidate{Ref: text, Confidence: 0.5})
		}
	}
	return refs
}

func inferEntityKind(key string) schema.EntityKind {
	lower := strings.ToLower(key)
	switch {
	case strings.Contains(lower, "tool"):
		return schema.EntityKindTool
	case strings.Contains(lower, "project"):
		return schema.EntityKindProject
	case strings.Contains(lower, "file"), strings.Contains(lower, "path"):
		return schema.EntityKindFile
	case strings.Contains(lower, "person"), strings.Contains(lower, "user"), strings.Contains(lower, "agent"), strings.Contains(lower, "subject"):
		return schema.EntityKindPerson
	default:
		return schema.EntityKindConcept
	}
}

func inferProposedType(sourceKind string) schema.MemoryType {
	switch sourceKind {
	case "working_state":
		return schema.MemoryTypeWorking
	case "observation":
		return schema.MemoryTypeSemantic
	default:
		return schema.MemoryTypeEpisodic
	}
}

func captureCandidateScore(rec *schema.MemoryRecord, req CaptureMemoryRequest, interpretation *schema.Interpretation) float64 {
	score := 0.0
	if req.Scope != "" && rec.Scope == req.Scope {
		score += 100
	}
	if rec.Type == schema.MemoryTypeEntity {
		score += 15
	}
	if interpretation != nil && interpretation.ProposedType != "" && rec.Type == interpretation.ProposedType {
		score += 10
	}
	for _, tag := range req.Tags {
		for _, recTag := range rec.Tags {
			if strings.EqualFold(tag, recTag) {
				score += 8
				break
			}
		}
	}
	for _, term := range candidateQueryTerms(req, interpretation) {
		if recordMatchesTerm(rec, term) {
			score += 25
		}
	}
	if !rec.CreatedAt.IsZero() {
		score += 1.0 / (1.0 + time.Since(rec.CreatedAt).Hours()/24.0)
	}
	return score
}

func candidateQueryTerms(req CaptureMemoryRequest, interpretation *schema.Interpretation) []string {
	terms := make([]string, 0)
	terms = append(terms, req.Tags...)
	terms = append(terms, stringList(asObject(req.Context)["active_entities"])...)
	terms = append(terms, stringValue(asObject(req.Content), "subject", "project", "tool_name", "file"))
	if interpretation != nil {
		terms = append(terms, interpretation.TopicalLabels...)
		for _, mention := range interpretation.Mentions {
			terms = append(terms, mention.Surface)
			terms = append(terms, mention.Aliases...)
		}
		for _, ref := range interpretation.ReferenceCandidates {
			terms = append(terms, ref.Ref)
		}
	}
	return uniqueStrings(terms)
}

func recordMatchesTerm(rec *schema.MemoryRecord, term string) bool {
	needle := normalizeMatchTerm(term)
	if needle == "" || rec == nil {
		return false
	}
	for _, candidate := range recordSearchTerms(rec) {
		if containsNormalized(candidate, needle) {
			return true
		}
	}
	return false
}

func recordSearchTerms(rec *schema.MemoryRecord) []string {
	terms := []string{rec.ID, rec.Scope}
	terms = append(terms, rec.Tags...)
	if rec.Interpretation != nil {
		terms = append(terms, rec.Interpretation.Summary)
		terms = append(terms, rec.Interpretation.TopicalLabels...)
		for _, mention := range rec.Interpretation.Mentions {
			terms = append(terms, mention.Surface)
			terms = append(terms, mention.Aliases...)
		}
	}
	switch payload := rec.Payload.(type) {
	case *schema.EntityPayload:
		terms = append(terms, payload.CanonicalName)
		terms = append(terms, payload.PrimaryType)
		terms = append(terms, payload.Types...)
		terms = append(terms, schema.EntityAliasValues(payload.Aliases)...)
		terms = append(terms, payload.Summary)
	case *schema.SemanticPayload:
		terms = append(terms, payload.Subject, payload.Predicate, fmt.Sprint(payload.Object))
	case *schema.EpisodicPayload:
		for _, item := range payload.Timeline {
			terms = append(terms, item.Ref, item.EventKind, item.Summary)
		}
	case *schema.WorkingPayload:
		terms = append(terms, payload.ThreadID, payload.ContextSummary)
		terms = append(terms, payload.NextActions...)
		terms = append(terms, payload.OpenQuestions...)
	case *schema.CompetencePayload:
		terms = append(terms, payload.SkillName)
		terms = append(terms, payload.RequiredTools...)
	case *schema.PlanGraphPayload:
		terms = append(terms, payload.PlanID, payload.Intent)
	}
	return uniqueStrings(terms)
}

func recordContainsReference(rec *schema.MemoryRecord, ref string) bool {
	ref = normalizeMatchTerm(ref)
	if ref == "" || rec == nil {
		return false
	}
	switch payload := rec.Payload.(type) {
	case *schema.EpisodicPayload:
		for _, item := range payload.Timeline {
			if normalizeMatchTerm(item.Ref) == ref {
				return true
			}
		}
	}
	return false
}

func containsNormalized(candidate, needle string) bool {
	value := normalizeMatchTerm(candidate)
	return value != "" && (value == needle || strings.Contains(value, needle) || strings.Contains(needle, value))
}

func normalizeMatchTerm(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}

func normalizeRelationPredicate(value string) string {
	return strings.TrimSpace(strings.ToLower(value))
}

func inverseRelationPredicate(predicate string) string {
	switch normalizeRelationPredicate(predicate) {
	case "mentions_entity":
		return "mentioned_in"
	case "mentioned_in":
		return "mentions_entity"
	case "references_record":
		return "referenced_by"
	case "referenced_by":
		return "references_record"
	case "derived_semantic":
		return "derived_from"
	case "derived_from":
		return "derived_semantic"
	case "subject_entity":
		return "fact_subject_of"
	case "fact_subject_of":
		return "subject_entity"
	case "object_entity":
		return "fact_object_of"
	case "fact_object_of":
		return "object_entity"
	case "depends_on":
		return "dependency_of"
	case "dependency_of":
		return "depends_on"
	case "uses":
		return "used_by"
	case "used_by":
		return "uses"
	case "caused_by":
		return "causes"
	case "causes":
		return "caused_by"
	case "supports":
		return "supported_by"
	case "supported_by":
		return "supports"
	case "contradicts":
		return "contradicted_by"
	case "contradicted_by":
		return "contradicts"
	default:
		return "inverse_of_" + predicate
	}
}

func findRecordByID(id string, candidates []*schema.MemoryRecord) *schema.MemoryRecord {
	for _, rec := range candidates {
		if rec != nil && rec.ID == id {
			return rec
		}
	}
	return nil
}

func extractSemanticFact(content any) (string, string, any, bool) {
	obj := asObject(content)
	if fact := asObject(obj["fact"]); len(fact) > 0 {
		obj = fact
	}
	subject := stringValue(obj, "subject")
	predicate := stringValue(obj, "predicate")
	object, ok := obj["object"]
	if subject == "" || predicate == "" || !ok {
		return "", "", nil, false
	}
	return subject, predicate, object, true
}

func canonicalizeSemanticSide(value string, interpretation *schema.Interpretation) string {
	if interpretation == nil {
		return value
	}
	for _, mention := range interpretation.Mentions {
		if strings.EqualFold(mention.Surface, value) && mention.CanonicalEntityID != "" {
			return mention.CanonicalEntityID
		}
		for _, alias := range mention.Aliases {
			if strings.EqualFold(alias, value) && mention.CanonicalEntityID != "" {
				return mention.CanonicalEntityID
			}
		}
	}
	return value
}

func canonicalizeSemanticObject(value any, interpretation *schema.Interpretation) any {
	text, ok := value.(string)
	if !ok {
		return value
	}
	return canonicalizeSemanticSide(text, interpretation)
}

func interpretationLabels(interpretation *schema.Interpretation) []string {
	if interpretation == nil {
		return nil
	}
	return interpretation.TopicalLabels
}

func interpretationConfidence(interpretation *schema.Interpretation, fallback float64) float64 {
	if interpretation == nil || interpretation.ExtractionConfidence <= 0 {
		return fallback
	}
	return interpretation.ExtractionConfidence
}

func deriveSemanticInterpretation(source *schema.Interpretation, rec *schema.MemoryRecord) *schema.Interpretation {
	if rec == nil {
		return nil
	}
	derived := &schema.Interpretation{
		Status:               schema.InterpretationStatusResolved,
		ProposedType:         schema.MemoryTypeSemantic,
		ExtractionConfidence: interpretationConfidence(source, 0.7),
	}
	if source != nil {
		derived.Summary = source.Summary
		derived.TopicalLabels = append([]string(nil), source.TopicalLabels...)
	}
	for _, entityID := range semanticEntityIDs(rec) {
		derived.Mentions = append(derived.Mentions, schema.Mention{
			Surface:           entityID,
			CanonicalEntityID: entityID,
			Confidence:        derived.ExtractionConfidence,
		})
	}
	if len(derived.Mentions) == 0 && derived.Summary == "" && len(derived.TopicalLabels) == 0 {
		return nil
	}
	return derived
}

type semanticEntityLink struct {
	id        string
	predicate string
	inverse   string
}

func semanticEntityIDs(rec *schema.MemoryRecord) []string {
	links := semanticEntityLinks(rec)
	ids := make([]string, 0, len(links))
	for _, link := range links {
		ids = append(ids, link.id)
	}
	return uniqueStrings(ids)
}

func semanticEntityLinks(rec *schema.MemoryRecord) []semanticEntityLink {
	if rec == nil {
		return nil
	}
	payload, ok := rec.Payload.(*schema.SemanticPayload)
	if !ok {
		return nil
	}
	links := make([]semanticEntityLink, 0, 2)
	if looksLikeCanonicalEntityID(payload.Subject) {
		links = append(links, semanticEntityLink{id: payload.Subject, predicate: "subject_entity", inverse: "fact_subject_of"})
	}
	if objectID, ok := payload.Object.(string); ok && looksLikeCanonicalEntityID(objectID) {
		links = append(links, semanticEntityLink{id: objectID, predicate: "object_entity", inverse: "fact_object_of"})
	}
	return links
}

func looksLikeCanonicalEntityID(value string) bool {
	value = strings.TrimSpace(value)
	return value != "" && (strings.HasPrefix(value, "entity-") || strings.Count(value, "-") >= 4)
}

func (s *Service) linkRecordToCanonicalEntities(ctx context.Context, rec *schema.MemoryRecord, ts time.Time) ([]schema.GraphEdge, error) {
	entityLinks := semanticEntityLinks(rec)
	if len(entityLinks) == 0 {
		return nil, nil
	}
	edges := make([]schema.GraphEdge, 0, len(entityLinks)*2)
	for _, link := range entityLinks {
		entity, err := s.store.Get(ctx, link.id)
		if err != nil || entity == nil || entity.Type != schema.MemoryTypeEntity {
			continue
		}
		edgeA := schema.GraphEdge{
			SourceID:  rec.ID,
			Predicate: link.predicate,
			TargetID:  entity.ID,
			Weight:    1.0,
			CreatedAt: ts,
		}
		edgeB := schema.GraphEdge{
			SourceID:  entity.ID,
			Predicate: link.inverse,
			TargetID:  rec.ID,
			Weight:    1.0,
			CreatedAt: ts,
		}
		if err := s.writeEdge(ctx, edgeA); err != nil {
			return nil, err
		}
		if err := s.writeEdge(ctx, edgeB); err != nil {
			return nil, err
		}
		rec.Relations = append(rec.Relations, schema.Relation{
			Predicate: edgeA.Predicate,
			TargetID:  edgeA.TargetID,
			Weight:    edgeA.Weight,
			CreatedAt: edgeA.CreatedAt,
		})
		edges = append(edges, edgeA, edgeB)
	}
	if len(edges) > 0 {
		rec.UpdatedAt = ts
		if err := s.store.Update(ctx, rec); err != nil {
			return nil, fmt.Errorf("ingestion: update semantic entity links: %w", err)
		}
	}
	return edges, nil
}

func finalInterpretationStatus(interpretation *schema.Interpretation) schema.InterpretationStatus {
	if interpretation == nil {
		return schema.InterpretationStatusTentative
	}
	for _, mention := range interpretation.Mentions {
		if strings.TrimSpace(mention.Surface) != "" && mention.CanonicalEntityID == "" {
			return schema.InterpretationStatusTentative
		}
	}
	for _, ref := range interpretation.ReferenceCandidates {
		if strings.TrimSpace(ref.Ref) != "" && !ref.Resolved {
			return schema.InterpretationStatusTentative
		}
	}
	for _, rel := range interpretation.RelationCandidates {
		if strings.TrimSpace(rel.Predicate) != "" && !rel.Resolved {
			return schema.InterpretationStatusTentative
		}
	}
	return schema.InterpretationStatusResolved
}

func maxFloat(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}

func clamp01(value float64) float64 {
	if value < 0 {
		return 0
	}
	if value > 1 {
		return 1
	}
	return value
}

func summarizeContent(content any) string {
	if s, ok := content.(string); ok {
		return s
	}
	if data, err := json.Marshal(content); err == nil {
		return string(data)
	}
	return ""
}

func asObject(value any) map[string]any {
	switch v := value.(type) {
	case map[string]any:
		return v
	case map[string]string:
		out := make(map[string]any, len(v))
		for key, val := range v {
			out[key] = val
		}
		return out
	default:
		return map[string]any{}
	}
}

func stringValue(obj map[string]any, keys ...string) string {
	for _, key := range keys {
		if val, ok := obj[key]; ok {
			text := strings.TrimSpace(fmt.Sprint(val))
			if text != "" {
				return text
			}
		}
	}
	return ""
}

func stringList(value any) []string {
	switch v := value.(type) {
	case []string:
		return uniqueStrings(v)
	case []any:
		out := make([]string, 0, len(v))
		for _, item := range v {
			text := strings.TrimSpace(fmt.Sprint(item))
			if text != "" {
				out = append(out, text)
			}
		}
		return uniqueStrings(out)
	default:
		return nil
	}
}

func constraintList(value any) []schema.Constraint {
	items, ok := value.([]any)
	if !ok {
		return nil
	}
	out := make([]schema.Constraint, 0, len(items))
	for _, item := range items {
		obj, ok := item.(map[string]any)
		if !ok {
			continue
		}
		out = append(out, schema.Constraint{
			Type:     stringValue(obj, "type"),
			Key:      stringValue(obj, "key"),
			Value:    obj["value"],
			Required: boolValue(obj["required"]),
		})
	}
	return out
}

func boolValue(value any) bool {
	b, _ := value.(bool)
	return b
}

func uniqueStrings(items []string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(items))
	for _, item := range items {
		trimmed := strings.TrimSpace(item)
		if trimmed == "" {
			continue
		}
		key := strings.ToLower(trimmed)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, trimmed)
	}
	return out
}

func uniqueMentions(items []schema.Mention) []schema.Mention {
	seen := map[string]struct{}{}
	out := make([]schema.Mention, 0, len(items))
	for _, item := range items {
		key := strings.ToLower(strings.TrimSpace(item.Surface))
		if key == "" {
			continue
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, item)
	}
	return out
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}
