package ingestion

import (
	"context"
	"encoding/json"
	"errors"
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

	// MaxCaptureMentions caps extracted mentions retained by one capture.
	MaxCaptureMentions = 64
	// MaxCaptureReferenceCandidates caps extracted references retained by one capture.
	MaxCaptureReferenceCandidates = 64
	// MaxCaptureRelationCandidates caps extracted relations retained by one capture.
	MaxCaptureRelationCandidates = 64
	// MaxCaptureInterpretationWorkItems is the request-wide cap shared by mentions,
	// references, and relation candidates before any derived lookup or write work.
	MaxCaptureInterpretationWorkItems = 128
	// MaxCaptureAliasesPerMention bounds per-mention lookup fan-out from interpreter output.
	MaxCaptureAliasesPerMention = 16
	// MaxCaptureTopicalLabels bounds interpreter-derived labels used during candidate scoring.
	MaxCaptureTopicalLabels = 64
	// MaxCaptureEntityLookupOperations caps request-wide entity lookup calls made while
	// materializing retained mentions.
	MaxCaptureEntityLookupOperations = 256
	// MaxCaptureCandidateQueryTerms caps request-derived terms scored against the
	// bounded capture candidate pool.
	MaxCaptureCandidateQueryTerms = 256
	// MaxCaptureCandidateSearchFields caps store-derived fields examined while
	// scoring or locally matching one bounded candidate. The hydrated candidate
	// itself remains intact for the resolver.
	MaxCaptureCandidateSearchFields = 256
	// MaxCaptureCandidateQueryBytes caps request strings normalized once for
	// candidate scoring, independent of the request's field count.
	MaxCaptureCandidateQueryBytes int64 = 256 << 10
	// MaxCaptureCandidateSearchBytes is one request-wide budget shared while
	// normalizing fields from every hydrated candidate.
	MaxCaptureCandidateSearchBytes int64 = 1 << 20
	// MaxCaptureCandidateMatchBytes caps aggregate exact/substring comparison
	// input after both query and record terms have been normalized once.
	MaxCaptureCandidateMatchBytes int64 = 8 << 20
	// MaxCaptureCandidateHydrationBytes is shared by the scoped and unscoped
	// candidate projections for one capture request.
	MaxCaptureCandidateHydrationBytes int64 = storage.MaxBoundedHydrationBytes
	// MaxCaptureResolutionCandidateFields caps request-wide candidate fields
	// normalized into the post-hydration entity/reference resolution index.
	MaxCaptureResolutionCandidateFields = captureCandidateLimit * MaxCaptureCandidateSearchFields
	// MaxCaptureResolutionCandidateBytes caps request-wide candidate bytes
	// normalized into the post-hydration entity/reference resolution index.
	MaxCaptureResolutionCandidateBytes int64 = MaxCaptureCandidateSearchBytes
	// MaxCaptureResolutionQueryFields caps request-wide mention and reference
	// fields normalized while resolving the bounded interpretation.
	MaxCaptureResolutionQueryFields = MaxCaptureMentions*(1+MaxCaptureAliasesPerMention) + MaxCaptureReferenceCandidates
	// MaxCaptureResolutionQueryBytes caps request-wide mention and reference
	// bytes normalized while resolving the bounded interpretation.
	MaxCaptureResolutionQueryBytes int64 = MaxCaptureCandidateQueryBytes
	// MaxCaptureResolutionMatchOperations caps request-wide comparisons against
	// the precomputed resolution index. Budget is checked before each comparison.
	MaxCaptureResolutionMatchOperations = 32 << 10
	// MaxCaptureResolutionMatchBytes caps the aggregate normalized string bytes
	// examined by post-hydration entity/reference resolution.
	MaxCaptureResolutionMatchBytes int64 = MaxCaptureCandidateMatchBytes
)

var errBoundedCaptureCandidateLookupUnsupported = errors.New("ingestion: bounded candidate lookup is unsupported by store")

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

// CaptureAccess applies caller-specific authorization to records consulted or
// mutated while resolving a capture. Nil functions preserve the unrestricted
// behavior of direct in-process callers.
type CaptureAccess struct {
	CanRead  func(*schema.MemoryRecord) bool
	CanWrite func(*schema.MemoryRecord) bool
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
	return s.CaptureMemoryWithAccess(ctx, req, CaptureAccess{})
}

// CaptureMemoryWithAccess captures memory while filtering resolver inputs and
// rechecking every existing record before graph materialization or mutation.
func (s *Service) CaptureMemoryWithAccess(ctx context.Context, req CaptureMemoryRequest, access CaptureAccess) (*CaptureMemoryResponse, error) {
	captureSvc := *s
	captureSvc.captureAccess = access
	s = &captureSvc
	if req.Sensitivity == "" && s.policy != nil {
		// Resolve once before interpretation and materialization so the primary
		// record and every derived record use the configured ingestion policy.
		req.Sensitivity = s.policy.assignSensitivity(&MemoryCandidate{})
	}
	if err := validateCaptureSourceKind(req.SourceKind); err != nil {
		return nil, err
	}
	if err := validateCaptureProposedType(req.ProposedType); err != nil {
		return nil, err
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
			Transaction:           tx,
			lookup:                entityLookup(s.store),
			semanticLookup:        semanticLookup(s.store),
			semanticLookupInScope: semanticLookupInScope(s.store),
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
	interpretation := boundCaptureInterpretation(buildFallbackInterpretation(req))
	if s.interpreter != nil {
		interpreted, err := s.interpreter.Interpret(ctx, interpretReq)
		if err == nil && interpreted != nil {
			interpretation = boundCaptureInterpretation(mergeInterpretations(interpretation, interpreted))
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
			interpretation = boundCaptureInterpretation(mergeInterpretations(interpretation, resolved))
		}
	}
	return boundCaptureInterpretation(interpretation), candidates, nil
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
	if interpretation == nil {
		interpretation = buildFallbackInterpretation(req)
	}
	interpretation = boundCaptureInterpretation(interpretation)
	sourceRecord, err := s.createPrimaryRecord(ctx, req, ts)
	if err != nil {
		return nil, err
	}
	candidates = append([]*schema.MemoryRecord(nil), candidates...)

	sourceRecord.Interpretation = interpretation
	if err := s.store.Update(ctx, sourceRecord); err != nil {
		return nil, fmt.Errorf("ingestion: update primary interpretation: %w", err)
	}
	candidates = append(candidates, sourceRecord)
	resolutionIndex := newCaptureResolutionIndex(candidates)

	createdRecords := make([]*schema.MemoryRecord, 0)
	edges := make([]schema.GraphEdge, 0)
	entityLookupBudget := &captureLookupBudget{remaining: MaxCaptureEntityLookupOperations}

	for idx := range sourceRecord.Interpretation.Mentions {
		mention := &sourceRecord.Interpretation.Mentions[idx]
		entity, created, entityEdges, err := s.resolveMentionWithResolutionIndex(ctx, sourceRecord, mention, req.Scope, req.Source, req.Sensitivity, ts, resolutionIndex, entityLookupBudget)
		if err != nil {
			return nil, err
		}
		mention.CanonicalEntityID = entity.ID
		if mention.Confidence == 0 {
			mention.Confidence = 1.0
		}
		if created != nil {
			createdRecords = append(createdRecords, created)
			candidates = append(candidates, created)
			resolutionIndex.addCandidate(created)
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
		target, ok := s.resolveReferenceCandidateWithIndex(ctx, ref, sourceRecord.Scope, resolutionIndex)
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
			Predicate: schema.GraphPredicateReferencesRecord,
			TargetID:  target.ID,
			Weight:    1.0,
			CreatedAt: ts,
		}
		edgeB := schema.GraphEdge{
			SourceID:  target.ID,
			Predicate: schema.GraphPredicateReferencedBy,
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

	if semanticRecord, semanticEdges, err := s.maybeCreateSemanticRecordWithResolutionIndex(ctx, sourceRecord, req, ts, resolutionIndex); err != nil {
		return nil, err
	} else if semanticRecord != nil || len(semanticEdges) > 0 {
		if semanticRecord != nil {
			createdRecords = append(createdRecords, semanticRecord)
		}
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

	edges = dedupeGraphEdges(edges)
	sourceRecord.Relations = dedupeRelations(sourceRecord.Relations)
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
	lookup                storage.EntityLookup
	semanticLookup        storage.SemanticLookup
	semanticLookupInScope storage.SemanticLookupInScope
}

func (s *captureTransactionStore) GetAuthorizationMetadata(ctx context.Context, ids []string) ([]storage.RecordAuthorizationMetadata, error) {
	lookup, ok := s.Transaction.(storage.AuthorizationMetadataStore)
	if !ok {
		return nil, storage.ErrAuthorizationMetadataUnsupported
	}
	return lookup.GetAuthorizationMetadata(ctx, ids)
}

func entityLookup(store storage.Store) storage.EntityLookup {
	lookup, _ := store.(storage.EntityLookup)
	return lookup
}

func semanticLookup(store storage.Store) storage.SemanticLookup {
	lookup, _ := store.(storage.SemanticLookup)
	return lookup
}

func semanticLookupInScope(store storage.Store) storage.SemanticLookupInScope {
	lookup, _ := store.(storage.SemanticLookupInScope)
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

func (s *captureTransactionStore) FindSemanticExact(ctx context.Context, subject, predicate, object string) (*schema.MemoryRecord, error) {
	if s.semanticLookup == nil {
		return nil, nil
	}
	return s.semanticLookup.FindSemanticExact(ctx, subject, predicate, object)
}

func (s *captureTransactionStore) FindSemanticExactInScope(ctx context.Context, subject, predicate, object, scope string) (*schema.MemoryRecord, error) {
	if s.semanticLookupInScope == nil {
		return findSemanticExactInScopeFallback(ctx, s.semanticLookup, subject, predicate, object, scope)
	}
	return s.semanticLookupInScope.FindSemanticExactInScope(ctx, subject, predicate, object, scope)
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
	if override.ProposedType != "" && schema.IsValidMemoryType(override.ProposedType) {
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

func boundCaptureInterpretation(interpretation *schema.Interpretation) *schema.Interpretation {
	if interpretation == nil {
		return nil
	}

	limits := [3]int{
		min(len(interpretation.Mentions), MaxCaptureMentions),
		min(len(interpretation.ReferenceCandidates), MaxCaptureReferenceCandidates),
		min(len(interpretation.RelationCandidates), MaxCaptureRelationCandidates),
	}
	counts := [3]int{}
	remaining := MaxCaptureInterpretationWorkItems
	for remaining > 0 {
		progressed := false
		for i := range counts {
			if counts[i] >= limits[i] {
				continue
			}
			counts[i]++
			remaining--
			progressed = true
			if remaining == 0 {
				break
			}
		}
		if !progressed {
			break
		}
	}

	bounded := *interpretation
	bounded.TopicalLabels = append([]string(nil), interpretation.TopicalLabels[:min(len(interpretation.TopicalLabels), MaxCaptureTopicalLabels)]...)
	bounded.Mentions = append([]schema.Mention(nil), interpretation.Mentions[:counts[0]]...)
	for i := range bounded.Mentions {
		aliases := bounded.Mentions[i].Aliases
		bounded.Mentions[i].Aliases = append([]string(nil), aliases[:min(len(aliases), MaxCaptureAliasesPerMention)]...)
	}
	bounded.ReferenceCandidates = append([]schema.ReferenceCandidate(nil), interpretation.ReferenceCandidates[:counts[1]]...)
	bounded.RelationCandidates = append([]schema.RelationCandidate(nil), interpretation.RelationCandidates[:counts[2]]...)
	return &bounded
}

func (s *Service) fetchCaptureCandidates(ctx context.Context, req CaptureMemoryRequest, interpretation *schema.Interpretation) ([]*schema.MemoryRecord, error) {
	querySearch := captureCandidateQuerySearch(req, interpretation)
	boundedStore, ok := s.store.(storage.BoundedListStore)
	if !ok {
		return nil, errBoundedCaptureCandidateLookupUnsupported
	}
	remainingBytes := MaxCaptureCandidateHydrationBytes
	records := make([]*schema.MemoryRecord, 0, captureCandidateSearchPool)
	fetch := func(opts storage.ListOptions, exactScope string) error {
		if remainingBytes < storage.ProjectedRecordOverheadBytes {
			return nil
		}
		opts.Limit = captureCandidateSearchPool
		opts.OmitRelations = true
		opts.OmitHistory = true
		opts.MaxHydratedBytes = remainingBytes
		result, err := boundedStore.ListBounded(ctx, opts)
		if err != nil {
			return err
		}
		if result.ProjectedBytes < 0 || result.ProjectedBytes > remainingBytes {
			return fmt.Errorf("ingestion: bounded candidate lookup reported invalid projected bytes: %d (remaining %d)", result.ProjectedBytes, remainingBytes)
		}
		batch, measuredBytes := sanitizeCaptureCandidateBatch(result.Records, exactScope, remainingBytes)
		chargedBytes := max(result.ProjectedBytes, measuredBytes)
		remainingBytes -= chargedBytes
		records = append(records, batch...)
		return nil
	}
	if req.Scope != "" {
		if err := fetch(storage.ListOptions{Scope: req.Scope}, req.Scope); err != nil {
			return nil, err
		}
		if err := fetch(storage.ListOptions{Scopes: []string{""}, IncludeUnscoped: true}, ""); err != nil {
			return nil, err
		}
	} else {
		if err := fetch(storage.ListOptions{Scopes: []string{""}, IncludeUnscoped: true}, ""); err != nil {
			return nil, err
		}
	}
	seen := make(map[string]struct{}, len(records))
	type scoredCandidate struct {
		record *schema.MemoryRecord
		score  float64
	}
	scored := make([]scoredCandidate, 0, len(records))
	scoreNow := time.Now().UTC()
	remainingSearchBytes := MaxCaptureCandidateSearchBytes
	remainingMatchBytes := MaxCaptureCandidateMatchBytes
	for _, rec := range records {
		if rec == nil || !s.captureCanRead(rec) {
			continue
		}
		if _, ok := seen[rec.ID]; ok {
			continue
		}
		seen[rec.ID] = struct{}{}
		recordSearch := captureCandidateRecordSearch(rec, &remainingSearchBytes)
		scored = append(scored, scoredCandidate{
			record: rec,
			score:  captureCandidateScoreAtWithSearch(rec, req, interpretation, querySearch, recordSearch, scoreNow, &remainingMatchBytes),
		})
	}
	sort.Slice(scored, func(i, j int) bool {
		if scored[i].score != scored[j].score {
			return scored[i].score > scored[j].score
		}
		if !scored[i].record.CreatedAt.Equal(scored[j].record.CreatedAt) {
			return scored[i].record.CreatedAt.After(scored[j].record.CreatedAt)
		}
		return scored[i].record.ID < scored[j].record.ID
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

// sanitizeCaptureCandidateBatch distrusts optional-store implementations: it
// enforces the row/scope/byte projection again and removes histories that the
// caller explicitly requested the store not hydrate. Payload and interpretation
// projections remain available to the resolver and are never mutated in place.
func sanitizeCaptureCandidateBatch(records []*schema.MemoryRecord, exactScope string, maxProjectedBytes int64) ([]*schema.MemoryRecord, int64) {
	if maxProjectedBytes <= 0 {
		return nil, 0
	}
	limit := min(len(records), captureCandidateSearchPool)
	bounded := make([]*schema.MemoryRecord, 0, limit)
	var projectedBytes int64
	for idx := 0; idx < limit; idx++ {
		record := records[idx]
		if record == nil || record.Scope != exactScope {
			continue
		}
		projection := *record
		projection.Relations = nil
		projection.AuditLog = nil
		projection.Provenance.Sources = nil
		remaining := maxProjectedBytes - projectedBytes
		projected := storage.ProjectedRecordBytes(&projection, remaining)
		if projected > remaining {
			break
		}
		projectedBytes += projected
		bounded = append(bounded, &projection)
	}
	return bounded, projectedBytes
}

type captureResolutionEntity struct {
	record *schema.MemoryRecord
	terms  []string
}

// captureResolutionIndex owns every post-hydration string-work budget for one
// CaptureMemory operation. Candidate fields are normalized only as records are
// added; mention/reference lookups reuse those projections and charge one
// aggregate comparison budget before examining them.
type captureResolutionIndex struct {
	recordsByID       map[string]*schema.MemoryRecord
	entities          []captureResolutionEntity
	references        map[string][]*schema.MemoryRecord
	indexedCandidates map[*schema.MemoryRecord]struct{}

	remainingCandidateFields int
	remainingCandidateBytes  int64
	remainingQueryFields     int
	remainingQueryBytes      int64
	remainingMatchOperations int
	remainingMatchBytes      int64

	// Counters make the request-wide invariants directly testable. They count
	// attempted bounded normalizations/comparisons, never raw input sizes.
	candidateFieldsNormalized int
	candidateBytesNormalized  int64
	queryFieldsNormalized     int
	queryBytesNormalized      int64
	matchOperations           int
	matchBytes                int64
}

func newCaptureResolutionIndex(candidates []*schema.MemoryRecord) *captureResolutionIndex {
	index := &captureResolutionIndex{
		recordsByID:              make(map[string]*schema.MemoryRecord, len(candidates)),
		entities:                 make([]captureResolutionEntity, 0, min(len(candidates), captureCandidateLimit)),
		references:               make(map[string][]*schema.MemoryRecord),
		indexedCandidates:        make(map[*schema.MemoryRecord]struct{}, len(candidates)),
		remainingCandidateFields: MaxCaptureResolutionCandidateFields,
		remainingCandidateBytes:  MaxCaptureResolutionCandidateBytes,
		remainingQueryFields:     MaxCaptureResolutionQueryFields,
		remainingQueryBytes:      MaxCaptureResolutionQueryBytes,
		remainingMatchOperations: MaxCaptureResolutionMatchOperations,
		remainingMatchBytes:      MaxCaptureResolutionMatchBytes,
	}
	for _, candidate := range candidates {
		index.addCandidate(candidate)
	}
	return index
}

func (i *captureResolutionIndex) addCandidate(record *schema.MemoryRecord) {
	if i == nil || record == nil {
		return
	}
	if _, exists := i.indexedCandidates[record]; exists {
		return
	}
	i.indexedCandidates[record] = struct{}{}
	if record.ID != "" {
		if _, exists := i.recordsByID[record.ID]; !exists {
			i.recordsByID[record.ID] = record
		}
	}

	remainingRecordFields := MaxCaptureCandidateSearchFields
	switch payload := record.Payload.(type) {
	case *schema.EntityPayload:
		if payload == nil {
			return
		}
		seen := make(map[string]struct{})
		terms := make([]string, 0, min(MaxCaptureCandidateSearchFields, 1+len(payload.Aliases)+len(payload.Identifiers)))
		add := func(value string) bool {
			if remainingRecordFields <= 0 {
				return false
			}
			remainingRecordFields--
			term, ok := i.normalizeCandidate(value, schema.NormalizeEntityTerm)
			if !ok {
				return false
			}
			if term != "" {
				if _, exists := seen[term]; !exists {
					seen[term] = struct{}{}
					terms = append(terms, term)
				}
			}
			return i.hasCandidateNormalizationBudget() && remainingRecordFields > 0
		}
		if add(payload.CanonicalName) {
			for aliasIdx := 0; aliasIdx < len(payload.Aliases) && add(payload.Aliases[aliasIdx].Value); aliasIdx++ {
			}
			for identifierIdx := 0; identifierIdx < len(payload.Identifiers) && remainingRecordFields > 0 && i.hasCandidateNormalizationBudget(); identifierIdx++ {
				identifier := payload.Identifiers[identifierIdx]
				if !add(i.boundedCandidateJoin(identifier.Namespace, ":", identifier.Value)) {
					break
				}
			}
		}
		if len(terms) > 0 {
			i.entities = append(i.entities, captureResolutionEntity{record: record, terms: terms})
		}
	case *schema.EpisodicPayload:
		if payload == nil {
			return
		}
		seen := make(map[string]struct{})
		for eventIdx := 0; eventIdx < len(payload.Timeline) && remainingRecordFields > 0 && i.hasCandidateNormalizationBudget(); eventIdx++ {
			remainingRecordFields--
			term, ok := i.normalizeCandidate(payload.Timeline[eventIdx].Ref, normalizeMatchTerm)
			if !ok {
				break
			}
			if term == "" {
				continue
			}
			if _, exists := seen[term]; exists {
				continue
			}
			seen[term] = struct{}{}
			i.references[term] = append(i.references[term], record)
		}
	}
}

func (i *captureResolutionIndex) hasCandidateNormalizationBudget() bool {
	return i != nil && i.remainingCandidateFields > 0 && i.remainingCandidateBytes > 0
}

func (i *captureResolutionIndex) normalizeCandidate(value string, normalizer func(string) string) (string, bool) {
	if !i.hasCandidateNormalizationBudget() {
		return "", false
	}
	i.remainingCandidateFields--
	i.candidateFieldsNormalized++
	consumed := min(int64(len(value)), i.remainingCandidateBytes)
	i.remainingCandidateBytes -= consumed
	i.candidateBytesNormalized += consumed
	if consumed <= 0 {
		return "", i.hasCandidateNormalizationBudget()
	}
	normalized := normalizer(value[:int(consumed)])
	if int64(len(normalized)) > consumed {
		normalized = normalized[:int(consumed)]
	}
	return normalized, true
}

func (i *captureResolutionIndex) boundedCandidateJoin(parts ...string) string {
	if i == nil || i.remainingCandidateBytes <= 0 {
		return ""
	}
	maxBytes := i.remainingCandidateBytes
	var joined strings.Builder
	joined.Grow(int(min(maxBytes, 4096)))
	for _, part := range parts {
		if maxBytes <= 0 {
			break
		}
		length := min(int64(len(part)), maxBytes)
		joined.WriteString(part[:int(length)])
		maxBytes -= length
	}
	return joined.String()
}

func (i *captureResolutionIndex) normalizeQuery(values []string, limit int, normalizer func(string) string) []string {
	if i == nil || limit <= 0 || i.remainingQueryFields <= 0 || i.remainingQueryBytes <= 0 || !i.hasMatchBudget() {
		return nil
	}
	seen := make(map[string]struct{}, min(len(values), limit))
	terms := make([]string, 0, min(len(values), limit))
	for valueIdx := 0; valueIdx < len(values) && valueIdx < limit && i.remainingQueryFields > 0 && i.remainingQueryBytes > 0; valueIdx++ {
		i.remainingQueryFields--
		i.queryFieldsNormalized++
		consumed := min(int64(len(values[valueIdx])), i.remainingQueryBytes)
		i.remainingQueryBytes -= consumed
		i.queryBytesNormalized += consumed
		if consumed <= 0 {
			continue
		}
		term := normalizer(values[valueIdx][:int(consumed)])
		if int64(len(term)) > consumed {
			term = term[:int(consumed)]
		}
		if term == "" {
			continue
		}
		if _, exists := seen[term]; exists {
			continue
		}
		seen[term] = struct{}{}
		terms = append(terms, term)
	}
	return terms
}

func (i *captureResolutionIndex) hasMatchBudget() bool {
	return i != nil && i.remainingMatchOperations > 0 && i.remainingMatchBytes > 0
}

func (i *captureResolutionIndex) consumeMatch(values ...string) bool {
	if !i.hasMatchBudget() {
		return false
	}
	var cost int64
	for _, value := range values {
		length := int64(len(value))
		if length > i.remainingMatchBytes-cost {
			i.remainingMatchBytes = 0
			return false
		}
		cost += length
	}
	i.remainingMatchOperations--
	i.remainingMatchBytes -= cost
	i.matchOperations++
	i.matchBytes += cost
	return true
}

func (i *captureResolutionIndex) recordByID(id, scope string) *schema.MemoryRecord {
	if i == nil || id == "" {
		return nil
	}
	record := i.recordsByID[id]
	if !captureScopeAllows(record, scope) {
		return nil
	}
	return record
}

func (i *captureResolutionIndex) findMatchingEntity(mention *schema.Mention, scope string) *schema.MemoryRecord {
	return i.findMatchingEntityWithScopePolicy(mention, scope, true)
}

func (i *captureResolutionIndex) findMatchingEntityAnyScope(mention *schema.Mention) *schema.MemoryRecord {
	return i.findMatchingEntityWithScopePolicy(mention, "", false)
}

func (i *captureResolutionIndex) findMatchingEntityWithScopePolicy(mention *schema.Mention, scope string, enforceScope bool) *schema.MemoryRecord {
	if i == nil || mention == nil || !i.hasMatchBudget() {
		return nil
	}
	queryValues := make([]string, 0, 1+min(len(mention.Aliases), MaxCaptureAliasesPerMention))
	queryValues = append(queryValues, mention.Surface)
	queryValues = append(queryValues, mention.Aliases[:min(len(mention.Aliases), MaxCaptureAliasesPerMention)]...)
	queryTerms := i.normalizeQuery(queryValues, 1+MaxCaptureAliasesPerMention, schema.NormalizeEntityTerm)
	if len(queryTerms) == 0 {
		return nil
	}
	var best *schema.MemoryRecord
	bestRank := 3
	for _, entity := range i.entities {
		if enforceScope && !captureScopeAllows(entity.record, scope) {
			continue
		}
		for _, indexedTerm := range entity.terms {
			for _, queryTerm := range queryTerms {
				if !i.consumeMatch(indexedTerm, queryTerm) {
					return best
				}
				rank := schema.NormalizedEntityTermMatchRank(indexedTerm, queryTerm)
				if rank >= bestRank {
					continue
				}
				best = entity.record
				bestRank = rank
				if rank == 0 {
					return entity.record
				}
			}
		}
	}
	return best
}

func (i *captureResolutionIndex) findReference(ref, scope string) *schema.MemoryRecord {
	if i == nil || !i.hasMatchBudget() {
		return nil
	}
	terms := i.normalizeQuery([]string{ref}, 1, normalizeMatchTerm)
	if len(terms) == 0 {
		return nil
	}
	term := terms[0]
	for _, record := range i.references[term] {
		if !i.consumeMatch(term, term) {
			return nil
		}
		if captureScopeAllows(record, scope) {
			return record
		}
	}
	return nil
}

type captureLookupBudget struct {
	remaining int
}

func (b *captureLookupBudget) take() bool {
	if b == nil {
		return true
	}
	if b.remaining <= 0 {
		return false
	}
	b.remaining--
	return true
}

func (s *Service) resolveMention(ctx context.Context, source *schema.MemoryRecord, mention *schema.Mention, scope, actor string, sensitivity schema.Sensitivity, ts time.Time, candidates []*schema.MemoryRecord) (*schema.MemoryRecord, *schema.MemoryRecord, []schema.GraphEdge, error) {
	return s.resolveMentionWithLookupBudget(ctx, source, mention, scope, actor, sensitivity, ts, candidates, nil)
}

func (s *Service) resolveMentionWithLookupBudget(ctx context.Context, source *schema.MemoryRecord, mention *schema.Mention, scope, actor string, sensitivity schema.Sensitivity, ts time.Time, candidates []*schema.MemoryRecord, lookupBudget *captureLookupBudget) (*schema.MemoryRecord, *schema.MemoryRecord, []schema.GraphEdge, error) {
	return s.resolveMentionWithResolutionIndex(ctx, source, mention, scope, actor, sensitivity, ts, newCaptureResolutionIndex(candidates), lookupBudget)
}

func (s *Service) resolveMentionWithResolutionIndex(ctx context.Context, source *schema.MemoryRecord, mention *schema.Mention, scope, actor string, sensitivity schema.Sensitivity, ts time.Time, index *captureResolutionIndex, lookupBudget *captureLookupBudget) (*schema.MemoryRecord, *schema.MemoryRecord, []schema.GraphEdge, error) {
	entity := s.resolveMentionEntityWithResolutionIndex(ctx, mention, scope, index, lookupBudget)
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
		Predicate: schema.GraphPredicateMentionsEntity,
		TargetID:  entity.ID,
		Weight:    1.0,
		CreatedAt: ts,
	}
	edgeB := schema.GraphEdge{
		SourceID:  entity.ID,
		Predicate: schema.GraphPredicateMentionedIn,
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
	return s.resolveMentionEntityWithLookupBudget(ctx, mention, scope, candidates, nil)
}

func (s *Service) resolveMentionEntityWithLookupBudget(ctx context.Context, mention *schema.Mention, scope string, candidates []*schema.MemoryRecord, lookupBudget *captureLookupBudget) *schema.MemoryRecord {
	return s.resolveMentionEntityWithResolutionIndex(ctx, mention, scope, newCaptureResolutionIndex(candidates), lookupBudget)
}

func (s *Service) resolveMentionEntityWithResolutionIndex(ctx context.Context, mention *schema.Mention, scope string, index *captureResolutionIndex, lookupBudget *captureLookupBudget) *schema.MemoryRecord {
	if mention == nil {
		return nil
	}
	if mention.CanonicalEntityID != "" && lookupBudget.take() {
		if rec := s.captureTargetByIDWithResolutionIndex(ctx, mention.CanonicalEntityID, scope, index); rec != nil && rec.Type == schema.MemoryTypeEntity {
			return rec
		}
	}
	// Legacy entity lookup interfaces return complete records and are retained
	// only for trusted in-process captures. Network captures resolve exclusively
	// from the bounded candidate index.
	if !s.captureAccessRestricted() {
		if lookup, ok := s.store.(storage.EntityLookup); ok {
			for _, identifier := range mentionEntityIdentifiers(mention) {
				if !lookupBudget.take() {
					break
				}
				rec, err := lookup.FindEntityByIdentifier(ctx, identifier.Namespace, identifier.Value, scope)
				if err == nil && rec != nil {
					if current := s.captureResolvedTarget(ctx, rec, scope); current != nil && current.Type == schema.MemoryTypeEntity {
						return current
					}
				}
			}
			for _, term := range uniqueStrings(append([]string{mention.Surface}, mention.Aliases...)) {
				if !lookupBudget.take() {
					break
				}
				matches, err := lookup.FindEntitiesByTerm(ctx, term, scope, 1)
				if err == nil && len(matches) > 0 {
					if current := s.captureResolvedTarget(ctx, matches[0], scope); current != nil && current.Type == schema.MemoryTypeEntity {
						return current
					}
				}
			}
		}
	}
	if index == nil || !index.hasMatchBudget() || !lookupBudget.take() {
		return nil
	}
	if rec := index.findMatchingEntity(mention, scope); rec != nil {
		return s.captureResolvedTarget(ctx, rec, scope)
	}
	return nil
}

func mentionEntityIdentifiers(mention *schema.Mention) []schema.EntityIdentifier {
	if mention == nil {
		return nil
	}
	terms := uniqueStrings(append([]string{mention.Surface}, mention.Aliases...))
	identifiers := make([]schema.EntityIdentifier, 0, len(terms))
	seen := make(map[string]struct{}, len(terms))
	for _, term := range terms {
		for _, identifier := range schema.ParseEntityIdentifierTokens(term) {
			namespace := schema.NormalizeEntityIdentifierNamespace(identifier.Namespace)
			value := strings.TrimSpace(identifier.Value)
			key := namespace + "\x00" + value
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			identifiers = append(identifiers, schema.EntityIdentifier{Namespace: namespace, Value: value})
		}
	}
	return identifiers
}

func captureScopeAllows(rec *schema.MemoryRecord, scope string) bool {
	return rec != nil && (rec.Scope == "" || (scope != "" && rec.Scope == scope))
}

func (s *Service) captureCanRead(rec *schema.MemoryRecord) bool {
	return s.captureAccess.CanRead == nil || s.captureAccess.CanRead(rec)
}

func (s *Service) captureCanMutate(rec *schema.MemoryRecord) bool {
	return s.captureCanRead(rec) && (s.captureAccess.CanWrite == nil || s.captureAccess.CanWrite(rec))
}

func (s *Service) captureAccessRestricted() bool {
	return s.captureAccess.CanRead != nil || s.captureAccess.CanWrite != nil
}

func (s *Service) captureResolvedTarget(ctx context.Context, rec *schema.MemoryRecord, scope string) *schema.MemoryRecord {
	if rec == nil || !captureScopeAllows(rec, scope) {
		return nil
	}
	if !s.captureAccessRestricted() {
		return rec
	}
	if !s.captureCanMutate(rec) {
		return nil
	}
	lookup, ok := s.store.(storage.AuthorizationMetadataStore)
	if !ok {
		return nil
	}
	metadata, err := lookup.GetAuthorizationMetadata(ctx, []string{rec.ID})
	if err != nil {
		return nil
	}
	var current *schema.MemoryRecord
	for _, item := range metadata {
		if item.ID != rec.ID {
			continue
		}
		if current != nil {
			return nil
		}
		candidate := *rec
		candidate.Scope = item.Scope
		candidate.Sensitivity = item.Sensitivity
		current = &candidate
	}
	if current == nil || !captureScopeAllows(current, scope) || !s.captureCanMutate(current) {
		return nil
	}
	return current
}

func (s *Service) captureTargetByID(ctx context.Context, id, scope string, candidates []*schema.MemoryRecord) *schema.MemoryRecord {
	if rec := findRecordByID(id, candidates); rec != nil {
		if current := s.captureResolvedTarget(ctx, rec, scope); current != nil {
			return current
		}
	}
	if s.captureAccessRestricted() {
		return nil
	}
	return s.captureExistingTarget(ctx, id, scope)
}

func (s *Service) captureTargetByIDWithResolutionIndex(ctx context.Context, id, scope string, index *captureResolutionIndex) *schema.MemoryRecord {
	if index != nil {
		if rec := index.recordByID(id, scope); rec != nil {
			if current := s.captureResolvedTarget(ctx, rec, scope); current != nil {
				return current
			}
		}
	}
	if s.captureAccessRestricted() {
		return nil
	}
	return s.captureExistingTarget(ctx, id, scope)
}

func (s *Service) captureExistingTarget(ctx context.Context, id, scope string) *schema.MemoryRecord {
	if s.captureAccessRestricted() || strings.TrimSpace(id) == "" {
		return nil
	}
	rec, err := s.store.Get(ctx, id)
	if err != nil || !captureScopeAllows(rec, scope) || !s.captureCanMutate(rec) {
		return nil
	}
	return rec
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

func dedupeGraphEdges(edges []schema.GraphEdge) []schema.GraphEdge {
	seen := make(map[string]int, len(edges))
	out := make([]schema.GraphEdge, 0, len(edges))
	for _, edge := range edges {
		key := graphEdgeKey(edge)
		if idx, ok := seen[key]; ok {
			out[idx] = edge
			continue
		}
		seen[key] = len(out)
		out = append(out, edge)
	}
	return out
}

func dedupeRelations(relations []schema.Relation) []schema.Relation {
	seen := make(map[string]int, len(relations))
	out := make([]schema.Relation, 0, len(relations))
	for _, rel := range relations {
		key := relationKey(rel)
		if idx, ok := seen[key]; ok {
			out[idx] = rel
			continue
		}
		seen[key] = len(out)
		out = append(out, rel)
	}
	return out
}

func graphEdgeKey(edge schema.GraphEdge) string {
	return edge.SourceID + "\x00" + schema.NormalizeGraphPredicate(edge.Predicate) + "\x00" + edge.TargetID
}

func relationKey(rel schema.Relation) string {
	return schema.NormalizeGraphPredicate(rel.Predicate) + "\x00" + rel.TargetID
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
	return newCaptureResolutionIndex(candidates).findMatchingEntityAnyScope(mention)
}

func (s *Service) resolveReferenceCandidate(ctx context.Context, candidate *schema.ReferenceCandidate, scope string, candidates []*schema.MemoryRecord) (*schema.MemoryRecord, bool) {
	return s.resolveReferenceCandidateWithIndex(ctx, candidate, scope, newCaptureResolutionIndex(candidates))
}

func (s *Service) resolveReferenceCandidateWithIndex(ctx context.Context, candidate *schema.ReferenceCandidate, scope string, index *captureResolutionIndex) (*schema.MemoryRecord, bool) {
	if candidate == nil {
		return nil, false
	}
	if candidate.TargetRecordID != "" {
		if rec := s.captureTargetByIDWithResolutionIndex(ctx, candidate.TargetRecordID, scope, index); rec != nil {
			return rec, true
		}
	}
	if candidate.TargetEntityID != "" {
		if rec := s.captureTargetByIDWithResolutionIndex(ctx, candidate.TargetEntityID, scope, index); rec != nil {
			return rec, true
		}
	}
	if index != nil {
		if rec := index.recordByID(candidate.Ref, scope); rec != nil {
			if current := s.captureResolvedTarget(ctx, rec, scope); current != nil {
				return current, true
			}
		}
		if rec := index.findReference(candidate.Ref, scope); rec != nil {
			if current := s.captureResolvedTarget(ctx, rec, scope); current != nil {
				return current, true
			}
		}
	}
	return nil, false
}

func (s *Service) materializeRelationCandidate(ctx context.Context, source *schema.MemoryRecord, candidate *schema.RelationCandidate, ts time.Time, candidates []*schema.MemoryRecord) ([]schema.GraphEdge, error) {
	scope := ""
	if source != nil {
		scope = source.Scope
	}
	target, ok := s.resolveRelationCandidateTarget(ctx, candidate, scope, candidates)
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

func (s *Service) resolveRelationCandidateTarget(ctx context.Context, candidate *schema.RelationCandidate, scope string, candidates []*schema.MemoryRecord) (*schema.MemoryRecord, bool) {
	if candidate == nil {
		return nil, false
	}
	if candidate.TargetRecordID != "" {
		if rec := s.captureTargetByID(ctx, candidate.TargetRecordID, scope, candidates); rec != nil {
			return rec, true
		}
	}
	if candidate.TargetEntityID != "" {
		if rec := s.captureTargetByID(ctx, candidate.TargetEntityID, scope, candidates); rec != nil {
			return rec, true
		}
	}
	return nil, false
}

func (s *Service) maybeCreateSemanticRecord(ctx context.Context, sourceRecord *schema.MemoryRecord, req CaptureMemoryRequest, ts time.Time) (*schema.MemoryRecord, []schema.GraphEdge, error) {
	return s.maybeCreateSemanticRecordWithResolutionIndex(ctx, sourceRecord, req, ts, nil)
}

func (s *Service) maybeCreateSemanticRecordWithResolutionIndex(ctx context.Context, sourceRecord *schema.MemoryRecord, req CaptureMemoryRequest, ts time.Time, index *captureResolutionIndex) (*schema.MemoryRecord, []schema.GraphEdge, error) {
	subject, predicate, object, ok := extractSemanticFact(req.Content)
	if !ok {
		return nil, nil, nil
	}
	if req.SourceKind != "observation" && sourceRecord.Interpretation != nil &&
		sourceRecord.Interpretation.ProposedType != schema.MemoryTypeSemantic &&
		sourceRecord.Interpretation.ExtractionConfidence < 0.5 {
		return nil, nil, nil
	}

	canonicalSubject := s.canonicalizeSemanticSideForScopeWithResolutionIndex(ctx, subject, req.Scope, sourceRecord.Interpretation, index)
	canonicalObject := s.canonicalizeSemanticObjectForScopeWithResolutionIndex(ctx, object, req.Scope, sourceRecord.Interpretation, index)
	predicate = schema.NormalizeSemanticPredicate(predicate)
	// Legacy exact-semantic lookup hydrates a complete record. Restricted
	// network captures therefore create a fresh, fully-audited fact instead of
	// risking unbounded history hydration or overwriting omitted history.
	if !s.captureAccessRestricted() {
		if existing, err := s.findExistingSemanticFact(ctx, req.Scope, canonicalSubject, predicate, canonicalObject); err != nil {
			return nil, nil, err
		} else if existing != nil && s.captureExistingTarget(ctx, existing.ID, req.Scope) != nil {
			edges, err := s.linkExistingSemanticRecord(ctx, sourceRecord, existing, req.Source, ts)
			return nil, edges, err
		}
	}

	rec := schema.NewMemoryRecord(uuid.New().String(), schema.MemoryTypeSemantic, req.Sensitivity, &schema.SemanticPayload{
		Kind:      "semantic",
		Subject:   canonicalSubject,
		Predicate: predicate,
		Object:    canonicalObject,
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
	rec.Provenance.Sources = appendProvenanceSource(rec.Provenance.Sources, semanticProvenanceSource(sourceRecord, req.Source, ts))
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
		Predicate: schema.GraphPredicateDerivedSemantic,
		TargetID:  rec.ID,
		Weight:    1.0,
		CreatedAt: ts,
	}
	edgeB := schema.GraphEdge{
		SourceID:  rec.ID,
		Predicate: schema.GraphPredicateDerivedFrom,
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
	entityEdges, err := s.linkRecordToCanonicalEntitiesWithResolutionIndex(ctx, rec, ts, index)
	if err != nil {
		return nil, nil, err
	}
	edges = append(edges, entityEdges...)
	return rec, edges, nil
}

func (s *Service) findExistingSemanticFact(ctx context.Context, scope, subject, predicate string, object any) (*schema.MemoryRecord, error) {
	objectKey := schema.SemanticObjectKey(object)
	if lookup, ok := s.store.(storage.SemanticLookupInScope); ok {
		existing, err := lookup.FindSemanticExactInScope(ctx, subject, predicate, objectKey, scope)
		if err != nil {
			return nil, fmt.Errorf("ingestion: find existing semantic record: %w", err)
		}
		return existing, nil
	}
	lookup, ok := s.store.(storage.SemanticLookup)
	if !ok {
		return nil, nil
	}
	existing, err := findSemanticExactInScopeFallback(ctx, lookup, subject, predicate, objectKey, scope)
	if err != nil {
		return nil, fmt.Errorf("ingestion: find existing semantic record: %w", err)
	}
	return existing, nil
}

func findSemanticExactInScopeFallback(ctx context.Context, lookup storage.SemanticLookup, subject, predicate, object, scope string) (*schema.MemoryRecord, error) {
	if lookup == nil {
		return nil, nil
	}
	existing, err := lookup.FindSemanticExact(ctx, subject, predicate, object)
	if err != nil || existing == nil {
		return existing, err
	}
	if existing.Scope != scope {
		return nil, nil
	}
	return existing, nil
}

func (s *Service) linkExistingSemanticRecord(ctx context.Context, sourceRecord, semanticRecord *schema.MemoryRecord, actor string, ts time.Time) ([]schema.GraphEdge, error) {
	if sourceRecord == nil || semanticRecord == nil {
		return nil, nil
	}
	if s.captureAccessRestricted() {
		return nil, nil
	}
	newSalience := semanticRecord.Salience + 0.1
	if newSalience > 1.0 {
		newSalience = 1.0
	}
	current, err := s.store.Get(ctx, semanticRecord.ID)
	if err != nil {
		return nil, fmt.Errorf("ingestion: get semantic record for reinforcement: %w", err)
	}
	if !s.captureCanMutate(current) {
		return nil, nil
	}
	payload, ok := current.Payload.(*schema.SemanticPayload)
	if !ok || payload == nil {
		return nil, fmt.Errorf("ingestion: reinforce semantic record: payload is %T", current.Payload)
	}
	payload.Evidence = appendProvenanceEvidence(payload.Evidence, schema.ProvenanceRef{
		SourceType: string(sourceRecord.Type),
		SourceID:   sourceRecord.ID,
		Timestamp:  ts,
	})
	current.Payload = payload
	current.Provenance.Sources = appendProvenanceSource(current.Provenance.Sources, semanticProvenanceSource(sourceRecord, actor, ts))
	current.Salience = newSalience
	current.UpdatedAt = ts
	if err := s.store.Update(ctx, current); err != nil {
		return nil, fmt.Errorf("ingestion: reinforce semantic record: %w", err)
	}
	if err := s.store.AddAuditEntry(ctx, semanticRecord.ID, schema.AuditEntry{
		Action:    schema.AuditActionReinforce,
		Actor:     "ingestion/capture",
		Timestamp: ts,
		Rationale: fmt.Sprintf("Reused exact semantic fact during capture from %s", sourceRecord.ID),
	}); err != nil {
		return nil, fmt.Errorf("ingestion: audit semantic reuse: %w", err)
	}
	edgeA := schema.GraphEdge{
		SourceID:  sourceRecord.ID,
		Predicate: schema.GraphPredicateDerivedSemantic,
		TargetID:  semanticRecord.ID,
		Weight:    1.0,
		CreatedAt: ts,
	}
	edgeB := schema.GraphEdge{
		SourceID:  semanticRecord.ID,
		Predicate: schema.GraphPredicateDerivedFrom,
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
	return []schema.GraphEdge{edgeA, edgeB}, nil
}

func appendProvenanceEvidence(existing []schema.ProvenanceRef, next schema.ProvenanceRef) []schema.ProvenanceRef {
	if next.SourceID == "" {
		return existing
	}
	for _, item := range existing {
		if item.SourceType == next.SourceType && item.SourceID == next.SourceID {
			return existing
		}
	}
	return append(existing, next)
}

func appendProvenanceSource(existing []schema.ProvenanceSource, next schema.ProvenanceSource) []schema.ProvenanceSource {
	if next.Ref == "" {
		return existing
	}
	for _, item := range existing {
		if item.Kind == next.Kind && item.Ref == next.Ref {
			return existing
		}
	}
	return append(existing, next)
}

func semanticProvenanceSource(sourceRecord *schema.MemoryRecord, actor string, ts time.Time) schema.ProvenanceSource {
	if sourceRecord == nil {
		return schema.ProvenanceSource{}
	}
	return schema.ProvenanceSource{
		Kind:      semanticProvenanceKind(sourceRecord),
		Ref:       sourceRecord.ID,
		CreatedBy: actor,
		Timestamp: ts,
	}
}

func semanticProvenanceKind(sourceRecord *schema.MemoryRecord) schema.ProvenanceKind {
	if sourceRecord != nil && sourceRecord.Interpretation != nil {
		switch sourceRecord.Interpretation.ProposedType {
		case schema.MemoryTypeSemantic:
			return schema.ProvenanceKindObservation
		case schema.MemoryTypeWorking:
			return schema.ProvenanceKindArtifact
		}
	}
	if sourceRecord == nil || len(sourceRecord.Provenance.Sources) == 0 {
		return schema.ProvenanceKindEvent
	}
	return sourceRecord.Provenance.Sources[0].Kind
}

func inferMentionsFromContent(content any) []schema.Mention {
	obj := asObject(content)
	candidates := make([]schema.Mention, 0)
	for _, key := range sortedObjectKeys(obj) {
		value := obj[key]
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
		if len(candidates) >= MaxCaptureMentions {
			break
		}
	}
	return uniqueMentions(candidates)
}

func inferReferenceCandidates(content any, context any) []schema.ReferenceCandidate {
	refs := make([]schema.ReferenceCandidate, 0)
	for _, source := range []any{content, context} {
		obj := asObject(source)
		for _, key := range sortedObjectKeys(obj) {
			value := obj[key]
			if !strings.Contains(key, "ref") && !strings.Contains(key, "record_id") && key != "id" {
				continue
			}
			text := strings.TrimSpace(fmt.Sprint(value))
			if text == "" {
				continue
			}
			refs = append(refs, schema.ReferenceCandidate{Ref: text, Confidence: 0.5})
			if len(refs) >= MaxCaptureReferenceCandidates {
				return refs
			}
		}
	}
	return refs
}

func sortedObjectKeys(obj map[string]any) []string {
	keys := make([]string, 0, len(obj))
	for key := range obj {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
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
	return captureCandidateScoreAt(rec, req, interpretation, time.Now().UTC())
}

func captureCandidateScoreAt(rec *schema.MemoryRecord, req CaptureMemoryRequest, interpretation *schema.Interpretation, now time.Time) float64 {
	querySearch := captureCandidateQuerySearch(req, interpretation)
	remainingSearchBytes := MaxCaptureCandidateSearchBytes
	recordSearch := captureCandidateRecordSearch(rec, &remainingSearchBytes)
	remainingMatchBytes := MaxCaptureCandidateMatchBytes
	return captureCandidateScoreAtWithSearch(rec, req, interpretation, querySearch, recordSearch, now, &remainingMatchBytes)
}

type captureCandidateQueryProjection struct {
	terms []string
	tags  []string
}

type captureCandidateRecordProjection struct {
	terms []string
	tags  []string
}

func captureCandidateScoreAtWithSearch(rec *schema.MemoryRecord, req CaptureMemoryRequest, interpretation *schema.Interpretation, querySearch captureCandidateQueryProjection, recordSearch captureCandidateRecordProjection, now time.Time, remainingMatchBytes *int64) float64 {
	if rec == nil {
		return 0
	}
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
	for _, tag := range querySearch.tags {
		if normalizedCaptureSearchContainsExact(recordSearch.tags, tag, remainingMatchBytes) {
			score += 8
		}
	}
	for _, term := range querySearch.terms {
		if recordSearchTermsMatchNormalized(recordSearch.terms, term, remainingMatchBytes) {
			score += 25
		}
	}
	if !rec.CreatedAt.IsZero() {
		ageHours := now.Sub(rec.CreatedAt).Hours()
		if ageHours < 0 {
			ageHours = 0
		}
		score += 1.0 / (1.0 + ageHours/24.0)
	}
	return score
}

func candidateQueryTerms(req CaptureMemoryRequest, interpretation *schema.Interpretation) []string {
	return captureCandidateQuerySearch(req, interpretation).terms
}

func captureCandidateQuerySearch(req CaptureMemoryRequest, interpretation *schema.Interpretation) captureCandidateQueryProjection {
	remainingBytes := MaxCaptureCandidateQueryBytes
	collector := newCaptureCandidateFieldCollectorWithBudget(MaxCaptureCandidateQueryTerms, &remainingBytes)
	projection := captureCandidateQueryProjection{}
	appendTerm := func(value string, tag bool) bool {
		before := len(collector.values)
		collector.add(value)
		if tag && len(collector.values) > before {
			projection.tags = append(projection.tags, collector.values[len(collector.values)-1])
		}
		return collector.hasBudget()
	}
	appendTerms := func(values []string, tags bool) bool {
		for idx := 0; idx < len(values) && collector.hasBudget(); idx++ {
			if !appendTerm(values[idx], tags) {
				return false
			}
		}
		return collector.hasBudget()
	}

	if !appendTerms(req.Tags, true) {
		projection.terms = collector.values
		return projection
	}
	switch activeEntities := asObject(req.Context)["active_entities"].(type) {
	case []string:
		if !appendTerms(activeEntities, false) {
			projection.terms = collector.values
			return projection
		}
	case []any:
		for idx := 0; idx < len(activeEntities) && collector.hasBudget(); idx++ {
			if !appendTerm(fmt.Sprint(activeEntities[idx]), false) {
				projection.terms = collector.values
				return projection
			}
		}
	}
	if !appendTerm(stringValue(asObject(req.Content), "subject", "project", "tool_name", "file"), false) {
		projection.terms = collector.values
		return projection
	}
	if interpretation != nil {
		if !appendTerms(interpretation.TopicalLabels, false) {
			projection.terms = collector.values
			return projection
		}
		for idx := 0; idx < len(interpretation.Mentions) && collector.hasBudget(); idx++ {
			mention := interpretation.Mentions[idx]
			if !appendTerm(mention.Surface, false) || !appendTerms(mention.Aliases, false) {
				projection.terms = collector.values
				return projection
			}
		}
		for idx := 0; idx < len(interpretation.ReferenceCandidates) && collector.hasBudget(); idx++ {
			if !appendTerm(interpretation.ReferenceCandidates[idx].Ref, false) {
				projection.terms = collector.values
				return projection
			}
		}
	}
	projection.terms = collector.values
	return projection
}

func recordMatchesTerm(rec *schema.MemoryRecord, term string) bool {
	return recordSearchTermsMatch(recordSearchTerms(rec), term)
}

func recordSearchTermsMatch(searchTerms []string, term string) bool {
	remainingQueryBytes := MaxCaptureCandidateQueryBytes
	normalized := normalizeCaptureSearchStrings([]string{term}, 1, &remainingQueryBytes)
	if len(normalized) == 0 {
		return false
	}
	remainingMatchBytes := MaxCaptureCandidateMatchBytes
	return recordSearchTermsMatchNormalized(searchTerms, normalized[0], &remainingMatchBytes)
}

func recordSearchTermsMatchNormalized(searchTerms []string, needle string, remainingMatchBytes *int64) bool {
	if needle == "" {
		return false
	}
	for _, candidate := range searchTerms {
		if !consumeCaptureCandidateMatchBytes(remainingMatchBytes, candidate, needle) {
			return false
		}
		if candidate != "" && (candidate == needle || strings.Contains(candidate, needle) || containsNormalizedToken(needle, candidate)) {
			return true
		}
	}
	return false
}

func normalizedCaptureSearchContainsExact(values []string, needle string, remainingMatchBytes *int64) bool {
	for _, value := range values {
		if !consumeCaptureCandidateMatchBytes(remainingMatchBytes, value, needle) {
			return false
		}
		if value == needle {
			return true
		}
	}
	return false
}

func consumeCaptureCandidateMatchBytes(remaining *int64, values ...string) bool {
	if remaining == nil {
		return true
	}
	var cost int64
	for _, value := range values {
		length := int64(len(value))
		if length > *remaining-cost {
			*remaining = 0
			return false
		}
		cost += length
	}
	*remaining -= cost
	return true
}

func recordSearchTerms(rec *schema.MemoryRecord) []string {
	remainingBytes := MaxCaptureCandidateSearchBytes
	return captureCandidateRecordSearch(rec, &remainingBytes).terms
}

func captureCandidateRecordSearch(rec *schema.MemoryRecord, remainingBytes *int64) captureCandidateRecordProjection {
	if rec == nil {
		return captureCandidateRecordProjection{}
	}
	collector := newCaptureCandidateFieldCollectorWithBudget(MaxCaptureCandidateSearchFields, remainingBytes)
	projection := captureCandidateRecordProjection{}
	collector.add(rec.ID)
	collector.add(rec.Scope)
	for idx := 0; idx < len(rec.Tags) && collector.hasBudget(); idx++ {
		before := len(collector.values)
		collector.add(rec.Tags[idx])
		if len(collector.values) > before {
			projection.tags = append(projection.tags, collector.values[len(collector.values)-1])
		}
	}
	if rec.Interpretation != nil {
		collector.add(rec.Interpretation.Summary)
		collector.addStrings(rec.Interpretation.TopicalLabels)
		for idx := 0; idx < len(rec.Interpretation.Mentions) && collector.hasBudget(); idx++ {
			mention := rec.Interpretation.Mentions[idx]
			collector.add(mention.Surface)
			collector.addStrings(mention.Aliases)
		}
	}
	switch payload := rec.Payload.(type) {
	case *schema.EntityPayload:
		if payload == nil {
			break
		}
		collector.add(payload.CanonicalName)
		collector.add(payload.PrimaryType)
		collector.addStrings(payload.Types)
		for idx := 0; idx < len(payload.Aliases) && collector.hasBudget(); idx++ {
			collector.add(payload.Aliases[idx].Value)
		}
		for idx := 0; idx < len(payload.Identifiers) && collector.hasBudget(); idx++ {
			identifier := payload.Identifiers[idx]
			collector.addJoined(identifier.Namespace, ":", identifier.Value)
		}
		collector.add(payload.Summary)
	case *schema.SemanticPayload:
		if payload != nil {
			collector.add(payload.Subject)
			collector.add(payload.Predicate)
			if collector.hasBudget() {
				collector.add(fmt.Sprint(payload.Object))
			}
		}
	case *schema.EpisodicPayload:
		if payload == nil {
			break
		}
		for idx := 0; idx < len(payload.Timeline) && collector.hasBudget(); idx++ {
			item := payload.Timeline[idx]
			collector.add(item.Ref)
			collector.add(item.EventKind)
			collector.add(item.Summary)
		}
	case *schema.WorkingPayload:
		if payload != nil {
			collector.add(payload.ThreadID)
			collector.add(payload.ContextSummary)
			collector.addStrings(payload.NextActions)
			collector.addStrings(payload.OpenQuestions)
		}
	case *schema.CompetencePayload:
		if payload != nil {
			collector.add(payload.SkillName)
			collector.addStrings(payload.RequiredTools)
		}
	case *schema.PlanGraphPayload:
		if payload != nil {
			collector.add(payload.PlanID)
			collector.add(payload.Intent)
		}
	}
	projection.terms = collector.values
	return projection
}

type captureCandidateFieldCollector struct {
	remainingFields int
	remainingBytes  *int64
	seen            map[string]struct{}
	values          []string
}

func newCaptureCandidateFieldCollectorWithBudget(limit int, remainingBytes *int64) *captureCandidateFieldCollector {
	return &captureCandidateFieldCollector{
		remainingFields: max(limit, 0),
		remainingBytes:  remainingBytes,
		seen:            make(map[string]struct{}, max(limit, 0)),
		values:          make([]string, 0, max(limit, 0)),
	}
}

func (c *captureCandidateFieldCollector) hasBudget() bool {
	return c != nil && c.remainingFields > 0 && (c.remainingBytes == nil || *c.remainingBytes > 0)
}

func (c *captureCandidateFieldCollector) consume() bool {
	if !c.hasBudget() {
		return false
	}
	c.remainingFields--
	return true
}

func (c *captureCandidateFieldCollector) add(value string) bool {
	if !c.consume() {
		return false
	}
	maxBytes := int64(len(value))
	if c.remainingBytes != nil && maxBytes > *c.remainingBytes {
		maxBytes = *c.remainingBytes
	}
	if maxBytes <= 0 {
		return false
	}
	if c.remainingBytes != nil {
		*c.remainingBytes -= maxBytes
	}
	return c.addNormalized(normalizeMatchTerm(value[:int(maxBytes)]), maxBytes)
}

func (c *captureCandidateFieldCollector) addJoined(first, separator, second string) bool {
	if !c.consume() {
		return false
	}
	available := MaxCaptureCandidateSearchBytes
	if c.remainingBytes != nil {
		available = *c.remainingBytes
	}
	var maxBytes int64
	for _, part := range []string{first, separator, second} {
		length := int64(len(part))
		if length > available-maxBytes {
			maxBytes = available
			break
		}
		maxBytes += length
	}
	if maxBytes <= 0 {
		return false
	}
	var joined strings.Builder
	joined.Grow(int(maxBytes))
	remaining := maxBytes
	for _, part := range []string{first, separator, second} {
		if remaining <= 0 {
			break
		}
		length := min(int64(len(part)), remaining)
		joined.WriteString(part[:int(length)])
		remaining -= length
	}
	if c.remainingBytes != nil {
		*c.remainingBytes -= maxBytes
	}
	return c.addNormalized(normalizeMatchTerm(joined.String()), maxBytes)
}

func (c *captureCandidateFieldCollector) addNormalized(normalized string, maxBytes int64) bool {
	if int64(len(normalized)) > maxBytes {
		normalized = normalized[:int(maxBytes)]
	}
	if normalized == "" {
		return c.hasBudget()
	}
	if _, ok := c.seen[normalized]; ok {
		return c.hasBudget()
	}
	c.seen[normalized] = struct{}{}
	c.values = append(c.values, normalized)
	return c.hasBudget()
}

func (c *captureCandidateFieldCollector) addStrings(values []string) {
	for idx := 0; idx < len(values) && c.hasBudget(); idx++ {
		c.add(values[idx])
	}
}

func normalizeCaptureSearchStrings(values []string, limit int, remainingBytes *int64) []string {
	collector := newCaptureCandidateFieldCollectorWithBudget(limit, remainingBytes)
	collector.addStrings(values)
	return collector.values
}

func recordContainsReference(rec *schema.MemoryRecord, ref string) bool {
	if rec == nil {
		return false
	}
	return newCaptureResolutionIndex([]*schema.MemoryRecord{rec}).findReference(ref, rec.Scope) == rec
}

func containsNormalizedToken(haystack, needle string) bool {
	for _, token := range strings.Fields(haystack) {
		if token == needle {
			return true
		}
	}
	return false
}

func normalizeMatchTerm(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}

func normalizeRelationPredicate(value string) string {
	return schema.NormalizeGraphPredicate(value)
}

func inverseRelationPredicate(predicate string) string {
	return schema.InverseGraphPredicate(normalizeRelationPredicate(predicate))
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
	predicate := schema.NormalizeSemanticPredicate(stringValue(obj, "predicate"))
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

func (s *Service) canonicalizeSemanticSideForScopeWithResolutionIndex(ctx context.Context, value, scope string, interpretation *schema.Interpretation, index *captureResolutionIndex) string {
	canonical := canonicalizeSemanticSide(value, interpretation)
	if s.captureAccessRestricted() {
		if index == nil {
			return value
		}
		if looksLikeCanonicalEntityID(canonical) {
			entity := index.recordByID(canonical, scope)
			if current := s.captureResolvedTarget(ctx, entity, scope); current != nil && current.Type == schema.MemoryTypeEntity {
				return current.ID
			}
			return value
		}
		entity := index.findMatchingEntity(&schema.Mention{Surface: canonical}, scope)
		if current := s.captureResolvedTarget(ctx, entity, scope); current != nil && current.Type == schema.MemoryTypeEntity {
			return current.ID
		}
		return canonical
	}
	if looksLikeCanonicalEntityID(canonical) {
		if entity := s.captureExistingTarget(ctx, canonical, scope); entity != nil && entity.Type == schema.MemoryTypeEntity {
			return canonical
		}
		return value
	}
	if entity := s.lookupEntityByTerm(ctx, canonical, scope); entity != nil {
		if current := s.captureExistingTarget(ctx, entity.ID, scope); current != nil && current.Type == schema.MemoryTypeEntity {
			return current.ID
		}
	}
	return canonical
}

func (s *Service) canonicalizeSemanticObjectForScopeWithResolutionIndex(ctx context.Context, value any, scope string, interpretation *schema.Interpretation, index *captureResolutionIndex) any {
	text, ok := value.(string)
	if !ok {
		return value
	}
	return s.canonicalizeSemanticSideForScopeWithResolutionIndex(ctx, text, scope, interpretation, index)
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
		links = append(links, semanticEntityLink{id: payload.Subject, predicate: schema.GraphPredicateSubjectEntity, inverse: schema.GraphPredicateFactSubjectOf})
	}
	if objectID, ok := payload.Object.(string); ok && looksLikeCanonicalEntityID(objectID) {
		links = append(links, semanticEntityLink{id: objectID, predicate: schema.GraphPredicateObjectEntity, inverse: schema.GraphPredicateFactObjectOf})
	}
	return links
}

func looksLikeCanonicalEntityID(value string) bool {
	value = strings.TrimSpace(value)
	return value != "" && (strings.HasPrefix(value, "entity-") || strings.Count(value, "-") >= 4)
}

func (s *Service) linkRecordToCanonicalEntities(ctx context.Context, rec *schema.MemoryRecord, ts time.Time) ([]schema.GraphEdge, error) {
	return s.linkRecordToCanonicalEntitiesWithResolutionIndex(ctx, rec, ts, nil)
}

func (s *Service) linkRecordToCanonicalEntitiesWithResolutionIndex(ctx context.Context, rec *schema.MemoryRecord, ts time.Time, index *captureResolutionIndex) ([]schema.GraphEdge, error) {
	entityLinks := semanticEntityLinks(rec)
	if len(entityLinks) == 0 {
		return nil, nil
	}
	edges := make([]schema.GraphEdge, 0, len(entityLinks)*2)
	for _, link := range entityLinks {
		var entity *schema.MemoryRecord
		if s.captureAccessRestricted() {
			if index != nil {
				entity = s.captureResolvedTarget(ctx, index.recordByID(link.id, rec.Scope), rec.Scope)
			}
		} else {
			var err error
			entity, err = s.store.Get(ctx, link.id)
			if err != nil {
				continue
			}
		}
		if entity == nil || entity.Type != schema.MemoryTypeEntity || !s.captureCanMutate(entity) {
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
	if constraints, ok := value.([]schema.Constraint); ok {
		return constraints
	}
	if items, ok := value.([]map[string]any); ok {
		out := make([]schema.Constraint, 0, len(items))
		for _, obj := range items {
			out = append(out, schema.Constraint{
				Type:     stringValue(obj, "type"),
				Key:      stringValue(obj, "key"),
				Value:    obj["value"],
				Required: boolValue(obj["required"]),
			})
		}
		return out
	}

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
	return uniqueStringsLimited(items, len(items))
}

func uniqueStringsLimited(items []string, limit int) []string {
	if limit <= 0 {
		return nil
	}
	seen := make(map[string]struct{}, min(len(items), limit))
	out := make([]string, 0, min(len(items), limit))
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
		if len(out) >= limit {
			break
		}
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
