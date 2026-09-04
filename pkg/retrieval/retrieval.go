package retrieval

import (
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"sort"
	"unicode/utf8"

	"github.com/BennettSchwartz/membrane/pkg/schema"
	"github.com/BennettSchwartz/membrane/pkg/storage"
)

// Common retrieval errors.
var (
	// ErrAccessDenied is returned when a trust context denies access to a record.
	ErrAccessDenied = errors.New("access denied by trust context")

	// ErrNilTrust is returned when a nil trust context is provided.
	ErrNilTrust = errors.New("trust context is required")

	// ErrBoundedGraphLookupUnsupported reports that a store does not implement
	// the finite lookup contract required for graph expansion.
	ErrBoundedGraphLookupUnsupported = errors.New("store does not support bounded graph lookup")

	// ErrBoundedRetrievalUnsupported reports that a store cannot provide the
	// pre-hydration projection required at a network retrieval boundary.
	ErrBoundedRetrievalUnsupported = errors.New("store does not support bounded retrieval")
)

// ProjectedRecordTooLargeError reports that one exact record cannot fit in the
// network projection budget even after append-only history and relations are
// omitted.
type ProjectedRecordTooLargeError struct {
	Limit int64
}

func (e *ProjectedRecordTooLargeError) Error() string {
	return fmt.Sprintf("projected record exceeds the %d-byte response limit", e.Limit)
}

// Service is the main retrieval service implementing layered memory retrieval
// per RFC 15.8.
type Service struct {
	store        storage.Store
	selector     *Selector
	embedding    EmbeddingService
	vectorRanker VectorRanker
}

// EmbeddingService generates query embeddings for retrieval-time applicability scoring.
type EmbeddingService interface {
	EmbedQuery(ctx context.Context, taskDescriptor string) ([]float32, error)
}

// VectorRanker searches stored embeddings by cosine similarity and returns
// record IDs ordered by relevance. Used to rank ALL record types when a
// query embedding is available.
type VectorRanker interface {
	SearchByEmbedding(ctx context.Context, query []float32, limit int) ([]string, error)
}

// CandidateVectorRanker searches stored embeddings for an already-filtered
// candidate set. Stores can implement this to avoid letting unrelated record
// types or out-of-scope records consume the global vector search window.
type CandidateVectorRanker interface {
	SearchByEmbeddingCandidates(ctx context.Context, query []float32, recordIDs []string, limit int) ([]string, error)
}

// RetrieveRequest specifies parameters for a layered retrieval query.
type RetrieveRequest struct {
	// TaskDescriptor describes the current task for contextual retrieval.
	TaskDescriptor string

	// QueryEmbedding is an optional pre-computed embedding for the query.
	// When set, Retrieve uses this instead of calling EmbedQuery, avoiding
	// an extra API round-trip and potential rate-limit failures.
	QueryEmbedding []float32

	// Trust is the trust context that gates what records can be returned.
	Trust *TrustContext

	// MemoryTypes optionally restricts retrieval to specific memory types.
	// If empty, all types are queried in layered order. If non-empty, the
	// service de-duplicates values and keeps canonical layered order.
	MemoryTypes []schema.MemoryType

	// MinSalience filters out records below this salience threshold.
	MinSalience float64

	// Limit caps the total number of returned records. A value of 0 uses the
	// service's hard candidate ceiling rather than allowing an unbounded scan.
	Limit int
}

// RecordProjection describes metadata deliberately omitted from bounded
// retrieval results. The trusted in-process RetrieveByID remains complete;
// network transports use RetrieveProjectedByID.
type RecordProjection struct {
	RelationsOmitted   bool
	RelationsTruncated bool
	HistoryOmitted     bool
	RecordsTruncated   bool
}

// ProjectedRecordResponse is the bounded single-record representation used by
// network transports. The trusted in-process RetrieveByID API remains full.
type ProjectedRecordResponse struct {
	Record     *schema.MemoryRecord
	Projection RecordProjection
}

// RetrieveResponse contains the results of a layered retrieval query.
type RetrieveResponse struct {
	// Records contains the filtered, sorted, and limited results.
	Records []*schema.MemoryRecord

	// Selection is non-nil when competence or plan_graph candidates were
	// evaluated through the multi-solution selector (RFC 15A.11).
	Selection *SelectionResult

	// Diagnostics reports non-fatal retrieval degradations. Retrieve still
	// returns records when it can fall back to salience or selector ranking.
	Diagnostics []RetrievalDiagnostic

	// Projection identifies fields omitted to keep storage and response work
	// finite independently of append-only record history and graph fan-out.
	Projection RecordProjection
}

// Retrieval diagnostic codes.
const (
	DiagnosticEmbeddingQueryFailed     = "embedding_query_failed"
	DiagnosticVectorRankFailed         = "vector_rank_failed"
	DiagnosticGraphExpandFailed        = "graph_expand_failed"
	DiagnosticGraphHistoryOmitted      = "graph_history_omitted"
	DiagnosticResponseByteLimitApplied = "response_byte_limit_applied"
)

// RetrievalDiagnostic describes a non-fatal degradation encountered while
// ranking retrieval results.
type RetrievalDiagnostic struct {
	Code    string
	Message string
}

// layerOrder defines the canonical retrieval order per RFC 15.8:
// working -> entity -> semantic -> competence -> plan_graph -> episodic.
var layerOrder = []schema.MemoryType{
	schema.MemoryTypeWorking,
	schema.MemoryTypeEntity,
	schema.MemoryTypeSemantic,
	schema.MemoryTypeCompetence,
	schema.MemoryTypePlanGraph,
	schema.MemoryTypeEpisodic,
}

// MaxGraphLimit is the hard service-side ceiling for graph response and work
// limits. It applies even when callers bypass the gRPC validation layer.
const MaxGraphLimit = storage.MaxBoundedLookupLimit

// MaxProjectedResponseBytes caps aggregate projected record bytes for one
// bounded retrieval response.
const MaxProjectedResponseBytes = storage.MaxBoundedHydrationBytes

const maxRetrievalCandidates = MaxGraphLimit

// NewService creates a new retrieval Service backed by the given store and selector.
func NewService(store storage.Store, selector *Selector) *Service {
	return &Service{
		store:    store,
		selector: selector,
	}
}

// NewServiceWithEmbedding creates a new retrieval Service with embedding support.
func NewServiceWithEmbedding(store storage.Store, selector *Selector, embedding EmbeddingService) *Service {
	return &Service{
		store:     store,
		selector:  selector,
		embedding: embedding,
	}
}

// NewServiceWithVectorRanker creates a retrieval Service that uses vector
// similarity to rank ALL record types, not just competence/plan_graph.
func NewServiceWithVectorRanker(store storage.Store, selector *Selector, embedding EmbeddingService, ranker VectorRanker) *Service {
	return &Service{
		store:        store,
		selector:     selector,
		embedding:    embedding,
		vectorRanker: ranker,
	}
}

// Retrieve performs layered retrieval as specified in RFC 15.8.
// It queries the store for each memory type layer in order, applies trust and
// salience filtering, runs competence/plan_graph results through the selector,
// ranks records, and applies the limit. Returned records are bounded
// projections without relation, audit, or provenance history; callers that
// select a record in trusted Go code can use RetrieveByID for its complete
// representation. Network transports keep the bounded projection.
func (svc *Service) Retrieve(ctx context.Context, req *RetrieveRequest) (*RetrieveResponse, error) {
	if req == nil || req.Trust == nil {
		return nil, ErrNilTrust
	}

	layers, err := memoryTypeLayers(req.MemoryTypes)
	if err != nil {
		return nil, err
	}
	if err := validateMinSalience(req.MinSalience); err != nil {
		return nil, err
	}

	var allRecords []*schema.MemoryRecord
	var selectionCandidates []*schema.MemoryRecord
	var selection *SelectionResult
	var diagnostics []RetrievalDiagnostic

	// Use pre-computed embedding if provided, otherwise generate one.
	queryEmbedding := req.QueryEmbedding
	if len(queryEmbedding) > 0 {
		if err := validateQueryEmbedding(queryEmbedding); err != nil {
			return nil, fmt.Errorf("query_embedding: %w", err)
		}
	}
	if len(queryEmbedding) == 0 && svc.embedding != nil && req.TaskDescriptor != "" {
		var err error
		queryEmbedding, err = svc.embedding.EmbedQuery(ctx, req.TaskDescriptor)
		if err != nil {
			diagnostics = append(diagnostics, newRetrievalDiagnostic(DiagnosticEmbeddingQueryFailed, err))
			queryEmbedding = nil
		} else if err := validateQueryEmbedding(queryEmbedding); err != nil {
			diagnostics = append(diagnostics, newRetrievalDiagnostic(DiagnosticEmbeddingQueryFailed, err))
			queryEmbedding = nil
		}
	}

	listOptions := storage.ListOptions{
		Types:            layers,
		Scopes:           append([]string(nil), req.Trust.Scopes...),
		IncludeUnscoped:  len(req.Trust.Scopes) > 0,
		MaxSensitivity:   retrievalHydrationSensitivity(req.Trust.MaxSensitivity),
		MinSalience:      req.MinSalience,
		Limit:            retrievalCandidateLimit(req.Limit),
		OmitRelations:    true,
		OmitHistory:      true,
		MaxHydratedBytes: MaxProjectedResponseBytes,
	}
	boundedStore, ok := svc.store.(storage.BoundedListStore)
	if !ok {
		return nil, ErrBoundedRetrievalUnsupported
	}
	result, err := boundedStore.ListBounded(ctx, listOptions)
	if err != nil {
		return nil, fmt.Errorf("failed to list bounded retrieval candidates: %w", err)
	}
	records := result.Records
	byteLimitApplied := result.HydrationBytesTruncated
	// ListOptions projection fields are an optimization contract, not a trust
	// boundary. Custom stores may ignore them, so cap and sanitize
	// the returned slice before any filtering, selection, or ranking work.
	records = boundedRetrieveRecords(records, listOptions.Limit)

	// Re-check policy in memory as defense in depth and to create the supported
	// one-level-redacted view for records hydrated at the redaction boundary.
	records = FilterByTrust(records, req.Trust)
	if req.MinSalience > 0 {
		records = FilterBySalience(records, req.MinSalience)
	}
	var serviceByteTruncated bool
	records, serviceByteTruncated = boundRecordsByProjectedBytes(records, MaxProjectedResponseBytes)
	byteLimitApplied = byteLimitApplied || serviceByteTruncated
	if byteLimitApplied {
		diagnostics = append(diagnostics, RetrievalDiagnostic{
			Code:    DiagnosticResponseByteLimitApplied,
			Message: fmt.Sprintf("bounded retrieval stopped at the %d-byte projected response budget", MaxProjectedResponseBytes),
		})
	}
	for _, rec := range records {
		if rec != nil && (rec.Type == schema.MemoryTypeCompetence || rec.Type == schema.MemoryTypePlanGraph) {
			selectionCandidates = append(selectionCandidates, rec)
		}
	}
	allRecords = append(allRecords, records...)

	// Run selector on competence/plan_graph candidates if any exist.
	if len(selectionCandidates) > 0 && svc.selector != nil {
		selection = svc.selector.Select(ctx, selectionCandidates, queryEmbedding)
	}

	// Rank results. Priority:
	// 1. Vector similarity ranking for ALL records when a vector ranker
	//    and query embedding are available. Competence/plan_graph selection
	//    results are still promoted to the front.
	// 2. Selection-based ranking (competence/plan_graph scored by selector,
	//    remaining records by salience).
	// 3. Pure salience ranking (fallback when no embeddings are available).
	if svc.vectorRanker != nil && len(queryEmbedding) > 0 {
		var diagnostic *RetrievalDiagnostic
		allRecords, diagnostic = rankByVector(ctx, allRecords, svc.vectorRanker, queryEmbedding, selection)
		if diagnostic != nil {
			diagnostics = append(diagnostics, *diagnostic)
		}
	} else if selection != nil && len(selection.Selected) > 0 && req.TaskDescriptor != "" {
		allRecords = rankRecordsWithSelection(allRecords, selection)
	} else {
		SortBySalience(allRecords)
	}

	// Apply the service-side response ceiling even for direct Go callers.
	responseLimit := effectiveRetrievalResponseLimit(req.Limit)
	if len(allRecords) > responseLimit {
		allRecords = allRecords[:responseLimit]
	}
	selection = boundSelection(selection, responseLimit, allRecords)
	_, selectionByteBudget, _ := takeRecordsByProjectedBytes(allRecords, MaxProjectedResponseBytes)
	var selectionByteTruncated bool
	selection, selectionByteTruncated = boundSelectionByProjectedBytes(selection, selectionByteBudget)
	if selectionByteTruncated {
		byteLimitApplied = true
		diagnostics = appendResponseByteLimitDiagnostic(diagnostics)
	}

	return &RetrieveResponse{
		Records:     allRecords,
		Selection:   selection,
		Diagnostics: diagnostics,
		Projection: RecordProjection{
			RelationsOmitted:   true,
			RelationsTruncated: false,
			HistoryOmitted:     true,
			RecordsTruncated:   byteLimitApplied,
		},
	}, nil
}

func boundRecordsByProjectedBytes(records []*schema.MemoryRecord, budget int64) ([]*schema.MemoryRecord, bool) {
	bounded, _, truncated := takeRecordsByProjectedBytes(records, budget)
	return bounded, truncated
}

func takeRecordsByProjectedBytes(records []*schema.MemoryRecord, budget int64) ([]*schema.MemoryRecord, int64, bool) {
	if budget <= 0 {
		return []*schema.MemoryRecord{}, 0, len(records) > 0
	}
	bounded := make([]*schema.MemoryRecord, 0, len(records))
	remaining := budget
	for _, rec := range records {
		if rec == nil {
			continue
		}
		size := projectedRecordBytes(rec, remaining)
		if size > remaining {
			return bounded, remaining, true
		}
		remaining -= size
		bounded = append(bounded, rec)
	}
	return bounded, remaining, false
}

func projectedRecordBytes(rec *schema.MemoryRecord, capBytes int64) int64 {
	return storage.ProjectedRecordBytes(rec, capBytes)
}

func projectedValueBytes(value reflect.Value, capBytes int64, depth int) int64 {
	if !value.IsValid() || capBytes < 0 {
		return 0
	}
	if depth > 64 {
		return capBytes + 1
	}
	for value.Kind() == reflect.Interface || value.Kind() == reflect.Pointer {
		if value.IsNil() {
			return 4
		}
		value = value.Elem()
		depth++
		if depth > 64 {
			return capBytes + 1
		}
	}
	var total int64
	add := func(amount int64) bool {
		if amount < 0 || total > capBytes-amount {
			total = capBytes + 1
			return false
		}
		total += amount
		return true
	}
	switch value.Kind() {
	case reflect.String:
		return projectedJSONStringBytes(value.String(), capBytes)
	case reflect.Bool:
		return 5
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64:
		return 32
	case reflect.Slice:
		if value.IsNil() {
			return 4
		}
		if value.Type().Elem().Kind() == reflect.Uint8 {
			encoded := int64(value.Len())*4/3 + 8
			if encoded > capBytes {
				return capBytes + 1
			}
			return encoded
		}
		fallthrough
	case reflect.Array:
		if !add(2) {
			return total
		}
		for i := 0; i < value.Len(); i++ {
			if !add(projectedValueBytes(value.Index(i), capBytes-total, depth+1)) || !add(1) {
				return total
			}
		}
		return total
	case reflect.Map:
		if value.IsNil() {
			return 4
		}
		if !add(2) {
			return total
		}
		iter := value.MapRange()
		for iter.Next() {
			if !add(projectedValueBytes(iter.Key(), capBytes-total, depth+1)) ||
				!add(projectedValueBytes(iter.Value(), capBytes-total, depth+1)) || !add(2) {
				return total
			}
		}
		return total
	case reflect.Struct:
		if !add(2) {
			return total
		}
		for i := 0; i < value.NumField(); i++ {
			if !add(projectedValueBytes(value.Field(i), capBytes-total, depth+1)) || !add(1) {
				return total
			}
		}
		return total
	default:
		return 32
	}
}

func projectedJSONStringBytes(value string, capBytes int64) int64 {
	if capBytes < 2 {
		return capBytes + 1
	}
	total := int64(2)
	for len(value) > 0 {
		r, size := utf8.DecodeRuneInString(value)
		amount := int64(size)
		switch {
		case r == utf8.RuneError && size == 1:
			amount = 6
		case r == '"' || r == '\\':
			amount = 2
		case r < 0x20 || r == '\u2028' || r == '\u2029':
			amount = 6
		}
		if amount > capBytes-total {
			return capBytes + 1
		}
		total += amount
		value = value[size:]
	}
	return total
}

func effectiveRetrievalResponseLimit(requested int) int {
	if requested <= 0 || requested > maxRetrievalCandidates {
		return maxRetrievalCandidates
	}
	return requested
}

func boundedRetrieveRecords(records []*schema.MemoryRecord, limit int) []*schema.MemoryRecord {
	if limit <= 0 || limit > maxRetrievalCandidates {
		limit = maxRetrievalCandidates
	}
	if len(records) > limit {
		records = records[:limit]
	}
	bounded := make([]*schema.MemoryRecord, 0, len(records))
	for _, rec := range records {
		if rec == nil {
			continue
		}
		projection := *rec
		projection.Relations = nil
		projection.AuditLog = nil
		projection.Provenance.Sources = nil
		bounded = append(bounded, &projection)
	}
	return bounded
}

// boundSelection copies selection metadata while retaining at most limit
// selected records and scores. When allowed is non-nil, records outside the
// corresponding response are omitted as well.
func boundSelection(selection *SelectionResult, limit int, allowed []*schema.MemoryRecord) *SelectionResult {
	if selection == nil {
		return nil
	}
	if limit < 0 {
		limit = 0
	}
	if limit > maxRetrievalCandidates {
		limit = maxRetrievalCandidates
	}
	var allowedByID map[string]*schema.MemoryRecord
	if allowed != nil {
		allowedByID = make(map[string]*schema.MemoryRecord, min(len(allowed), limit))
		for _, rec := range allowed {
			if rec != nil {
				allowedByID[rec.ID] = rec
			}
		}
	}
	selected := make([]*schema.MemoryRecord, 0, min(len(selection.Selected), limit))
	scores := make(map[string]float64, min(len(selection.Scores), limit))
	seen := make(map[string]struct{}, min(len(selection.Selected), limit))
	for _, rec := range selection.Selected {
		if len(selected) >= limit {
			break
		}
		if rec == nil {
			continue
		}
		selectedRecord := rec
		if allowedByID != nil {
			var ok bool
			selectedRecord, ok = allowedByID[rec.ID]
			if !ok {
				continue
			}
		}
		if _, ok := seen[rec.ID]; ok {
			continue
		}
		seen[rec.ID] = struct{}{}
		selected = append(selected, selectedRecord)
		if score, ok := selection.Scores[rec.ID]; ok {
			scores[rec.ID] = score
		}
	}
	return &SelectionResult{
		Selected:   selected,
		Confidence: selection.Confidence,
		NeedsMore:  selection.NeedsMore,
		Scores:     scores,
	}
}

func boundSelectionByProjectedBytes(selection *SelectionResult, budget int64) (*SelectionResult, bool) {
	if selection == nil {
		return nil, false
	}
	if budget < 0 {
		budget = 0
	}
	selected := make([]*schema.MemoryRecord, 0, len(selection.Selected))
	scores := make(map[string]float64, len(selection.Scores))
	remaining := budget
	truncated := false
	for _, record := range selection.Selected {
		if record == nil {
			continue
		}
		recordBytes := projectedRecordBytes(record, remaining)
		if recordBytes > remaining {
			truncated = true
			break
		}
		remaining -= recordBytes
		selected = append(selected, record)
		if score, ok := selection.Scores[record.ID]; ok {
			scores[record.ID] = score
		}
	}
	return &SelectionResult{
		Selected:   selected,
		Confidence: selection.Confidence,
		NeedsMore:  selection.NeedsMore,
		Scores:     scores,
	}, truncated
}

func retrievalCandidateLimit(responseLimit int) int {
	if responseLimit <= 0 {
		return maxRetrievalCandidates
	}
	if responseLimit > maxRetrievalCandidates/4 {
		return maxRetrievalCandidates
	}
	limit := responseLimit * 4
	if limit < 256 {
		limit = 256
	}
	if limit > maxRetrievalCandidates || limit < 0 {
		limit = maxRetrievalCandidates
	}
	return limit
}

func retrievalHydrationSensitivity(max schema.Sensitivity) schema.Sensitivity {
	ordered := []schema.Sensitivity{
		schema.SensitivityPublic,
		schema.SensitivityLow,
		schema.SensitivityMedium,
		schema.SensitivityHigh,
		schema.SensitivityHyper,
	}
	level := SensitivityLevel(max)
	if level < 0 {
		return max
	}
	if level+1 < len(ordered) {
		return ordered[level+1]
	}
	return max
}

func memoryTypeLayers(types []schema.MemoryType) ([]schema.MemoryType, error) {
	if len(types) == 0 {
		return layerOrder, nil
	}
	requested := make(map[schema.MemoryType]struct{}, len(types))
	for _, mt := range types {
		if !schema.IsValidMemoryType(mt) {
			return nil, fmt.Errorf("invalid memory type %q", mt)
		}
		requested[mt] = struct{}{}
	}
	layers := make([]schema.MemoryType, 0, len(requested))
	for _, mt := range layerOrder {
		if _, ok := requested[mt]; ok {
			layers = append(layers, mt)
		}
	}
	return layers, nil
}

func newRetrievalDiagnostic(code string, err error) RetrievalDiagnostic {
	return RetrievalDiagnostic{
		Code:    code,
		Message: err.Error(),
	}
}

func validateQueryEmbedding(values []float32) error {
	nonZero := false
	for i, v := range values {
		if math.IsNaN(float64(v)) || math.IsInf(float64(v), 0) {
			return fmt.Errorf("embedding contains non-finite value at index %d", i)
		}
		if v != 0 {
			nonZero = true
		}
	}
	if !nonZero {
		return errors.New("embedding must contain at least one non-zero value")
	}
	return nil
}

func validateMinSalience(value float64) error {
	if math.IsNaN(value) || math.IsInf(value, 0) || value < 0 || value > 1 {
		return errors.New("min_salience must be finite and between 0 and 1")
	}
	return nil
}

func rankRecordsWithSelection(records []*schema.MemoryRecord, selection *SelectionResult) []*schema.MemoryRecord {
	ranked := make([]*schema.MemoryRecord, 0, len(records))
	seen := make(map[string]struct{}, len(records))
	for _, rec := range selection.Selected {
		ranked = append(ranked, rec)
		seen[rec.ID] = struct{}{}
	}

	remaining := make([]*schema.MemoryRecord, 0, len(records))
	for _, rec := range records {
		if _, ok := seen[rec.ID]; ok {
			continue
		}
		remaining = append(remaining, rec)
	}
	SortBySalience(remaining)
	return append(ranked, remaining...)
}

// rankByVector re-orders records using vector similarity from the VectorRanker,
// while keeping selector-promoted competence/plan_graph records at the front.
func rankByVector(ctx context.Context, records []*schema.MemoryRecord, ranker VectorRanker, query []float32, selection *SelectionResult) ([]*schema.MemoryRecord, *RetrievalDiagnostic) {
	if len(records) == 0 {
		return records, nil
	}

	// Build an index of records by ID before searching so stores that support
	// candidate-restricted vector search can rank exactly the allowed records.
	byID := make(map[string]*schema.MemoryRecord, len(records))
	candidateIDs := make([]string, 0, len(records))
	for _, rec := range records {
		if rec == nil {
			continue
		}
		byID[rec.ID] = rec
		candidateIDs = append(candidateIDs, rec.ID)
	}

	var rankedIDs []string
	var err error
	if candidateRanker, ok := ranker.(CandidateVectorRanker); ok {
		rankedIDs, err = candidateRanker.SearchByEmbeddingCandidates(ctx, query, candidateIDs, len(candidateIDs))
	} else {
		// Legacy rankers can only search globally, so request a wider window and
		// intersect with byID below.
		searchLimit := len(records) * 10
		if searchLimit < 500 {
			searchLimit = 500
		}
		if searchLimit > maxRetrievalCandidates {
			searchLimit = maxRetrievalCandidates
		}
		rankedIDs, err = ranker.SearchByEmbedding(ctx, query, searchLimit)
	}
	if err != nil {
		// Fall back to salience if vector search fails.
		SortBySalience(records)
		diagnostic := newRetrievalDiagnostic(DiagnosticVectorRankFailed, err)
		return records, &diagnostic
	}
	if len(rankedIDs) == 0 {
		// Fall back to salience if vector search has no matches.
		SortBySalience(records)
		return records, nil
	}

	// Assign vector rank scores after filtering to accessible candidates. Legacy
	// global rankers may return many unrelated IDs before the records in this
	// request, but those outside IDs should not dilute the vector signal for the
	// candidate set that survived memory-type, trust, and salience filtering.
	var matchedRecords []*schema.MemoryRecord
	seenRankedIDs := make(map[string]struct{}, len(rankedIDs))
	for _, id := range rankedIDs {
		rec, ok := byID[id]
		if !ok {
			continue
		}
		if _, ok := seenRankedIDs[id]; ok {
			continue
		}
		seenRankedIDs[id] = struct{}{}
		matchedRecords = append(matchedRecords, rec)
	}
	if len(matchedRecords) == 0 {
		SortBySalience(records)
		return records, nil
	}

	// Top candidate gets 1.0, linearly decreasing, then blends with salience.
	type scored struct {
		rec   *schema.MemoryRecord
		score float64
	}
	matched := make([]scored, 0, len(matchedRecords))
	for rank, rec := range matchedRecords {
		// Vector similarity score: 1.0 for rank 0, decreasing.
		vecScore := 1.0
		if len(matchedRecords) > 1 {
			vecScore = 1.0 - float64(rank)/float64(len(matchedRecords))
		}
		// Hybrid: 70% vector similarity, 30% salience.
		hybrid := 0.7*vecScore + 0.3*rec.Salience
		matched = append(matched, scored{rec, hybrid})
	}

	// Sort by hybrid score descending.
	sort.Slice(matched, func(i, j int) bool {
		if matched[i].score != matched[j].score {
			return matched[i].score > matched[j].score
		}
		return recordTieLess(matched[i].rec, matched[j].rec)
	})

	result := make([]*schema.MemoryRecord, 0, len(records))
	seen := make(map[string]struct{}, len(matched))
	if selection != nil {
		for _, rec := range selection.Selected {
			if _, ok := byID[rec.ID]; !ok {
				continue
			}
			if _, ok := seen[rec.ID]; ok {
				continue
			}
			result = append(result, rec)
			seen[rec.ID] = struct{}{}
		}
	}
	for _, s := range matched {
		if _, ok := seen[s.rec.ID]; ok {
			continue
		}
		result = append(result, s.rec)
		seen[s.rec.ID] = struct{}{}
	}

	// Any records not in the vector search results go last, sorted by salience.
	var remainder []*schema.MemoryRecord
	for _, rec := range records {
		if _, ok := seen[rec.ID]; !ok {
			remainder = append(remainder, rec)
		}
	}
	SortBySalience(remainder)
	result = append(result, remainder...)

	return result, nil
}

// RetrieveByID fetches a single record by ID and checks it against the trust context.
// Returns storage.ErrNotFound if the record does not exist, or ErrAccessDenied
// if the trust context does not allow access.
func (svc *Service) RetrieveByID(ctx context.Context, id string, trust *TrustContext) (*schema.MemoryRecord, error) {
	if trust == nil {
		return nil, ErrNilTrust
	}

	record, err := svc.store.Get(ctx, id)
	if err != nil {
		return nil, err
	}

	if !trust.Allows(record) {
		return nil, ErrAccessDenied
	}

	return record, nil
}

// RetrieveProjectedByID retrieves one exact record through the bounded list
// contract so payload size is checked before production storage hydrates it.
// Returned data omits relation and append-only history fields and is sanitized
// again in service code for custom stores that ignore projection hints.
func (svc *Service) RetrieveProjectedByID(ctx context.Context, id string, trust *TrustContext) (*ProjectedRecordResponse, error) {
	if trust == nil {
		return nil, ErrNilTrust
	}
	boundedStore, ok := svc.store.(storage.BoundedListStore)
	if !ok {
		return nil, ErrBoundedRetrievalUnsupported
	}

	result, err := boundedStore.ListBounded(ctx, storage.ListOptions{
		ID:               id,
		Scopes:           append([]string(nil), trust.Scopes...),
		IncludeUnscoped:  len(trust.Scopes) > 0,
		MaxSensitivity:   trust.MaxSensitivity,
		Limit:            1,
		OmitRelations:    true,
		OmitHistory:      true,
		MaxHydratedBytes: MaxProjectedResponseBytes,
	})
	if err != nil {
		return nil, fmt.Errorf("retrieve projected record: %w", err)
	}

	// A conforming exact-ID query returns at most one row. Inspect no more than
	// that even if a custom implementation violates the contract.
	records := boundedRetrieveRecords(result.Records, 1)
	if len(records) == 0 || records[0].ID != id {
		if result.HydrationBytesTruncated {
			return nil, &ProjectedRecordTooLargeError{Limit: MaxProjectedResponseBytes}
		}
		return nil, storage.ErrNotFound
	}
	record := records[0]
	if !trust.Allows(record) {
		return nil, ErrAccessDenied
	}
	if storage.ProjectedRecordBytes(record, MaxProjectedResponseBytes) > MaxProjectedResponseBytes {
		return nil, &ProjectedRecordTooLargeError{Limit: MaxProjectedResponseBytes}
	}

	return &ProjectedRecordResponse{
		Record: record,
		Projection: RecordProjection{
			RelationsOmitted: true,
			HistoryOmitted:   true,
		},
	}, nil
}

// RetrieveByType fetches all records of a given type that pass the trust check.
func (svc *Service) RetrieveByType(ctx context.Context, memType schema.MemoryType, trust *TrustContext) ([]*schema.MemoryRecord, error) {
	if trust == nil {
		return nil, ErrNilTrust
	}

	records, err := svc.store.ListByType(ctx, memType)
	if err != nil {
		return nil, err
	}

	records = FilterByTrust(records, trust)
	SortBySalience(records)

	return records, nil
}
