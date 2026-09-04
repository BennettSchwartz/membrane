package retrieval

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/BennettSchwartz/membrane/pkg/schema"
	"github.com/BennettSchwartz/membrane/pkg/storage"
)

// RetrieveGraphRequest expands ranked root memories into a bounded graph.
type RetrieveGraphRequest struct {
	TaskDescriptor string
	QueryEmbedding []float32
	Trust          *TrustContext
	MemoryTypes    []schema.MemoryType
	MinSalience    float64
	RootLimit      int
	NodeLimit      int
	EdgeLimit      int
	MaxHops        int
}

// GraphNode is a graph retrieval node annotated with root/hop metadata.
type GraphNode struct {
	Record *schema.MemoryRecord `json:"record"`
	Root   bool                 `json:"root"`
	Hop    int                  `json:"hop"`
}

// RetrieveGraphResponse contains the expanded graph rooted at the ranked roots.
type RetrieveGraphResponse struct {
	Nodes       []GraphNode           `json:"nodes"`
	Edges       []schema.GraphEdge    `json:"edges"`
	RootIDs     []string              `json:"root_ids"`
	Selection   *SelectionResult      `json:"selection,omitempty"`
	Diagnostics []RetrievalDiagnostic `json:"diagnostics,omitempty"`
	Projection  RecordProjection      `json:"projection"`
}

func (svc *Service) RetrieveGraph(ctx context.Context, req *RetrieveGraphRequest) (*RetrieveGraphResponse, error) {
	if req == nil {
		req = &RetrieveGraphRequest{}
	}
	if req.Trust == nil {
		return nil, ErrNilTrust
	}

	rootLimit := effectiveGraphLimit(req.RootLimit, 10)
	nodeLimit := effectiveGraphLimit(req.NodeLimit, 25)
	edgeLimit := effectiveGraphLimit(req.EdgeLimit, 100)
	maxHops := req.MaxHops
	if maxHops == -1 {
		maxHops = 0
	} else if maxHops < -1 {
		return nil, fmt.Errorf("max_hops must be -1 or non-negative")
	} else if maxHops > MaxGraphLimit {
		maxHops = MaxGraphLimit
	}

	baseResp, err := svc.Retrieve(ctx, &RetrieveRequest{
		TaskDescriptor: req.TaskDescriptor,
		QueryEmbedding: req.QueryEmbedding,
		Trust:          req.Trust,
		MemoryTypes:    req.MemoryTypes,
		MinSalience:    req.MinSalience,
		Limit:          min(maxRetrievalCandidates, max(rootLimit*3, rootLimit)),
	})
	if err != nil {
		return nil, err
	}

	baseRecords := boundedGraphRecords(baseResp.Records, edgeLimit)
	entityReq := *req
	entityReq.RootLimit = rootLimit
	_, entityHydrationBudget, _ := takeRecordsByProjectedBytes(baseResp.Records, MaxProjectedResponseBytes)
	entityResult := svc.entityRootCandidatesBounded(ctx, &entityReq, entityHydrationBudget)
	baseRecords = append(baseRecords, boundedGraphRecords(entityResult.Records, edgeLimit)...)
	baseRecords = uniqueRecords(baseRecords)
	roots := svc.rerankGraphRootsLimited(ctx, baseRecords, req.TaskDescriptor, req.Trust, min(edgeLimit, maxGraphRootBoostRelations))
	if len(roots) > rootLimit {
		roots = roots[:rootLimit]
	}
	diagnostics := append([]RetrievalDiagnostic(nil), baseResp.Diagnostics...)
	recordsTruncated := baseResp.Projection.RecordsTruncated || entityResult.HydrationBytesTruncated
	if entityResult.HydrationBytesTruncated {
		diagnostics = appendResponseByteLimitDiagnostic(diagnostics)
	}
	if len(roots) > nodeLimit {
		roots = roots[:nodeLimit]
	}
	var rootsByteTruncated bool
	var responseBytesRemaining int64
	roots, responseBytesRemaining, rootsByteTruncated = takeRecordsByProjectedBytes(roots, MaxProjectedResponseBytes)
	if rootsByteTruncated {
		recordsTruncated = true
		diagnostics = appendResponseByteLimitDiagnostic(diagnostics)
	}

	nodeByID := make(map[string]GraphNode, nodeLimit)
	rootIDs := make([]string, 0, len(roots))
	queue := make([]GraphNode, 0, len(roots))
	for _, rec := range roots {
		node := GraphNode{Record: rec, Root: true, Hop: 0}
		nodeByID[rec.ID] = node
		rootIDs = append(rootIDs, rec.ID)
		queue = append(queue, node)
		if len(nodeByID) >= nodeLimit {
			break
		}
	}

	edges := make([]schema.GraphEdge, 0, edgeLimit)
	seenEdges := make(map[string]struct{}, edgeLimit)
	targetCache := make(map[string]boundedGraphRecordProjection)
	workRemaining := graphExpansionWorkBudget(edgeLimit)
	relationHydrationRemaining := responseBytesRemaining
	recordHydrationRemaining := responseBytesRemaining
	byteBudgetExhausted := false

	for i := 0; i < len(queue); i++ {
		if len(edges) >= edgeLimit || workRemaining <= 0 || byteBudgetExhausted {
			break
		}
		current := queue[i]
		// A redacted node is a terminal metadata view. Loading its relations
		// would restore content removed by Redact and reveal private neighbors.
		if current.Hop >= maxHops || current.Record == nil || !req.Trust.Allows(current.Record) {
			continue
		}
		lookupByteBudget := min(responseBytesRemaining, relationHydrationRemaining)
		graphEdges, examined, hydratedBytes, relationBytesTruncated, err := svc.graphEdges(ctx, current.Record, edgeLimit-len(edges), workRemaining, lookupByteBudget)
		workRemaining -= examined
		if hydratedBytes > relationHydrationRemaining {
			hydratedBytes = relationHydrationRemaining
			relationBytesTruncated = true
		}
		relationHydrationRemaining -= hydratedBytes
		if relationBytesTruncated {
			recordsTruncated = true
			diagnostics = appendResponseByteLimitDiagnostic(diagnostics)
		}
		if err != nil {
			diagnostics = appendGraphDiagnostic(diagnostics, err)
			if len(graphEdges) == 0 {
				continue
			}
		}
		prioritizeGraphEdges(graphEdges)
		for _, edge := range graphEdges {
			if len(edges) >= edgeLimit {
				break
			}
			edgeKey := edge.SourceID + "|" + edge.Predicate + "|" + edge.TargetID
			if _, ok := seenEdges[edgeKey]; ok {
				continue
			}
			neighborID := graphEdgeNeighborID(edge, current.Record.ID)
			if neighborID == "" {
				continue
			}
			var target *schema.MemoryRecord
			var targetProjectedBytes int64
			addTargetNode := false
			if existing, ok := nodeByID[neighborID]; ok {
				target = existing.Record
			} else {
				cachedTarget, cached := targetCache[neighborID]
				if !cached {
					if workRemaining <= 0 {
						break
					}
					workRemaining--
					lookupBudget := min(responseBytesRemaining, recordHydrationRemaining)
					fetched, err := svc.getGraphRecord(ctx, neighborID, req.Trust, lookupBudget, true)
					if fetched.HydratedBytes > recordHydrationRemaining {
						fetched.HydratedBytes = recordHydrationRemaining
					}
					recordHydrationRemaining -= fetched.HydratedBytes
					if err != nil {
						targetCache[neighborID] = boundedGraphRecordProjection{}
						var tooLarge *ProjectedRecordTooLargeError
						if errors.As(err, &tooLarge) {
							recordsTruncated = true
							byteBudgetExhausted = true
							diagnostics = appendResponseByteLimitDiagnostic(diagnostics)
							break
						}
						diagnostics = appendGraphDiagnostic(diagnostics, err)
						continue
					}
					cachedTarget = fetched
					targetCache[neighborID] = fetched
				}
				target = cachedTarget.Record
				targetProjectedBytes = cachedTarget.ProjectedBytes
				if target == nil {
					continue
				}

				if len(nodeByID) >= nodeLimit {
					continue
				}
				addTargetNode = true
			}
			// Incoming edges belong to the neighbor's record. Its metadata
			// visibility alone does not authorize exposing those relationships.
			if edge.SourceID != current.Record.ID && !req.Trust.Allows(target) {
				continue
			}

			projectedBytes := projectedGraphEdgeBytes(edge, responseBytesRemaining)
			if addTargetNode && projectedBytes <= responseBytesRemaining {
				if targetProjectedBytes > responseBytesRemaining-projectedBytes {
					projectedBytes = responseBytesRemaining + 1
				} else {
					projectedBytes += targetProjectedBytes
				}
			}
			if edge.SourceID == current.Record.ID && projectedBytes <= responseBytesRemaining {
				projectedBytes += projectedRelationBytes(edge, responseBytesRemaining-projectedBytes)
			}
			if projectedBytes > responseBytesRemaining {
				recordsTruncated = true
				byteBudgetExhausted = true
				diagnostics = appendResponseByteLimitDiagnostic(diagnostics)
				break
			}
			responseBytesRemaining -= projectedBytes
			if addTargetNode {
				node := GraphNode{Record: target, Hop: current.Hop + 1}
				nodeByID[target.ID] = node
				queue = append(queue, node)
			}
			if edge.SourceID == current.Record.ID {
				current.Record.Relations = append(current.Record.Relations, schema.Relation{
					Predicate: edge.Predicate,
					TargetID:  edge.TargetID,
					Weight:    edge.Weight,
					CreatedAt: edge.CreatedAt,
				})
			}

			edges = append(edges, edge)
			seenEdges[edgeKey] = struct{}{}
		}
		if relationBytesTruncated {
			byteBudgetExhausted = true
		}
	}

	nodes := make([]GraphNode, 0, len(nodeByID))
	for _, node := range nodeByID {
		nodes = append(nodes, node)
	}
	sort.Slice(nodes, func(i, j int) bool {
		if nodes[i].Root != nodes[j].Root {
			return nodes[i].Root
		}
		if nodes[i].Hop != nodes[j].Hop {
			return nodes[i].Hop < nodes[j].Hop
		}
		if nodes[i].Record.Salience != nodes[j].Record.Salience {
			return nodes[i].Record.Salience > nodes[j].Record.Salience
		}
		return nodes[i].Record.ID < nodes[j].Record.ID
	})

	selection := boundSelection(baseResp.Selection, min(rootLimit, nodeLimit), roots)
	var selectionByteTruncated bool
	selection, selectionByteTruncated = boundSelectionByProjectedBytes(selection, responseBytesRemaining)
	if selectionByteTruncated {
		recordsTruncated = true
		diagnostics = appendResponseByteLimitDiagnostic(diagnostics)
	}

	return &RetrieveGraphResponse{
		Nodes:       nodes,
		Edges:       edges,
		RootIDs:     rootIDs,
		Selection:   selection,
		Diagnostics: diagnostics,
		Projection: RecordProjection{
			RelationsOmitted:   maxHops == 0,
			RelationsTruncated: maxHops > 0,
			HistoryOmitted:     true,
			RecordsTruncated:   recordsTruncated,
		},
	}, nil
}

const (
	// Root boosts are optional ranking hints. Bound their relation fan-out,
	// identifier parsing, normalization bytes, and aggregate comparison work
	// independently of the number or size of root candidates.
	maxGraphRootBoostRelationLookups      = 4
	maxGraphRootBoostRelations            = 4
	maxGraphRootBoostWork                 = 4096
	maxGraphRootBoostNormalizationBytes   = 256 << 10
	maxGraphRootBoostIdentifierParseBytes = 8 << 10
	maxGraphRootBoostIdentifiers          = 32
	maxGraphRootBoostTermBytes            = 4 << 10
	maxGraphRootBoostTermsPerRecord       = 64
	maxGraphRootBoostEntityIdentifiers    = 32
	maxGraphDiagnostics                   = 16
)

type boundedGraphRecordProjection struct {
	Record         *schema.MemoryRecord
	HydratedBytes  int64
	ProjectedBytes int64
}

type graphRootBoostBudget struct {
	hydrationRemaining          int64
	workRemaining               int
	normalizationBytesRemaining int
	workUsed                    int
	normalizationBytesUsed      int
	normalizationCalls          int
	queryNormalizations         int
	comparisons                 int
	identifierTokensParsed      int
	identifierBytesParsed       int
}

func newGraphRootBoostBudget() *graphRootBoostBudget {
	return &graphRootBoostBudget{
		hydrationRemaining:          MaxProjectedResponseBytes,
		workRemaining:               maxGraphRootBoostWork,
		normalizationBytesRemaining: maxGraphRootBoostNormalizationBytes,
	}
}

func (b *graphRootBoostBudget) consumeWork() bool {
	if b == nil || b.workRemaining <= 0 {
		return false
	}
	b.workRemaining--
	b.workUsed++
	return true
}

func (b *graphRootBoostBudget) normalizeTerm(value string, maxBytes int) (string, bool) {
	if b == nil || b.normalizationBytesRemaining <= 0 || !b.consumeWork() {
		return "", false
	}
	if value == "" || len(value) > maxBytes || len(value) > b.normalizationBytesRemaining {
		return "", false
	}
	b.normalizationBytesRemaining -= len(value)
	b.normalizationBytesUsed += len(value)
	b.normalizationCalls++
	return schema.NormalizeEntityTerm(value), true
}

func (b *graphRootBoostBudget) normalizedTermMatches(term, query string) bool {
	if term == "" || query == "" || !b.consumeWork() {
		return false
	}
	b.comparisons++
	return schema.NormalizedEntityTermMatchesQuery(term, query)
}

func effectiveGraphLimit(requested, defaultValue int) int {
	if requested <= 0 {
		return defaultValue
	}
	if requested > MaxGraphLimit {
		return MaxGraphLimit
	}
	return requested
}

func graphExpansionWorkBudget(edgeLimit int) int {
	budget := edgeLimit * 4
	if budget < 256 {
		budget = 256
	}
	if budget > MaxGraphLimit {
		budget = MaxGraphLimit
	}
	return budget
}

func appendGraphDiagnostic(diagnostics []RetrievalDiagnostic, err error) []RetrievalDiagnostic {
	// Filtered and absent neighbors are intentionally indistinguishable. Other
	// storage errors can contain hidden IDs or database details, so expose only
	// the operational failure category to the caller.
	if err == nil || errors.Is(err, storage.ErrNotFound) || errors.Is(err, ErrAccessDenied) || len(diagnostics) >= maxGraphDiagnostics {
		return diagnostics
	}
	return append(diagnostics, RetrievalDiagnostic{
		Code:    DiagnosticGraphExpandFailed,
		Message: "some graph relationships could not be retrieved",
	})
}

func appendResponseByteLimitDiagnostic(diagnostics []RetrievalDiagnostic) []RetrievalDiagnostic {
	for _, diagnostic := range diagnostics {
		if diagnostic.Code == DiagnosticResponseByteLimitApplied {
			return diagnostics
		}
	}
	return append(diagnostics, RetrievalDiagnostic{
		Code:    DiagnosticResponseByteLimitApplied,
		Message: fmt.Sprintf("bounded retrieval stopped at the %d-byte projected response budget", MaxProjectedResponseBytes),
	})
}

func projectedGraphEdgeBytes(edge schema.GraphEdge, capBytes int64) int64 {
	const envelopeBytes = 128
	if capBytes < envelopeBytes {
		return capBytes + 1
	}
	size := projectedValueBytes(reflect.ValueOf(edge), capBytes-envelopeBytes, 0)
	if size > capBytes-envelopeBytes {
		return capBytes + 1
	}
	return envelopeBytes + size
}

func projectedRelationBytes(edge schema.GraphEdge, capBytes int64) int64 {
	return projectedValueBytes(reflect.ValueOf(schema.Relation{
		Predicate: edge.Predicate,
		TargetID:  edge.TargetID,
		Weight:    edge.Weight,
		CreatedAt: edge.CreatedAt,
	}), capBytes, 0)
}

func sanitizeBoundedRelations(result storage.BoundedRelationResult, limit int, maxHydratedBytes int64) storage.BoundedRelationResult {
	limit = min(max(limit, 0), MaxGraphLimit)
	maxHydratedBytes = min(maxHydratedBytes, MaxProjectedResponseBytes)
	relations := append([]schema.Relation(nil), result.Relations...)
	prioritizeRelations(relations)
	out := make([]schema.Relation, 0, min(len(relations), limit))
	remaining := maxHydratedBytes
	actualBytes := int64(0)
	truncated := result.HydrationBytesTruncated || len(relations) > limit
	for _, relation := range relations {
		if len(out) >= limit {
			break
		}
		rowBytes := projectedRelationHydrationBytes(relation.Predicate, relation.TargetID, "", remaining)
		if rowBytes > remaining {
			truncated = true
			break
		}
		remaining -= rowBytes
		actualBytes += rowBytes
		out = append(out, relation)
	}
	projectedBytes := max(actualBytes, result.ProjectedBytes)
	if projectedBytes > maxHydratedBytes {
		projectedBytes = maxHydratedBytes
		truncated = true
	}
	return storage.BoundedRelationResult{
		Relations:               out,
		ProjectedBytes:          projectedBytes,
		HydrationBytesTruncated: truncated,
	}
}

func sanitizeBoundedIncomingRelations(result storage.BoundedIncomingRelationResult, limit int, maxHydratedBytes int64) storage.BoundedIncomingRelationResult {
	limit = min(max(limit, 0), MaxGraphLimit)
	maxHydratedBytes = min(maxHydratedBytes, MaxProjectedResponseBytes)
	edges := append([]schema.GraphEdge(nil), result.Edges...)
	prioritizeGraphEdges(edges)
	out := make([]schema.GraphEdge, 0, min(len(edges), limit))
	remaining := maxHydratedBytes
	actualBytes := int64(0)
	truncated := result.HydrationBytesTruncated || len(edges) > limit
	for _, edge := range edges {
		rowBytes := projectedRelationHydrationBytes(edge.SourceID, edge.Predicate, edge.TargetID, remaining)
		if rowBytes > remaining {
			truncated = true
			break
		}
		remaining -= rowBytes
		actualBytes += rowBytes
		out = append(out, edge)
	}
	projectedBytes := max(actualBytes, result.ProjectedBytes)
	if projectedBytes > maxHydratedBytes {
		projectedBytes = maxHydratedBytes
		truncated = true
	}
	return storage.BoundedIncomingRelationResult{
		Edges:                   out,
		ProjectedBytes:          projectedBytes,
		HydrationBytesTruncated: truncated,
	}
}

func projectedRelationHydrationBytes(first, second, third string, capBytes int64) int64 {
	if capBytes < storage.ProjectedRelationOverheadBytes {
		return capBytes + 1
	}
	total := storage.ProjectedRelationOverheadBytes
	for _, value := range []string{first, second, third} {
		length := int64(len(value))
		if length > capBytes-total {
			return capBytes + 1
		}
		total += length
	}
	return total
}

func boundedGraphRecords(records []*schema.MemoryRecord, relationLimit int) []*schema.MemoryRecord {
	// Root candidates must begin without relation/history hydration. Relations
	// are populated only through graphEdges' bounded store contract.
	return boundedRetrieveRecords(records, min(len(records), maxRetrievalCandidates))
}

func (svc *Service) getGraphRecord(ctx context.Context, id string, trust *TrustContext, maxHydratedBytes int64, allowRedaction bool) (boundedGraphRecordProjection, error) {
	if id == "" {
		return boundedGraphRecordProjection{}, storage.ErrNotFound
	}
	maxHydratedBytes = min(maxHydratedBytes, MaxProjectedResponseBytes)
	if maxHydratedBytes <= 0 {
		return boundedGraphRecordProjection{}, &ProjectedRecordTooLargeError{Limit: maxHydratedBytes}
	}
	boundedStore, ok := svc.store.(storage.BoundedListStore)
	if !ok {
		return boundedGraphRecordProjection{}, fmt.Errorf("%w: BoundedListStore", ErrBoundedGraphLookupUnsupported)
	}

	opts := storage.ListOptions{
		ID:               id,
		Limit:            1,
		OmitRelations:    true,
		OmitHistory:      true,
		MaxHydratedBytes: maxHydratedBytes,
	}
	if trust != nil {
		opts.Scopes = append([]string(nil), trust.Scopes...)
		opts.IncludeUnscoped = len(trust.Scopes) > 0
		opts.MaxSensitivity = trust.MaxSensitivity
		if allowRedaction {
			opts.MaxSensitivity = retrievalHydrationSensitivity(trust.MaxSensitivity)
		}
	}
	result, err := boundedStore.ListBounded(ctx, opts)
	if err != nil {
		return boundedGraphRecordProjection{}, fmt.Errorf("bounded graph record %s: %w", id, err)
	}
	if result.ProjectedBytes < 0 || result.ProjectedBytes > maxHydratedBytes || len(result.Records) > 1 {
		return boundedGraphRecordProjection{}, fmt.Errorf("%w: invalid exact-ID bounded projection", ErrBoundedGraphLookupUnsupported)
	}
	records := boundedRetrieveRecords(result.Records, 1)
	if len(records) == 0 {
		if result.HydrationBytesTruncated {
			return boundedGraphRecordProjection{}, &ProjectedRecordTooLargeError{Limit: maxHydratedBytes}
		}
		if result.ProjectedBytes != 0 {
			return boundedGraphRecordProjection{}, fmt.Errorf("%w: empty exact-ID projection reported hydrated bytes", ErrBoundedGraphLookupUnsupported)
		}
		return boundedGraphRecordProjection{}, storage.ErrNotFound
	}
	if records[0].ID != id {
		return boundedGraphRecordProjection{}, fmt.Errorf("%w: exact-ID bounded projection returned %q for %q", ErrBoundedGraphLookupUnsupported, records[0].ID, id)
	}
	if result.HydrationBytesTruncated {
		return boundedGraphRecordProjection{}, &ProjectedRecordTooLargeError{Limit: maxHydratedBytes}
	}

	record := records[0]
	actualHydratedBytes := storage.ProjectedRecordBytes(record, maxHydratedBytes)
	if actualHydratedBytes > maxHydratedBytes {
		return boundedGraphRecordProjection{}, &ProjectedRecordTooLargeError{Limit: maxHydratedBytes}
	}
	hydratedBytes := max(actualHydratedBytes, result.ProjectedBytes)
	view := record
	if trust != nil && !trust.Allows(record) {
		if allowRedaction && trust.AllowsRedacted(record) {
			view = Redact(record)
		} else {
			return boundedGraphRecordProjection{HydratedBytes: hydratedBytes}, nil
		}
	}
	projectedBytes := storage.ProjectedRecordBytes(view, maxHydratedBytes)
	if projectedBytes > maxHydratedBytes {
		return boundedGraphRecordProjection{HydratedBytes: hydratedBytes}, &ProjectedRecordTooLargeError{Limit: maxHydratedBytes}
	}
	return boundedGraphRecordProjection{
		Record:         view,
		HydratedBytes:  hydratedBytes,
		ProjectedBytes: projectedBytes,
	}, nil
}

func (svc *Service) graphRelations(ctx context.Context, rec *schema.MemoryRecord, limit int, maxHydratedBytes int64) (storage.BoundedRelationResult, error) {
	if rec == nil || limit <= 0 {
		return storage.BoundedRelationResult{Relations: []schema.Relation{}}, nil
	}
	limit = min(limit, MaxGraphLimit)
	maxHydratedBytes = min(maxHydratedBytes, MaxProjectedResponseBytes)
	if maxHydratedBytes <= 0 {
		return storage.BoundedRelationResult{Relations: []schema.Relation{}, HydrationBytesTruncated: true}, nil
	}
	if rec.Relations != nil {
		relations := append([]schema.Relation(nil), rec.Relations...)
		prioritizeRelations(relations)
		return sanitizeBoundedRelations(storage.BoundedRelationResult{Relations: relations}, limit, maxHydratedBytes), nil
	}
	if lookup, ok := svc.store.(storage.ByteBoundedRelationLookup); ok {
		result, err := lookup.GetRelationsBounded(ctx, rec.ID, limit, maxHydratedBytes)
		if err != nil {
			return storage.BoundedRelationResult{}, err
		}
		return sanitizeBoundedRelations(result, limit, maxHydratedBytes), nil
	}
	return storage.BoundedRelationResult{}, fmt.Errorf("%w: ByteBoundedRelationLookup", ErrBoundedGraphLookupUnsupported)
}

func (svc *Service) graphEdges(ctx context.Context, rec *schema.MemoryRecord, limit, workLimit int, maxHydratedBytes int64) ([]schema.GraphEdge, int, int64, bool, error) {
	if rec == nil || limit <= 0 || workLimit <= 0 {
		return nil, 0, 0, false, nil
	}
	if maxHydratedBytes <= 0 {
		return nil, 0, 0, true, nil
	}
	outgoingLookupCost := 0
	if rec.Relations == nil {
		outgoingLookupCost = 1
	}
	if workLimit <= outgoingLookupCost {
		return nil, workLimit, 0, false, nil
	}
	outgoingLimit := min(limit, workLimit-outgoingLookupCost)
	outgoingResult, err := svc.graphRelations(ctx, rec, outgoingLimit, maxHydratedBytes)
	if err != nil {
		return nil, outgoingLookupCost, 0, false, err
	}
	outgoing := outgoingResult.Relations
	examined := outgoingLookupCost + len(outgoing)
	hydratedBytes := outgoingResult.ProjectedBytes
	edges := make([]schema.GraphEdge, 0, len(outgoing))
	outboundPredicatesByTarget := make(map[string]map[string]struct{}, len(outgoing))
	for _, rel := range outgoing {
		rel.Predicate = schema.NormalizeGraphPredicate(rel.Predicate)
		edges = append(edges, schema.GraphEdge{
			SourceID:  rec.ID,
			Predicate: rel.Predicate,
			TargetID:  rel.TargetID,
			Weight:    rel.Weight,
			CreatedAt: rel.CreatedAt,
		})
		if outboundPredicatesByTarget[rel.TargetID] == nil {
			outboundPredicatesByTarget[rel.TargetID] = make(map[string]struct{})
		}
		outboundPredicatesByTarget[rel.TargetID][rel.Predicate] = struct{}{}
	}
	if outgoingResult.HydrationBytesTruncated {
		prioritizeGraphEdges(edges)
		return edges, examined, hydratedBytes, true, nil
	}

	var incoming []schema.GraphEdge
	incomingTruncated := false
	if boundedLookup, ok := svc.store.(storage.ByteBoundedIncomingRelationLookup); ok {
		remainingWork := workLimit - examined
		remainingOutput := limit - len(edges)
		remainingBytes := maxHydratedBytes - hydratedBytes
		if remainingWork > 1 && remainingOutput > 0 && remainingBytes > 0 {
			incomingLimit := min(remainingWork-1, remainingOutput)
			incomingResult, lookupErr := boundedLookup.GetIncomingRelationsBounded(ctx, rec.ID, incomingLimit, remainingBytes)
			err = lookupErr
			examined++
			incomingResult = sanitizeBoundedIncomingRelations(incomingResult, incomingLimit, remainingBytes)
			incoming = incomingResult.Edges
			hydratedBytes += incomingResult.ProjectedBytes
			examined += len(incoming)
			incomingTruncated = incomingResult.HydrationBytesTruncated
		}
	} else if _, ok := svc.store.(storage.IncomingRelationLookup); ok {
		prioritizeGraphEdges(edges)
		return edges, examined, hydratedBytes, false, fmt.Errorf("%w: ByteBoundedIncomingRelationLookup", ErrBoundedGraphLookupUnsupported)
	} else {
		prioritizeGraphEdges(edges)
		if len(edges) > limit {
			edges = edges[:limit]
		}
		return edges, examined, hydratedBytes, false, nil
	}
	if err != nil {
		return edges, examined, hydratedBytes, false, fmt.Errorf("incoming relations: %w", err)
	}
	for _, edge := range incoming {
		edge.Predicate = schema.NormalizeGraphPredicate(edge.Predicate)
		if edge.SourceID == "" || edge.TargetID != rec.ID {
			continue
		}
		if graphHasInverseOutbound(outboundPredicatesByTarget[edge.SourceID], edge.Predicate) {
			continue
		}
		edges = append(edges, edge)
	}
	prioritizeGraphEdges(edges)
	if len(edges) > limit {
		edges = edges[:limit]
	}
	return edges, examined, hydratedBytes, incomingTruncated, nil
}

func graphHasInverseOutbound(outboundPredicates map[string]struct{}, incomingPredicate string) bool {
	if len(outboundPredicates) == 0 {
		return false
	}
	inverse := schema.InverseGraphPredicate(incomingPredicate)
	_, ok := outboundPredicates[inverse]
	return ok
}

func graphEdgeNeighborID(edge schema.GraphEdge, currentID string) string {
	switch currentID {
	case "":
		return ""
	case edge.SourceID:
		return edge.TargetID
	case edge.TargetID:
		return edge.SourceID
	default:
		return ""
	}
}

const maxEntityRootLookupQueries = 128

func (svc *Service) entityRootCandidates(ctx context.Context, req *RetrieveGraphRequest) []*schema.MemoryRecord {
	return svc.entityRootCandidatesBounded(ctx, req, MaxProjectedResponseBytes).Records
}

func (svc *Service) entityRootCandidatesBounded(ctx context.Context, req *RetrieveGraphRequest, maxHydratedBytes int64) storage.BoundedGraphEntityResult {
	if req == nil || req.Trust == nil || strings.TrimSpace(req.TaskDescriptor) == "" || !allowsMemoryType(req.MemoryTypes, schema.MemoryTypeEntity) {
		return storage.BoundedGraphEntityResult{}
	}
	graphLookup, hasGraphLookup := svc.store.(storage.BoundedGraphEntityLookup)
	if !hasGraphLookup {
		return storage.BoundedGraphEntityResult{}
	}
	if maxHydratedBytes <= 0 {
		return storage.BoundedGraphEntityResult{HydrationBytesTruncated: true}
	}
	if maxHydratedBytes > MaxProjectedResponseBytes {
		maxHydratedBytes = MaxProjectedResponseBytes
	}
	hydrationRemaining := maxHydratedBytes
	findByTerm := func(ctx context.Context, term, scope string, limit int) (storage.BoundedGraphEntityResult, error) {
		return graphLookup.FindGraphEntitiesByTermBounded(ctx, term, scope, limit, hydrationRemaining)
	}
	findByIdentifier := func(ctx context.Context, namespace, value, scope string) (storage.BoundedGraphEntityResult, error) {
		return graphLookup.FindGraphEntityByIdentifierBounded(ctx, namespace, value, scope, hydrationRemaining)
	}
	candidateLimit := req.RootLimit
	if candidateLimit <= 0 {
		candidateLimit = 10
	}
	if candidateLimit > maxRetrievalCandidates {
		candidateLimit = maxRetrievalCandidates
	}
	matches := make([]*schema.MemoryRecord, 0, candidateLimit)
	seen := make(map[string]struct{}, candidateLimit)
	truncated := false
	appendMatches := func(candidates []*schema.MemoryRecord) {
		for _, rec := range filterEntityRootCandidates(candidates, req.Trust) {
			if len(matches) >= candidateLimit {
				return
			}
			if _, ok := seen[rec.ID]; ok {
				continue
			}
			seen[rec.ID] = struct{}{}
			matches = append(matches, rec)
		}
	}
	consumeResult := func(result storage.BoundedGraphEntityResult) {
		if result.ProjectedBytes > hydrationRemaining {
			hydrationRemaining = 0
			truncated = true
			return
		} else {
			hydrationRemaining -= result.ProjectedBytes
		}
		truncated = truncated || result.HydrationBytesTruncated
		appendMatches(result.Records)
	}
	identifiers := schema.ParseEntityIdentifierTokensBounded(req.TaskDescriptor, maxGraphRootBoostIdentifierParseBytes, maxGraphRootBoostIdentifiers)
	lookupCalls := 0
	unrestrictedScopes := len(req.Trust.Scopes) == 0 || stringSliceContains(req.Trust.Scopes, "*")
	if unrestrictedScopes {
		if allScopes, ok := svc.store.(storage.BoundedGraphEntityLookupAllScopes); ok {
			lookupCalls++
			scopeMatches, err := allScopes.FindGraphEntitiesByTermAllScopesBounded(ctx, req.TaskDescriptor, candidateLimit, hydrationRemaining)
			if err == nil {
				consumeResult(scopeMatches)
			}
			for _, identifier := range identifiers {
				if lookupCalls >= maxEntityRootLookupQueries || len(matches) >= candidateLimit || hydrationRemaining <= 0 || truncated {
					break
				}
				lookupCalls++
				rec, err := allScopes.FindGraphEntityByIdentifierAllScopesBounded(ctx, identifier.Namespace, identifier.Value, hydrationRemaining)
				if err == nil {
					consumeResult(rec)
				}
			}
			return storage.BoundedGraphEntityResult{
				Records:                 matches,
				ProjectedBytes:          maxHydratedBytes - hydrationRemaining,
				HydrationBytesTruncated: truncated,
			}
		}
	}
	scopes := make([]string, 0, len(req.Trust.Scopes)+1)
	seenScopes := make(map[string]struct{}, len(req.Trust.Scopes)+1)
	if !unrestrictedScopes {
		for _, scope := range req.Trust.Scopes {
			scope = strings.TrimSpace(scope)
			if scope == "" {
				continue
			}
			if _, ok := seenScopes[scope]; ok {
				continue
			}
			seenScopes[scope] = struct{}{}
			scopes = append(scopes, scope)
		}
	}
	scopes = append(scopes, "")
	for _, scope := range scopes {
		if lookupCalls >= maxEntityRootLookupQueries || len(matches) >= candidateLimit || hydrationRemaining <= 0 || truncated {
			break
		}
		remaining := candidateLimit - len(matches)
		lookupCalls++
		scopeMatches, err := findByTerm(ctx, req.TaskDescriptor, scope, remaining)
		if err != nil {
			continue
		}
		consumeResult(scopeMatches)
		for _, identifier := range identifiers {
			if lookupCalls >= maxEntityRootLookupQueries || len(matches) >= candidateLimit || hydrationRemaining <= 0 || truncated {
				break
			}
			lookupCalls++
			rec, err := findByIdentifier(ctx, identifier.Namespace, identifier.Value, scope)
			if err != nil {
				continue
			}
			consumeResult(rec)
		}
	}
	return storage.BoundedGraphEntityResult{
		Records:                 matches,
		ProjectedBytes:          maxHydratedBytes - hydrationRemaining,
		HydrationBytesTruncated: truncated,
	}
}

func stringSliceContains(values []string, want string) bool {
	for _, value := range values {
		if strings.TrimSpace(value) == want {
			return true
		}
	}
	return false
}

func filterEntityRootCandidates(matches []*schema.MemoryRecord, trust *TrustContext) []*schema.MemoryRecord {
	if trust == nil {
		return nil
	}
	filtered := make([]*schema.MemoryRecord, 0, len(matches))
	for _, rec := range matches {
		if rec == nil {
			continue
		}
		if trust.Allows(rec) {
			filtered = append(filtered, rec)
		} else if trust.AllowsRedacted(rec) {
			filtered = append(filtered, Redact(rec))
		}
	}
	return uniqueRecords(filtered)
}

func allowsMemoryType(types []schema.MemoryType, mt schema.MemoryType) bool {
	if len(types) == 0 {
		return true
	}
	for _, candidate := range types {
		if candidate == mt {
			return true
		}
	}
	return false
}

func uniqueRecords(records []*schema.MemoryRecord) []*schema.MemoryRecord {
	out := make([]*schema.MemoryRecord, 0, len(records))
	seen := make(map[string]struct{}, len(records))
	for _, rec := range records {
		if rec == nil {
			continue
		}
		if _, ok := seen[rec.ID]; ok {
			continue
		}
		seen[rec.ID] = struct{}{}
		out = append(out, rec)
	}
	return out
}

func (svc *Service) rerankGraphRoots(ctx context.Context, records []*schema.MemoryRecord, query string, trust *TrustContext) []*schema.MemoryRecord {
	return svc.rerankGraphRootsLimited(ctx, records, query, trust, maxGraphRootBoostRelations)
}

func (svc *Service) rerankGraphRootsLimited(ctx context.Context, records []*schema.MemoryRecord, query string, trust *TrustContext, relationLimit int) []*schema.MemoryRecord {
	return svc.rerankGraphRootsLimitedWithBudget(ctx, records, query, trust, relationLimit, newGraphRootBoostBudget())
}

func (svc *Service) rerankGraphRootsLimitedWithBudget(ctx context.Context, records []*schema.MemoryRecord, query string, trust *TrustContext, relationLimit int, boostBudget *graphRootBoostBudget) []*schema.MemoryRecord {
	if len(records) == 0 || query == "" {
		return records
	}
	if boostBudget == nil {
		boostBudget = newGraphRootBoostBudget()
	}
	type scored struct {
		record *schema.MemoryRecord
		score  float64
	}
	items := make([]scored, 0, len(records))
	state := newGraphRootBoostState(query, boostBudget, nil)
	for idx, rec := range records {
		relationBoostLimit := min(relationLimit, maxGraphRootBoostRelations)
		if idx >= maxGraphRootBoostRelationLookups {
			relationBoostLimit = 0
		}
		base := float64(len(records) - idx)
		items = append(items, scored{record: rec, score: base + svc.rootBoostPrepared(ctx, rec, trust, relationBoostLimit, state)})
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].score != items[j].score {
			return items[i].score > items[j].score
		}
		left, right := items[i].record, items[j].record
		if left == nil || right == nil {
			return left != nil
		}
		if !left.UpdatedAt.Equal(right.UpdatedAt) {
			return left.UpdatedAt.After(right.UpdatedAt)
		}
		return left.ID < right.ID
	})
	ranked := make([]*schema.MemoryRecord, 0, len(items))
	for _, item := range items {
		ranked = append(ranked, item.record)
	}
	return ranked
}

type graphRootBoostQuery struct {
	normalized    string
	identifierSet map[string]struct{}
}

type graphEntityBoostTerms struct {
	canonical      string
	aliases        []string
	types          []string
	identifierKeys []string
}

type graphMentionBoostTerms struct {
	surface string
	aliases []string
}

type graphRecordBoostTerms struct {
	entity   *graphEntityBoostTerms
	mentions []graphMentionBoostTerms
}

type graphRootBoostState struct {
	query              graphRootBoostQuery
	budget             *graphRootBoostBudget
	entityPayloadCache map[string]*schema.EntityPayload
	entityTermsCache   map[string]*graphEntityBoostTerms
	recordTermsCache   map[*schema.MemoryRecord]*graphRecordBoostTerms
}

func newGraphRootBoostState(query string, budget *graphRootBoostBudget, entityPayloadCache map[string]*schema.EntityPayload) *graphRootBoostState {
	if budget == nil {
		budget = newGraphRootBoostBudget()
	}
	if entityPayloadCache == nil {
		entityPayloadCache = make(map[string]*schema.EntityPayload)
	}
	state := &graphRootBoostState{
		budget:             budget,
		entityPayloadCache: entityPayloadCache,
		entityTermsCache:   make(map[string]*graphEntityBoostTerms),
		recordTermsCache:   make(map[*schema.MemoryRecord]*graphRecordBoostTerms),
	}
	queryPrefix := boundedGraphRootBoostText(query, maxGraphRootBoostIdentifierParseBytes)
	if normalized, ok := budget.normalizeTerm(queryPrefix, maxGraphRootBoostIdentifierParseBytes); ok {
		state.query.normalized = normalized
		budget.queryNormalizations++
	}
	budget.identifierBytesParsed = min(len(query), maxGraphRootBoostIdentifierParseBytes)
	identifiers := schema.ParseEntityIdentifierTokensBounded(query, maxGraphRootBoostIdentifierParseBytes, maxGraphRootBoostIdentifiers)
	budget.identifierTokensParsed = len(identifiers)
	state.addQueryIdentifiers(identifiers)
	return state
}

func (state *graphRootBoostState) addQueryIdentifiers(identifiers []schema.EntityIdentifier) {
	if state == nil || state.budget == nil || len(identifiers) == 0 {
		return
	}
	if state.query.identifierSet == nil {
		state.query.identifierSet = make(map[string]struct{}, min(len(identifiers), maxGraphRootBoostIdentifiers))
	}
	for _, identifier := range identifiers {
		if len(state.query.identifierSet) >= maxGraphRootBoostIdentifiers || !state.budget.consumeWork() {
			break
		}
		if identifier.Namespace == "" || identifier.Value == "" || len(identifier.Namespace)+len(identifier.Value) > maxGraphRootBoostIdentifierParseBytes {
			continue
		}
		namespace := schema.NormalizeEntityIdentifierNamespace(identifier.Namespace)
		value := strings.TrimSpace(identifier.Value)
		if namespace == "" || value == "" {
			continue
		}
		state.query.identifierSet[namespace+"\x00"+value] = struct{}{}
	}
}

func boundedGraphRootBoostText(value string, maxBytes int) string {
	if maxBytes <= 0 || value == "" {
		return ""
	}
	if len(value) <= maxBytes {
		return value
	}
	prefix := value[:maxBytes]
	lastRune, _ := utf8.DecodeLastRuneInString(prefix)
	if unicode.IsSpace(lastRune) {
		return prefix
	}
	lastBoundary := strings.LastIndexFunc(prefix, unicode.IsSpace)
	if lastBoundary < 0 {
		return ""
	}
	return prefix[:lastBoundary]
}

func (svc *Service) rootBoost(ctx context.Context, rec *schema.MemoryRecord, query string, queryIdentifiers []schema.EntityIdentifier, trust *TrustContext, entityCache map[string]*schema.EntityPayload) float64 {
	return svc.rootBoostLimited(ctx, rec, query, queryIdentifiers, trust, entityCache, maxGraphRootBoostRelations)
}

func (svc *Service) rootBoostLimited(ctx context.Context, rec *schema.MemoryRecord, query string, queryIdentifiers []schema.EntityIdentifier, trust *TrustContext, entityCache map[string]*schema.EntityPayload, relationLimit int) float64 {
	return svc.rootBoostLimitedWithBudget(ctx, rec, query, queryIdentifiers, trust, entityCache, relationLimit, newGraphRootBoostBudget())
}

func (svc *Service) rootBoostLimitedWithBudget(ctx context.Context, rec *schema.MemoryRecord, query string, queryIdentifiers []schema.EntityIdentifier, trust *TrustContext, entityCache map[string]*schema.EntityPayload, relationLimit int, budget *graphRootBoostBudget) float64 {
	state := newGraphRootBoostState(query, budget, entityCache)
	state.addQueryIdentifiers(queryIdentifiers)
	return svc.rootBoostPrepared(ctx, rec, trust, relationLimit, state)
}

func (svc *Service) rootBoostPrepared(ctx context.Context, rec *schema.MemoryRecord, trust *TrustContext, relationLimit int, state *graphRootBoostState) float64 {
	if rec == nil || (trust != nil && !trust.Allows(rec)) {
		return 0
	}
	if state == nil || state.budget == nil || state.budget.workRemaining <= 0 {
		return 0
	}
	terms := state.recordTerms(rec)
	score := 0.0
	if terms.entity != nil {
		if state.identifiersMatch(terms.entity.identifierKeys) {
			score += 125
		}
		if state.budget.normalizedTermMatches(terms.entity.canonical, state.query.normalized) {
			score += 100
		}
		for _, alias := range terms.entity.aliases {
			if state.budget.normalizedTermMatches(alias, state.query.normalized) {
				score += 75
				break
			}
		}
		for _, entityType := range terms.entity.types {
			if state.budget.normalizedTermMatches(entityType, state.query.normalized) {
				score += 15
				break
			}
		}
	}
	for _, mention := range terms.mentions {
		if state.budget.normalizedTermMatches(mention.surface, state.query.normalized) {
			score += 30
			break
		}
		for _, alias := range mention.aliases {
			if state.budget.normalizedTermMatches(alias, state.query.normalized) {
				score += 20
				break
			}
		}
	}
	for _, rel := range svc.rootBoostRelationsLimitedWithBudget(ctx, rec, relationLimit, state.budget) {
		targetEntity := svc.relatedEntityTermsWithBudget(ctx, rel.TargetID, trust, state)
		if targetEntity == nil {
			continue
		}
		if state.budget.normalizedTermMatches(targetEntity.canonical, state.query.normalized) {
			score += relationBoost(rel.Predicate, 60)
		}
		for _, alias := range targetEntity.aliases {
			if state.budget.normalizedTermMatches(alias, state.query.normalized) {
				score += relationBoost(rel.Predicate, 45)
				break
			}
		}
		if state.identifiersMatch(targetEntity.identifierKeys) {
			score += relationBoost(rel.Predicate, 70)
		}
	}
	return score
}

func (state *graphRootBoostState) recordTerms(rec *schema.MemoryRecord) *graphRecordBoostTerms {
	if terms, ok := state.recordTermsCache[rec]; ok {
		return terms
	}
	remaining := maxGraphRootBoostTermsPerRecord
	terms := &graphRecordBoostTerms{}
	if payload, ok := rec.Payload.(*schema.EntityPayload); ok {
		terms.entity = prepareGraphEntityBoostTerms(payload, state.budget, &remaining)
	}
	if rec.Interpretation != nil {
		for _, mention := range rec.Interpretation.Mentions {
			if remaining <= 0 || state.budget.workRemaining <= 0 {
				break
			}
			prepared := graphMentionBoostTerms{}
			if normalized, ok := takeGraphBoostTerm(mention.Surface, state.budget, &remaining); ok {
				prepared.surface = normalized
			}
			for _, alias := range mention.Aliases {
				if remaining <= 0 || state.budget.workRemaining <= 0 {
					break
				}
				if normalized, ok := takeGraphBoostTerm(alias, state.budget, &remaining); ok {
					prepared.aliases = append(prepared.aliases, normalized)
				}
			}
			terms.mentions = append(terms.mentions, prepared)
		}
	}
	state.recordTermsCache[rec] = terms
	return terms
}

func prepareGraphEntityBoostTerms(payload *schema.EntityPayload, budget *graphRootBoostBudget, remaining *int) *graphEntityBoostTerms {
	if payload == nil {
		return nil
	}
	terms := &graphEntityBoostTerms{}
	if normalized, ok := takeGraphBoostTerm(payload.CanonicalName, budget, remaining); ok {
		terms.canonical = normalized
	}
	for _, alias := range payload.Aliases {
		if *remaining <= 0 || budget.workRemaining <= 0 {
			break
		}
		if normalized, ok := takeGraphBoostTerm(alias.Value, budget, remaining); ok {
			terms.aliases = append(terms.aliases, normalized)
		}
	}
	if normalized, ok := takeGraphBoostTerm(payload.PrimaryType, budget, remaining); ok {
		terms.types = append(terms.types, normalized)
	}
	for _, entityType := range payload.Types {
		if *remaining <= 0 || budget.workRemaining <= 0 {
			break
		}
		if normalized, ok := takeGraphBoostTerm(entityType, budget, remaining); ok {
			terms.types = append(terms.types, normalized)
		}
	}
	for idx, identifier := range payload.Identifiers {
		if idx >= maxGraphRootBoostEntityIdentifiers || budget.workRemaining <= 0 {
			break
		}
		if key, ok := normalizeGraphBoostIdentifier(identifier, budget); ok {
			terms.identifierKeys = append(terms.identifierKeys, key)
		}
	}
	return terms
}

func takeGraphBoostTerm(value string, budget *graphRootBoostBudget, remaining *int) (string, bool) {
	if budget == nil || remaining == nil || *remaining <= 0 {
		return "", false
	}
	*remaining = *remaining - 1
	return budget.normalizeTerm(value, maxGraphRootBoostTermBytes)
}

func normalizeGraphBoostIdentifier(identifier schema.EntityIdentifier, budget *graphRootBoostBudget) (string, bool) {
	if budget == nil || !budget.consumeWork() {
		return "", false
	}
	if identifier.Namespace == "" || identifier.Value == "" || len(identifier.Namespace) > maxGraphRootBoostTermBytes || len(identifier.Value) > maxGraphRootBoostTermBytes {
		return "", false
	}
	bytes := len(identifier.Namespace) + len(identifier.Value)
	if bytes > budget.normalizationBytesRemaining {
		return "", false
	}
	budget.normalizationBytesRemaining -= bytes
	budget.normalizationBytesUsed += bytes
	budget.normalizationCalls++
	namespace := schema.NormalizeEntityIdentifierNamespace(identifier.Namespace)
	value := strings.TrimSpace(identifier.Value)
	if namespace == "" || value == "" {
		return "", false
	}
	return namespace + "\x00" + value, true
}

func (state *graphRootBoostState) identifiersMatch(indexed []string) bool {
	if state == nil || len(state.query.identifierSet) == 0 {
		return false
	}
	for _, key := range indexed {
		if !state.budget.consumeWork() {
			return false
		}
		state.budget.comparisons++
		if _, ok := state.query.identifierSet[key]; ok {
			return true
		}
	}
	return false
}

func (svc *Service) relatedEntityTermsWithBudget(ctx context.Context, id string, trust *TrustContext, state *graphRootBoostState) *graphEntityBoostTerms {
	if id == "" || state == nil {
		return nil
	}
	if cached, ok := state.entityTermsCache[id]; ok {
		return cached
	}
	payload := svc.relatedEntityWithBudget(ctx, id, trust, state.entityPayloadCache, state.budget)
	if payload == nil {
		state.entityTermsCache[id] = nil
		return nil
	}
	remaining := maxGraphRootBoostTermsPerRecord
	terms := prepareGraphEntityBoostTerms(payload, state.budget, &remaining)
	state.entityTermsCache[id] = terms
	return terms
}

func (svc *Service) rootBoostRelationsLimitedWithBudget(ctx context.Context, rec *schema.MemoryRecord, relationLimit int, budget *graphRootBoostBudget) []schema.Relation {
	if relationLimit <= 0 {
		return nil
	}
	if budget == nil {
		budget = newGraphRootBoostBudget()
	}
	if budget.hydrationRemaining <= 0 {
		return nil
	}
	if rec != nil && rec.Relations == nil {
		if !budget.consumeWork() {
			return nil
		}
	}
	maxBytes := min(budget.hydrationRemaining, int64(relationLimit)*(64<<10))
	result, err := svc.graphRelations(ctx, rec, relationLimit, maxBytes)
	if err != nil {
		return nil
	}
	if result.ProjectedBytes > budget.hydrationRemaining {
		budget.hydrationRemaining = 0
		return nil
	}
	budget.hydrationRemaining -= result.ProjectedBytes
	return result.Relations
}

func prioritizeRelations(rels []schema.Relation) {
	sort.SliceStable(rels, func(i, j int) bool {
		if rels[i].Weight != rels[j].Weight {
			return rels[i].Weight > rels[j].Weight
		}
		if !rels[i].CreatedAt.Equal(rels[j].CreatedAt) {
			return rels[i].CreatedAt.After(rels[j].CreatedAt)
		}
		if rels[i].Predicate != rels[j].Predicate {
			return rels[i].Predicate < rels[j].Predicate
		}
		return rels[i].TargetID < rels[j].TargetID
	})
}

func prioritizeGraphEdges(edges []schema.GraphEdge) {
	sort.SliceStable(edges, func(i, j int) bool {
		if edges[i].Weight != edges[j].Weight {
			return edges[i].Weight > edges[j].Weight
		}
		if !edges[i].CreatedAt.Equal(edges[j].CreatedAt) {
			return edges[i].CreatedAt.After(edges[j].CreatedAt)
		}
		if edges[i].Predicate != edges[j].Predicate {
			return edges[i].Predicate < edges[j].Predicate
		}
		if edges[i].SourceID != edges[j].SourceID {
			return edges[i].SourceID < edges[j].SourceID
		}
		return edges[i].TargetID < edges[j].TargetID
	})
}

func (svc *Service) relatedEntity(ctx context.Context, id string, trust *TrustContext, entityCache map[string]*schema.EntityPayload) *schema.EntityPayload {
	return svc.relatedEntityWithBudget(ctx, id, trust, entityCache, newGraphRootBoostBudget())
}

func (svc *Service) relatedEntityWithBudget(ctx context.Context, id string, trust *TrustContext, entityCache map[string]*schema.EntityPayload, budget *graphRootBoostBudget) *schema.EntityPayload {
	if id == "" {
		return nil
	}
	if cached, ok := entityCache[id]; ok {
		return cached
	}
	if budget == nil {
		budget = newGraphRootBoostBudget()
	}
	if budget.hydrationRemaining <= 0 || !budget.consumeWork() {
		entityCache[id] = nil
		return nil
	}
	projection, err := svc.getGraphRecord(ctx, id, trust, budget.hydrationRemaining, false)
	if projection.HydratedBytes > budget.hydrationRemaining {
		projection.HydratedBytes = budget.hydrationRemaining
	}
	budget.hydrationRemaining -= projection.HydratedBytes
	if err != nil {
		var tooLarge *ProjectedRecordTooLargeError
		if errors.As(err, &tooLarge) {
			budget.hydrationRemaining = 0
		}
		if errors.Is(err, ErrBoundedGraphLookupUnsupported) {
			budget.workRemaining = 0
		}
		entityCache[id] = nil
		return nil
	}
	rec := projection.Record
	if rec == nil {
		entityCache[id] = nil
		return nil
	}
	payload, ok := rec.Payload.(*schema.EntityPayload)
	if !ok {
		entityCache[id] = nil
		return nil
	}
	entityCache[id] = payload
	return payload
}

func relationBoost(predicate string, fallback float64) float64 {
	switch schema.NormalizeGraphPredicate(predicate) {
	case schema.GraphPredicateMentionsEntity, schema.GraphPredicateMentionedIn:
		return fallback + 20
	case schema.GraphPredicateSubjectEntity, schema.GraphPredicateFactSubjectOf, schema.GraphPredicateObjectEntity, schema.GraphPredicateFactObjectOf:
		return fallback + 25
	case schema.GraphPredicateSupports,
		schema.GraphPredicateSupportedBy,
		schema.GraphPredicateDependsOn,
		schema.GraphPredicateDependencyOf,
		schema.GraphPredicateUses,
		schema.GraphPredicateUsedBy,
		schema.GraphPredicateCausedBy,
		schema.GraphPredicateCauses,
		schema.GraphPredicateContradicts,
		schema.GraphPredicateContradictedBy,
		schema.GraphPredicateSupersedes,
		schema.GraphPredicateSupersededBy,
		schema.GraphPredicateContestedBy,
		schema.GraphPredicateContests:
		return fallback + 10
	default:
		return fallback
	}
}

func lexicalMatch(value, query string) bool {
	return schema.EntityTermMatchesQuery(value, query)
}

func normalizeSearchText(s string) string {
	return strings.TrimSpace(strings.ToLower(s))
}
