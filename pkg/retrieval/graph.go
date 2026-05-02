package retrieval

import (
	"context"
	"sort"
	"strings"

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
	Nodes     []GraphNode        `json:"nodes"`
	Edges     []schema.GraphEdge `json:"edges"`
	RootIDs   []string           `json:"root_ids"`
	Selection *SelectionResult   `json:"selection,omitempty"`
}

func (svc *Service) RetrieveGraph(ctx context.Context, req *RetrieveGraphRequest) (*RetrieveGraphResponse, error) {
	if req == nil {
		req = &RetrieveGraphRequest{}
	}
	if req.Trust == nil {
		return nil, ErrNilTrust
	}

	rootLimit := req.RootLimit
	if rootLimit <= 0 {
		rootLimit = 10
	}
	nodeLimit := req.NodeLimit
	if nodeLimit <= 0 {
		nodeLimit = 25
	}
	edgeLimit := req.EdgeLimit
	if edgeLimit <= 0 {
		edgeLimit = 100
	}
	maxHops := req.MaxHops
	if maxHops < 0 {
		maxHops = 0
	}

	baseResp, err := svc.Retrieve(ctx, &RetrieveRequest{
		TaskDescriptor: req.TaskDescriptor,
		QueryEmbedding: req.QueryEmbedding,
		Trust:          req.Trust,
		MemoryTypes:    req.MemoryTypes,
		MinSalience:    req.MinSalience,
		Limit:          max(rootLimit*3, rootLimit),
	})
	if err != nil {
		return nil, err
	}

	baseRecords := append([]*schema.MemoryRecord(nil), baseResp.Records...)
	baseRecords = append(baseRecords, svc.entityRootCandidates(ctx, req)...)
	baseRecords = uniqueRecords(baseRecords)
	roots := svc.rerankGraphRoots(ctx, baseRecords, req.TaskDescriptor, req.Trust)
	if len(roots) > rootLimit {
		roots = roots[:rootLimit]
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

	for i := 0; i < len(queue); i++ {
		current := queue[i]
		if current.Hop >= maxHops || current.Record == nil {
			continue
		}
		rels, err := svc.store.GetRelations(ctx, current.Record.ID)
		if err != nil {
			continue
		}
		for _, rel := range rels {
			if len(edges) >= edgeLimit {
				break
			}
			edgeKey := current.Record.ID + "|" + rel.Predicate + "|" + rel.TargetID
			if _, ok := seenEdges[edgeKey]; ok {
				continue
			}
			if _, ok := nodeByID[rel.TargetID]; !ok {
				target, err := svc.store.Get(ctx, rel.TargetID)
				if err != nil {
					continue
				}
				if !req.Trust.Allows(target) {
					if req.Trust.AllowsRedacted(target) {
						target = Redact(target)
					} else {
						continue
					}
				}

				if len(nodeByID) >= nodeLimit {
					continue
				}
				node := GraphNode{Record: target, Hop: current.Hop + 1}
				nodeByID[target.ID] = node
				queue = append(queue, node)
			}

			edges = append(edges, schema.GraphEdge{
				SourceID:  current.Record.ID,
				Predicate: rel.Predicate,
				TargetID:  rel.TargetID,
				Weight:    rel.Weight,
				CreatedAt: rel.CreatedAt,
			})
			seenEdges[edgeKey] = struct{}{}
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
		return nodes[i].Record.Salience > nodes[j].Record.Salience
	})

	return &RetrieveGraphResponse{
		Nodes:     nodes,
		Edges:     edges,
		RootIDs:   rootIDs,
		Selection: baseResp.Selection,
	}, nil
}

func (svc *Service) entityRootCandidates(ctx context.Context, req *RetrieveGraphRequest) []*schema.MemoryRecord {
	if req == nil || strings.TrimSpace(req.TaskDescriptor) == "" || !allowsMemoryType(req.MemoryTypes, schema.MemoryTypeEntity) {
		return nil
	}
	lookup, ok := svc.store.(storage.EntityLookup)
	if !ok {
		return nil
	}
	scopes := []string{""}
	if req.Trust != nil && len(req.Trust.Scopes) > 0 {
		scopes = append([]string(nil), req.Trust.Scopes...)
	}
	matches := make([]*schema.MemoryRecord, 0)
	for _, scope := range scopes {
		scopeMatches, err := lookup.FindEntitiesByTerm(ctx, req.TaskDescriptor, scope, req.RootLimit)
		if err != nil {
			continue
		}
		matches = append(matches, scopeMatches...)
	}
	filtered := make([]*schema.MemoryRecord, 0, len(matches))
	for _, rec := range matches {
		if rec == nil {
			continue
		}
		if req.Trust.Allows(rec) {
			filtered = append(filtered, rec)
		} else if req.Trust.AllowsRedacted(rec) {
			filtered = append(filtered, Redact(rec))
		}
	}
	return filtered
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
	if len(records) == 0 || query == "" {
		return records
	}
	type scored struct {
		record *schema.MemoryRecord
		score  float64
	}
	items := make([]scored, 0, len(records))
	entityCache := make(map[string]*schema.EntityPayload)
	for idx, rec := range records {
		base := float64(len(records) - idx)
		items = append(items, scored{record: rec, score: base + svc.rootBoost(ctx, rec, query, trust, entityCache)})
	}
	sort.Slice(items, func(i, j int) bool {
		return items[i].score > items[j].score
	})
	ranked := make([]*schema.MemoryRecord, 0, len(items))
	for _, item := range items {
		ranked = append(ranked, item.record)
	}
	return ranked
}

func (svc *Service) rootBoost(ctx context.Context, rec *schema.MemoryRecord, query string, trust *TrustContext, entityCache map[string]*schema.EntityPayload) float64 {
	if rec == nil {
		return 0
	}
	score := 0.0
	if ep, ok := rec.Payload.(*schema.EntityPayload); ok {
		if lexicalMatch(ep.CanonicalName, query) {
			score += 100
		}
		for _, alias := range ep.Aliases {
			if lexicalMatch(alias.Value, query) {
				score += 75
				break
			}
		}
		for _, entityType := range schema.EntityTypes(ep) {
			if lexicalMatch(entityType, query) {
				score += 15
				break
			}
		}
	}
	if rec.Interpretation != nil {
		for _, mention := range rec.Interpretation.Mentions {
			if lexicalMatch(mention.Surface, query) {
				score += 30
				break
			}
			for _, alias := range mention.Aliases {
				if lexicalMatch(alias, query) {
					score += 20
					break
				}
			}
		}
	}
	for _, rel := range rec.Relations {
		targetEntity := svc.relatedEntity(ctx, rel.TargetID, trust, entityCache)
		if targetEntity == nil {
			continue
		}
		if lexicalMatch(targetEntity.CanonicalName, query) {
			score += relationBoost(rel.Predicate, 60)
		}
		for _, alias := range targetEntity.Aliases {
			if lexicalMatch(alias.Value, query) {
				score += relationBoost(rel.Predicate, 45)
				break
			}
		}
	}
	return score
}

func (svc *Service) relatedEntity(ctx context.Context, id string, trust *TrustContext, entityCache map[string]*schema.EntityPayload) *schema.EntityPayload {
	if id == "" {
		return nil
	}
	if cached, ok := entityCache[id]; ok {
		return cached
	}
	rec, err := svc.store.Get(ctx, id)
	if err != nil || rec == nil {
		entityCache[id] = nil
		return nil
	}
	if trust != nil && !trust.Allows(rec) {
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
	switch normalizeSearchText(predicate) {
	case "mentions_entity", "mentioned_in":
		return fallback + 20
	case "subject_entity", "fact_subject_of", "object_entity", "fact_object_of":
		return fallback + 25
	case "supports", "supported_by", "depends_on", "dependency_of":
		return fallback + 10
	default:
		return fallback
	}
}

func lexicalMatch(value, query string) bool {
	v := normalizeSearchText(value)
	q := normalizeSearchText(query)
	if v == "" || q == "" {
		return false
	}
	return v == q || containsToken(v, q) || containsToken(q, v)
}

func normalizeSearchText(s string) string {
	return strings.TrimSpace(strings.ToLower(s))
}

func containsToken(haystack, needle string) bool {
	return strings.Contains(haystack, needle)
}
