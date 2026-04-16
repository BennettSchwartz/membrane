package retrieval

import (
	"context"
	"sort"
	"strings"

	"github.com/BennettSchwartz/membrane/pkg/schema"
)

// RetrieveGraphRequest expands ranked root memories into a bounded graph.
type RetrieveGraphRequest struct {
	TaskDescriptor string
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
		Trust:          req.Trust,
		MemoryTypes:    req.MemoryTypes,
		MinSalience:    req.MinSalience,
		Limit:          max(rootLimit*3, rootLimit),
	})
	if err != nil {
		return nil, err
	}

	roots := rerankGraphRoots(baseResp.Records, req.TaskDescriptor)
	if len(roots) > rootLimit {
		roots = roots[:rootLimit]
	}

	nodeByID := make(map[string]GraphNode, nodeLimit)
	rootIDs := make([]string, 0, len(roots))
	queue := make([]GraphNode, 0, len(roots))
	for _, rec := range roots {
		if rec == nil {
			continue
		}
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

			if _, ok := nodeByID[target.ID]; !ok {
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

func rerankGraphRoots(records []*schema.MemoryRecord, query string) []*schema.MemoryRecord {
	if len(records) == 0 || query == "" {
		return records
	}
	type scored struct {
		record *schema.MemoryRecord
		score  float64
	}
	items := make([]scored, 0, len(records))
	for idx, rec := range records {
		base := float64(len(records) - idx)
		items = append(items, scored{record: rec, score: base + rootBoost(rec, query)})
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

func rootBoost(rec *schema.MemoryRecord, query string) float64 {
	if rec == nil {
		return 0
	}
	score := 0.0
	if ep, ok := rec.Payload.(*schema.EntityPayload); ok {
		if lexicalMatch(ep.CanonicalName, query) {
			score += 100
		}
		for _, alias := range ep.Aliases {
			if lexicalMatch(alias, query) {
				score += 75
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
	return score
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
