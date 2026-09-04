package consolidation

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/BennettSchwartz/membrane/pkg/schema"
	"github.com/BennettSchwartz/membrane/pkg/storage"
)

// minPatternOccurrences is the minimum number of times a tool pattern
// must appear in successful episodes before a competence record is
// created from it.
const minPatternOccurrences = 2

const mixedScopeFallback = "consolidated:mixed-scope"

// CompetenceConsolidator extracts competence records from repeated
// successful episodic patterns. It groups episodic records by their
// tool usage signature and promotes patterns that appear at least
// minPatternOccurrences times.
type CompetenceConsolidator struct {
	store    storage.Store
	embedder Embedder
}

// NewCompetenceConsolidator creates a CompetenceConsolidator backed by store.
func NewCompetenceConsolidator(store storage.Store) *CompetenceConsolidator {
	return &CompetenceConsolidator{store: store}
}

// NewCompetenceConsolidatorWithEmbedder creates a competence consolidator with embedding support.
func NewCompetenceConsolidatorWithEmbedder(store storage.Store, embedder Embedder) *CompetenceConsolidator {
	return &CompetenceConsolidator{store: store, embedder: embedder}
}

// Consolidate finds episodic records with tool graphs and successful
// outcomes, groups them by similar tool patterns, and creates
// competence records for patterns that repeat. It returns the number
// of new competence records created, the number of existing records
// reinforced, and any error.
func (c *CompetenceConsolidator) Consolidate(ctx context.Context) (int, int, error) {
	episodics, err := c.store.ListByType(ctx, schema.MemoryTypeEpisodic)
	if err != nil {
		return 0, 0, err
	}

	// Load existing competence records so we do not duplicate skills.
	competences, err := c.store.ListByType(ctx, schema.MemoryTypeCompetence)
	if err != nil {
		return 0, 0, err
	}

	existingSkills := make(map[competenceSkillKey]*schema.MemoryRecord, len(competences))
	for _, cr := range competences {
		cp, ok := cr.Payload.(*schema.CompetencePayload)
		if !ok {
			continue
		}
		existingSkills[competenceScopedSkillKey(cr.Scope, cp.SkillName)] = cr
	}

	// Group episodes by their tool pattern signature.
	type patternGroup struct {
		signature string
		tools     []string
		records   []*schema.MemoryRecord
	}
	groups := make(map[competenceSkillKey]*patternGroup)

	for _, rec := range episodics {
		ep, ok := rec.Payload.(*schema.EpisodicPayload)
		if !ok {
			continue
		}
		if ep.Outcome != schema.OutcomeStatusSuccess {
			continue
		}
		if len(ep.ToolGraph) == 0 {
			continue
		}

		tools := extractToolNames(ep.ToolGraph)
		if len(tools) == 0 {
			continue
		}
		sig := toolSignature(tools)

		groupKey := competenceScopedSkillKey(rec.Scope, sig)
		g, found := groups[groupKey]
		if !found {
			g = &patternGroup{signature: sig, tools: tools}
			groups[groupKey] = g
		}
		g.records = append(g.records, rec)
	}

	now := time.Now().UTC()
	created := 0
	reinforced := 0

	for _, g := range groups {
		if len(g.records) < minPatternOccurrences {
			continue
		}

		skillName := "skill:" + g.signature
		derivedSensitivity := deriveMaxSensitivity(g.records)
		derivedScope, scopePolicy := deriveConservativeScope(g.records)
		skillKey := competenceScopedSkillKey(derivedScope, skillName)
		if existingRec, found := existingSkills[skillKey]; found {
			didReinforce := false
			err := storage.WithTransaction(ctx, c.store, func(tx storage.Transaction) error {
				current, err := storage.GetDerivedDestination(ctx, tx, existingRec.ID)
				if err != nil {
					return err
				}
				cp, ok := current.Payload.(*schema.CompetencePayload)
				if !ok || cp == nil || cp.SkillName != skillName {
					return fmt.Errorf("competence destination changed")
				}
				oldSensitivity := current.Sensitivity
				var admitted, newSources []*schema.MemoryRecord
				for _, source := range g.records {
					known := competenceHasSource(current, source.ID)
					allowed, err := storage.DerivedSourceMayReinforce(ctx, tx, current, source, known)
					if err != nil {
						return err
					}
					if !allowed {
						continue
					}
					admitted = append(admitted, source)
					if !known {
						newSources = append(newSources, source)
					}
				}
				if len(admitted) == 0 {
					return nil
				}
				if err := storage.ApplyDerivedSourcePolicy(ctx, tx, current, admitted); err != nil {
					return err
				}
				if len(newSources) == 0 && oldSensitivity == current.Sensitivity {
					return nil
				}
				if len(newSources) > 0 {
					current.Salience += 0.1
					if current.Salience > 1.0 {
						current.Salience = 1.0
					}
					appendCompetenceSources(current, newSources, now)
				}
				if oldSensitivity != current.Sensitivity {
					if err := storage.PruneDerivedInverseRelations(ctx, tx, current); err != nil {
						return err
					}
				}
				if err := tx.Update(ctx, current); err != nil {
					return err
				}
				if err := tx.AddAuditEntry(ctx, current.ID, schema.AuditEntry{
					Action: schema.AuditActionReinforce, Actor: "consolidation/competence", Timestamp: now,
					Rationale: fmt.Sprintf("Reinforced: %d new episodes match pattern", len(newSources)),
				}); err != nil {
					return err
				}
				existingSkills[skillKey] = current
				didReinforce = true
				return nil
			})
			if err != nil {
				return created, reinforced, err
			}
			if didReinforce {
				reinforced++
			}
			continue
		}

		// Build recipe steps from the tool sequence.
		recipe := make([]schema.RecipeStep, 0, len(g.tools))
		for i, tool := range g.tools {
			recipe = append(recipe, schema.RecipeStep{
				Step: fmt.Sprintf("Step %d: invoke %s", i+1, tool),
				Tool: tool,
			})
		}

		payload := &schema.CompetencePayload{
			Kind:          "competence",
			SkillName:     skillName,
			Triggers:      []schema.Trigger{{Signal: g.signature}},
			Recipe:        recipe,
			RequiredTools: g.tools,
			Performance: &schema.PerformanceStats{
				SuccessCount: int64(len(g.records)),
				SuccessRate:  1.0,
				LastUsedAt:   &now,
			},
			Version: "1",
		}

		newRec := schema.NewMemoryRecord(
			uuid.New().String(),
			schema.MemoryTypeCompetence,
			derivedSensitivity,
			payload,
		)
		newRec.Confidence = 0.8
		newRec.Scope = derivedScope
		newRec.Tags = []string{"consolidated", "auto-competence"}
		newRec.Provenance = schema.Provenance{
			Sources:   buildProvenanceSources(g.records, now),
			CreatedBy: "consolidation/competence",
		}
		newRec.AuditLog = []schema.AuditEntry{
			{
				Action:    schema.AuditActionCreate,
				Actor:     "consolidation/competence",
				Timestamp: now,
				Rationale: fmt.Sprintf(
					"Extracted from %d episodic records with pattern %s; policy: sensitivity=max(%s), scope=%s",
					len(g.records),
					g.signature,
					derivedSensitivity,
					scopePolicy,
				),
			},
		}

		candidates := snapshotEntityCandidates(ctx, c.store, newRec.Scope, g.tools...)
		err := storage.WithTransaction(ctx, c.store, func(tx storage.Transaction) error {
			if err := storage.ApplyDerivedSourcePolicy(ctx, tx, newRec, g.records); err != nil {
				return err
			}
			entityEdges := linkRecordToEntityTerms(ctx, entityStoreInTransaction(candidates, tx), newRec, g.tools, schema.GraphPredicateUses, schema.GraphPredicateUsedBy, now)
			if err := tx.Create(ctx, newRec); err != nil {
				return err
			}
			for _, src := range g.records {
				rel := schema.Relation{
					Predicate: schema.GraphPredicateDerivedFrom,
					TargetID:  src.ID,
					Weight:    1.0,
					CreatedAt: now,
				}
				if err := tx.AddRelation(ctx, newRec.ID, rel); err != nil {
					return err
				}
			}
			for _, edge := range entityEdges {
				if edge.SourceID == newRec.ID {
					continue
				}
				if err := tx.AddRelation(ctx, edge.SourceID, schema.Relation{
					Predicate: edge.Predicate,
					TargetID:  edge.TargetID,
					Weight:    edge.Weight,
					CreatedAt: edge.CreatedAt,
				}); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			return created, reinforced, err
		}
		if c.embedder != nil {
			_ = c.embedder.EmbedRecord(ctx, newRec)
		}

		existingSkills[skillKey] = newRec
		created++
	}

	return created, reinforced, nil
}

func competenceHasSource(rec *schema.MemoryRecord, sourceID string) bool {
	if rec == nil || strings.TrimSpace(sourceID) == "" {
		return false
	}
	for _, source := range rec.Provenance.Sources {
		if source.Ref == sourceID {
			return true
		}
	}
	for _, rel := range rec.Relations {
		if schema.NormalizeGraphPredicate(rel.Predicate) == schema.GraphPredicateDerivedFrom && rel.TargetID == sourceID {
			return true
		}
	}
	return false
}

func appendCompetenceSources(rec *schema.MemoryRecord, sources []*schema.MemoryRecord, now time.Time) {
	if rec == nil {
		return
	}
	if rec.Provenance.CreatedBy == "" {
		rec.Provenance.CreatedBy = "consolidation/competence"
	}
	payload, _ := rec.Payload.(*schema.CompetencePayload)
	for _, source := range sources {
		if source == nil || competenceHasSource(rec, source.ID) {
			continue
		}
		rec.Provenance.Sources = append(rec.Provenance.Sources, schema.ProvenanceSource{
			Kind:      schema.ProvenanceKindOutcome,
			Ref:       source.ID,
			CreatedBy: "consolidation/competence",
			Timestamp: now,
		})
		rec.Relations = append(rec.Relations, schema.Relation{
			Predicate: schema.GraphPredicateDerivedFrom,
			TargetID:  source.ID,
			Weight:    1.0,
			CreatedAt: now,
		})
		if payload != nil {
			if payload.Performance == nil {
				payload.Performance = &schema.PerformanceStats{SuccessRate: 1.0}
			}
			payload.Performance.SuccessCount++
			payload.Performance.SuccessRate = 1.0
			payload.Performance.LastUsedAt = &now
		}
	}
	if payload != nil {
		rec.Payload = payload
	}
}

type competenceSkillKey struct {
	scope string
	name  string
}

func competenceScopedSkillKey(scope, name string) competenceSkillKey {
	return competenceSkillKey{scope: scope, name: name}
}

// extractToolNames returns the ordered list of tool names from a tool graph.
func extractToolNames(nodes []schema.ToolNode) []string {
	names := make([]string, 0, len(nodes))
	for _, n := range nodes {
		tool := strings.TrimSpace(n.Tool)
		if tool == "" {
			continue
		}
		names = append(names, tool)
	}
	return names
}

// toolSignature produces a deterministic string key from a list of
// tool names. The names are sorted to allow matching regardless of
// invocation order.
func toolSignature(tools []string) string {
	sorted := make([]string, len(tools))
	copy(sorted, tools)
	sort.Strings(sorted)
	return strings.Join(sorted, "+")
}

// buildProvenanceSources creates provenance sources from a set of
// episodic records.
func buildProvenanceSources(records []*schema.MemoryRecord, now time.Time) []schema.ProvenanceSource {
	sources := make([]schema.ProvenanceSource, 0, len(records))
	for _, rec := range records {
		sources = append(sources, schema.ProvenanceSource{
			Kind:      schema.ProvenanceKindOutcome,
			Ref:       rec.ID,
			CreatedBy: "consolidation/competence",
			Timestamp: now,
		})
	}
	return sources
}

func deriveMaxSensitivity(records []*schema.MemoryRecord) schema.Sensitivity {
	maxSensitivity := schema.SensitivityPublic
	maxLevel := sensitivityRank(maxSensitivity)
	seenValid := false
	for _, rec := range records {
		level := sensitivityRank(rec.Sensitivity)
		if level < 0 {
			continue
		}
		if !seenValid || level > maxLevel {
			maxLevel = level
			maxSensitivity = rec.Sensitivity
			seenValid = true
		}
	}
	if !seenValid {
		return schema.SensitivityLow
	}
	return maxSensitivity
}

func deriveConservativeScope(records []*schema.MemoryRecord) (string, string) {
	if len(records) == 0 {
		return "", "preserved(unscoped)"
	}

	scopes := make(map[string]struct{}, len(records))
	for _, rec := range records {
		scopes[rec.Scope] = struct{}{}
	}
	if len(scopes) == 1 {
		for scope := range scopes {
			if scope == "" {
				return "", "preserved(unscoped)"
			}
			return scope, fmt.Sprintf("preserved(%s)", scope)
		}
	}

	parts := make([]string, 0, len(scopes))
	for scope := range scopes {
		if scope == "" {
			parts = append(parts, "unscoped")
			continue
		}
		parts = append(parts, scope)
	}
	sort.Strings(parts)
	return mixedScopeFallback, fmt.Sprintf("%s from %s", mixedScopeFallback, strings.Join(parts, ", "))
}

func sensitivityRank(s schema.Sensitivity) int {
	switch s {
	case schema.SensitivityPublic:
		return 0
	case schema.SensitivityLow:
		return 1
	case schema.SensitivityMedium:
		return 2
	case schema.SensitivityHigh:
		return 3
	case schema.SensitivityHyper:
		return 4
	default:
		return -1
	}
}
