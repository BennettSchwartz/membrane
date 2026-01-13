package revision

import (
	"errors"
	"fmt"

	"github.com/GustyCube/membrane/pkg/schema"
	"github.com/GustyCube/membrane/pkg/storage"
)

// Common errors returned by revision operations.
var (
	// ErrEpisodicImmutable is returned when attempting to revise an episodic record.
	// RFC Section 5: Episodic memory is append-only and MUST NOT be revised.
	ErrEpisodicImmutable = errors.New("episodic memory is immutable and cannot be revised")

	// ErrRecordNotFound is returned when a referenced record does not exist.
	ErrRecordNotFound = storage.ErrNotFound
)

// Service provides revision operations for memory records.
// All operations are atomic — partial revisions are never externally visible (RFC 15.7).
type Service struct {
	store storage.Store
}

// NewService creates a new revision Service backed by the given Store.
func NewService(store storage.Store) *Service {
	return &Service{store: store}
}

// ensureRevisable checks that a record exists and is not episodic.
// Returns an error if the record cannot be revised.
func ensureRevisable(rec *schema.MemoryRecord) error {
	if rec.Type == schema.MemoryTypeEpisodic {
		return fmt.Errorf("%w: record %s", ErrEpisodicImmutable, rec.ID)
	}
	return nil
}

// retractRecord marks a record as retracted by setting salience to 0 and,
// for semantic records, setting the revision status to "retracted".
func retractRecord(rec *schema.MemoryRecord) {
	rec.Salience = 0
	if sp, ok := rec.Payload.(*schema.SemanticPayload); ok {
		if sp.Revision == nil {
			sp.Revision = &schema.RevisionState{}
		}
		sp.Revision.Status = schema.RevisionStatusRetracted
	}
}
