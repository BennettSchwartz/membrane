package ingestion

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/BennettSchwartz/membrane/pkg/schema"
	"github.com/BennettSchwartz/membrane/pkg/storage"
)

type fakeCaptureLookup struct {
	termRecords      []*schema.MemoryRecord
	identifierRecord *schema.MemoryRecord

	term      string
	termScope string
	termLimit int

	namespace       string
	identifierValue string
	identifierScope string
}

func (f *fakeCaptureLookup) FindEntitiesByTerm(_ context.Context, term, scope string, limit int) ([]*schema.MemoryRecord, error) {
	f.term = term
	f.termScope = scope
	f.termLimit = limit
	return f.termRecords, nil
}

func (f *fakeCaptureLookup) FindEntityByIdentifier(_ context.Context, namespace, value, scope string) (*schema.MemoryRecord, error) {
	f.namespace = namespace
	f.identifierValue = value
	f.identifierScope = scope
	if f.identifierRecord == nil {
		return nil, storage.ErrNotFound
	}
	return f.identifierRecord, nil
}

func TestCaptureTransactionStoreLifecycle(t *testing.T) {
	txStore := &captureTransactionStore{}

	tx, err := txStore.Begin(context.Background())
	if err == nil {
		t.Fatal("Begin: err = nil, want nested transaction error")
	}
	if tx != nil {
		t.Fatalf("Begin: tx = %#v, want nil", tx)
	}
	if !strings.Contains(err.Error(), "nested capture transaction") {
		t.Fatalf("Begin error = %q, want nested capture transaction", err)
	}

	if err := txStore.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestCaptureTransactionStoreLookupFallbacks(t *testing.T) {
	txStore := &captureTransactionStore{}
	ctx := context.Background()

	records, err := txStore.FindEntitiesByTerm(ctx, "orchid", "project", 3)
	if err != nil {
		t.Fatalf("FindEntitiesByTerm without lookup: %v", err)
	}
	if records != nil {
		t.Fatalf("FindEntitiesByTerm without lookup = %#v, want nil", records)
	}

	record, err := txStore.FindEntityByIdentifier(ctx, "name", "orchid", "project")
	if !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("FindEntityByIdentifier without lookup: err = %v, want ErrNotFound", err)
	}
	if record != nil {
		t.Fatalf("FindEntityByIdentifier without lookup = %#v, want nil", record)
	}
}

func TestCaptureTransactionStoreDelegatesLookup(t *testing.T) {
	ctx := context.Background()
	termRecord := schema.NewMemoryRecord("entity-orchid", schema.MemoryTypeEntity, schema.SensitivityLow, &schema.EntityPayload{
		Kind:          "entity",
		CanonicalName: "Orchid",
		PrimaryType:   schema.EntityTypeProject,
	})
	identifierRecord := schema.NewMemoryRecord("entity-borealis", schema.MemoryTypeEntity, schema.SensitivityLow, &schema.EntityPayload{
		Kind:          "entity",
		CanonicalName: "Borealis",
		PrimaryType:   schema.EntityTypeProject,
	})
	lookup := &fakeCaptureLookup{
		termRecords:      []*schema.MemoryRecord{termRecord},
		identifierRecord: identifierRecord,
	}
	txStore := &captureTransactionStore{lookup: lookup}

	records, err := txStore.FindEntitiesByTerm(ctx, "orchid", "project", 2)
	if err != nil {
		t.Fatalf("FindEntitiesByTerm: %v", err)
	}
	if len(records) != 1 || records[0].ID != termRecord.ID {
		t.Fatalf("FindEntitiesByTerm = %+v, want %s", records, termRecord.ID)
	}
	if lookup.term != "orchid" || lookup.termScope != "project" || lookup.termLimit != 2 {
		t.Fatalf("FindEntitiesByTerm args = (%q, %q, %d), want (orchid, project, 2)", lookup.term, lookup.termScope, lookup.termLimit)
	}

	record, err := txStore.FindEntityByIdentifier(ctx, "slug", "borealis", "project")
	if err != nil {
		t.Fatalf("FindEntityByIdentifier: %v", err)
	}
	if record.ID != identifierRecord.ID {
		t.Fatalf("FindEntityByIdentifier = %s, want %s", record.ID, identifierRecord.ID)
	}
	if lookup.namespace != "slug" || lookup.identifierValue != "borealis" || lookup.identifierScope != "project" {
		t.Fatalf("FindEntityByIdentifier args = (%q, %q, %q), want (slug, borealis, project)", lookup.namespace, lookup.identifierValue, lookup.identifierScope)
	}
}
