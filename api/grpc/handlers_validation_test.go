package grpc

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"

	pb "github.com/BennettSchwartz/membrane/api/grpc/gen/membranev1"
	"github.com/BennettSchwartz/membrane/pkg/retrieval"
	"github.com/BennettSchwartz/membrane/pkg/revision"
	"github.com/BennettSchwartz/membrane/pkg/schema"
	"github.com/BennettSchwartz/membrane/pkg/storage"
)

func TestValidationHelpers(t *testing.T) {
	if err := validateStringField("summary", strings.Repeat("x", maxStringLength+1)); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("validateStringField long code = %v, want InvalidArgument", status.Code(err))
	}
	if err := validateStringField("summary", "ok"); err != nil {
		t.Fatalf("validateStringField ok: %v", err)
	}

	if err := validateTags(make([]string, maxTags+1)); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("validateTags too many code = %v, want InvalidArgument", status.Code(err))
	}
	if err := validateTags([]string{strings.Repeat("x", maxTagLength+1)}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("validateTags too long code = %v, want InvalidArgument", status.Code(err))
	}
	if err := validateTags([]string{"ok"}); err != nil {
		t.Fatalf("validateTags ok: %v", err)
	}

	if err := validateJSONPayload("content", make([]byte, maxPayloadSize+1)); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("validateJSONPayload code = %v, want InvalidArgument", status.Code(err))
	}
	if err := validateValuePayload("content", nil); err != nil {
		t.Fatalf("validateValuePayload nil: %v", err)
	}
	value, err := structpb.NewValue(map[string]any{"k": "v"})
	if err != nil {
		t.Fatalf("NewValue: %v", err)
	}
	if err := validateValuePayload("content", value); err != nil {
		t.Fatalf("validateValuePayload value: %v", err)
	}
	largeValue, err := structpb.NewValue(strings.Repeat("x", maxPayloadSize+1))
	if err != nil {
		t.Fatalf("NewValue large: %v", err)
	}
	if err := validateValuePayload("content", largeValue); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("validateValuePayload large code = %v, want InvalidArgument", status.Code(err))
	}

	if err := validateSensitivity("sensitivity", "", true); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("validateSensitivity required code = %v, want InvalidArgument", status.Code(err))
	}
	if err := validateSensitivity("sensitivity", "private", false); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("validateSensitivity invalid code = %v, want InvalidArgument", status.Code(err))
	}
	if err := validateSensitivity("sensitivity", string(schema.SensitivityLow), true); err != nil {
		t.Fatalf("validateSensitivity ok: %v", err)
	}

	if err := validateMemoryType("type", "unknown"); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("validateMemoryType invalid code = %v, want InvalidArgument", status.Code(err))
	}
	if err := validateMemoryType("type", string(schema.MemoryTypeSemantic)); err != nil {
		t.Fatalf("validateMemoryType ok: %v", err)
	}
}

func TestHandlerRejectsNilRequests(t *testing.T) {
	ctx := context.Background()
	h := &Handler{}
	for _, tc := range []struct {
		name string
		call func() error
	}{
		{name: "CaptureMemory", call: func() error { _, err := h.CaptureMemory(ctx, nil); return err }},
		{name: "RetrieveGraph", call: func() error { _, err := h.RetrieveGraph(ctx, nil); return err }},
		{name: "RetrieveByID", call: func() error { _, err := h.RetrieveByID(ctx, nil); return err }},
		{name: "Supersede", call: func() error { _, err := h.Supersede(ctx, nil); return err }},
		{name: "Fork", call: func() error { _, err := h.Fork(ctx, nil); return err }},
		{name: "Retract", call: func() error { _, err := h.Retract(ctx, nil); return err }},
		{name: "Merge", call: func() error { _, err := h.Merge(ctx, nil); return err }},
		{name: "Reinforce", call: func() error { _, err := h.Reinforce(ctx, nil); return err }},
		{name: "Penalize", call: func() error { _, err := h.Penalize(ctx, nil); return err }},
		{name: "Contest", call: func() error { _, err := h.Contest(ctx, nil); return err }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.call(); status.Code(err) != codes.InvalidArgument {
				t.Fatalf("nil request code = %v, want InvalidArgument; err=%v", status.Code(err), err)
			}
		})
	}
}

func TestTimeTrustAndValueHelpers(t *testing.T) {
	if got, err := parseOptionalTime(""); err != nil || !got.IsZero() {
		t.Fatalf("parseOptionalTime empty = %s, %v; want zero nil", got, err)
	}
	ts := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	if got, err := parseOptionalTime(ts.Format(time.RFC3339)); err != nil || !got.Equal(ts) {
		t.Fatalf("parseOptionalTime = %s, %v; want %s", got, err, ts)
	}
	if _, err := parseOptionalTime("bad"); err == nil {
		t.Fatalf("parseOptionalTime bad error = nil")
	}

	trust := toTrustContext(&pb.TrustContext{MaxSensitivity: string(schema.SensitivityMedium), Authenticated: true, ActorId: "actor", Scopes: []string{"project"}})
	if trust.MaxSensitivity != schema.SensitivityMedium || !trust.Authenticated || trust.ActorID != "actor" || len(trust.Scopes) != 1 {
		t.Fatalf("trust = %+v, want converted trust context", trust)
	}

	pbValue, err := valueToPB(map[string]any{"n": 1, "s": "two"})
	if err != nil {
		t.Fatalf("valueToPB: %v", err)
	}
	got := valueFromPB(pbValue)
	gotMap, ok := got.(map[string]any)
	if !ok || gotMap["s"] != "two" {
		t.Fatalf("valueFromPB = %#v, want map", got)
	}
	nullValue, err := valueToPB(nil)
	if err != nil {
		t.Fatalf("valueToPB nil: %v", err)
	}
	if valueFromPB(nullValue) != nil {
		t.Fatalf("valueFromPB null = %#v, want nil", valueFromPB(nullValue))
	}
	if _, err := valueToPB(make(chan int)); err == nil {
		t.Fatalf("valueToPB unsupported error = nil")
	}

	values, err := valueMapToPB(map[string]any{"a": 1})
	if err != nil {
		t.Fatalf("valueMapToPB: %v", err)
	}
	if from := valueMapFromPB(values); from["a"].(float64) != 1 {
		t.Fatalf("valueMapFromPB = %#v, want a=1", from)
	}
	if values, err := valueMapToPB(nil); err != nil || values != nil {
		t.Fatalf("valueMapToPB nil = %#v, %v; want nil nil", values, err)
	}
	if from := valueMapFromPB(nil); from != nil {
		t.Fatalf("valueMapFromPB nil = %#v, want nil", from)
	}

	if got := timeToString(time.Time{}); got != "" {
		t.Fatalf("timeToString zero = %q, want empty", got)
	}
	if got := timePtrToString(nil); got != "" {
		t.Fatalf("timePtrToString nil = %q, want empty", got)
	}
	if got := timePtrToString(&ts); got == "" {
		t.Fatalf("timePtrToString returned empty for non-zero time")
	}
	if got, err := parseTimeValue(""); err != nil || !got.IsZero() {
		t.Fatalf("parseTimeValue empty = %s, %v; want zero nil", got, err)
	}
	if ptr, err := parseTimePtr(""); err != nil || ptr != nil {
		t.Fatalf("parseTimePtr empty = %+v, %v; want nil nil", ptr, err)
	}
	if ptr, err := parseTimePtr(ts.Format(time.RFC3339Nano)); err != nil || ptr == nil || !ptr.Equal(ts) {
		t.Fatalf("parseTimePtr = %+v, %v; want %s", ptr, err, ts)
	}
	if _, err := parseTimePtr("bad"); err == nil {
		t.Fatalf("parseTimePtr bad error = nil")
	}
}

func TestServiceErrMapping(t *testing.T) {
	for _, tc := range []struct {
		err  error
		code codes.Code
	}{
		{err: nil, code: codes.OK},
		{err: status.Error(codes.PermissionDenied, "already coded"), code: codes.PermissionDenied},
		{err: &schema.ValidationError{Field: "id", Message: "required"}, code: codes.InvalidArgument},
		{err: storage.ErrNotFound, code: codes.NotFound},
		{err: storage.ErrAlreadyExists, code: codes.AlreadyExists},
		{err: retrieval.ErrAccessDenied, code: codes.PermissionDenied},
		{err: retrieval.ErrNilTrust, code: codes.InvalidArgument},
		{err: revision.ErrEpisodicImmutable, code: codes.FailedPrecondition},
		{err: errors.New("ingestion policy: candidate kind is required"), code: codes.InvalidArgument},
		{err: errors.New("semantic revision requires evidence"), code: codes.InvalidArgument},
		{err: errors.New("record is not episodic"), code: codes.InvalidArgument},
		{err: errors.New("database failed"), code: codes.Internal},
	} {
		got := serviceErr(tc.err)
		if status.Code(got) != tc.code {
			t.Fatalf("serviceErr(%v) code = %v, want %v", tc.err, status.Code(got), tc.code)
		}
	}
	if status.Code(internalErr(errors.New("boom"))) != codes.Internal {
		t.Fatalf("internalErr code = %v, want Internal", status.Code(internalErr(errors.New("boom"))))
	}
}
