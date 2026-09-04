package grpc

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"testing"

	pb "github.com/BennettSchwartz/membrane/api/grpc/gen/membranev1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
)

func nestedPayload(depth int, object bool) *structpb.Value {
	value := structpb.NewStringValue("leaf")
	for i := 0; i < depth; i++ {
		if object {
			value = structpb.NewStructValue(&structpb.Struct{Fields: map[string]*structpb.Value{"child": value}})
		} else {
			value = structpb.NewListValue(&structpb.ListValue{Values: []*structpb.Value{value}})
		}
	}
	return value
}

func TestValuePayloadRejectsExcessiveStructure(t *testing.T) {
	for _, object := range []bool{false, true} {
		t.Run(fmt.Sprintf("depth/object=%t", object), func(t *testing.T) {
			if err := validateValuePayload("content", nestedPayload(65, object)); status.Code(err) != codes.InvalidArgument {
				t.Fatalf("deep payload error = %v, want InvalidArgument", err)
			}
			if err := validateValuePayload("content", nestedPayload(64, object)); err != nil {
				t.Fatalf("bounded nested payload: %v", err)
			}
		})
	}
	values := make([]*structpb.Value, 100_000)
	fields := make(map[string]*structpb.Value, len(values))
	for i := range values {
		values[i] = structpb.NewNullValue()
		fields[fmt.Sprint(i)] = values[i]
	}
	for name, value := range map[string]*structpb.Value{
		"wide_list":   structpb.NewListValue(&structpb.ListValue{Values: values}),
		"wide_object": structpb.NewStructValue(&structpb.Struct{Fields: fields}),
	} {
		t.Run(name, func(t *testing.T) {
			if err := validateValuePayload("context", value); status.Code(err) != codes.InvalidArgument {
				t.Fatalf("wide payload error = %v, want InvalidArgument", err)
			}
		})
	}
}

func TestCaptureMemoryRejectsDeepContentAndContextBeforeDelegating(t *testing.T) {
	for _, field := range []string{"content", "context"} {
		for _, object := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/object=%t", field, object), func(t *testing.T) {
				req := &pb.CaptureMemoryRequest{Source: "test", Content: structpb.NewStringValue("ok")}
				if field == "content" {
					req.Content = nestedPayload(65, object)
				} else {
					req.Context = nestedPayload(65, object)
				}
				_, err := (&Handler{}).CaptureMemory(context.Background(), req)
				if status.Code(err) != codes.InvalidArgument || !strings.Contains(err.Error(), field) {
					t.Fatalf("error = %v, want InvalidArgument for %s", err, field)
				}
			})
		}
	}
}

func TestValuePayloadPreservesNestedNonFiniteValidation(t *testing.T) {
	for _, number := range []float64{math.NaN(), math.Inf(1), math.Inf(-1)} {
		value := structpb.NewStructValue(&structpb.Struct{Fields: map[string]*structpb.Value{
			"list": structpb.NewListValue(&structpb.ListValue{Values: []*structpb.Value{structpb.NewNumberValue(number)}}),
		}})
		if err := validateValuePayload("context", value); status.Code(err) != codes.InvalidArgument || !strings.Contains(err.Error(), "finite") {
			t.Fatalf("nested non-finite error = %v", err)
		}
	}
}

func TestValuePayloadBudgetMatchesEncodedJSONSize(t *testing.T) {
	values := map[string]*structpb.Value{
		"quoted":       structpb.NewStringValue("\"\\\n\r\t\b\f\x00\x1f"),
		"html":         structpb.NewStringValue("<&>"),
		"unicode":      structpb.NewStringValue("é🌍\u2028\u2029"),
		"invalid_utf8": structpb.NewStringValue(string([]byte{0xff, 'a', 0xc0})),
		"object": structpb.NewStructValue(&structpb.Struct{Fields: map[string]*structpb.Value{
			"<&>\n": structpb.NewStringValue("value"),
			"empty": structpb.NewStructValue(nil),
			"list": structpb.NewListValue(&structpb.ListValue{Values: []*structpb.Value{
				nil, {}, structpb.NewNullValue(), structpb.NewBoolValue(true), structpb.NewBoolValue(false), structpb.NewListValue(nil),
			}}),
		}}),
	}
	for _, number := range []float64{0, math.Copysign(0, -1), 1, -1, 1.25, 1e-6, 1e-7, -1e-9, 1e20, 1e21, math.MaxFloat64, math.SmallestNonzeroFloat64} {
		values[fmt.Sprint(number)] = structpb.NewNumberValue(number)
	}
	for name, value := range values {
		t.Run(name, func(t *testing.T) {
			encoded, err := json.Marshal(value.AsInterface())
			if err != nil {
				t.Fatal(err)
			}
			budget := jsonPayloadBudget{values: maxJSONValues, bytes: len(encoded)}
			if err := budget.validate(value, 0); err != nil || budget.bytes != 0 {
				t.Fatalf("encoded size %d: budget remaining = %d, error = %v", len(encoded), budget.bytes, err)
			}
			budget = jsonPayloadBudget{values: maxJSONValues, bytes: len(encoded) - 1}
			if err := budget.validate(value, 0); err == nil {
				t.Fatal("accepted JSON exceeding pre-conversion byte budget")
			}
		})
	}
}

func TestValuePayloadRejectsEscapingAndAggregateKeyBytes(t *testing.T) {
	for name, value := range map[string]*structpb.Value{
		"escaped_string": structpb.NewStringValue(strings.Repeat("<", maxPayloadSize/6+1)),
		"escaped_key": structpb.NewStructValue(&structpb.Struct{Fields: map[string]*structpb.Value{
			strings.Repeat("\x00", maxPayloadSize/6): structpb.NewNullValue(),
		}}),
		"aggregate_keys_and_strings": structpb.NewStructValue(&structpb.Struct{Fields: map[string]*structpb.Value{
			strings.Repeat("k", maxPayloadSize/2): structpb.NewStringValue(strings.Repeat("v", maxPayloadSize/2)),
		}}),
	} {
		t.Run(name, func(t *testing.T) {
			budget := jsonPayloadBudget{values: maxJSONValues, bytes: maxPayloadSize}
			if err := budget.validate(value, 0); err == nil {
				t.Fatal("accepted oversized encoded JSON before conversion")
			}
			if err := validateValuePayload("content", value); status.Code(err) != codes.InvalidArgument {
				t.Fatalf("error = %v, want InvalidArgument", err)
			}
		})
	}
}
