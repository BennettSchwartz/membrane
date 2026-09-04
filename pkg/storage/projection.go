package storage

import (
	"reflect"
	"unicode/utf8"

	"github.com/BennettSchwartz/membrane/pkg/schema"
)

// ProjectedRecordBytes conservatively estimates the serialized bytes of a
// bounded record projection without marshaling or allocating proportional to
// the record. Values larger than capBytes are reported as capBytes+1.
func ProjectedRecordBytes(rec *schema.MemoryRecord, capBytes int64) int64 {
	if rec == nil {
		return 0
	}
	if capBytes < 0 {
		return 0
	}
	base := ProjectedRecordOverheadBytes
	if base > capBytes {
		return capBytes + 1
	}
	remaining := capBytes - base
	metadata := projectedRecordMetadataBytes(rec, remaining)
	if metadata > remaining {
		return capBytes + 1
	}
	remaining -= metadata
	payload := projectedValueBytes(reflect.ValueOf(rec.Payload), remaining, 0)
	if payload > remaining {
		return capBytes + 1
	}
	remaining -= payload
	interpretation := projectedValueBytes(reflect.ValueOf(rec.Interpretation), remaining, 0)
	if interpretation > remaining {
		return capBytes + 1
	}
	remaining -= interpretation
	relations := projectedValueBytes(reflect.ValueOf(rec.Relations), remaining, 0)
	if relations > remaining {
		return capBytes + 1
	}
	return base + metadata + payload + interpretation + relations
}

func projectedRecordMetadataBytes(rec *schema.MemoryRecord, capBytes int64) int64 {
	values := []string{
		rec.ID,
		string(rec.Type),
		string(rec.Sensitivity),
		rec.Scope,
		string(rec.Lifecycle.Decay.Curve),
		string(rec.Lifecycle.DeletionPolicy),
		rec.Provenance.CreatedBy,
	}
	total := int64(0)
	addString := func(value string) bool {
		size := projectedJSONStringBytes(value, capBytes-total)
		if size > capBytes-total {
			return false
		}
		total += size
		return true
	}
	for _, value := range values {
		if !addString(value) {
			return capBytes + 1
		}
	}
	for _, tag := range rec.Tags {
		if !addString(tag) {
			return capBytes + 1
		}
	}
	return total
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
	total := int64(0)
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
