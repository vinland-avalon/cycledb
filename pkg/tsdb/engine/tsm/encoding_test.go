package tsm_test

import (
	"fmt"
	"math/rand"
	"reflect"
	"testing"
	"time"

	"cycledb/pkg/tsdb/engine/tsm"

	"github.com/davecgh/go-spew/spew"
)

func TestEncoding_FloatBlock(t *testing.T) {
	valueCount := 1000
	times := getTimes(valueCount, 60, time.Second)
	values := make([]tsm.Value, len(times))
	for i, t := range times {
		values[i] = tsm.NewValue(t, float64(i))
	}

	b, err := tsm.Values(values).Encode(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var decodedValues []tsm.Value
	decodedValues, err = tsm.DecodeBlock(b, decodedValues)
	if err != nil {
		t.Fatalf("unexpected error decoding block: %v", err)
	}

	if !reflect.DeepEqual(decodedValues, values) {
		t.Fatalf("unexpected results:\n\tgot: %s\n\texp: %s\n", spew.Sdump(decodedValues), spew.Sdump(values))
	}
}

func TestEncoding_FloatBlock_ZeroTime(t *testing.T) {
	values := make([]tsm.Value, 3)
	for i := 0; i < 3; i++ {
		values[i] = tsm.NewValue(0, float64(i))
	}

	b, err := tsm.Values(values).Encode(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var decodedValues []tsm.Value
	decodedValues, err = tsm.DecodeBlock(b, decodedValues)
	if err != nil {
		t.Fatalf("unexpected error decoding block: %v", err)
	}

	if !reflect.DeepEqual(decodedValues, values) {
		t.Fatalf("unexpected results:\n\tgot: %v\n\texp: %v\n", decodedValues, values)
	}
}

func TestEncoding_FloatBlock_SimilarFloats(t *testing.T) {
	values := make([]tsm.Value, 5)
	values[0] = tsm.NewValue(1444238178437870000, 6.00065e+06)
	values[1] = tsm.NewValue(1444238185286830000, 6.000656e+06)
	values[2] = tsm.NewValue(1444238188441501000, 6.000657e+06)
	values[3] = tsm.NewValue(1444238195286811000, 6.000659e+06)
	values[4] = tsm.NewValue(1444238198439917000, 6.000661e+06)

	b, err := tsm.Values(values).Encode(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var decodedValues []tsm.Value
	decodedValues, err = tsm.DecodeBlock(b, decodedValues)
	if err != nil {
		t.Fatalf("unexpected error decoding block: %v", err)
	}

	if !reflect.DeepEqual(decodedValues, values) {
		t.Fatalf("unexpected results:\n\tgot: %v\n\texp: %v\n", decodedValues, values)
	}
}

func TestEncoding_IntBlock_Basic(t *testing.T) {
	valueCount := 1000
	times := getTimes(valueCount, 60, time.Second)
	values := make([]tsm.Value, len(times))
	for i, t := range times {
		values[i] = tsm.NewValue(t, int64(i))
	}

	b, err := tsm.Values(values).Encode(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var decodedValues []tsm.Value
	decodedValues, err = tsm.DecodeBlock(b, decodedValues)
	if err != nil {
		t.Fatalf("unexpected error decoding block: %v", err)
	}

	if len(decodedValues) != len(values) {
		t.Fatalf("unexpected results length:\n\tgot: %v\n\texp: %v\n", len(decodedValues), len(values))
	}

	for i := 0; i < len(decodedValues); i++ {
		if decodedValues[i].UnixNano() != values[i].UnixNano() {
			t.Fatalf("unexpected results:\n\tgot: %v\n\texp: %v\n", decodedValues[i].UnixNano(), values[i].UnixNano())
		}

		if decodedValues[i].Value() != values[i].Value() {
			t.Fatalf("unexpected results:\n\tgot: %v\n\texp: %v\n", decodedValues[i].Value(), values[i].Value())
		}
	}
}

func TestEncoding_IntBlock_Negatives(t *testing.T) {
	valueCount := 1000
	times := getTimes(valueCount, 60, time.Second)
	values := make([]tsm.Value, len(times))
	for i, t := range times {
		v := int64(i)
		if i%2 == 0 {
			v = -v
		}
		values[i] = tsm.NewValue(t, int64(v))
	}

	b, err := tsm.Values(values).Encode(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var decodedValues []tsm.Value
	decodedValues, err = tsm.DecodeBlock(b, decodedValues)
	if err != nil {
		t.Fatalf("unexpected error decoding block: %v", err)
	}

	if !reflect.DeepEqual(decodedValues, values) {
		t.Fatalf("unexpected results:\n\tgot: %v\n\texp: %v\n", decodedValues, values)
	}
}

func TestEncoding_UIntBlock_Basic(t *testing.T) {
	valueCount := 1000
	times := getTimes(valueCount, 60, time.Second)
	values := make([]tsm.Value, len(times))
	for i, t := range times {
		values[i] = tsm.NewValue(t, uint64(i))
	}

	b, err := tsm.Values(values).Encode(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var decodedValues []tsm.Value
	decodedValues, err = tsm.DecodeBlock(b, decodedValues)
	if err != nil {
		t.Fatalf("unexpected error decoding block: %v", err)
	}

	if len(decodedValues) != len(values) {
		t.Fatalf("unexpected results length:\n\tgot: %v\n\texp: %v\n", len(decodedValues), len(values))
	}

	for i := 0; i < len(decodedValues); i++ {
		if decodedValues[i].UnixNano() != values[i].UnixNano() {
			t.Fatalf("unexpected results:\n\tgot: %v\n\texp: %v\n", decodedValues[i].UnixNano(), values[i].UnixNano())
		}

		if decodedValues[i].Value() != values[i].Value() {
			t.Fatalf("unexpected results:\n\tgot: %v\n\texp: %v\n", decodedValues[i].Value(), values[i].Value())
		}
	}
}

// TestEncoding_UIntBlock_MaxValues encodes uint64 numbers starting at max (18446744073709551615)
// down to 18446744073709550616
func TestEncoding_UIntBlock_MaxValues(t *testing.T) {
	valueCount := 1000
	times := getTimes(valueCount, 60, time.Second)
	values := make([]tsm.Value, len(times))
	for i, t := range times {
		values[i] = tsm.NewValue(t, ^uint64(i))
	}

	b, err := tsm.Values(values).Encode(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var decodedValues []tsm.Value
	decodedValues, err = tsm.DecodeBlock(b, decodedValues)
	if err != nil {
		t.Fatalf("unexpected error decoding block: %v", err)
	}

	if !reflect.DeepEqual(decodedValues, values) {
		t.Fatalf("unexpected results:\n\tgot: %v\n\texp: %v\n", decodedValues, values)
	}
}

func TestEncoding_BooleanBlock_Basic(t *testing.T) {
	valueCount := 1000
	times := getTimes(valueCount, 60, time.Second)
	values := make([]tsm.Value, len(times))
	for i, t := range times {
		v := true
		if i%2 == 0 {
			v = false
		}
		values[i] = tsm.NewValue(t, v)
	}

	b, err := tsm.Values(values).Encode(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var decodedValues []tsm.Value
	decodedValues, err = tsm.DecodeBlock(b, decodedValues)
	if err != nil {
		t.Fatalf("unexpected error decoding block: %v", err)
	}

	if !reflect.DeepEqual(decodedValues, values) {
		t.Fatalf("unexpected results:\n\tgot: %v\n\texp: %v\n", decodedValues, values)
	}
}

func TestEncoding_StringBlock_Basic(t *testing.T) {
	valueCount := 1000
	times := getTimes(valueCount, 60, time.Second)
	values := make([]tsm.Value, len(times))
	for i, t := range times {
		values[i] = tsm.NewValue(t, fmt.Sprintf("value %d", i))
	}

	b, err := tsm.Values(values).Encode(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var decodedValues []tsm.Value
	decodedValues, err = tsm.DecodeBlock(b, decodedValues)
	if err != nil {
		t.Fatalf("unexpected error decoding block: %v", err)
	}

	if !reflect.DeepEqual(decodedValues, values) {
		t.Fatalf("unexpected results:\n\tgot: %v\n\texp: %v\n", decodedValues, values)
	}
}

func TestEncoding_BlockType(t *testing.T) {
	tests := []struct {
		value     interface{}
		blockType byte
	}{
		{value: float64(1.0), blockType: tsm.BlockFloat64},
		{value: int64(1), blockType: tsm.BlockInteger},
		{value: uint64(1), blockType: tsm.BlockUnsigned},
		{value: true, blockType: tsm.BlockBoolean},
		{value: "string", blockType: tsm.BlockString},
	}

	for _, test := range tests {
		var values []tsm.Value
		values = append(values, tsm.NewValue(0, test.value))

		b, err := tsm.Values(values).Encode(nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		bt, err := tsm.BlockType(b)
		if err != nil {
			t.Fatalf("unexpected error decoding block type: %v", err)
		}

		if got, exp := bt, test.blockType; got != exp {
			t.Fatalf("block type mismatch: got %v, exp %v", got, exp)
		}
	}

	_, err := tsm.BlockType([]byte{10})
	if err == nil {
		t.Fatalf("expected error decoding block type, got nil")
	}
}

func TestEncoding_Count(t *testing.T) {
	tests := []struct {
		value     interface{}
		blockType byte
	}{
		{value: float64(1.0), blockType: tsm.BlockFloat64},
		{value: int64(1), blockType: tsm.BlockInteger},
		{value: uint64(1), blockType: tsm.BlockUnsigned},
		{value: true, blockType: tsm.BlockBoolean},
		{value: "string", blockType: tsm.BlockString},
	}

	for _, test := range tests {
		var values []tsm.Value
		values = append(values, tsm.NewValue(0, test.value))

		b, err := tsm.Values(values).Encode(nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		cnt, err := tsm.BlockCount(b)
		if err != nil {
			t.Fatalf("Block is corrupted: %v", err)
		}
		if got, exp := cnt, 1; got != exp {
			t.Fatalf("block count mismatch: got %v, exp %v", got, exp)
		}
	}
}

func TestValues_MergeFloat(t *testing.T) {
	tests := []struct {
		a, b, exp []tsm.Value
	}{

		{ // empty a
			a: []tsm.Value{},

			b: []tsm.Value{
				tsm.NewValue(1, 1.2),
				tsm.NewValue(2, 2.2),
			},
			exp: []tsm.Value{
				tsm.NewValue(1, 1.2),
				tsm.NewValue(2, 2.2),
			},
		},
		{ // empty b
			a: []tsm.Value{
				tsm.NewValue(1, 1.1),
				tsm.NewValue(2, 2.1),
			},

			b: []tsm.Value{},
			exp: []tsm.Value{
				tsm.NewValue(1, 1.1),
				tsm.NewValue(2, 2.1),
			},
		},
		{
			a: []tsm.Value{
				tsm.NewValue(0, 0.0),
				tsm.NewValue(1, 1.1),
				tsm.NewValue(2, 2.1),
			},
			b: []tsm.Value{
				tsm.NewValue(2, 2.2),
				tsm.NewValue(2, 2.2), // duplicate data
			},
			exp: []tsm.Value{
				tsm.NewValue(0, 0.0),
				tsm.NewValue(1, 1.1),
				tsm.NewValue(2, 2.2),
			},
		},
		{
			a: []tsm.Value{
				tsm.NewValue(0, 0.0),
				tsm.NewValue(1, 1.1),
				tsm.NewValue(1, 1.1), // duplicate data
				tsm.NewValue(2, 2.1),
			},
			b: []tsm.Value{
				tsm.NewValue(2, 2.2),
				tsm.NewValue(2, 2.2), // duplicate data
			},
			exp: []tsm.Value{
				tsm.NewValue(0, 0.0),
				tsm.NewValue(1, 1.1),
				tsm.NewValue(2, 2.2),
			},
		},

		{
			a: []tsm.Value{
				tsm.NewValue(1, 1.1),
			},
			b: []tsm.Value{
				tsm.NewValue(0, 0.0),
				tsm.NewValue(1, 1.2), // overwrites a
				tsm.NewValue(2, 2.2),
				tsm.NewValue(3, 3.2),
				tsm.NewValue(4, 4.2),
			},
			exp: []tsm.Value{
				tsm.NewValue(0, 0.0),
				tsm.NewValue(1, 1.2),
				tsm.NewValue(2, 2.2),
				tsm.NewValue(3, 3.2),
				tsm.NewValue(4, 4.2),
			},
		},
		{
			a: []tsm.Value{
				tsm.NewValue(1, 1.1),
				tsm.NewValue(2, 2.1),
				tsm.NewValue(3, 3.1),
				tsm.NewValue(4, 4.1),
			},

			b: []tsm.Value{
				tsm.NewValue(1, 1.2), // overwrites a
				tsm.NewValue(2, 2.2), // overwrites a
			},
			exp: []tsm.Value{
				tsm.NewValue(1, 1.2),
				tsm.NewValue(2, 2.2),
				tsm.NewValue(3, 3.1),
				tsm.NewValue(4, 4.1),
			},
		},
		{
			a: []tsm.Value{
				tsm.NewValue(1, 1.1),
				tsm.NewValue(2, 2.1),
				tsm.NewValue(3, 3.1),
				tsm.NewValue(4, 4.1),
			},

			b: []tsm.Value{
				tsm.NewValue(1, 1.2), // overwrites a
				tsm.NewValue(2, 2.2), // overwrites a
				tsm.NewValue(3, 3.2),
				tsm.NewValue(4, 4.2),
			},
			exp: []tsm.Value{
				tsm.NewValue(1, 1.2),
				tsm.NewValue(2, 2.2),
				tsm.NewValue(3, 3.2),
				tsm.NewValue(4, 4.2),
			},
		},
		{
			a: []tsm.Value{
				tsm.NewValue(0, 0.0),
				tsm.NewValue(1, 1.1),
				tsm.NewValue(2, 2.1),
				tsm.NewValue(3, 3.1),
				tsm.NewValue(4, 4.1),
			},
			b: []tsm.Value{
				tsm.NewValue(0, 0.0),
				tsm.NewValue(2, 2.2),
				tsm.NewValue(4, 4.2),
			},
			exp: []tsm.Value{
				tsm.NewValue(0, 0.0),
				tsm.NewValue(1, 1.1),
				tsm.NewValue(2, 2.2),
				tsm.NewValue(3, 3.1),
				tsm.NewValue(4, 4.2),
			},
		},

		{
			a: []tsm.Value{
				tsm.NewValue(1462498658242869207, 0.0),
				tsm.NewValue(1462498658288956853, 1.1),
			},
			b: []tsm.Value{
				tsm.NewValue(1462498658242870810, 0.0),
				tsm.NewValue(1462498658262911238, 2.2),
				tsm.NewValue(1462498658282415038, 4.2),
				tsm.NewValue(1462498658282417760, 4.2),
			},
			exp: []tsm.Value{
				tsm.NewValue(1462498658242869207, 0.0),
				tsm.NewValue(1462498658242870810, 0.0),
				tsm.NewValue(1462498658262911238, 2.2),
				tsm.NewValue(1462498658282415038, 4.2),
				tsm.NewValue(1462498658282417760, 4.2),
				tsm.NewValue(1462498658288956853, 1.1),
			},
		},
		{
			a: []tsm.Value{
				tsm.NewValue(4, 4.0),
				tsm.NewValue(5, 5.0),
				tsm.NewValue(6, 6.0),
			},
			b: []tsm.Value{
				tsm.NewValue(1, 1.0),
				tsm.NewValue(2, 2.0),
				tsm.NewValue(3, 3.0),
			},
			exp: []tsm.Value{
				tsm.NewValue(1, 1.0),
				tsm.NewValue(2, 2.0),
				tsm.NewValue(3, 3.0),
				tsm.NewValue(4, 4.0),
				tsm.NewValue(5, 5.0),
				tsm.NewValue(6, 6.0),
			},
		},
		{
			a: []tsm.Value{
				tsm.NewValue(5, 5.0),
				tsm.NewValue(6, 6.0),
			},
			b: []tsm.Value{
				tsm.NewValue(1, 1.0),
				tsm.NewValue(2, 2.0),
				tsm.NewValue(3, 3.0),
				tsm.NewValue(4, 4.0),
				tsm.NewValue(7, 7.0),
				tsm.NewValue(8, 8.0),
			},
			exp: []tsm.Value{
				tsm.NewValue(1, 1.0),
				tsm.NewValue(2, 2.0),
				tsm.NewValue(3, 3.0),
				tsm.NewValue(4, 4.0),
				tsm.NewValue(5, 5.0),
				tsm.NewValue(6, 6.0),
				tsm.NewValue(7, 7.0),
				tsm.NewValue(8, 8.0),
			},
		},
		{
			a: []tsm.Value{
				tsm.NewValue(1, 1.0),
				tsm.NewValue(2, 2.0),
				tsm.NewValue(3, 3.0),
			},
			b: []tsm.Value{
				tsm.NewValue(4, 4.0),
				tsm.NewValue(5, 5.0),
				tsm.NewValue(6, 6.0),
			},
			exp: []tsm.Value{
				tsm.NewValue(1, 1.0),
				tsm.NewValue(2, 2.0),
				tsm.NewValue(3, 3.0),
				tsm.NewValue(4, 4.0),
				tsm.NewValue(5, 5.0),
				tsm.NewValue(6, 6.0),
			},
		},
	}

	for i, test := range tests {
		got := tsm.Values(test.a).Merge(test.b)
		if exp, got := len(test.exp), len(got); exp != got {
			t.Fatalf("test(%d): value length mismatch: exp %v, got %v", i, exp, got)
		}

		dedup := tsm.Values(append(test.a, test.b...)).Deduplicate()

		for i := range test.exp {
			if exp, got := test.exp[i].String(), got[i].String(); exp != got {
				t.Fatalf("value mismatch:\n exp %v\n got %v", exp, got)
			}

			if exp, got := test.exp[i].String(), dedup[i].String(); exp != got {
				t.Fatalf("value mismatch:\n exp %v\n got %v", exp, got)
			}
		}
	}
}

func TestIntegerValues_Merge(t *testing.T) {
	integerValue := func(t int64, f int64) tsm.IntegerValue {
		return tsm.NewValue(t, f).(tsm.IntegerValue)
	}

	tests := []struct {
		a, b, exp []tsm.IntegerValue
	}{

		{ // empty a
			a: []tsm.IntegerValue{},

			b: []tsm.IntegerValue{
				integerValue(1, 10),
				integerValue(2, 20),
			},
			exp: []tsm.IntegerValue{
				integerValue(1, 10),
				integerValue(2, 20),
			},
		},
		{ // empty b
			a: []tsm.IntegerValue{
				integerValue(1, 1),
				integerValue(2, 2),
			},

			b: []tsm.IntegerValue{},
			exp: []tsm.IntegerValue{
				integerValue(1, 1),
				integerValue(2, 2),
			},
		},
		{
			a: []tsm.IntegerValue{
				integerValue(1, 1),
			},
			b: []tsm.IntegerValue{
				integerValue(0, 0),
				integerValue(1, 10), // overwrites a
				integerValue(2, 20),
				integerValue(3, 30),
				integerValue(4, 40),
			},
			exp: []tsm.IntegerValue{
				integerValue(0, 0),
				integerValue(1, 10),
				integerValue(2, 20),
				integerValue(3, 30),
				integerValue(4, 40),
			},
		},
		{
			a: []tsm.IntegerValue{
				integerValue(1, 1),
				integerValue(2, 2),
				integerValue(3, 3),
				integerValue(4, 4),
			},

			b: []tsm.IntegerValue{
				integerValue(1, 10), // overwrites a
				integerValue(2, 20), // overwrites a
			},
			exp: []tsm.IntegerValue{
				integerValue(1, 10),
				integerValue(2, 20),
				integerValue(3, 3),
				integerValue(4, 4),
			},
		},
		{
			a: []tsm.IntegerValue{
				integerValue(1, 1),
				integerValue(2, 2),
				integerValue(3, 3),
				integerValue(4, 4),
			},

			b: []tsm.IntegerValue{
				integerValue(1, 10), // overwrites a
				integerValue(2, 20), // overwrites a
				integerValue(3, 30),
				integerValue(4, 40),
			},
			exp: []tsm.IntegerValue{
				integerValue(1, 10),
				integerValue(2, 20),
				integerValue(3, 30),
				integerValue(4, 40),
			},
		},
		{
			a: []tsm.IntegerValue{
				integerValue(0, 0),
				integerValue(1, 1),
				integerValue(2, 2),
				integerValue(3, 3),
				integerValue(4, 4),
			},
			b: []tsm.IntegerValue{
				integerValue(0, 0),
				integerValue(2, 20),
				integerValue(4, 40),
			},
			exp: []tsm.IntegerValue{
				integerValue(0, 0.0),
				integerValue(1, 1),
				integerValue(2, 20),
				integerValue(3, 3),
				integerValue(4, 40),
			},
		},
	}

	for i, test := range tests {
		if i != 2 {
			continue
		}

		got := tsm.IntegerValues(test.a).Merge(test.b)
		if exp, got := len(test.exp), len(got); exp != got {
			t.Fatalf("test(%d): value length mismatch: exp %v, got %v", i, exp, got)
		}

		dedup := tsm.IntegerValues(append(test.a, test.b...)).Deduplicate()

		for i := range test.exp {
			if exp, got := test.exp[i].String(), got[i].String(); exp != got {
				t.Fatalf("value mismatch:\n exp %v\n got %v", exp, got)
			}

			if exp, got := test.exp[i].String(), dedup[i].String(); exp != got {
				t.Fatalf("value mismatch:\n exp %v\n got %v", exp, got)
			}
		}
	}
}

func TestUnsignedValues_Merge(t *testing.T) {
	uintValue := func(t int64, f uint64) tsm.UnsignedValue {
		return tsm.NewValue(t, f).(tsm.UnsignedValue)
	}

	tests := []struct {
		a, b, exp []tsm.UnsignedValue
	}{

		{ // empty a
			a: []tsm.UnsignedValue{},

			b: []tsm.UnsignedValue{
				uintValue(1, 10),
				uintValue(2, 20),
			},
			exp: []tsm.UnsignedValue{
				uintValue(1, 10),
				uintValue(2, 20),
			},
		},
		{ // empty b
			a: []tsm.UnsignedValue{
				uintValue(1, 1),
				uintValue(2, 2),
			},

			b: []tsm.UnsignedValue{},
			exp: []tsm.UnsignedValue{
				uintValue(1, 1),
				uintValue(2, 2),
			},
		},
		{
			a: []tsm.UnsignedValue{
				uintValue(1, 1),
			},
			b: []tsm.UnsignedValue{
				uintValue(0, 0),
				uintValue(1, 10), // overwrites a
				uintValue(2, 20),
				uintValue(3, 30),
				uintValue(4, 40),
			},
			exp: []tsm.UnsignedValue{
				uintValue(0, 0),
				uintValue(1, 10),
				uintValue(2, 20),
				uintValue(3, 30),
				uintValue(4, 40),
			},
		},
		{
			a: []tsm.UnsignedValue{
				uintValue(1, 1),
				uintValue(2, 2),
				uintValue(3, 3),
				uintValue(4, 4),
			},

			b: []tsm.UnsignedValue{
				uintValue(1, ^uint64(0)), // overwrites a
				uintValue(2, 20),         // overwrites a
			},
			exp: []tsm.UnsignedValue{
				uintValue(1, ^uint64(0)),
				uintValue(2, 20),
				uintValue(3, 3),
				uintValue(4, 4),
			},
		},
		{
			a: []tsm.UnsignedValue{
				uintValue(1, 1),
				uintValue(2, 2),
				uintValue(3, 3),
				uintValue(4, 4),
			},

			b: []tsm.UnsignedValue{
				uintValue(1, 10), // overwrites a
				uintValue(2, 20), // overwrites a
				uintValue(3, 30),
				uintValue(4, 40),
			},
			exp: []tsm.UnsignedValue{
				uintValue(1, 10),
				uintValue(2, 20),
				uintValue(3, 30),
				uintValue(4, 40),
			},
		},
		{
			a: []tsm.UnsignedValue{
				uintValue(0, 0),
				uintValue(1, 1),
				uintValue(2, 2),
				uintValue(3, 3),
				uintValue(4, 4),
			},
			b: []tsm.UnsignedValue{
				uintValue(0, 0),
				uintValue(2, 20),
				uintValue(4, 40),
			},
			exp: []tsm.UnsignedValue{
				uintValue(0, 0.0),
				uintValue(1, 1),
				uintValue(2, 20),
				uintValue(3, 3),
				uintValue(4, 40),
			},
		},
	}

	for i, test := range tests {
		if i != 2 {
			continue
		}

		got := tsm.UnsignedValues(test.a).Merge(test.b)
		if exp, got := len(test.exp), len(got); exp != got {
			t.Fatalf("test(%d): value length mismatch: exp %v, got %v", i, exp, got)
		}

		dedup := tsm.UnsignedValues(append(test.a, test.b...)).Deduplicate()

		for i := range test.exp {
			if exp, got := test.exp[i].String(), got[i].String(); exp != got {
				t.Fatalf("value mismatch:\n exp %v\n got %v", exp, got)
			}

			if exp, got := test.exp[i].String(), dedup[i].String(); exp != got {
				t.Fatalf("value mismatch:\n exp %v\n got %v", exp, got)
			}
		}
	}
}

func TestFloatValues_Merge(t *testing.T) {
	floatValue := func(t int64, f float64) tsm.FloatValue {
		return tsm.NewValue(t, f).(tsm.FloatValue)
	}

	tests := []struct {
		a, b, exp []tsm.FloatValue
	}{

		{ // empty a
			a: []tsm.FloatValue{},

			b: []tsm.FloatValue{
				floatValue(1, 1.2),
				floatValue(2, 2.2),
			},
			exp: []tsm.FloatValue{
				floatValue(1, 1.2),
				floatValue(2, 2.2),
			},
		},
		{ // empty b
			a: []tsm.FloatValue{
				floatValue(1, 1.1),
				floatValue(2, 2.1),
			},

			b: []tsm.FloatValue{},
			exp: []tsm.FloatValue{
				floatValue(1, 1.1),
				floatValue(2, 2.1),
			},
		},
		{
			a: []tsm.FloatValue{
				floatValue(1, 1.1),
			},
			b: []tsm.FloatValue{
				floatValue(0, 0.0),
				floatValue(1, 1.2), // overwrites a
				floatValue(2, 2.2),
				floatValue(3, 3.2),
				floatValue(4, 4.2),
			},
			exp: []tsm.FloatValue{
				floatValue(0, 0.0),
				floatValue(1, 1.2),
				floatValue(2, 2.2),
				floatValue(3, 3.2),
				floatValue(4, 4.2),
			},
		},
		{
			a: []tsm.FloatValue{
				floatValue(1, 1.1),
				floatValue(2, 2.1),
				floatValue(3, 3.1),
				floatValue(4, 4.1),
			},

			b: []tsm.FloatValue{
				floatValue(1, 1.2), // overwrites a
				floatValue(2, 2.2), // overwrites a
			},
			exp: []tsm.FloatValue{
				floatValue(1, 1.2),
				floatValue(2, 2.2),
				floatValue(3, 3.1),
				floatValue(4, 4.1),
			},
		},
		{
			a: []tsm.FloatValue{
				floatValue(1, 1.1),
				floatValue(2, 2.1),
				floatValue(3, 3.1),
				floatValue(4, 4.1),
			},

			b: []tsm.FloatValue{
				floatValue(1, 1.2), // overwrites a
				floatValue(2, 2.2), // overwrites a
				floatValue(3, 3.2),
				floatValue(4, 4.2),
			},
			exp: []tsm.FloatValue{
				floatValue(1, 1.2),
				floatValue(2, 2.2),
				floatValue(3, 3.2),
				floatValue(4, 4.2),
			},
		},
		{
			a: []tsm.FloatValue{
				floatValue(0, 0.0),
				floatValue(1, 1.1),
				floatValue(2, 2.1),
				floatValue(3, 3.1),
				floatValue(4, 4.1),
			},
			b: []tsm.FloatValue{
				floatValue(0, 0.0),
				floatValue(2, 2.2),
				floatValue(4, 4.2),
			},
			exp: []tsm.FloatValue{
				floatValue(0, 0.0),
				floatValue(1, 1.1),
				floatValue(2, 2.2),
				floatValue(3, 3.1),
				floatValue(4, 4.2),
			},
		},
	}

	for i, test := range tests {
		got := tsm.FloatValues(test.a).Merge(test.b)
		if exp, got := len(test.exp), len(got); exp != got {
			t.Fatalf("test(%d): value length mismatch: exp %v, got %v", i, exp, got)
		}

		dedup := tsm.FloatValues(append(test.a, test.b...)).Deduplicate()

		for i := range test.exp {
			if exp, got := test.exp[i].String(), got[i].String(); exp != got {
				t.Fatalf("value mismatch:\n exp %v\n got %v", exp, got)
			}

			if exp, got := test.exp[i].String(), dedup[i].String(); exp != got {
				t.Fatalf("value mismatch:\n exp %v\n got %v", exp, got)
			}
		}
	}
}

func TestBooleanValues_Merge(t *testing.T) {
	booleanValue := func(t int64, f bool) tsm.BooleanValue {
		return tsm.NewValue(t, f).(tsm.BooleanValue)
	}

	tests := []struct {
		a, b, exp []tsm.BooleanValue
	}{

		{ // empty a
			a: []tsm.BooleanValue{},

			b: []tsm.BooleanValue{
				booleanValue(1, true),
				booleanValue(2, true),
			},
			exp: []tsm.BooleanValue{
				booleanValue(1, true),
				booleanValue(2, true),
			},
		},
		{ // empty b
			a: []tsm.BooleanValue{
				booleanValue(1, true),
				booleanValue(2, true),
			},

			b: []tsm.BooleanValue{},
			exp: []tsm.BooleanValue{
				booleanValue(1, true),
				booleanValue(2, true),
			},
		},
		{
			a: []tsm.BooleanValue{
				booleanValue(1, true),
			},
			b: []tsm.BooleanValue{
				booleanValue(0, false),
				booleanValue(1, false), // overwrites a
				booleanValue(2, false),
				booleanValue(3, false),
				booleanValue(4, false),
			},
			exp: []tsm.BooleanValue{
				booleanValue(0, false),
				booleanValue(1, false),
				booleanValue(2, false),
				booleanValue(3, false),
				booleanValue(4, false),
			},
		},
		{
			a: []tsm.BooleanValue{
				booleanValue(1, true),
				booleanValue(2, true),
				booleanValue(3, true),
				booleanValue(4, true),
			},

			b: []tsm.BooleanValue{
				booleanValue(1, false), // overwrites a
				booleanValue(2, false), // overwrites a
			},
			exp: []tsm.BooleanValue{
				booleanValue(1, false), // overwrites a
				booleanValue(2, false), // overwrites a
				booleanValue(3, true),
				booleanValue(4, true),
			},
		},
		{
			a: []tsm.BooleanValue{
				booleanValue(1, true),
				booleanValue(2, true),
				booleanValue(3, true),
				booleanValue(4, true),
			},

			b: []tsm.BooleanValue{
				booleanValue(1, false), // overwrites a
				booleanValue(2, false), // overwrites a
				booleanValue(3, false),
				booleanValue(4, false),
			},
			exp: []tsm.BooleanValue{
				booleanValue(1, false),
				booleanValue(2, false),
				booleanValue(3, false),
				booleanValue(4, false),
			},
		},
		{
			a: []tsm.BooleanValue{
				booleanValue(0, true),
				booleanValue(1, true),
				booleanValue(2, true),
				booleanValue(3, true),
				booleanValue(4, true),
			},
			b: []tsm.BooleanValue{
				booleanValue(0, false),
				booleanValue(2, false),
				booleanValue(4, false),
			},
			exp: []tsm.BooleanValue{
				booleanValue(0, false),
				booleanValue(1, true),
				booleanValue(2, false),
				booleanValue(3, true),
				booleanValue(4, false),
			},
		},
	}

	for i, test := range tests {
		got := tsm.BooleanValues(test.a).Merge(test.b)
		if exp, got := len(test.exp), len(got); exp != got {
			t.Fatalf("test(%d): value length mismatch: exp %v, got %v", i, exp, got)
		}

		dedup := tsm.BooleanValues(append(test.a, test.b...)).Deduplicate()

		for i := range test.exp {
			if exp, got := test.exp[i].String(), got[i].String(); exp != got {
				t.Fatalf("value mismatch:\n exp %v\n got %v", exp, got)
			}

			if exp, got := test.exp[i].String(), dedup[i].String(); exp != got {
				t.Fatalf("value mismatch:\n exp %v\n got %v", exp, got)
			}
		}
	}
}

func TestStringValues_Merge(t *testing.T) {
	stringValue := func(t int64, f string) tsm.StringValue {
		return tsm.NewValue(t, f).(tsm.StringValue)
	}

	tests := []struct {
		a, b, exp []tsm.StringValue
	}{

		{ // empty a
			a: []tsm.StringValue{},

			b: []tsm.StringValue{
				stringValue(1, "10"),
				stringValue(2, "20"),
			},
			exp: []tsm.StringValue{
				stringValue(1, "10"),
				stringValue(2, "20"),
			},
		},
		{ // empty b
			a: []tsm.StringValue{
				stringValue(1, "1"),
				stringValue(2, "2"),
			},

			b: []tsm.StringValue{},
			exp: []tsm.StringValue{
				stringValue(1, "1"),
				stringValue(2, "2"),
			},
		},
		{
			a: []tsm.StringValue{
				stringValue(1, "1"),
			},
			b: []tsm.StringValue{
				stringValue(0, "0"),
				stringValue(1, "10"), // overwrites a
				stringValue(2, "20"),
				stringValue(3, "30"),
				stringValue(4, "40"),
			},
			exp: []tsm.StringValue{
				stringValue(0, "0"),
				stringValue(1, "10"),
				stringValue(2, "20"),
				stringValue(3, "30"),
				stringValue(4, "40"),
			},
		},
		{
			a: []tsm.StringValue{
				stringValue(1, "1"),
				stringValue(2, "2"),
				stringValue(3, "3"),
				stringValue(4, "4"),
			},

			b: []tsm.StringValue{
				stringValue(1, "10"), // overwrites a
				stringValue(2, "20"), // overwrites a
			},
			exp: []tsm.StringValue{
				stringValue(1, "10"),
				stringValue(2, "20"),
				stringValue(3, "3"),
				stringValue(4, "4"),
			},
		},
		{
			a: []tsm.StringValue{
				stringValue(1, "1"),
				stringValue(2, "2"),
				stringValue(3, "3"),
				stringValue(4, "4"),
			},

			b: []tsm.StringValue{
				stringValue(1, "10"), // overwrites a
				stringValue(2, "20"), // overwrites a
				stringValue(3, "30"),
				stringValue(4, "40"),
			},
			exp: []tsm.StringValue{
				stringValue(1, "10"),
				stringValue(2, "20"),
				stringValue(3, "30"),
				stringValue(4, "40"),
			},
		},
		{
			a: []tsm.StringValue{
				stringValue(0, "0"),
				stringValue(1, "1"),
				stringValue(2, "2"),
				stringValue(3, "3"),
				stringValue(4, "4"),
			},
			b: []tsm.StringValue{
				stringValue(0, "0"),
				stringValue(2, "20"),
				stringValue(4, "40"),
			},
			exp: []tsm.StringValue{
				stringValue(0, "0.0"),
				stringValue(1, "1"),
				stringValue(2, "20"),
				stringValue(3, "3"),
				stringValue(4, "40"),
			},
		},
	}

	for i, test := range tests {
		if i != 2 {
			continue
		}

		got := tsm.StringValues(test.a).Merge(test.b)
		if exp, got := len(test.exp), len(got); exp != got {
			t.Fatalf("test(%d): value length mismatch: exp %v, got %v", i, exp, got)
		}

		dedup := tsm.StringValues(append(test.a, test.b...)).Deduplicate()

		for i := range test.exp {
			if exp, got := test.exp[i].String(), got[i].String(); exp != got {
				t.Fatalf("value mismatch:\n exp %v\n got %v", exp, got)
			}

			if exp, got := test.exp[i].String(), dedup[i].String(); exp != got {
				t.Fatalf("value mismatch:\n exp %v\n got %v", exp, got)
			}
		}
	}
}
func getTimes(n, step int, precision time.Duration) []int64 {
	t := time.Now().Round(precision).UnixNano()
	a := make([]int64, n)
	for i := 0; i < n; i++ {
		a[i] = t + (time.Duration(i*60) * precision).Nanoseconds()
	}
	return a
}

func BenchmarkDecodeBlock_Float_Empty(b *testing.B) {
	valueCount := 1000
	times := getTimes(valueCount, 60, time.Second)
	values := make([]tsm.Value, len(times))
	for i, t := range times {
		values[i] = tsm.NewValue(t, float64(i))
	}

	bytes, err := tsm.Values(values).Encode(nil)
	if err != nil {
		b.Fatalf("unexpected error: %v", err)
	}

	var decodedValues []tsm.Value
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err = tsm.DecodeBlock(bytes, decodedValues)
		if err != nil {
			b.Fatalf("unexpected error decoding block: %v", err)
		}
	}
}

func BenchmarkDecodeBlock_Float_EqualSize(b *testing.B) {
	valueCount := 1000
	times := getTimes(valueCount, 60, time.Second)
	values := make([]tsm.Value, len(times))
	for i, t := range times {
		values[i] = tsm.NewValue(t, float64(i))
	}

	bytes, err := tsm.Values(values).Encode(nil)
	if err != nil {
		b.Fatalf("unexpected error: %v", err)
	}

	decodedValues := make([]tsm.Value, len(values))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err = tsm.DecodeBlock(bytes, decodedValues)
		if err != nil {
			b.Fatalf("unexpected error decoding block: %v", err)
		}
	}
}

func BenchmarkDecodeBlock_Float_TypeSpecific(b *testing.B) {
	valueCount := 1000
	times := getTimes(valueCount, 60, time.Second)
	values := make([]tsm.Value, len(times))
	for i, t := range times {
		values[i] = tsm.NewValue(t, float64(i))
	}

	bytes, err := tsm.Values(values).Encode(nil)
	if err != nil {
		b.Fatalf("unexpected error: %v", err)
	}

	decodedValues := make([]tsm.FloatValue, len(values))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err = tsm.DecodeFloatBlock(bytes, &decodedValues)
		if err != nil {
			b.Fatalf("unexpected error decoding block: %v", err)
		}
	}
}

func BenchmarkDecodeBlock_Integer_Empty(b *testing.B) {
	valueCount := 1000
	times := getTimes(valueCount, 60, time.Second)
	values := make([]tsm.Value, len(times))
	for i, t := range times {
		values[i] = tsm.NewValue(t, int64(i))
	}

	bytes, err := tsm.Values(values).Encode(nil)
	if err != nil {
		b.Fatalf("unexpected error: %v", err)
	}

	var decodedValues []tsm.Value
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err = tsm.DecodeBlock(bytes, decodedValues)
		if err != nil {
			b.Fatalf("unexpected error decoding block: %v", err)
		}
	}
}

func BenchmarkDecodeBlock_Integer_EqualSize(b *testing.B) {
	valueCount := 1000
	times := getTimes(valueCount, 60, time.Second)
	values := make([]tsm.Value, len(times))
	for i, t := range times {
		values[i] = tsm.NewValue(t, int64(i))
	}

	bytes, err := tsm.Values(values).Encode(nil)
	if err != nil {
		b.Fatalf("unexpected error: %v", err)
	}

	decodedValues := make([]tsm.Value, len(values))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err = tsm.DecodeBlock(bytes, decodedValues)
		if err != nil {
			b.Fatalf("unexpected error decoding block: %v", err)
		}
	}
}

func BenchmarkDecodeBlock_Integer_TypeSpecific(b *testing.B) {
	valueCount := 1000
	times := getTimes(valueCount, 60, time.Second)
	values := make([]tsm.Value, len(times))
	for i, t := range times {
		values[i] = tsm.NewValue(t, int64(i))
	}

	bytes, err := tsm.Values(values).Encode(nil)
	if err != nil {
		b.Fatalf("unexpected error: %v", err)
	}

	decodedValues := make([]tsm.IntegerValue, len(values))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err = tsm.DecodeIntegerBlock(bytes, &decodedValues)
		if err != nil {
			b.Fatalf("unexpected error decoding block: %v", err)
		}
	}
}

func BenchmarkDecodeBlock_Boolean_Empty(b *testing.B) {
	valueCount := 1000
	times := getTimes(valueCount, 60, time.Second)
	values := make([]tsm.Value, len(times))
	for i, t := range times {
		values[i] = tsm.NewValue(t, true)
	}

	bytes, err := tsm.Values(values).Encode(nil)
	if err != nil {
		b.Fatalf("unexpected error: %v", err)
	}

	var decodedValues []tsm.Value
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err = tsm.DecodeBlock(bytes, decodedValues)
		if err != nil {
			b.Fatalf("unexpected error decoding block: %v", err)
		}
	}
}

func BenchmarkDecodeBlock_Boolean_EqualSize(b *testing.B) {
	valueCount := 1000
	times := getTimes(valueCount, 60, time.Second)
	values := make([]tsm.Value, len(times))
	for i, t := range times {
		values[i] = tsm.NewValue(t, true)
	}

	bytes, err := tsm.Values(values).Encode(nil)
	if err != nil {
		b.Fatalf("unexpected error: %v", err)
	}

	decodedValues := make([]tsm.Value, len(values))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err = tsm.DecodeBlock(bytes, decodedValues)
		if err != nil {
			b.Fatalf("unexpected error decoding block: %v", err)
		}
	}
}

func BenchmarkDecodeBlock_Boolean_TypeSpecific(b *testing.B) {
	valueCount := 1000
	times := getTimes(valueCount, 60, time.Second)
	values := make([]tsm.Value, len(times))
	for i, t := range times {
		values[i] = tsm.NewValue(t, true)
	}

	bytes, err := tsm.Values(values).Encode(nil)
	if err != nil {
		b.Fatalf("unexpected error: %v", err)
	}

	decodedValues := make([]tsm.BooleanValue, len(values))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err = tsm.DecodeBooleanBlock(bytes, &decodedValues)
		if err != nil {
			b.Fatalf("unexpected error decoding block: %v", err)
		}
	}
}

func BenchmarkDecodeBooleanBlock(b *testing.B) {
	cases := []int{
		5,
		55,
		555,
		1000,
	}
	for _, n := range cases {
		b.Run(fmt.Sprintf("%d", n), func(b *testing.B) {
			valueCount := n
			times := getTimes(valueCount, 60, time.Second)
			values := make([]tsm.Value, len(times))
			for i, t := range times {
				values[i] = tsm.NewValue(t, true)
			}

			bytes, err := tsm.Values(values).Encode(nil)
			if err != nil {
				b.Fatalf("unexpected error: %v", err)
			}

			b.ResetTimer()
			b.ReportAllocs()
			b.SetBytes(int64(tsm.Values(values).Size()))

			b.RunParallel(func(pb *testing.PB) {
				decodedValues := make([]tsm.BooleanValue, len(values))

				for pb.Next() {
					_, err = tsm.DecodeBooleanBlock(bytes, &decodedValues)
					if err != nil {
						b.Fatalf("unexpected error decoding block: %v", err)
					}
				}
			})
		})
	}
}

func BenchmarkDecodeFloatBlock(b *testing.B) {
	cases := []int{
		5,
		55,
		555,
		1000,
	}
	for _, n := range cases {
		b.Run(fmt.Sprintf("%d", n), func(b *testing.B) {
			valueCount := n
			times := getTimes(valueCount, 60, time.Second)
			values := make([]tsm.Value, len(times))
			for i, t := range times {
				values[i] = tsm.NewValue(t, float64(i))
			}

			bytes, err := tsm.Values(values).Encode(nil)
			if err != nil {
				b.Fatalf("unexpected error: %v", err)
			}

			b.ResetTimer()
			b.ReportAllocs()
			b.SetBytes(int64(tsm.Values(values).Size()))

			b.RunParallel(func(pb *testing.PB) {
				decodedValues := make([]tsm.FloatValue, len(values))

				for pb.Next() {
					_, err = tsm.DecodeFloatBlock(bytes, &decodedValues)
					if err != nil {
						b.Fatalf("unexpected error decoding block: %v", err)
					}
				}
			})
		})
	}
}

func BenchmarkDecodeIntegerBlock(b *testing.B) {
	rle := func(_ *rand.Rand, i int) int64 { return int64(i) }
	s8b := func(r *rand.Rand, i int) int64 { return int64(i + int(r.Int31n(10))) }

	cases := []struct {
		enc string
		gen func(r *rand.Rand, i int) int64
		n   int
	}{
		{enc: "rle", gen: rle, n: 5},
		{enc: "rle", gen: rle, n: 55},
		{enc: "rle", gen: rle, n: 555},
		{enc: "rle", gen: rle, n: 1000},
		{enc: "s8b", gen: s8b, n: 5},
		{enc: "s8b", gen: s8b, n: 55},
		{enc: "s8b", gen: s8b, n: 555},
		{enc: "s8b", gen: s8b, n: 1000},
	}
	for _, bm := range cases {
		b.Run(fmt.Sprintf("%s_%d", bm.enc, bm.n), func(b *testing.B) {
			seededRand := rand.New(rand.NewSource(int64(bm.n * 1e3)))

			valueCount := bm.n
			times := getTimes(valueCount, 60, time.Second)
			values := make([]tsm.Value, len(times))
			for i, t := range times {
				values[i] = tsm.NewValue(t, bm.gen(seededRand, i))
			}

			bytes, err := tsm.Values(values).Encode(nil)
			if err != nil {
				b.Fatalf("unexpected error: %v", err)
			}

			b.ResetTimer()
			b.ReportAllocs()
			b.SetBytes(int64(tsm.Values(values).Size()))

			b.RunParallel(func(pb *testing.PB) {
				decodedValues := make([]tsm.IntegerValue, len(values))

				for pb.Next() {
					_, err = tsm.DecodeIntegerBlock(bytes, &decodedValues)
					if err != nil {
						b.Fatalf("unexpected error decoding block: %v", err)
					}
				}
			})
		})
	}
}

func BenchmarkDecodeStringBlock(b *testing.B) {
	cases := []int{
		5,
		55,
		555,
		1000,
	}
	for _, n := range cases {
		b.Run(fmt.Sprintf("%d", n), func(b *testing.B) {
			valueCount := n
			times := getTimes(valueCount, 60, time.Second)
			values := make([]tsm.Value, len(times))
			for i, t := range times {
				values[i] = tsm.NewValue(t, fmt.Sprintf("value %d", i))
			}

			bytes, err := tsm.Values(values).Encode(nil)
			if err != nil {
				b.Fatalf("unexpected error: %v", err)
			}

			b.ResetTimer()
			b.ReportAllocs()
			b.SetBytes(int64(tsm.Values(values).Size()))

			b.RunParallel(func(pb *testing.PB) {
				decodedValues := make([]tsm.StringValue, len(values))

				for pb.Next() {
					_, err = tsm.DecodeStringBlock(bytes, &decodedValues)
					if err != nil {
						b.Fatalf("unexpected error decoding block: %v", err)
					}
				}
			})
		})
	}
}

func BenchmarkDecodeBlock_String_Empty(b *testing.B) {
	valueCount := 1000
	times := getTimes(valueCount, 60, time.Second)
	values := make([]tsm.Value, len(times))
	for i, t := range times {
		values[i] = tsm.NewValue(t, fmt.Sprintf("value %d", i))
	}

	bytes, err := tsm.Values(values).Encode(nil)
	if err != nil {
		b.Fatalf("unexpected error: %v", err)
	}

	var decodedValues []tsm.Value
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err = tsm.DecodeBlock(bytes, decodedValues)
		if err != nil {
			b.Fatalf("unexpected error decoding block: %v", err)
		}
	}
}

func BenchmarkDecodeBlock_String_EqualSize(b *testing.B) {
	valueCount := 1000
	times := getTimes(valueCount, 60, time.Second)
	values := make([]tsm.Value, len(times))
	for i, t := range times {
		values[i] = tsm.NewValue(t, fmt.Sprintf("value %d", i))
	}

	bytes, err := tsm.Values(values).Encode(nil)
	if err != nil {
		b.Fatalf("unexpected error: %v", err)
	}

	decodedValues := make([]tsm.Value, len(values))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err = tsm.DecodeBlock(bytes, decodedValues)
		if err != nil {
			b.Fatalf("unexpected error decoding block: %v", err)
		}
	}
}

func BenchmarkDecodeBlock_String_TypeSpecific(b *testing.B) {
	valueCount := 1000
	times := getTimes(valueCount, 60, time.Second)
	values := make([]tsm.Value, len(times))
	for i, t := range times {
		values[i] = tsm.NewValue(t, fmt.Sprintf("value %d", i))
	}

	bytes, err := tsm.Values(values).Encode(nil)
	if err != nil {
		b.Fatalf("unexpected error: %v", err)
	}

	decodedValues := make([]tsm.StringValue, len(values))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err = tsm.DecodeStringBlock(bytes, &decodedValues)
		if err != nil {
			b.Fatalf("unexpected error decoding block: %v", err)
		}
	}
}

func BenchmarkValues_Deduplicate(b *testing.B) {
	valueCount := 1000
	times := getTimes(valueCount, 60, time.Second)
	values := make([]tsm.Value, len(times))
	for i, t := range times {
		values[i] = tsm.NewValue(t, fmt.Sprintf("value %d", i))
	}
	values = append(values, values...)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tsm.Values(values).Deduplicate()
	}
}

func BenchmarkValues_Merge(b *testing.B) {
	valueCount := 1000
	times := getTimes(valueCount, 60, time.Second)
	a := make([]tsm.Value, len(times))
	c := make([]tsm.Value, len(times))

	for i, t := range times {
		a[i] = tsm.NewValue(t, float64(i))
		c[i] = tsm.NewValue(t+1, float64(i))
	}

	b.ResetTimer()
	benchmarkMerge(a, c, b)
}

func BenchmarkValues_MergeDisjoint(b *testing.B) {
	valueCount := 1000
	times := getTimes(valueCount, 60, time.Second)
	a := make([]tsm.Value, len(times))
	c := make([]tsm.Value, len(times))

	for i, t := range times {
		a[i] = tsm.NewValue(t, float64(i))
		c[i] = tsm.NewValue(times[len(times)-1]+int64((i+1)*1e9), float64(i))
	}

	b.ResetTimer()
	benchmarkMerge(a, c, b)
}

func BenchmarkValues_MergeSame(b *testing.B) {
	valueCount := 1000
	times := getTimes(valueCount, 60, time.Second)
	a := make([]tsm.Value, len(times))
	c := make([]tsm.Value, len(times))

	for i, t := range times {
		a[i] = tsm.NewValue(t, float64(i))
		c[i] = tsm.NewValue(t, float64(i))
	}

	b.ResetTimer()
	benchmarkMerge(a, c, b)
}

func BenchmarkValues_MergeSimilar(b *testing.B) {
	valueCount := 1000
	times := getTimes(valueCount, 60, time.Second)
	a := make([]tsm.Value, len(times))
	c := make([]tsm.Value, len(times))

	for i, t := range times {
		a[i] = tsm.NewValue(t, float64(i))
		if i == 0 {
			t++
		}
		c[i] = tsm.NewValue(t, float64(i))
	}

	b.ResetTimer()
	benchmarkMerge(a, c, b)
}

func BenchmarkValues_MergeUnevenA(b *testing.B) {
	valueCount := 1000
	times := getTimes(valueCount, 60, time.Second)
	a := make([]tsm.Value, len(times))
	c := make([]tsm.Value, len(times))

	for i, t := range times {
		a[i] = tsm.NewValue(t, float64(i))
		c[i] = tsm.NewValue(t, float64(i))
	}

	b.ResetTimer()
	benchmarkMerge(a[:700], c[:10], b)
}

func BenchmarkValues_MergeUnevenB(b *testing.B) {
	valueCount := 1000
	times := getTimes(valueCount, 60, time.Second)
	a := make([]tsm.Value, len(times))
	c := make([]tsm.Value, len(times))

	for i, t := range times {
		a[i] = tsm.NewValue(t, float64(i))
		c[i] = tsm.NewValue(t, float64(i))
	}

	b.ResetTimer()
	benchmarkMerge(a[:10], c[:700], b)
}

func benchmarkMerge(a, c tsm.Values, b *testing.B) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		aa := make(tsm.Values, len(a))
		copy(aa, a)
		cc := make(tsm.Values, len(c))
		copy(cc, c)
		b.StartTimer()
		tsm.Values(aa).Merge(tsm.Values(cc))
	}
}

func BenchmarkValues_EncodeInteger(b *testing.B) {
	valueCount := 1024
	times := getTimes(valueCount, 60, time.Second)
	a := make([]tsm.Value, len(times))

	for i, t := range times {
		a[i] = tsm.NewValue(t, int64(i))
	}

	buf := make([]byte, 1024*8)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tsm.Values(a).Encode(buf)
	}
}

func BenchmarkValues_EncodeFloat(b *testing.B) {
	valueCount := 1024
	times := getTimes(valueCount, 60, time.Second)
	a := make([]tsm.Value, len(times))

	for i, t := range times {
		a[i] = tsm.NewValue(t, float64(i))
	}

	buf := make([]byte, 1024*8)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tsm.Values(a).Encode(buf)
	}
}
func BenchmarkValues_EncodeString(b *testing.B) {
	valueCount := 1024
	times := getTimes(valueCount, 60, time.Second)
	a := make([]tsm.Value, len(times))

	for i, t := range times {
		a[i] = tsm.NewValue(t, fmt.Sprintf("%d", i))
	}

	buf := make([]byte, 1024*8)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tsm.Values(a).Encode(buf)
	}
}
func BenchmarkValues_EncodeBool(b *testing.B) {
	valueCount := 1024
	times := getTimes(valueCount, 60, time.Second)
	a := make([]tsm.Value, len(times))

	for i, t := range times {
		if i%2 == 0 {
			a[i] = tsm.NewValue(t, true)
		} else {
			a[i] = tsm.NewValue(t, false)
		}
	}

	buf := make([]byte, 1024*8)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tsm.Values(a).Encode(buf)
	}
}
