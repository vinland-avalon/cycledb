package tsm_test

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"cycledb/pkg/tsdb"
	"cycledb/pkg/tsdb/engine/tsm"

	"github.com/google/go-cmp/cmp"
)

func TestDecodeFloatArrayBlock(t *testing.T) {
	valueCount := 1000
	times := getTimes(valueCount, 60, time.Second)
	values := make(tsm.FloatValues, len(times))
	for i, t := range times {
		values[i] = tsm.NewFloatValue(t, float64(i)).(tsm.FloatValue)
	}
	exp := tsm.NewFloatArrayFromValues(values)

	b, err := values.Encode(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	got := tsdb.NewFloatArrayLen(exp.Len())
	tsm.DecodeFloatArrayBlock(b, got)
	if !cmp.Equal(got, exp) {
		t.Fatalf("unexpected values -got/+exp\n%s", cmp.Diff(got, exp))
	}
}

func BenchmarkDecodeBooleanArrayBlock(b *testing.B) {
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
				decodedValues := tsdb.NewBooleanArrayLen(len(values))

				for pb.Next() {
					err = tsm.DecodeBooleanArrayBlock(bytes, decodedValues)
					if err != nil {
						b.Fatalf("unexpected error decoding block: %v", err)
					}
				}
			})
		})
	}
}

func BenchmarkDecodeFloatArrayBlock(b *testing.B) {
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
				decodedValues := tsdb.NewFloatArrayLen(len(values))

				for pb.Next() {
					err = tsm.DecodeFloatArrayBlock(bytes, decodedValues)
					if err != nil {
						b.Fatalf("unexpected error decoding block: %v", err)
					}
				}
			})
		})
	}
}

func BenchmarkDecodeIntegerArrayBlock(b *testing.B) {
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
				decodedValues := tsdb.NewIntegerArrayLen(len(values))

				for pb.Next() {
					err = tsm.DecodeIntegerArrayBlock(bytes, decodedValues)
					if err != nil {
						b.Fatalf("unexpected error decoding block: %v", err)
					}
				}
			})
		})
	}
}

func BenchmarkDecodeStringArrayBlock(b *testing.B) {
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
				decodedValues := tsdb.NewStringArrayLen(len(values))

				for pb.Next() {
					err = tsm.DecodeStringArrayBlock(bytes, decodedValues)
					if err != nil {
						b.Fatalf("unexpected error decoding block: %v", err)
					}
				}
			})
		})
	}
}
