package tsm_test

import (
	"fmt"
	"math"
	"reflect"
	"testing"
	"testing/quick"

	"cycledb/pkg/tsdb/engine/tsm"

	"github.com/google/go-cmp/cmp"
)

func TestFloatEncoder_Simple(t *testing.T) {
	// Example from the paper
	s := tsm.NewFloatEncoder()

	s.Write(12)
	s.Write(12)
	s.Write(24)

	// extra tests

	// floating point masking/shifting bug
	s.Write(13)
	s.Write(24)

	// delta-of-delta sizes
	s.Write(24)
	s.Write(24)
	s.Write(24)

	s.Flush()

	b, err := s.Bytes()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var it tsm.FloatDecoder
	if err := it.SetBytes(b); err != nil {
		t.Fatalf("unexpected error creating float decoder: %v", err)
	}

	want := []float64{
		12,
		12,
		24,

		13,
		24,

		24,
		24,
		24,
	}

	for _, w := range want {
		if !it.Next() {
			t.Fatalf("Next()=false, want true")
		}
		vv := it.Values()
		if w != vv {
			t.Errorf("Values()=(%v), want (%v)\n", vv, w)
		}
	}

	if it.Next() {
		t.Fatalf("Next()=true, want false")
	}

	if err := it.Error(); err != nil {
		t.Errorf("it.Error()=%v, want nil", err)
	}
}

func TestFloatEncoder_SimilarFloats(t *testing.T) {
	s := tsm.NewFloatEncoder()
	want := []float64{
		6.00065e+06,
		6.000656e+06,
		6.000657e+06,

		6.000659e+06,
		6.000661e+06,
	}

	for _, v := range want {
		s.Write(v)
	}

	s.Flush()

	b, err := s.Bytes()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var it tsm.FloatDecoder
	if err := it.SetBytes(b); err != nil {
		t.Fatalf("unexpected error creating float decoder: %v", err)
	}

	for _, w := range want {
		if !it.Next() {
			t.Fatalf("Next()=false, want true")
		}
		vv := it.Values()
		if w != vv {
			t.Errorf("Values()=(%v), want (%v)\n", vv, w)
		}
	}

	if it.Next() {
		t.Fatalf("Next()=true, want false")
	}

	if err := it.Error(); err != nil {
		t.Errorf("it.Error()=%v, want nil", err)
	}
}

var twoHoursData = []float64{
	// 2h of data, rows of 10 values
	761, 727, 763, 706, 700, 679, 757, 708, 739, 707,
	699, 740, 729, 766, 730, 715, 705, 693, 765, 724,
	799, 761, 737, 766, 756, 719, 722, 801, 747, 731,
	742, 744, 791, 750, 759, 809, 751, 705, 770, 792,
	727, 762, 772, 721, 748, 753, 744, 716, 776, 659,
	789, 766, 758, 690, 795, 770, 758, 723, 767, 765,
	693, 706, 681, 727, 724, 780, 678, 696, 758, 740,
	735, 700, 742, 747, 752, 734, 743, 732, 746, 770,
	780, 710, 731, 712, 712, 741, 770, 770, 754, 718,
	670, 775, 749, 795, 756, 741, 787, 721, 745, 782,
	765, 780, 811, 790, 836, 743, 858, 739, 762, 770,
	752, 763, 795, 792, 746, 786, 785, 774, 786, 718,
}

func TestFloatEncoder_Roundtrip(t *testing.T) {
	s := tsm.NewFloatEncoder()
	for _, p := range twoHoursData {
		s.Write(p)
	}
	s.Flush()

	b, err := s.Bytes()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var it tsm.FloatDecoder
	if err := it.SetBytes(b); err != nil {
		t.Fatalf("unexpected error creating float decoder: %v", err)
	}

	for _, w := range twoHoursData {
		if !it.Next() {
			t.Fatalf("Next()=false, want true")
		}
		vv := it.Values()
		// t.Logf("it.Values()=(%+v, %+v)\n", time.Unix(int64(tt), 0), vv)
		if w != vv {
			t.Errorf("Values()=(%v), want (%v)\n", vv, w)
		}
	}

	if it.Next() {
		t.Fatalf("Next()=true, want false")
	}

	if err := it.Error(); err != nil {
		t.Errorf("it.Error()=%v, want nil", err)
	}
}

func TestFloatEncoder_Roundtrip_NaN(t *testing.T) {
	s := tsm.NewFloatEncoder()
	s.Write(1.0)
	s.Write(math.NaN())
	s.Write(2.0)
	s.Flush()

	_, err := s.Bytes()
	if err == nil {
		t.Fatalf("expected error. got nil")
	}
}

func TestFloatEncoder_Empty(t *testing.T) {
	s := tsm.NewFloatEncoder()
	s.Flush()

	b, err := s.Bytes()
	if err != nil {
		t.Fatal(err)
	}

	var dec tsm.FloatDecoder
	if err := dec.SetBytes(b); err != nil {
		t.Fatal(err)
	}

	var got []float64
	for dec.Next() {
		got = append(got, dec.Values())
	}

	if len(got) != 0 {
		t.Fatalf("got len %d, expected 0", len(got))
	}
}

func Test_FloatEncoder_Quick(t *testing.T) {
	quick.Check(func(values []float64) bool {
		if values == nil {
			values = []float64{}
		}

		// Write values to encoder.
		enc := tsm.NewFloatEncoder()
		for _, v := range values {
			enc.Write(v)
		}
		enc.Flush()

		// Read values out of decoder.
		got := make([]float64, 0, len(values))
		b, err := enc.Bytes()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		var dec tsm.FloatDecoder
		if err := dec.SetBytes(b); err != nil {
			t.Fatal(err)
		}
		for dec.Next() {
			got = append(got, dec.Values())
		}

		// Verify that input and output values match.
		if !reflect.DeepEqual(values, got) {
			t.Fatalf("mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", values, got)
		}

		return true
	}, nil)
}

func TestFloatDecoder_Empty(t *testing.T) {
	var dec tsm.FloatDecoder
	if err := dec.SetBytes([]byte{}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if dec.Next() {
		t.Fatalf("exp next == false, got true")
	}
}

func BenchmarkFloatEncoder(b *testing.B) {
	for i := 0; i < b.N; i++ {
		s := tsm.NewFloatEncoder()
		for _, tt := range twoHoursData {
			s.Write(tt)
		}
		s.Flush()
	}
}

func BenchmarkFloatDecoder(b *testing.B) {
	s := tsm.NewFloatEncoder()
	for _, tt := range twoHoursData {
		s.Write(tt)
	}
	s.Flush()
	bytes, err := s.Bytes()
	if err != nil {
		b.Fatalf("unexpected error: %v", err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		var it tsm.FloatDecoder
		if err := it.SetBytes(bytes); err != nil {
			b.Fatalf("unexpected error creating float decoder: %v", err)
		}

		for j := 0; j < len(twoHoursData); it.Next() {
			j++
		}
	}
}

func BenchmarkFloatDecoder_DecodeAll(b *testing.B) {
	benchmarks := []int{
		1,
		55,
		550,
		1000,
	}
	for _, size := range benchmarks {
		s := tsm.NewFloatEncoder()
		for c := 0; c < size; c++ {
			s.Write(twoHoursData[c%len(twoHoursData)])
		}
		s.Flush()
		bytes, err := s.Bytes()
		if err != nil {
			b.Fatalf("unexpected error: %v", err)
		}

		b.Run(fmt.Sprintf("%d", size), func(b *testing.B) {
			b.SetBytes(int64(len(bytes)))

			dst := make([]float64, size)
			for i := 0; i < b.N; i++ {
				var it tsm.FloatDecoder
				if err := it.SetBytes(bytes); err != nil {
					b.Fatalf("unexpected error creating float decoder: %v", err)
				}

				i := 0
				for it.Next() {
					dst[i] = it.Values()
					i++
				}

				if len(dst) != size {
					b.Fatalf("unexpected length -got/+exp\n%s", cmp.Diff(len(dst), size))
				}
			}
		})
	}
}
