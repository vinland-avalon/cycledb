package tsm1

import (
	"bytes"
	"fmt"
	"math/rand"
	"reflect"
	"strings"
	"testing"
	"testing/quick"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb/v2/uuid"
)

func equalError(a, b error) bool {
	return a == nil && b == nil || a != nil && b != nil && a.Error() == b.Error()
}

func TestStringArrayEncodeAll_NoValues(t *testing.T) {
	b, err := StringArrayEncodeAll(nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var dec StringDecoder
	if err := dec.SetBytes(b); err != nil {
		t.Fatalf("unexpected error creating string decoder: %v", err)
	}
	if dec.Next() {
		t.Fatalf("unexpected next value: got true, exp false")
	}
}

func TestStringArrayEncodeAll_ExceedsMaxEncodedLen(t *testing.T) {
	str := strings.Repeat(" ", 1<<23) // 8MB string
	var s []string
	for i := 0; i < 512; i++ {
		s = append(s, str)
	}

	_, got := StringArrayEncodeAll(s, nil)
	if !cmp.Equal(got, ErrStringArrayEncodeTooLarge, cmp.Comparer(equalError)) {
		t.Fatalf("expected error, got: %v", got)
	}
}

func TestStringArrayEncodeAll_Single(t *testing.T) {
	src := []string{"v1"}
	b, err := StringArrayEncodeAll(src, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var dec StringDecoder
	if dec.SetBytes(b); err != nil {
		t.Fatalf("unexpected error creating string decoder: %v", err)
	}
	if !dec.Next() {
		t.Fatalf("unexpected next value: got false, exp true")
	}

	if src[0] != dec.Read() {
		t.Fatalf("unexpected value: got %v, exp %v", dec.Read(), src[0])
	}
}

func TestStringArrayEncode_Compare(t *testing.T) {
	// generate random values
	input := make([]string, 1000)
	for i := 0; i < len(input); i++ {
		input[i] = uuid.TimeUUID().String()
	}

	// Example from the paper
	s := NewStringEncoder(1000)
	for _, v := range input {
		s.Write(v)
	}
	s.Flush()

	buf1, err := s.Bytes()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	buf2 := append([]byte("this is some jibberish"), make([]byte, 100, 200)...)
	buf2, err = StringArrayEncodeAll(input, buf2)
	if err != nil {
		t.Fatalf("unexpected error: %v\nbuf: %db %x", err, len(buf2), buf2)
	}

	result, err := StringArrayDecodeAll(buf2, nil)
	if err != nil {
		dumpBufs(buf1, buf2)
		t.Fatalf("unexpected error: %v\nbuf: %db %x", err, len(buf2), buf2)
	}

	if got, exp := result, input; !reflect.DeepEqual(got, exp) {
		t.Fatalf("got result %v, expected %v", got, exp)
	}

	// Check that the encoders are byte for byte the same...
	if !bytes.Equal(buf1, buf2) {
		dumpBufs(buf1, buf2)
		t.Fatalf("Raw bytes differ for encoders")
	}
}

func TestStringArrayEncodeAll_Multi_Compressed(t *testing.T) {
	src := make([]string, 10)
	for i := range src {
		src[i] = fmt.Sprintf("value %d", i)
	}

	b, err := StringArrayEncodeAll(src, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if b[0]>>4 != stringCompressedSnappy {
		t.Fatalf("unexpected encoding: got %v, exp %v", b[0], stringCompressedSnappy)
	}

	if exp := 51; len(b) != exp {
		t.Fatalf("unexpected length: got %v, exp %v", len(b), exp)
	}

	var dec StringDecoder
	if err := dec.SetBytes(b); err != nil {
		t.Fatalf("unexpected erorr creating string decoder: %v", err)
	}

	for i, v := range src {
		if !dec.Next() {
			t.Fatalf("unexpected next value: got false, exp true")
		}
		if v != dec.Read() {
			t.Fatalf("unexpected value at pos %d: got %v, exp %v", i, dec.Read(), v)
		}
	}

	if dec.Next() {
		t.Fatalf("unexpected next value: got true, exp false")
	}
}

func TestStringArrayEncodeAll_Quick(t *testing.T) {
	var base []byte
	quick.Check(func(values []string) bool {
		src := values
		if values == nil {
			src = []string{}
		}

		// Retrieve encoded bytes from encoder.
		buf, err := StringArrayEncodeAll(src, base)
		if err != nil {
			t.Fatal(err)
		}

		// Read values out of decoder.
		got := make([]string, 0, len(src))
		var dec StringDecoder
		if err := dec.SetBytes(buf); err != nil {
			t.Fatal(err)
		}
		for dec.Next() {
			if err := dec.Error(); err != nil {
				t.Fatal(err)
			}
			got = append(got, dec.Read())
		}

		// Verify that input and output values match.
		if !reflect.DeepEqual(src, got) {
			t.Fatalf("mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", src, got)
		}

		return true
	}, nil)
}

func TestStringArrayDecodeAll_NoValues(t *testing.T) {
	enc := NewStringEncoder(1024)
	b, err := enc.Bytes()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	got, err := StringArrayDecodeAll(b, nil)
	if err != nil {
		t.Fatalf("unexpected error creating string decoder: %v", err)
	}

	exp := []string{}
	if !cmp.Equal(got, exp) {
		t.Fatalf("unexpected value: -got/+exp\n%s", cmp.Diff(got, exp))
	}
}

func TestStringArrayDecodeAll_Single(t *testing.T) {
	enc := NewStringEncoder(1024)
	v1 := "v1"
	enc.Write(v1)
	b, err := enc.Bytes()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	got, err := StringArrayDecodeAll(b, nil)
	if err != nil {
		t.Fatalf("unexpected error creating string decoder: %v", err)
	}

	exp := []string{"v1"}
	if !cmp.Equal(got, exp) {
		t.Fatalf("unexpected value: -got/+exp\n%s", cmp.Diff(got, exp))
	}
}

func TestStringArrayDecodeAll_Multi_Compressed(t *testing.T) {
	enc := NewStringEncoder(1024)

	exp := make([]string, 10)
	for i := range exp {
		exp[i] = fmt.Sprintf("value %d", i)
		enc.Write(exp[i])
	}

	b, err := enc.Bytes()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if b[0]>>4 != stringCompressedSnappy {
		t.Fatalf("unexpected encoding: got %v, exp %v", b[0], stringCompressedSnappy)
	}

	if exp := 51; len(b) != exp {
		t.Fatalf("unexpected length: got %v, exp %v", len(b), exp)
	}

	got, err := StringArrayDecodeAll(b, nil)
	if err != nil {
		t.Fatalf("unexpected error creating string decoder: %v", err)
	}

	if !cmp.Equal(got, exp) {
		t.Fatalf("unexpected value: -got/+exp\n%s", cmp.Diff(got, exp))
	}
}

func TestStringArrayDecodeAll_Quick(t *testing.T) {
	quick.Check(func(values []string) bool {
		if values == nil {
			values = []string{}
		}
		// Write values to encoder.
		enc := NewStringEncoder(1024)
		for _, v := range values {
			enc.Write(v)
		}

		// Retrieve encoded bytes from encoder.
		buf, err := enc.Bytes()
		if err != nil {
			t.Fatal(err)
		}

		// Read values out of decoder.
		got, err := StringArrayDecodeAll(buf, nil)
		if err != nil {
			t.Fatalf("unexpected error creating string decoder: %v", err)
		}

		if !cmp.Equal(got, values) {
			t.Fatalf("unexpected value: -got/+exp\n%s", cmp.Diff(got, values))
		}

		return true
	}, nil)
}

func TestStringArrayDecodeAll_Empty(t *testing.T) {
	got, err := StringArrayDecodeAll([]byte{}, nil)
	if err != nil {
		t.Fatalf("unexpected error creating string decoder: %v", err)
	}

	exp := []string{}
	if !cmp.Equal(got, exp) {
		t.Fatalf("unexpected value: -got/+exp\n%s", cmp.Diff(got, exp))
	}
}

func TestStringArrayDecodeAll_CorruptBytes(t *testing.T) {
	cases := []string{
		"\x10\x03\b\x03Hi", // Higher length than actual data
		"\x10\x1dp\x9c\x90\x90\x90\x90\x90\x90\x90\x90\x90length overflow----",
		"0t\x00\x01\x000\x00\x01\x000\x00\x01\x000\x00\x01\x000\x00\x01" +
			"\x000\x00\x01\x000\x00\x01\x000\x00\x00\x00\xff:\x01\x00\x01\x00\x01" +
			"\x00\x01\x00\x01\x00\x01\x00\x010\x010\x000\x010\x010\x010\x01" +
			"0\x010\x010\x010\x010\x010\x010\x010\x010\x010\x010", // Upper slice bounds overflows negative
	}

	for _, c := range cases {
		t.Run(fmt.Sprintf("%q", c), func(t *testing.T) {
			got, err := StringArrayDecodeAll([]byte(c), nil)
			if err == nil {
				t.Fatal("exp an err, got nil")
			}

			exp := []string{}
			if !cmp.Equal(got, exp) {
				t.Fatalf("unexpected value: -got/+exp\n%s", cmp.Diff(got, exp))
			}
		})
	}
}

func BenchmarkEncodeStrings(b *testing.B) {
	var err error
	cases := []int{10, 100, 1000}

	for _, n := range cases {
		enc := NewStringEncoder(n)
		b.Run(fmt.Sprintf("%d", n), func(b *testing.B) {
			input := make([]string, n)
			for i := 0; i < n; i++ {
				input[i] = uuid.TimeUUID().String()
			}

			b.Run("itr", func(b *testing.B) {
				b.ReportAllocs()
				enc.Reset()
				b.ResetTimer()
				for n := 0; n < b.N; n++ {
					enc.Reset()
					for _, x := range input {
						enc.Write(x)
					}
					enc.Flush()
					if bufResult, err = enc.Bytes(); err != nil {
						b.Fatal(err)
					}
				}
			})

			b.Run("batch", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for n := 0; n < b.N; n++ {
					if bufResult, err = StringArrayEncodeAll(input, bufResult); err != nil {
						b.Fatal(err)
					}
				}
			})

		})
	}
}

func BenchmarkStringArrayDecodeAll(b *testing.B) {
	benchmarks := []struct {
		n int
		w int
	}{
		{1, 10},
		{55, 10},
		{550, 10},
		{1000, 10},
	}
	for _, bm := range benchmarks {
		s := NewStringEncoder(bm.n)
		for c := 0; c < bm.n; c++ {
			s.Write(MakeSentence(bm.w))
		}
		s.Flush()
		bytes, err := s.Bytes()
		if err != nil {
			b.Fatalf("unexpected error: %v", err)
		}

		b.Run(fmt.Sprintf("%d", bm.n), func(b *testing.B) {
			b.SetBytes(int64(len(bytes)))
			b.ReportAllocs()

			dst := make([]string, bm.n)
			for i := 0; i < b.N; i++ {
				got, err := StringArrayDecodeAll(bytes, dst)
				if err != nil {
					b.Fatalf("unexpected length -got/+exp\n%s", cmp.Diff(len(dst), bm.n))
				}
				if len(got) != bm.n {
					b.Fatalf("unexpected length -got/+exp\n%s", cmp.Diff(len(dst), bm.n))
				}
			}
		})
	}
}

// MakeSentence returns a string made up of n words.
// MakeSentence uses rand.Int31n and therefore calling rand.Seed will produce
// deterministic results.
func MakeSentence(n int) string {
	s := make([]string, n)
	for i := 0; i < n; i++ {
		s[i] = words[rand.Int31n(int32(len(words)))]
	}
	return strings.Join(s, " ")
}

var words = [...]string{
	"lorem", "ipsum", "dolor", "sit", "amet", "consectetuer", "adipiscing", "elit", "integer", "in", "mi", "a", "mauris",
	"ornare", "sagittis", "suspendisse", "potenti", "suspendisse", "dapibus", "dignissim", "dolor", "nam",
	"sapien", "tellus", "tempus", "et", "tempus", "ac", "tincidunt", "in", "arcu", "duis", "dictum", "proin", "magna",
	"nulla", "pellentesque", "non", "commodo", "et", "iaculis", "sit", "amet", "mi", "mauris", "condimentum", "massa",
	"ut", "metus", "donec", "viverra", "sapien", "mattis", "rutrum", "tristique", "lacus", "eros", "semper", "tellus",
	"et", "molestie", "nisi", "sapien", "eu", "massa", "vestibulum", "ante", "ipsum", "primis", "in", "faucibus", "orci",
	"luctus", "et", "ultrices", "posuere", "cubilia", "curae", "fusce", "erat", "tortor", "mollis", "ut", "accumsan",
	"ut", "lacinia", "gravida", "libero", "curabitur", "massa", "felis", "accumsan", "feugiat", "convallis", "sit",
	"amet", "porta", "vel", "neque", "duis", "et", "ligula", "non", "elit", "ultricies", "rutrum", "suspendisse",
	"tempor", "quisque", "posuere", "malesuada", "velit", "sed", "pellentesque", "mi", "a", "purus", "integer",
	"imperdiet", "orci", "a", "eleifend", "mollis", "velit", "nulla", "iaculis", "arcu", "eu", "rutrum", "magna", "quam",
	"sed", "elit", "nullam", "egestas", "integer", "interdum", "purus", "nec", "mauris", "vestibulum", "ac", "mi", "in",
	"nunc", "suscipit", "dapibus", "duis", "consectetuer", "ipsum", "et", "pharetra", "sollicitudin", "metus",
	"turpis", "facilisis", "magna", "vitae", "dictum", "ligula", "nulla", "nec", "mi", "nunc", "ante", "urna", "gravida",
	"sit", "amet", "congue", "et", "accumsan", "vitae", "magna", "praesent", "luctus", "nullam", "in", "velit",
	"praesent", "est", "curabitur", "turpis", "class", "aptent", "taciti", "sociosqu", "ad", "litora", "torquent",
	"per", "conubia", "nostra", "per", "inceptos", "hymenaeos", "cras", "consectetuer", "nibh", "in", "lacinia",
	"ornare", "turpis", "sem", "tempor", "massa", "sagittis", "feugiat", "mauris", "nibh", "non", "tellus",
	"phasellus", "mi", "fusce", "enim", "mauris", "ultrices", "turpis", "eu", "adipiscing", "viverra", "justo",
	"libero", "ullamcorper", "massa", "id", "ultrices", "velit", "est", "quis", "tortor", "quisque", "condimentum",
	"lacus", "volutpat", "nonummy", "accumsan", "est", "nunc", "imperdiet", "magna", "vulputate", "aliquet", "nisi",
	"risus", "at", "est", "aliquam", "imperdiet", "gravida", "tortor", "praesent", "interdum", "accumsan", "ante",
	"vivamus", "est", "ligula", "consequat", "sed", "pulvinar", "eu", "consequat", "vitae", "eros", "nulla", "elit",
	"nunc", "congue", "eget", "scelerisque", "a", "tempor", "ac", "nisi", "morbi", "facilisis", "pellentesque",
	"habitant", "morbi", "tristique", "senectus", "et", "netus", "et", "malesuada", "fames", "ac", "turpis", "egestas",
	"in", "hac", "habitasse", "platea", "dictumst", "suspendisse", "vel", "lorem", "ut", "ligula", "tempor",
	"consequat", "quisque", "consectetuer", "nisl", "eget", "elit", "proin", "quis", "mauris", "ac", "orci",
	"accumsan", "suscipit", "sed", "ipsum", "sed", "vel", "libero", "nec", "elit", "feugiat", "blandit", "vestibulum",
	"purus", "nulla", "accumsan", "et", "volutpat", "at", "pellentesque", "vel", "urna", "suspendisse", "nonummy",
	"aliquam", "pulvinar", "libero", "donec", "vulputate", "orci", "ornare", "bibendum", "condimentum", "lorem",
	"elit", "dignissim", "sapien", "ut", "aliquam", "nibh", "augue", "in", "turpis", "phasellus", "ac", "eros",
	"praesent", "luctus", "lorem", "a", "mollis", "lacinia", "leo", "turpis", "commodo", "sem", "in", "lacinia", "mi",
	"quam", "et", "quam", "curabitur", "a", "libero", "vel", "tellus", "mattis", "imperdiet", "in", "congue", "neque", "ut",
	"scelerisque", "bibendum", "libero", "lacus", "ullamcorper", "sapien", "quis", "aliquet", "massa", "velit",
	"vel", "orci", "fusce", "in", "nulla", "quis", "est", "cursus", "gravida", "in", "nibh", "lorem", "ipsum", "dolor", "sit",
	"amet", "consectetuer", "adipiscing", "elit", "integer", "fermentum", "pretium", "massa", "morbi", "feugiat",
	"iaculis", "nunc", "aenean", "aliquam", "pretium", "orci", "cum", "sociis", "natoque", "penatibus", "et", "magnis",
	"dis", "parturient", "montes", "nascetur", "ridiculus", "mus", "vivamus", "quis", "tellus", "vel", "quam",
	"varius", "bibendum", "fusce", "est", "metus", "feugiat", "at", "porttitor", "et", "cursus", "quis", "pede", "nam", "ut",
	"augue", "nulla", "posuere", "phasellus", "at", "dolor", "a", "enim", "cursus", "vestibulum", "duis", "id", "nisi",
	"duis", "semper", "tellus", "ac", "nulla", "vestibulum", "scelerisque", "lobortis", "dolor", "aenean", "a",
	"felis", "aliquam", "erat", "volutpat", "donec", "a", "magna", "vitae", "pede", "sagittis", "lacinia", "cras",
	"vestibulum", "diam", "ut", "arcu", "mauris", "a", "nunc", "duis", "sollicitudin", "erat", "sit", "amet", "turpis",
	"proin", "at", "libero", "eu", "diam", "lobortis", "fermentum", "nunc", "lorem", "turpis", "imperdiet", "id",
	"gravida", "eget", "aliquet", "sed", "purus", "ut", "vehicula", "laoreet", "ante", "mauris", "eu", "nunc", "sed", "sit",
	"amet", "elit", "nec", "ipsum", "aliquam", "egestas", "donec", "non", "nibh", "cras", "sodales", "pretium", "massa",
	"praesent", "hendrerit", "est", "et", "risus", "vivamus", "eget", "pede", "curabitur", "tristique",
	"scelerisque", "dui", "nullam", "ullamcorper", "vivamus", "venenatis", "velit", "eget", "enim", "nunc", "eu",
	"nunc", "eget", "felis", "malesuada", "fermentum", "quisque", "magna", "mauris", "ligula", "felis", "luctus", "a",
	"aliquet", "nec", "vulputate", "eget", "magna", "quisque", "placerat", "diam", "sed", "arcu", "praesent",
	"sollicitudin", "aliquam", "non", "sapien", "quisque", "id", "augue", "class", "aptent", "taciti", "sociosqu",
	"ad", "litora", "torquent", "per", "conubia", "nostra", "per", "inceptos", "hymenaeos", "etiam", "lacus", "lectus",
	"mollis", "quis", "mattis", "nec", "commodo", "facilisis", "nibh", "sed", "sodales", "sapien", "ac", "ante", "duis",
	"eget", "lectus", "in", "nibh", "lacinia", "auctor", "fusce", "interdum", "lectus", "non", "dui", "integer",
	"accumsan", "quisque", "quam", "curabitur", "scelerisque", "imperdiet", "nisl", "suspendisse", "potenti",
	"nam", "massa", "leo", "iaculis", "sed", "accumsan", "id", "ultrices", "nec", "velit", "suspendisse", "potenti",
	"mauris", "bibendum", "turpis", "ac", "viverra", "sollicitudin", "metus", "massa", "interdum", "orci", "non",
	"imperdiet", "orci", "ante", "at", "ipsum", "etiam", "eget", "magna", "mauris", "at", "tortor", "eu", "lectus",
	"tempor", "tincidunt", "phasellus", "justo", "purus", "pharetra", "ut", "ultricies", "nec", "consequat", "vel",
	"nisi", "fusce", "vitae", "velit", "at", "libero", "sollicitudin", "sodales", "aenean", "mi", "libero", "ultrices",
	"id", "suscipit", "vitae", "dapibus", "eu", "metus", "aenean", "vestibulum", "nibh", "ac", "massa", "vivamus",
	"vestibulum", "libero", "vitae", "purus", "in", "hac", "habitasse", "platea", "dictumst", "curabitur",
	"blandit", "nunc", "non", "arcu", "ut", "nec", "nibh", "morbi", "quis", "leo", "vel", "magna", "commodo", "rhoncus",
	"donec", "congue", "leo", "eu", "lacus", "pellentesque", "at", "erat", "id", "mi", "consequat", "congue", "praesent",
	"a", "nisl", "ut", "diam", "interdum", "molestie", "fusce", "suscipit", "rhoncus", "sem", "donec", "pretium",
	"aliquam", "molestie", "vivamus", "et", "justo", "at", "augue", "aliquet", "dapibus", "pellentesque", "felis",
	"morbi", "semper", "in", "venenatis", "imperdiet", "neque", "donec", "auctor", "molestie", "augue", "nulla", "id",
	"arcu", "sit", "amet", "dui", "lacinia", "convallis", "proin", "tincidunt", "proin", "a", "ante", "nunc", "imperdiet",
	"augue", "nullam", "sit", "amet", "arcu", "quisque", "laoreet", "viverra", "felis", "lorem", "ipsum", "dolor", "sit",
	"amet", "consectetuer", "adipiscing", "elit", "in", "hac", "habitasse", "platea", "dictumst", "pellentesque",
	"habitant", "morbi", "tristique", "senectus", "et", "netus", "et", "malesuada", "fames", "ac", "turpis", "egestas",
	"class", "aptent", "taciti", "sociosqu", "ad", "litora", "torquent", "per", "conubia", "nostra", "per", "inceptos",
	"hymenaeos", "nullam", "nibh", "sapien", "volutpat", "ut", "placerat", "quis", "ornare", "at", "lorem", "class",
	"aptent", "taciti", "sociosqu", "ad", "litora", "torquent", "per", "conubia", "nostra", "per", "inceptos",
	"hymenaeos", "morbi", "dictum", "massa", "id", "libero", "ut", "neque", "phasellus", "tincidunt", "nibh", "ut",
	"tincidunt", "lacinia", "lacus", "nulla", "aliquam", "mi", "a", "interdum", "dui", "augue", "non", "pede", "duis",
	"nunc", "magna", "vulputate", "a", "porta", "at", "tincidunt", "a", "nulla", "praesent", "facilisis",
	"suspendisse", "sodales", "feugiat", "purus", "cras", "et", "justo", "a", "mauris", "mollis", "imperdiet", "morbi",
	"erat", "mi", "ultrices", "eget", "aliquam", "elementum", "iaculis", "id", "velit", "in", "scelerisque", "enim",
	"sit", "amet", "turpis", "sed", "aliquam", "odio", "nonummy", "ullamcorper", "mollis", "lacus", "nibh", "tempor",
	"dolor", "sit", "amet", "varius", "sem", "neque", "ac", "dui", "nunc", "et", "est", "eu", "massa", "eleifend", "mollis",
	"mauris", "aliquet", "orci", "quis", "tellus", "ut", "mattis", "praesent", "mollis", "consectetuer", "quam",
	"nulla", "nulla", "nunc", "accumsan", "nunc", "sit", "amet", "scelerisque", "porttitor", "nibh", "pede", "lacinia",
	"justo", "tristique", "mattis", "purus", "eros", "non", "velit", "aenean", "sagittis", "commodo", "erat",
	"aliquam", "id", "lacus", "morbi", "vulputate", "vestibulum", "elit",
}

