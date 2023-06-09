package tsm

// Integer encoding uses two different strategies depending on the range of values in
// the uncompressed data.  Encoded values are first encoding used zig zag encoding.
// This interleaves positive and negative integers across a range of positive integers.
//
// For example, [-2,-1,0,1] becomes [3,1,0,2]. See
// https://developers.google.com/protocol-buffers/docs/encoding?hl=en#signed-integers
// for more information.
//
// If all the zig zag encoded values are less than 1 << 60 - 1, they are compressed using
// simple8b encoding.  If any value is larger than 1 << 60 - 1, the values are stored uncompressed.
//
// Each encoded byte slice contains a 1 byte header followed by multiple 8 byte packed integers
// or 8 byte uncompressed integers.  The 4 high bits of the first byte indicate the encoding type
// for the remaining bytes.
//
// There are currently two encoding types that can be used with room for 16 total.  These additional
// encoding slots are reserved for future use.  One improvement to be made is to use a patched
// encoding such as PFOR if only a small number of values exceed the max compressed value range.  This
// should improve compression ratios with very large integers near the ends of the int64 range.

import (
	"encoding/binary"
	"fmt"

	"github.com/jwilder/encoding/simple8b"
)

const (
	// intUncompressed is an uncompressed format using 8 bytes per point
	intUncompressed = 0
	// intCompressedSimple is a bit-packed format using simple8b encoding
	intCompressedSimple = 1
	// intCompressedRLE is a run-length encoding format
	intCompressedRLE = 2
)

// IntegerEncoder encodes int64s into byte slices.
type IntegerEncoder struct {
	prev   int64
	rle    bool
	values []uint64
}

// NewIntegerEncoder returns a new integer encoder with an initial buffer of values sized at sz.
func NewIntegerEncoder(sz int) IntegerEncoder {
	return IntegerEncoder{
		rle:    true,
		values: make([]uint64, 0, sz),
	}
}

// Flush is no-op
func (e *IntegerEncoder) Flush() {}

// Reset sets the encoder back to its initial state.
func (e *IntegerEncoder) Reset() {
	e.prev = 0
	e.rle = true
	e.values = e.values[:0]
}

// Write encodes v to the underlying buffers.
func (e *IntegerEncoder) Write(v int64) {
	// Delta-encode each value as it's written.  This happens before
	// ZigZagEncoding because the deltas could be negative.
	delta := v - e.prev
	e.prev = v
	enc := ZigZagEncode(delta)
	if len(e.values) > 1 {
		e.rle = e.rle && e.values[len(e.values)-1] == enc
	}

	e.values = append(e.values, enc)
}

// Bytes returns a copy of the underlying buffer.
func (e *IntegerEncoder) Bytes() ([]byte, error) {
	// Only run-length encode if it could reduce storage size.
	if e.rle && len(e.values) > 2 {
		return e.encodeRLE()
	}

	for _, v := range e.values {
		// Value is too large to encode using packed format
		if v > simple8b.MaxValue {
			return e.encodeUncompressed()
		}
	}

	return e.encodePacked()
}

func (e *IntegerEncoder) encodeRLE() ([]byte, error) {
	// Large varints can take up to 10 bytes.  We're storing 3 + 1
	// type byte.
	var b [31]byte

	// 4 high bits used for the encoding type
	b[0] = byte(intCompressedRLE) << 4

	i := 1
	// The first value
	binary.BigEndian.PutUint64(b[i:], e.values[0])
	i += 8
	// The first delta
	i += binary.PutUvarint(b[i:], e.values[1])
	// The number of times the delta is repeated
	i += binary.PutUvarint(b[i:], uint64(len(e.values)-1))

	return b[:i], nil
}

func (e *IntegerEncoder) encodePacked() ([]byte, error) {
	if len(e.values) == 0 {
		return nil, nil
	}

	// Encode all but the first value.  Fist value is written unencoded
	// using 8 bytes.
	encoded, err := simple8b.EncodeAll(e.values[1:])
	if err != nil {
		return nil, err
	}

	b := make([]byte, 1+(len(encoded)+1)*8)
	// 4 high bits of first byte store the encoding type for the block
	b[0] = byte(intCompressedSimple) << 4

	// Write the first value since it's not part of the encoded values
	binary.BigEndian.PutUint64(b[1:9], e.values[0])

	// Write the encoded values
	for i, v := range encoded {
		binary.BigEndian.PutUint64(b[9+i*8:9+i*8+8], v)
	}
	return b, nil
}

func (e *IntegerEncoder) encodeUncompressed() ([]byte, error) {
	if len(e.values) == 0 {
		return nil, nil
	}

	b := make([]byte, 1+len(e.values)*8)
	// 4 high bits of first byte store the encoding type for the block
	b[0] = byte(intUncompressed) << 4

	for i, v := range e.values {
		binary.BigEndian.PutUint64(b[1+i*8:1+i*8+8], v)
	}
	return b, nil
}

// IntegerDecoder decodes a byte slice into int64s.
type IntegerDecoder struct {
	// 240 is the maximum number of values that can be encoded into a single uint64 using simple8b
	values [240]uint64
	bytes  []byte
	i      int
	n      int
	prev   int64
	first  bool

	// The first value for a run-length encoded byte slice
	rleFirst uint64

	// The delta value for a run-length encoded byte slice
	rleDelta uint64
	encoding byte
	err      error
}

// SetBytes sets the underlying byte slice of the decoder.
func (d *IntegerDecoder) SetBytes(b []byte) {
	if len(b) > 0 {
		d.encoding = b[0] >> 4
		d.bytes = b[1:]
	} else {
		d.encoding = 0
		d.bytes = nil
	}

	d.i = 0
	d.n = 0
	d.prev = 0
	d.first = true

	d.rleFirst = 0
	d.rleDelta = 0
	d.err = nil
}

// Next returns true if there are any values remaining to be decoded.
func (d *IntegerDecoder) Next() bool {
	if d.i >= d.n && len(d.bytes) == 0 {
		return false
	}

	d.i++

	if d.i >= d.n {
		switch d.encoding {
		case intUncompressed:
			d.decodeUncompressed()
		case intCompressedSimple:
			d.decodePacked()
		case intCompressedRLE:
			d.decodeRLE()
		default:
			d.err = fmt.Errorf("unknown encoding %v", d.encoding)
		}
	}
	return d.err == nil && d.i < d.n
}

// Error returns the last error encountered by the decoder.
func (d *IntegerDecoder) Error() error {
	return d.err
}

// Read returns the next value from the decoder.
func (d *IntegerDecoder) Read() int64 {
	switch d.encoding {
	case intCompressedRLE:
		return ZigZagDecode(d.rleFirst) + int64(d.i)*ZigZagDecode(d.rleDelta)
	default:
		v := ZigZagDecode(d.values[d.i])
		// v is the delta encoded value, we need to add the prior value to get the original
		v = v + d.prev
		d.prev = v
		return v
	}
}

func (d *IntegerDecoder) decodeRLE() {
	if len(d.bytes) == 0 {
		return
	}

	if len(d.bytes) < 8 {
		d.err = fmt.Errorf("IntegerDecoder: not enough data to decode RLE starting value")
		return
	}

	var i, n int

	// Next 8 bytes is the starting value
	first := binary.BigEndian.Uint64(d.bytes[i : i+8])
	i += 8

	// Next 1-10 bytes is the delta value
	value, n := binary.Uvarint(d.bytes[i:])
	if n <= 0 {
		d.err = fmt.Errorf("IntegerDecoder: invalid RLE delta value")
		return
	}
	i += n

	// Last 1-10 bytes is how many times the value repeats
	count, n := binary.Uvarint(d.bytes[i:])
	if n <= 0 {
		d.err = fmt.Errorf("IntegerDecoder: invalid RLE repeat value")
		return
	}

	// Store the first value and delta value so we do not need to allocate
	// a large values slice.  We can compute the value at position d.i on
	// demand.
	d.rleFirst = first
	d.rleDelta = value
	d.n = int(count) + 1
	d.i = 0

	// We've process all the bytes
	d.bytes = nil
}

func (d *IntegerDecoder) decodePacked() {
	if len(d.bytes) == 0 {
		return
	}

	if len(d.bytes) < 8 {
		d.err = fmt.Errorf("IntegerDecoder: not enough data to decode packed value")
		return
	}

	v := binary.BigEndian.Uint64(d.bytes[0:8])
	// The first value is always unencoded
	if d.first {
		d.first = false
		d.n = 1
		d.values[0] = v
	} else {
		n, err := simple8b.Decode(&d.values, v)
		if err != nil {
			// Should never happen, only error that could be returned is if the value to be decoded was not
			// actually encoded by simple8b encoder.
			d.err = fmt.Errorf("failed to decode value %v: %v", v, err)
		}

		d.n = n
	}
	d.i = 0
	d.bytes = d.bytes[8:]
}

func (d *IntegerDecoder) decodeUncompressed() {
	if len(d.bytes) == 0 {
		return
	}

	if len(d.bytes) < 8 {
		d.err = fmt.Errorf("IntegerDecoder: not enough data to decode uncompressed value")
		return
	}

	d.values[0] = binary.BigEndian.Uint64(d.bytes[0:8])
	d.i = 0
	d.n = 1
	d.bytes = d.bytes[8:]
}
