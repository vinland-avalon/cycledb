package tsi2

import (
	"bytes"
	"io"
)

type GridBlock struct {
	data []byte

	gridData []byte
	// keyData   []byte
	// hashData  []byte

	version int // tag block version
}

// GridBlockEncoder encodes a grid to a GridBlock section.
type GridBlockEncoder struct {
	w   io.Writer
	buf bytes.Buffer

	// // Track value offsets.
	// offsets *rhh.HashMap

	// // Track bytes written, sections.
	n       int64
	trailer GridBlockTrailer

	// Track gridEntries.
	// gridEntries []GridEncodeEntry
	// prevValue []byte
}

// NewGridBlockEncoder returns a new TagBlockEncoder.
func NewGridBlockEncoder(w io.Writer) *GridBlockEncoder {
	return &GridBlockEncoder{
		w:       w,
		trailer: GridBlockTrailer{},
	}
}

// N returns the number of bytes written.
func (enc *GridBlockEncoder) N() int64 { return enc.n }

func (enc *GridBlockEncoder) EncodeGrid(g *Grid) error {
	// 8 bytes
	writeUint64To(enc.w, g.offset, &enc.n)

	// 8 + (8 + len(key) ...)
	writeUint64To(enc.w, uint64(len(g.tagKeys)), &enc.n)
	for _, key := range g.tagKeys {
		writeUint64To(enc.w, uint64(len(key)), &enc.n)
		writeTo(enc.w, []byte(key), &enc.n)
	}

	// 8 + (8 + 8 + (8 + len(value) ...) ... )
	writeUint64To(enc.w, uint64(len(g.tagValuesSlice)), &enc.n)
	for _, tagValues := range g.tagValuesSlice {
		writeUint64To(enc.w, tagValues.capacity, &enc.n)
		writeUint64To(enc.w, uint64(len(tagValues.values)), &enc.n)
		for _, value := range tagValues.values {
			writeUint64To(enc.w, uint64(len(value)), &enc.n)
			writeTo(enc.w, []byte(value), &enc.n)
		}
	}

	ss := g.seriesIDSet
	// Build series data in buffer.
	enc.buf.Reset()
	if _, err := ss.WriteTo(&enc.buf); err != nil {
		return err
	}

	// // Write series count.
	// if err := writeUvarintTo(enc.w, uint64(ss.Cardinality()), &enc.n); err != nil {
	// 	return err
	// }

	// Write data size & buffer.
	if err := writeUint64To(enc.w, uint64(enc.buf.Len()), &enc.n); err != nil {
		return err
	}
	// if err := writeUvarintTo(enc.w, uint64(enc.buf.Len()), &enc.n); err != nil {
	// 	return err
	// }
	nn, err := enc.buf.WriteTo(enc.w)
	if enc.n += nn; err != nil {
		return err
	}

	return nil
}

// ensureHeaderWritten writes a single byte to offset the rest of the block.
func (enc *GridBlockEncoder) ensureHeaderWritten() error {
	if enc.n > 0 {
		return nil
	} else if _, err := enc.w.Write([]byte{0}); err != nil {
		return err
	}

	enc.n++
	// enc.trailer.ValueData.Offset = enc.n

	return nil
}

type GridBlockTrailer struct {
}

// type GridEncodeEntry struct {
// 	grid []byte

// 	// deleted bool

// }

// func NewGridEncodeEntry(g *Grid) *GridEncodeEntry {

// }

// type Grid struct {
// 	// the grids are linked, so id should skip
// 	offset uint64
// 	// for each Grid, the size of tag values array (tagValuesSlice)
// 	// and size of element(capacity) within is pre-allocated
// 	tagValuesSlice []*TagValues
// 	tagKeys        []string
// 	// to accelerate search
// 	tagKeyToIndex map[string]int

// 	// bitmap
// 	seriesIDSet *tsdb.SeriesIDSet
// }
