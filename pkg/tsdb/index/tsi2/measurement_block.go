package tsi2

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"sort"

	"cycledb/pkg/tsdb"

	"github.com/influxdata/influxdb/pkg/rhh"
)

// LoadFactor is the fill percent for RHH indexes.
const LoadFactor = 80

// Measurement field size constants.
const (
	// Measurement key block fields.
	MeasurementNSize      = 8
	MeasurementOffsetSize = 8

	// Measurement trailer fields
	MeasurementTrailerSize = 0 +
		8 + 8 + // data offset/size
		8 + 8 // hash index offset/size
)

// MeasurementBlock represents a collection of all measurements in an index.
type MeasurementBlock struct {
	data     []byte
	hashData []byte

	// // Measurement sketch and tombstone sketch for cardinality estimation.
	// sketchData, tSketchData []byte

	// version int // block version
}

// UnmarshalBinary unpacks data into the block. Block is not copied so data
// should be retained and unchanged after being passed into this function.
func (blk *MeasurementBlock) UnmarshalBinary(data []byte) error {
	// Read trailer.
	t, err := ReadMeasurementBlockTrailer(data)
	if err != nil {
		return err
	}

	// Save data section.
	blk.data = data[t.Data.Offset:]
	blk.data = blk.data[:t.Data.Size]

	// Save hash index block.
	blk.hashData = data[t.HashIndex.Offset:]
	blk.hashData = blk.hashData[:t.HashIndex.Size]

	// // Initialise sketch data.
	// blk.sketchData = data[t.Sketch.Offset:][:t.Sketch.Size]
	// blk.tSketchData = data[t.TSketch.Offset:][:t.TSketch.Size]

	return nil
}

// Elem returns an element for a measurement. hash
func (blk *MeasurementBlock) Elem(name []byte) (e MeasurementBlockElem, ok bool) {
	n := int64(binary.BigEndian.Uint64(blk.hashData[:MeasurementNSize]))
	hash := rhh.HashKey(name)
	pos := hash % n

	// Track current distance
	var d int64
	for {
		// Find offset of measurement.
		offset := binary.BigEndian.Uint64(blk.hashData[MeasurementNSize+(pos*MeasurementOffsetSize):])
		if offset == 0 {
			return MeasurementBlockElem{}, false
		}

		// Evaluate name if offset is not empty.
		if offset > 0 {
			// Parse into element.
			var e MeasurementBlockElem
			e.UnmarshalBinary(blk.data[offset:])

			// Return if name match.
			if bytes.Equal(e.name, name) {
				return e, true
			}

			// Check if we've exceeded the probe distance.
			if d > rhh.Dist(rhh.HashKey(e.name), pos, n) {
				return MeasurementBlockElem{}, false
			}
		}

		// Move position forward.
		pos = (pos + 1) % n
		d++

		if d > n {
			return MeasurementBlockElem{}, false
		}
	}
}

type MeasurementBlockElem struct {
	// flag byte   // flag
	name []byte // measurement name

	idMap IdMap

	gridsBlock struct {
		offset int64
		size   int64
	}

	grids []struct {
		offset int64
		size   int64
	}

	series struct {
		n    uint64 // series count
		data []byte // serialized series data
	}

	seriesIDSet *tsdb.SeriesIDSet

	// size in bytes, set after unmarshalling.
	size int
}

// Name returns the measurement name.
func (e *MeasurementBlockElem) Name() []byte { return e.name }

// TagBlockOffset returns the offset of the measurement's tag block.
func (e *MeasurementBlockElem) GridBlockOffset() int64 { return e.gridsBlock.offset }

// TagBlockSize returns the size of the measurement's tag block.
func (e *MeasurementBlockElem) GridBlockSize() int64 { return e.gridsBlock.size }

// SeriesData returns the raw series data.
func (e *MeasurementBlockElem) SeriesData() []byte { return e.series.data }

// SeriesN returns the number of series associated with the measurement.
func (e *MeasurementBlockElem) SeriesN() uint64 { return e.series.n }

func (e *MeasurementBlockElem) SeriesIDSet() *tsdb.SeriesIDSet { return e.seriesIDSet }

// uvarint is a wrapper around binary.Uvarint.
// Returns a non-nil error when binary.Uvarint returns n <= 0 or n > len(data).
func uvarint(data []byte) (value uint64, n int, err error) {
	if len(data) < 1 {
		err = io.ErrShortBuffer
	} else if value, n = binary.Uvarint(data); n == 0 || n > len(data) {
		err = io.ErrShortBuffer
	} else if n < 0 {
		err = fmt.Errorf("parsing binary-encoded uint64 value failed; binary.Uvarint() returned %d", n)
	}
	return
}

// UnmarshalBinary unmarshals data into e.
func (e *MeasurementBlockElem) UnmarshalBinary(data []byte) error {
	start := len(data)

	// Parse tag block offset.
	e.gridsBlock.offset, data = int64(binary.BigEndian.Uint64(data)), data[8:]
	e.gridsBlock.size, data = int64(binary.BigEndian.Uint64(data)), data[8:]

	// Parse each grid
	gridsNum, data := int64(binary.BigEndian.Uint64(data)), data[8:]
	var offset int64
	var size int64
	for i := int64(0); i < gridsNum; i++ {
		offset, data = int64(binary.BigEndian.Uint64(data)), data[8:]
		size, data = int64(binary.BigEndian.Uint64(data)), data[8:]
		e.grids = append(e.grids, struct {
			offset int64
			size   int64
		}{offset: offset, size: size})
	}

	// Parse name.
	sz, n, err := uvarint(data)
	if err != nil {
		return err
	}
	e.name, data = data[n:n+int(sz)], data[n+int(sz):]

	// Parse series count.
	v, n, err := uvarint(data)
	if err != nil {
		return err
	}
	e.series.n, data = uint64(v), data[n:]

	// Parse series data size.
	sz, n, err = uvarint(data)
	if err != nil {
		return err
	}
	data = data[n:]

	// // Parse series data (original uvarint encoded or roaring bitmap).
	// if e.flag&MeasurementSeriesIDSetFlag == 0 {
	// 	e.series.data, data = data[:sz], data[sz:]
	// } else {
	// // data = memalign(data)
	e.seriesIDSet = tsdb.NewSeriesIDSet()
	if err = e.seriesIDSet.UnmarshalBinaryUnsafe(data[:sz]); err != nil {
		return err
	}
	data = data[sz:]
	// }

	// map map
	sz, n, err = uvarint(data)
	if err != nil {
		return err
	}
	data = data[n:]
	// look up a number to fillup the IdMap
	e.idMap.ReadFrom(data)
	data = data[sz:]

	// Save length of elem.
	e.size = start - len(data)

	return nil
}

func (e *MeasurementBlockElem) GetSeriesFileId(k uint64) (uint64, bool) {
	return e.idMap.Get(k)
}

// MeasurementBlockWriter writes a measurement block.
type MeasurementBlockWriter struct {
	buf bytes.Buffer
	mms map[string]CompactedMeasurement
}

// NewMeasurementBlockWriter returns a new MeasurementBlockWriter.
func NewMeasurementBlockWriter() *MeasurementBlockWriter {
	return &MeasurementBlockWriter{
		mms: make(map[string]CompactedMeasurement),
	}
}

// Add adds a measurement with series and tag set offset/size.
func (mw *MeasurementBlockWriter) Add(name []byte, mmInfo *IndexFileMeasurementCompactInfo, m map[uint64]uint64, seriesIDSet *tsdb.SeriesIDSet) {
	mm := mw.mms[string(name)]
	mm.gridBlock.offset = mmInfo.Offset
	mm.gridBlock.size = mmInfo.Size

	for _, grid := range mmInfo.gridInfos {
		mm.grids = append(mm.grids, struct {
			offset int64
			size   int64
		}{
			offset: grid.offset,
			size:   grid.size,
		})
	}

	mm.indexIdToFileId = m
	mm.seriesIDSet = seriesIDSet
	mw.mms[string(name)] = mm

	// if deleted {
	// 	mw.tSketch.Add(name)
	// } else {
	// 	mw.sketch.Add(name)
	// }
}

// WriteTo encodes the measurements to w.
func (mw *MeasurementBlockWriter) WriteTo(w io.Writer) (n int64, err error) {
	var t MeasurementBlockTrailer

	// // The sketches must be set before calling WriteTo.
	// if mw.sketch == nil {
	// 	return 0, errors.New("measurement sketch not set")
	// } else if mw.tSketch == nil {
	// 	return 0, errors.New("measurement tombstone sketch not set")
	// }

	// Sort names.
	names := make([]string, 0, len(mw.mms))
	for name := range mw.mms {
		names = append(names, name)
	}
	sort.Strings(names)

	// Begin data section.
	t.Data.Offset = n

	// Write padding byte so no offsets are zero.
	if err := writeUint8To(w, 0, &n); err != nil {
		return n, err
	}

	// Encode grids.
	for _, name := range names {
		// Retrieve measurement and save offset.
		mm := mw.mms[name]
		mm.offset = n
		mw.mms[name] = mm

		// Write measurement
		if err := mw.writeMeasurementTo(w, []byte(name), &mm, &n); err != nil {
			return n, err
		}
	}
	t.Data.Size = n - t.Data.Offset

	// Build measurement hash map
	m := rhh.NewHashMap(rhh.Options{
		Capacity:   int64(len(names)),
		LoadFactor: LoadFactor,
	})
	for name := range mw.mms {
		mm := mw.mms[name]
		m.Put([]byte(name), &mm)
	}

	t.HashIndex.Offset = n

	// Encode hash map length.
	if err := writeUint64To(w, uint64(m.Cap()), &n); err != nil {
		return n, err
	}

	// Encode hash map offset entries.
	for i := int64(0); i < m.Cap(); i++ {
		_, v := m.Elem(i)

		var offset int64
		if mm, ok := v.(*CompactedMeasurement); ok {
			offset = mm.offset
		}

		if err := writeUint64To(w, uint64(offset), &n); err != nil {
			return n, err
		}
	}
	t.HashIndex.Size = n - t.HashIndex.Offset

	// // Write the sketches out.
	// t.Sketch.Offset = n
	// if err := writeSketchTo(w, mw.sketch, &n); err != nil {
	// 	return n, err
	// }
	// t.Sketch.Size = n - t.Sketch.Offset

	// t.TSketch.Offset = n
	// if err := writeSketchTo(w, mw.tSketch, &n); err != nil {
	// 	return n, err
	// }
	// t.TSketch.Size = n - t.TSketch.Offset

	// Write trailer.
	nn, err := t.WriteTo(w)
	n += nn
	return n, err
}

// writeMeasurementTo encodes a single measurement entry into w.
func (mw *MeasurementBlockWriter) writeMeasurementTo(w io.Writer, name []byte, mm *CompactedMeasurement, n *int64) error {
	// // Write flag & tag block offset.
	// if err := writeUint8To(w, mm.flag(), n); err != nil {
	// 	return err
	// }
	if err := writeUint64To(w, uint64(mm.gridBlock.offset), n); err != nil {
		return err
	} else if err := writeUint64To(w, uint64(mm.gridBlock.size), n); err != nil {
		return err
	}

	// Write meta data for each grid
	if err := writeUint64To(w, uint64(len(mm.grids)), n); err != nil {
		return err
	} else {
		for _, grid := range mm.grids {
			if err := writeUint64To(w, uint64(grid.offset), n); err != nil {
				return err
			} else if err := writeUint64To(w, uint64(grid.size), n); err != nil {
				return err
			}
		}
	}

	// Write measurement name.
	if err := writeUvarintTo(w, uint64(len(name)), n); err != nil {
		return err
	}
	if err := writeTo(w, name, n); err != nil {
		return err
	}

	// Write series data to buffer.
	mw.buf.Reset()
	if _, err := mm.seriesIDSet.WriteTo(&mw.buf); err != nil {
		return err
	}

	// Write series count.
	if err := writeUvarintTo(w, mm.seriesIDSet.Cardinality(), n); err != nil {
		return err
	}

	// Write data size & buffer.
	if err := writeUvarintTo(w, uint64(mw.buf.Len()), n); err != nil {
		return err
	}

	// Word align bitmap data.
	// if offset := (*n) % 8; offset != 0 {
	// 	if err := writeTo(w, make([]byte, 8-offset), n); err != nil {
	// 		return err
	// 	}
	// }

	nn, _ := mw.buf.WriteTo(w)
	*n += nn

	// Write indexId-seriesFileId map to buffer.
	mw.buf.Reset()
	mapEnc := NewIdMap()
	if err := mapEnc.FlushTo(&mw.buf, mm.indexIdToFileId); err != nil {
		return err
	}

	// Write data size & buffer.
	if err := writeUvarintTo(w, uint64(mw.buf.Len()), n); err != nil {
		return err
	}

	// Word align bitmap data.
	// if offset := (*n) % 8; offset != 0 {
	// 	if err := writeTo(w, make([]byte, 8-offset), n); err != nil {
	// 		return err
	// 	}
	// }

	nn, err := mw.buf.WriteTo(w)
	*n += nn
	fmt.Printf("The size of map block:%v\n", nn)
	// fmt.Printf("The size of map block if use gob:%v\n", encodeWithGob(mm.indexIdToFileId))
	// fmt.Printf("The size of map block if use messagePack:%v\n", encodeWithMessagePack(mm.indexIdToFileId))
	// fmt.Printf("The size of map block if just store nums:%v\n", eoncodeNums(mm.indexIdToFileId))

	return err
}

// func encodeWithGob(m map[uint64]uint64) int {
// 	// Create a file to write the encoded data
// 	var baf bytes.Buffer

// 	// Encode the map and write it to the file
// 	encoder := gob.NewEncoder(&baf)
// 	if err := encoder.Encode(m); err != nil {
// 		panic(err)
// 	}
// 	return baf.Len()
// }

// func encodeWithMessagePack(m map[uint64]uint64) int {
// 	serializedMsgPack, err := msgpack.Marshal(m)
// 	if err != nil {
// 		fmt.Println("Error:", err)
// 		return 0
// 	}
// 	return len(serializedMsgPack)
// }

// func eoncodeNums(m map[uint64]uint64) int {
// 	return 8*len(m)
// }

type CompactedMeasurement struct {
	gridBlock struct {
		offset int64
		size   int64
	}
	grids []struct {
		offset int64
		size   int64
	}
	seriesIDSet     *tsdb.SeriesIDSet
	indexIdToFileId map[uint64]uint64
	offset          int64
}

// MeasurementBlockTrailer represents meta data at the end of a MeasurementBlock.
type MeasurementBlockTrailer struct {
	// Version int // Encoding version

	// Offset & size of data section.
	Data struct {
		Offset int64
		Size   int64
	}

	// Offset & size of hash map section.
	HashIndex struct {
		Offset int64
		Size   int64
	}

	// // Offset and size of cardinality sketch for measurements.
	// Sketch struct {
	// 	Offset int64
	// 	Size   int64
	// }

	// // Offset and size of cardinality sketch for tombstoned measurements.
	// TSketch struct {
	// 	Offset int64
	// 	Size   int64
	// }
}

// ReadMeasurementBlockTrailer returns the block trailer from data.
func ReadMeasurementBlockTrailer(data []byte) (MeasurementBlockTrailer, error) {
	var t MeasurementBlockTrailer

	// // Read version (which is located in the last two bytes of the trailer).
	// t.Version = int(binary.BigEndian.Uint16(data[len(data)-2:]))
	// if t.Version != MeasurementBlockVersion {
	// 	return t, ErrUnsupportedIndexFileVersion
	// }

	// Slice trailer data.
	buf := data[len(data)-MeasurementTrailerSize:]

	// Read data section info.
	t.Data.Offset, buf = int64(binary.BigEndian.Uint64(buf[0:8])), buf[8:]
	t.Data.Size, buf = int64(binary.BigEndian.Uint64(buf[0:8])), buf[8:]

	// Read measurement block info.
	t.HashIndex.Offset, buf = int64(binary.BigEndian.Uint64(buf[0:8])), buf[8:]
	t.HashIndex.Size, buf = int64(binary.BigEndian.Uint64(buf[0:8])), buf[8:]

	// // Read measurement sketch info.
	// t.Sketch.Offset, buf = int64(binary.BigEndian.Uint64(buf[0:8])), buf[8:]
	// t.Sketch.Size, buf = int64(binary.BigEndian.Uint64(buf[0:8])), buf[8:]

	// // Read tombstone measurement sketch info.
	// t.TSketch.Offset, buf = int64(binary.BigEndian.Uint64(buf[0:8])), buf[8:]
	// t.TSketch.Size = int64(binary.BigEndian.Uint64(buf[0:8]))

	return t, nil
}

// WriteTo writes the trailer to w.
func (t *MeasurementBlockTrailer) WriteTo(w io.Writer) (n int64, err error) {
	// Write data section info.
	if err := writeUint64To(w, uint64(t.Data.Offset), &n); err != nil {
		return n, err
	} else if err := writeUint64To(w, uint64(t.Data.Size), &n); err != nil {
		return n, err
	}

	// Write hash index section info.
	if err := writeUint64To(w, uint64(t.HashIndex.Offset), &n); err != nil {
		return n, err
	} else if err := writeUint64To(w, uint64(t.HashIndex.Size), &n); err != nil {
		return n, err
	}

	// // Write measurement sketch info.
	// if err := writeUint64To(w, uint64(t.Sketch.Offset), &n); err != nil {
	// 	return n, err
	// } else if err := writeUint64To(w, uint64(t.Sketch.Size), &n); err != nil {
	// 	return n, err
	// }

	// // Write tombstone measurement sketch info.
	// if err := writeUint64To(w, uint64(t.TSketch.Offset), &n); err != nil {
	// 	return n, err
	// } else if err := writeUint64To(w, uint64(t.TSketch.Size), &n); err != nil {
	// 	return n, err
	// }

	// // Write measurement block version.
	// if err := writeUint16To(w, MeasurementBlockVersion, &n); err != nil {
	// 	return n, err
	// }

	return n, nil
}
