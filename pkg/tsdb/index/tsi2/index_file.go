package tsi2

import (
	"encoding/binary"
	"fmt"
	"io"
)

// IndexFileVersion is the current TSI1 index file version.
const IndexFileVersion = 1

// FileSignature represents a magic number at the header of the index file.
const FileSignature = "TSI2"

// IndexFileTrailer represents meta data written to the end of the index file.
type IndexFileTrailer struct {
	Version int

	MeasurementBlock struct {
		Offset int64
		Size   int64
	}

	// SeriesIDSet struct {
	// 	Offset int64
	// 	Size   int64
	// }

	IndexIdToSeriesFileId struct {
		Offset int64
		Size   int64
	}

	// TombstoneSeriesIDSet struct {
	// 	Offset int64
	// 	Size   int64
	// }

	// SeriesSketch struct {
	// 	Offset int64
	// 	Size   int64
	// }

	// TombstoneSeriesSketch struct {
	// 	Offset int64
	// 	Size   int64
	// }
}

// WriteTo writes the trailer to w.
func (t *IndexFileTrailer) WriteTo(w io.Writer) (n int64, err error) {
	// Write measurement block info.
	if err := writeUint64To(w, uint64(t.MeasurementBlock.Offset), &n); err != nil {
		return n, err
	} else if err := writeUint64To(w, uint64(t.MeasurementBlock.Size), &n); err != nil {
		return n, err
	}

	// Write series id set info.
	if err := writeUint64To(w, uint64(t.IndexIdToSeriesFileId.Offset), &n); err != nil {
		return n, err
	} else if err := writeUint64To(w, uint64(t.IndexIdToSeriesFileId.Size), &n); err != nil {
		return n, err
	}

	// // Write series id set info.
	// if err := writeUint64To(w, uint64(t.SeriesIDSet.Offset), &n); err != nil {
	// 	return n, err
	// } else if err := writeUint64To(w, uint64(t.SeriesIDSet.Size), &n); err != nil {
	// 	return n, err
	// }

	// // Write tombstone series id set info.
	// if err := writeUint64To(w, uint64(t.TombstoneSeriesIDSet.Offset), &n); err != nil {
	// 	return n, err
	// } else if err := writeUint64To(w, uint64(t.TombstoneSeriesIDSet.Size), &n); err != nil {
	// 	return n, err
	// }

	// // Write series sketch info.
	// if err := writeUint64To(w, uint64(t.SeriesSketch.Offset), &n); err != nil {
	// 	return n, err
	// } else if err := writeUint64To(w, uint64(t.SeriesSketch.Size), &n); err != nil {
	// 	return n, err
	// }

	// // Write series tombstone sketch info.
	// if err := writeUint64To(w, uint64(t.TombstoneSeriesSketch.Offset), &n); err != nil {
	// 	return n, err
	// } else if err := writeUint64To(w, uint64(t.TombstoneSeriesSketch.Size), &n); err != nil {
	// 	return n, err
	// }

	// Write index file encoding version.
	if err := writeUint16To(w, IndexFileVersion, &n); err != nil {
		return n, err
	}

	return n, nil
}

// FormatIndexFileName generates an index filename for the given index.
func FormatIndexFileName(id, level int) string {
	return fmt.Sprintf("L%d-%08d%s", level, id, IndexFileExt)
}

// writeTo writes write v into w. Updates n.
func writeTo(w io.Writer, v []byte, n *int64) error {
	nn, err := w.Write(v)
	*n += int64(nn)
	return err
}

// writeUint8To writes write v into w. Updates n.
func writeUint8To(w io.Writer, v uint8, n *int64) error {
	nn, err := w.Write([]byte{v})
	*n += int64(nn)
	return err
}

// writeUint16To writes write v into w using big endian encoding. Updates n.
func writeUint16To(w io.Writer, v uint16, n *int64) error {
	var buf [2]byte
	binary.BigEndian.PutUint16(buf[:], v)
	nn, err := w.Write(buf[:])
	*n += int64(nn)
	return err
}

// writeUint64To writes write v into w using big endian encoding. Updates n.
func writeUint64To(w io.Writer, v uint64, n *int64) error {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], v)
	nn, err := w.Write(buf[:])
	*n += int64(nn)
	return err
}

// writeUvarintTo writes write v into w using variable length encoding. Updates n.
func writeUvarintTo(w io.Writer, v uint64, n *int64) error {
	var buf [binary.MaxVarintLen64]byte
	i := binary.PutUvarint(buf[:], v)
	nn, err := w.Write(buf[:i])
	*n += int64(nn)
	return err
}

// IndexFileCompactInfo is a context object to track compaction position info.
type IndexFileCompactInfo struct {
	// cancel                <-chan struct{}
	Mms map[string]*IndexFileMeasurementCompactInfo
}

// NewIndexFileCompactInfo returns a new instance of logFileCompactInfo.
func NewIndexFileCompactInfo() *IndexFileCompactInfo {
	return &IndexFileCompactInfo{
		Mms: make(map[string]*IndexFileMeasurementCompactInfo),
	}
}

func (info *IndexFileCompactInfo) Show() string {
	var s string
	for name, measurement := range info.Mms {
		s = fmt.Sprintf("%s%v: %+v\n", s, name, measurement.Show())
	}
	return s
}

type IndexFileMeasurementCompactInfo struct {
	Offset int64
	Size   int64

	gridInfos []*GridCompactInfo
}

func (info *IndexFileMeasurementCompactInfo) Show() string {
	s := fmt.Sprintf("offset: %d, size: %d, gridInfos: {", info.Offset, info.Size)
	for _, inf := range info.gridInfos {
		s = fmt.Sprintf("%s{grid_offset: %v, gridsize:%+v}", s, inf.offset, inf.size)
	}
	s += "}\n"
	return s
}

type GridCompactInfo struct {
	offset int64
	size   int64
}
