package tsi2

import (
	"cycledb/pkg/tsdb"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"log"

	"github.com/influxdata/influxdb/v2/models"
)

// IndexFileVersion is the current TSI1 index file version.
const IndexFileVersion = 1

// FileSignature represents a magic number at the header of the index file.
const FileSignature = "TSI2"

// IndexFileTrailer represents meta data written to the end of the index file.
type IndexFileTrailer struct {
	// Version int

	MeasurementBlock struct {
		Offset int64
		Size   int64
	}

	// SeriesIDSet struct {
	// 	Offset int64
	// 	Size   int64
	// }

	// IndexIdToSeriesFileId struct {
	// 	Offset int64
	// 	Size   int64
	// }

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

	// // Write series id set info.
	// if err := writeUint64To(w, uint64(t.IndexIdToSeriesFileId.Offset), &n); err != nil {
	// 	return n, err
	// } else if err := writeUint64To(w, uint64(t.IndexIdToSeriesFileId.Size), &n); err != nil {
	// 	return n, err
	// }

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
	Offset        int64
	Size          int64
	MeasurementID uint64

	// todo(vinland): have not been compacted to measurement block
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

type IndexFile struct {
	// id    int
	// level int
	name string

	gridBlock []byte
	mblk      MeasurementBlock
}

func NewIndexFile(name string) *IndexFile {
	return &IndexFile{
		name: name,
	}
}

func (ifile *IndexFile) Restore() error {
	// if not read data, read it
	// reference count ++
	// now, only read
	// todo(vinland): use name format to generate filepath with id and level
	buf, err := ioutil.ReadFile(ifile.name)
	if err != nil {
		return err
	}
	fileSize := len(buf)

	msz := int64(binary.BigEndian.Uint64(buf[fileSize-10 : fileSize-2]))
	moffset := int64(binary.BigEndian.Uint64(buf[fileSize-18 : fileSize-10]))
	ifile.gridBlock = buf[:moffset]

	// Unmarshal into a block.
	err = ifile.mblk.UnmarshalBinary(buf[moffset : moffset+msz])
	if err != nil {
		return err
	}

	return nil
}

func (ifile *IndexFile) SeriesIDSet(name []byte) *tsdb.SeriesIDSet {
	resSet := tsdb.NewSeriesIDSet()
	e, ok := ifile.mblk.Elem(name)
	if !ok {
		return resSet
	}
	e.SeriesIDSet().ForEachNoLock(func(id uint64) {
		if v, ok := e.FormatIdWithMeasurementID(id); ok {
			resSet.AddNoLock(v)
		}
	})
	return e.SeriesIDSet()
}

func (ifile *IndexFile) SeriesIDSetForTagKey(name, key []byte) *tsdb.SeriesIDSet {
	resSet := tsdb.NewSeriesIDSet()

	e, ok := ifile.mblk.Elem(name)
	if !ok {
		return resSet
	}

	// todo(vinland): can judge first
	grids, err := DecodeGrids(ifile.gridBlock, e)
	if err != nil {
		log.Fatalf("fail to decode grids")
		return resSet
	}
	for _, g := range grids {
		if g.HasTagKey(string(key)) {
			idsSet := g.GetSeriesIDSetForTags(nil)
			idsSet.ForEachNoLock(func(id uint64) {
				if v, ok := e.FormatIdWithMeasurementID(id); ok {
					resSet.AddNoLock(v)
				}
			})
		}
	}
	return resSet
}

func (ifile *IndexFile) SeriesIDSetForTagValue(name, key, value []byte) *tsdb.SeriesIDSet {
	resSet := tsdb.NewSeriesIDSet()

	e, ok := ifile.mblk.Elem(name)
	if !ok {
		return resSet
	}

	// todo(vinland): can judge first
	grids, err := DecodeGrids(ifile.gridBlock, e)
	if err != nil {
		log.Fatalf("fail to decode grids")
		return resSet
	}
	for _, g := range grids {
		if g.HasTagValue(string(key), string(value)) {
			idsSet := g.GetSeriesIDSetForTags(models.NewTags(
				map[string]string{
					string(key): string(value),
				},
			))
			idsSet.ForEachNoLock(func(id uint64) {
				if v, ok := e.FormatIdWithMeasurementID(id); ok {
					resSet.AddNoLock(v)
				}
			})
		}
	}
	return resSet
}

func DecodeGrids(buf []byte, e MeasurementBlockElem) ([]*Grid, error) {
	grids := make([]*Grid, 0, len(e.grids))
	for _, gridInfo := range e.grids {
		grid, err := DecodeGrid(buf[gridInfo.offset : gridInfo.offset+gridInfo.size])
		if err != nil {
			return nil, err
		}
		grids = append(grids, grid)
	}
	return grids, nil
}

func DecodeGrid(buf []byte) (*Grid, error) {
	offset, buf := uint64(binary.BigEndian.Uint64(buf[0:8])), buf[8:]

	keyNum, buf := uint64(binary.BigEndian.Uint64(buf[0:8])), buf[8:]
	keys := make([]string, 0, keyNum)
	var sz uint64
	for i := uint64(0); i < keyNum; i++ {
		sz, buf = uint64(binary.BigEndian.Uint64(buf[0:8])), buf[8:]
		value := buf[0:sz]
		keys = append(keys, string(value))
		buf = buf[sz:]
	}

	valueSliceNum, buf := uint64(binary.BigEndian.Uint64(buf[0:8])), buf[8:]
	valuesSlice := make([]*TagValues, 0, valueSliceNum)
	var capacity uint64
	var valuesNum uint64
	for i := uint64(0); i < valueSliceNum; i++ {
		capacity, buf = uint64(binary.BigEndian.Uint64(buf[0:8])), buf[8:]
		values := newTagValues(capacity)
		valuesNum, buf = uint64(binary.BigEndian.Uint64(buf[0:8])), buf[8:]
		for j := uint64(0); j < valuesNum; j++ {
			sz, buf = uint64(binary.BigEndian.Uint64(buf[0:8])), buf[8:]
			value := buf[0:sz]
			buf = buf[sz:]
			values.SetValue(string(value))
		}
		valuesSlice = append(valuesSlice, values)
	}

	// Parse data block size.
	sz, buf = uint64(binary.BigEndian.Uint64(buf[0:8])), buf[8:]

	ss := tsdb.NewSeriesIDSet()
	err := ss.UnmarshalBinaryUnsafe(buf[:sz])
	if err != nil {
		return nil, err
	}

	// fmt.Printf("offset: %v\nkeys:%+v\nvaluesSlice:%+v\n", offset, keys, valuesSlice)
	grid := NewGridWithKeysAndValuesSlice(offset, keys, valuesSlice, ss)
	return grid, nil
}
