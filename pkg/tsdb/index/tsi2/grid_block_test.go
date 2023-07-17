package tsi2

import (
	"bytes"
	"cycledb/pkg/tsdb"
	"encoding/binary"
	"fmt"
	"os"
	"reflect"
	"testing"

	// "cycledb/pkg/tsdb/index/tsi2"

	"github.com/influxdata/influxdb/v2/models"
	"github.com/influxdata/influxdb/v2/pkg/testing/assert"
)

func TestSizeOfData(t *testing.T) {
	s := "string"
	assert.Equal(t, len(s), 6)
	assert.Equal(t, len(s), len([]byte(s)))
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

func TestEncodeGrid(t *testing.T) {
	cpuValues := newTagValues(5)
	cpuValues.SetValue("1")
	memoryValues := newTagValues(5)
	memoryValues.SetValue("16G")
	grid := NewGridWithSingleTags(10, models.NewTags(map[string]string{
		"cpu":    "1",
		"memory": "16G",
	}), []*TagValues{
		cpuValues,
		memoryValues,
	})

	id, success := grid.SetTags(models.Tags{
		{Key: []byte("cpu"), Value: []byte("2")},
		{Key: []byte("memory"), Value: []byte("32G")},
	})
	assert.Equal(t, success, true)
	assert.Equal(t, id, uint64(16))

	f, err := os.CreateTemp("./", "encodetest_")
	if err != nil {
		panic(fmt.Sprintf("failed to create temp file: %v", err))
	}
	t.Cleanup(func() {
		f.Close()
		os.Remove(f.Name())
	})

	enc := NewGridBlockEncoder(f)
	enc.EncodeGrid(grid)
	// fmt.Printf("enc.n: %d\n", enc.n)

	buf := make([]byte, enc.n)
	n, err := f.ReadAt(buf, 0)
	assert.Equal(t, err, nil)
	assert.NotEqual(t, n, 0)

	g, err := DecodeGrid(buf)
	assert.Equal(t, err, nil)

	assert.Equal(t, grid.offset, g.offset)
	assert.Equal(t, reflect.DeepEqual(grid.tagKeyToIndex, g.tagKeyToIndex), true)
	assert.Equal(t, reflect.DeepEqual(grid.tagKeys, g.tagKeys), true)
	assert.Equal(t, reflect.DeepEqual(grid.tagValuesSlice, g.tagValuesSlice), true)
	assert.Equal(t, grid.seriesIDSet.Cardinality(), g.seriesIDSet.Cardinality())
	// assert.Equal(t, reflect.DeepEqual(grid, g), true)
}

// func TestWriter(t *testing.T) {
// 	f, err := os.CreateTemp("./", "encodetest_")
// 	if err != nil {
// 		panic(fmt.Sprintf("failed to create temp file: %v", err))
// 	}
// 	t.Cleanup(func() {
// 		f.Close()
// 		os.Remove(f.Name())
// 	})
// 	n := int64(0)
// 	err = writeUint64To(f, uint64(1), &n)
// 	assert.Equal(t, err, nil)
// 	assert.Equal(t, n, int64(8))

// 	buf := make([]byte, 8)
// 	num, err := f.ReadAt(buf, 0)
// 	assert.Equal(t, err, nil)
// 	assert.Equal(t, num, 8)
// }

func TestSeriesIDSet(t *testing.T) {
	var buf bytes.Buffer

	ss := tsdb.NewSeriesIDSet(16)
	n, err := ss.WriteTo(&buf)

	assert.NotEqual(t, n, 0)
	assert.Equal(t, err, nil)

	sss := tsdb.NewSeriesIDSet()
	sss.UnmarshalBinaryUnsafe(buf.Bytes())

	assert.Equal(t, sss.Cardinality(), uint64(1))
	// assert.Equal(t, reflect.DeepEqual(ss, sss), true)
}

func TestSeriesIDSet2(t *testing.T) {
	ss := tsdb.NewSeriesIDSet(16)

	sss := tsdb.NewSeriesIDSet(16)

	assert.Equal(t, reflect.DeepEqual(ss, sss), true)
}
