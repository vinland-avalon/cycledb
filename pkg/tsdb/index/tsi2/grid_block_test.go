package tsi2

import (
	"fmt"
	"os"
	"reflect"
	"testing"

	"github.com/influxdata/influxdb/v2/models"
	"github.com/influxdata/influxdb/v2/pkg/testing/assert"
)

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
