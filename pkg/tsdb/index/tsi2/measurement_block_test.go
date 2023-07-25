package tsi2

import (
	"bytes"
	"reflect"
	"testing"

	"cycledb/pkg/tsdb"
)

type MeasurementInfo struct {
	Name   []byte
	Offset int64
	Size   int64
	idsSet *tsdb.SeriesIDSet
}

func NewMeasurementInfo(name []byte, offset, size int64, idsSet *tsdb.SeriesIDSet) MeasurementInfo {
	return MeasurementInfo{
		Name:   name,
		Offset: offset,
		Size:   size,
		idsSet: idsSet,
	}
}

// Ensure measurement blocks can be written and opened.
func TestMeasurementBlockWriter(t *testing.T) {
	ms := []MeasurementInfo{
		NewMeasurementInfo([]byte("foo"), 100, 10, tsdb.NewSeriesIDSet([]uint64{1, 3, 4}...)),
		NewMeasurementInfo([]byte("bar"), 200, 20, tsdb.NewSeriesIDSet([]uint64{2}...)),
		NewMeasurementInfo([]byte("baz"), 300, 30, tsdb.NewSeriesIDSet([]uint64{5, 6}...)),
	}

	grids := [][]*GridCompactInfo{
		{
			{offset: 100, size: 20},
		},
		{
			{offset: 200, size: 20},
			{offset: 200, size: 40},
		},
		{
			{offset: 300, size: 20},
			{offset: 300, size: 40},
			{offset: 300, size: 60},
		},
	}

	// Write the measurements to writer.
	mw := NewMeasurementBlockWriter()
	for i, m := range ms {
		mw.Add(m.Name, &IndexFileMeasurementCompactInfo{Offset: m.Offset, Size: m.Size, gridInfos: grids[i], MeasurementID: uint64(i)}, m.idsSet)
	}

	// Encode into buffer.
	var buf bytes.Buffer
	if n, err := mw.WriteTo(&buf); err != nil {
		t.Fatal(err)
	} else if n == 0 {
		t.Fatal("expected bytes written")
	}

	// Unmarshal into a block.
	var blk MeasurementBlock
	if err := blk.UnmarshalBinary(buf.Bytes()); err != nil {
		t.Fatal(err)
	}

	// Verify data in block.
	if e, ok := blk.Elem([]byte("foo")); !ok {
		t.Fatal("expected element")
	} else if e.gridsBlock.offset != 100 || e.gridsBlock.size != 10 {
		t.Fatalf("unexpected offset/size: %v/%v", e.gridsBlock.offset, e.gridsBlock.size)
	} else if e.seriesIDSet.Cardinality() != 3 {
		t.Fatalf("unexpected series data: %#v", e.seriesIDSet)
	} else if reflect.DeepEqual(grids[0], e.grids[0]) {
		t.Fatalf("unexpected grids: %+v", e.grids)
	} else if e.id != 0 {
		t.Fatalf("unexpected id: %+v", e.id)
	}

	if e, ok := blk.Elem([]byte("bar")); !ok {
		t.Fatal("expected element")
	} else if e.gridsBlock.offset != 200 || e.gridsBlock.size != 20 {
		t.Fatalf("unexpected offset/size: %v/%v", e.gridsBlock.offset, e.gridsBlock.size)
	} else if e.seriesIDSet.Cardinality() != 1 {
		t.Fatalf("unexpected series data: %#v", e.seriesIDSet)
	} else if reflect.DeepEqual(grids[1], e.grids[1]) {
		t.Fatalf("unexpected grids: %+v", e.grids)
	}  else if e.id != 1 {
		t.Fatalf("unexpected id: %+v", e.id)
	}

	if e, ok := blk.Elem([]byte("baz")); !ok {
		t.Fatal("expected element")
	} else if e.gridsBlock.offset != 300 || e.gridsBlock.size != 30 {
		t.Fatalf("unexpected offset/size: %v/%v", e.gridsBlock.offset, e.gridsBlock.size)
	} else if e.seriesIDSet.Cardinality() != 2 {
		t.Fatalf("unexpected series data: %#v", e.seriesIDSet)
	} else if reflect.DeepEqual(grids[2], e.grids[2]) {
		t.Fatalf("unexpected grids: %+v", e.grids)
	}  else if e.id != 2 {
		t.Fatalf("unexpected id: %+v", e.id)
	}

	// Verify non-existent measurement doesn't exist.
	if _, ok := blk.Elem([]byte("BAD_MEASUREMENT")); ok {
		t.Fatal("expected no element")
	}
}
