package tsi2_test

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"

	"cycledb/pkg/tsdb/index/tsi2"
)

func TestInvertIndex(t *testing.T) {
	// local variable to overlap `gen` in tsi2_test package
	gen := generators[DiagonalGen]
	index := tsi2.NewInvertIndex()
	tagPairSets := gen.GenerateInsertTagsSlice(2, 5)
	// do insert
	for i, tagPairSet := range tagPairSets {
		_, id := index.SetTagPairSet(tagPairSet)
		assert.Equal(t, uint64(i+1), id)
	}
	// do query
	for i, tagPairSet := range tagPairSets {
		realIds := index.GetSeriesIDsWithTagPairSet(tagPairSet)
		assert.True(t, Contains(realIds, []uint64{uint64(i + 1)}))
	}
}

func TestLargeScaleInvertIndex(t *testing.T) {
	gen := generators[DiagonalGen]
	index := tsi2.NewInvertIndex()
	tagPairSets := gen.GenerateInsertTagsSlice(10, 20)
	ids := make([]uint64, 0, len(tagPairSets))

	for _, tagPairSet := range tagPairSets {
		_, id := index.SetTagPairSet(tagPairSet)
		ids = append(ids, id)
	}

	for i, tagPairSet := range tagPairSets {
		id := index.GetSeriesIDsWithTagPairSet(tagPairSet)
		assert.True(t, Contains(id, []uint64{ids[i]}))
	}
}

// go test -race
func TestIfThreadSafeForInvertIndex(t *testing.T) {
	gen := generators[DiagonalGen]
	index := tsi2.NewInvertIndex()
	tagPairSets := gen.GenerateInsertTagsSlice(10, 20)
	wantedIds := sync.Map{}
	insertRace := uint64(0)
	insertData := func(mod int) {
		for i, tagPairSet := range tagPairSets {
			if i%mod == 0 {
				newlyInsert, id := index.SetTagPairSet(tagPairSet)
				wantedIds.Store(i, id)
				if !newlyInsert {
					atomic.AddUint64(&insertRace, 1)
				}
			}
		}
	}
	calOverlap := func(total, modA, modB int) uint64 {
		res := uint64(0)
		for i := 0; i < total; i++ {
			if i%modA == 0 && i%modB == 0 {
				res++
			}
		}
		return res
	}
	queryData := func() {
		for _, query := range tagPairSets {
			index.GetSeriesIDsWithTagPairSet(query)
		}
	}
	var wg sync.WaitGroup
	wg.Add(4)
	go func() {
		insertData(1)
		wg.Done()
	}()
	go func() {
		insertData(2)
		wg.Done()
	}()
	go func() {
		queryData()
		wg.Done()
	}()
	go func() {
		queryData()
		wg.Done()
	}()
	wg.Wait()
	assert.Equal(t, calOverlap(len(tagPairSets), 1, 2), insertRace)
	for i, tagPairSet := range tagPairSets {
		id, ok := wantedIds.Load(i)
		assert.True(t, ok)
		assert.True(t, Contains(index.GetSeriesIDsWithTagPairSet(tagPairSet), []uint64{id.(uint64)}))
	}
}

func TestIfThreadSafeForSyncMap(t *testing.T) {
	m := sync.Map{}
	insertData := func() {
		for i := 0; i < 1000; i++ {
			m.Store(i, i)
		}
	}
	go insertData()
	go insertData()
}
