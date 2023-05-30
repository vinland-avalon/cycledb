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
	tagPairSets := gen.GenerateInsertTagPairSets(2, 5)
	// do insert
	for i, tagPairSet := range tagPairSets {
		_, id := index.SetTagPairSet(tagPairSet)
		assert.Equal(t, int64(i), id)
	}
	// do query
	for i, tagPairSet := range tagPairSets {
		realIds := index.GetSeriesIDsWithTagPairSet(tagPairSet)
		assert.True(t, Contains(realIds, []int64{int64(i)}))
	}
}

func TestLargeScaleInvertIndex(t *testing.T) {
	gen := generators[DiagonalGen]
	index := tsi2.NewInvertIndex()
	tagPairSets := gen.GenerateInsertTagPairSets(10, 20)
	ids := make([]int64, 0, len(tagPairSets))

	for _, tagPairSet := range tagPairSets {
		_, id := index.SetTagPairSet(tagPairSet)
		ids = append(ids, id)
	}

	for i, tagPairSet := range tagPairSets {
		id := index.GetSeriesIDsWithTagPairSet(tagPairSet)
		assert.True(t, Contains(id, []int64{ids[i]}))
	}
}

// go test -race
func TestIfThreadSafeForInvertIndex(t *testing.T) {
	gen := generators[DiagonalGen]
	index := tsi2.NewInvertIndex()
	tagPairSets := gen.GenerateInsertTagPairSets(10, 20)
	wantedIds := sync.Map{}
	insertRace := int64(0)
	insertData := func(mod int) {
		for i, tagPairSet := range tagPairSets {
			if i%mod == 0 {
				newlyInsert, id := index.SetTagPairSet(tagPairSet)
				wantedIds.Store(i, id)
				if !newlyInsert {
					atomic.AddInt64(&insertRace, 1)
				}
			}
		}
	}
	calOverlap := func(total, modA, modB int) int64 {
		res := int64(0)
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
		assert.True(t, Contains(index.GetSeriesIDsWithTagPairSet(tagPairSet), []int64{id.(int64)}))
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
