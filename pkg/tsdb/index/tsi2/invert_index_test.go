package tsi2_test

import (
	"sync"
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
		_, id := index.InitNewSeriesID(tagPairSet)
		assert.Equal(t, int64(i), id)
	}
	// do query
	for i, tagPairSet := range tagPairSets {
		realIds := index.GetSeriesIDsWithTagPairs(tagPairSet)
		assert.True(t, Contains(realIds, []int64{int64(i)}))
	}
}

func TestLargeScaleInvertIndex(t *testing.T) {
	gen := generators[DiagonalGen]
	index := tsi2.NewInvertIndex()
	tagPairSets := gen.GenerateInsertTagPairSets(10, 20)
	ids := make([]int64, 0, len(tagPairSets))

	for _, tagPairSet := range tagPairSets {
		_, id := index.InitNewSeriesID(tagPairSet)
		ids = append(ids, id)
	}

	for i, tagPairSet := range tagPairSets {
		id := index.GetSeriesIDsWithTagPairs(tagPairSet)
		assert.True(t, Contains(id, []int64{ids[i]}))
	}
}

// go test -race
func TestIfThreadSafe(t *testing.T) {
	gen := generators[DiagonalGen]
	index := tsi2.NewInvertIndex()
	tagPairSets := gen.GenerateInsertTagPairSets(10, 20)
	insertData := func (){
		for _, tagPairSet := range tagPairSets {
			index.InitNewSeriesID(tagPairSet)
		}
	}
	go insertData()
	go insertData()
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
