package tsi2_test

import (
	"sync"
	"testing"

	"cycledb/pkg/tsdb/index/tsi2"

	"github.com/stretchr/testify/assert"
)

func TestNothing(t *testing.T) {
	assert.True(t, true)
}

func TestInitAndGetSeriesID(t *testing.T) {
	gi := tsi2.NewGridIndex(tsi2.NewMultiplierOptimizer(10, 1))
	// first series
	id, ok := gi.SetTagPairSet(GetTagPairsExample("0"))
	assert.True(t, ok)
	assert.True(t, id == 1)
	// insert the same series twice, should return false and 0
	id, ok = gi.SetTagPairSet(GetTagPairsExample("0"))
	assert.True(t, id == 1)
	assert.True(t, !ok)
	// println(idSet.Cardinality())
	// idSet.ForEach(func (id uint64) {
	// 	println(id)
	// })
	// insert the second series
	id, ok = gi.SetTagPairSet(GetTagPairsExample("1"))
	assert.Equal(t, uint64(1112), id)
	assert.True(t, ok)
	// insert the same series twice, should return false and 1111
	id, ok = gi.SetTagPairSet(GetTagPairsExample("1"))
	assert.Equal(t, uint64(1112), id)
	assert.True(t, !ok)
	// get ids of tag pairs (specific)
	idSet := gi.GetSeriesIDsWithTagPairSet(GetTagPairsExample("0"))
	assert.True(t, idSet.Contains(1))
	idSet = gi.GetSeriesIDsWithTagPairSet(GetTagPairsExample("1"))
	assert.True(t, idSet.Contains(1112))
	idSet = gi.GetSeriesIDsWithTagPairSet(GetTagPairsExample("2"))
	assert.True(t, !idSet.Contains(1112))

	// get ids of tag pairs (multiple ids)
	tagPairSet := GetTagPairsExample("2")
	// incoming tagpairs: [a, 02][b, 12][c, 21][d, 32]
	// similar to previous one: [a, 01][b, 11][c, 21][d, 31] for c
	tagPairSet[2].TagValue = "21"
	id, ok = gi.SetTagPairSet(tagPairSet)
	assert.Equal(t, id, uint64(2213))
	assert.True(t, ok)
	// when looking up with {c, 21}, should return multiple ids
	idSet = gi.GetSeriesIDsWithTagPairSet([]tsi2.TagPair{
		{
			TagKey:   "c",
			TagValue: "21",
		},
	})
	assert.Equal(t, uint64(2), idSet.Cardinality())
	for _, id := range []uint64{2213, 1112} {
		assert.True(t, idSet.Contains(id))
	}
}

func TestMultiGrid(t *testing.T) {
	// local variable to overlap `gen` in tsi2_test package
	gen := generators[DiagonalGen]
	gi := tsi2.NewGridIndex(tsi2.NewMultiplierOptimizer(2, 1))
	tagPairSets := gen.GenerateInsertTagPairSets(2, 5)
	tagPairSets = append(tagPairSets, gen.GenerateInsertTagPairSets(3, 2)...)
	wanted := []uint64{0, 3, 4, 7, 8, 12, 19}
	for i, tagPairSet := range tagPairSets {
		id, ok := gi.SetTagPairSet(tagPairSet)
		assert.Equal(t, wanted[i]+1, id)
		assert.True(t, ok)
	}

	for i, tagPairs := range tagPairSets {
		ids := gi.GetSeriesIDsWithTagPairSet(tagPairs)
		assert.True(t, ids.Contains(wanted[i]+1))
	}
}

func TestMultiGridWithMultiplier(t *testing.T) {
	// local variable to overlap `gen` in tsi2_test package
	gen := generators[DiagonalGen]
	gi := tsi2.NewGridIndex(tsi2.NewMultiplierOptimizer(2, 2))
	tagPairSets := gen.GenerateInsertTagPairSets(2, 5)
	tagPairSets = append(tagPairSets, gen.GenerateInsertTagPairSets(3, 2)...)
	// tagKeyNum = 2, tagValueNum = 5
	// [[{a 0} {b 0}] [{a 1} {b 1}]     [{a 2} {b 2}] [{a 3} {b 3}] [{a 4} {b 4}]].
	//            2 * 2									4 * 4
	//        0             3                 4             9            14
	// tagKeyNum = 2, tagValueNum = 5
	// [[{a 0} {b 0} {c 0}] [{a 1} {b 1} {c 1}]]
	// 					4 * 4 * 2
	//           20		            31
	wanted := []uint64{0, 3, 4, 9, 14, 20, 31}
	for i, tagPairSet := range tagPairSets {
		id, ok := gi.SetTagPairSet(tagPairSet)
		assert.Equal(t, wanted[i]+1, id)
		assert.True(t, ok)
	}

	for i, tagPairSet := range tagPairSets {
		ids := gi.GetSeriesIDsWithTagPairSet(tagPairSet)
		assert.True(t, ids.Contains(wanted[i]+1))
	}
}

func TestMultiGridWithMultiplier2(t *testing.T) {
	gen := generators[DiagonalGen]
	gi := tsi2.NewGridIndex(tsi2.NewMultiplierOptimizer(2, 2))
	tagPairSets := gen.GenerateInsertTagPairSets(10, 20)
	ids := make([]uint64, 0, len(tagPairSets))

	for _, tagPairSet := range tagPairSets {
		id, ok := gi.SetTagPairSet(tagPairSet)
		ids = append(ids, id)
		assert.True(t, ok)
	}

	for i, tagPairSet := range tagPairSets {
		idSet := gi.GetSeriesIDsWithTagPairSet(tagPairSet)
		assert.Equal(t, uint64(1), idSet.Cardinality())
		assert.True(t, idSet.Contains(ids[i]))
	}
}

func TestIfThreadSafeForGridIndex(t *testing.T) {
	gen := generators[DiagonalGen]
	index := tsi2.NewGridIndex(tsi2.NewMultiplierOptimizer(2, 2))
	tagPairSets := gen.GenerateInsertTagPairSets(10, 20)
	wantedIds := sync.Map{}
	queryTagPairSets := randomSelectTagPairSets(tagPairSets, queryNum)
	insertData := func(mod int) {
		for i, tagPairSet := range tagPairSets {
			if i%mod == 0 {
				id, okInsert := index.SetTagPairSet(tagPairSet)
				if existedId, ok := wantedIds.Load(i); ok {
					assert.Equal(t, existedId.(uint64), id)
					assert.True(t, !okInsert)
				} else {
					wantedIds.Store(i, id)
					assert.True(t, okInsert)
				}
			}
		}
	}
	queryData := func() {
		for _, query := range queryTagPairSets {
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
	for i, tagPairSet := range tagPairSets {
		id, ok := wantedIds.Load(i)
		assert.True(t, ok)
		idSet := index.GetSeriesIDsWithTagPairSet(tagPairSet)
		assert.True(t, idSet.Contains(id.(uint64)))
	}
}
