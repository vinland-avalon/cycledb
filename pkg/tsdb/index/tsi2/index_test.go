package tsi2_test

import (
	"fmt"
	"testing"

	"cycledb/pkg/tsdb/index/tsi2"

	"github.com/stretchr/testify/assert"
)

func GetTagPairsExample(suffix string) []tsi2.TagPair {
	tagPairs := []tsi2.TagPair{
		{
			TagKey:   "a",
			TagValue: "0" + suffix,
		},
		{
			TagKey:   "b",
			TagValue: "1" + suffix,
		},
		{
			TagKey:   "c",
			TagValue: "2" + suffix,
		},
		{
			TagKey:   "d",
			TagValue: "3" + suffix,
		},
	}
	return tagPairs
}

func TestInitAndGetSeriesID(t *testing.T) {
	gi := tsi2.NewGridIndex(tsi2.NewMultiplierOptimizer(10, 1))
	// first series
	id := gi.InitNewSeriesID(GetTagPairsExample("0"))
	assert.Equal(t, id, int64(0))
	// insert the same series twice, should return false and 0
	id = gi.InitNewSeriesID(GetTagPairsExample("0"))

	assert.Equal(t, id, int64(0))
	// insert the second series
	id = gi.InitNewSeriesID(GetTagPairsExample("1"))
	assert.Equal(t, id, int64(1111))
	// insert the same series twice, should return false and 1111
	id = gi.InitNewSeriesID(GetTagPairsExample("1"))

	assert.Equal(t, id, int64(1111))

	// get ids of tag pairs (specific)
	ids := gi.GetSeriesIDsWithTagPairs(GetTagPairsExample("0"))
	assert.Equal(t, ids, []int64{int64(0)})
	ids = gi.GetSeriesIDsWithTagPairs(GetTagPairsExample("1"))
	assert.Equal(t, ids, []int64{int64(1111)})
	ids = gi.GetSeriesIDsWithTagPairs(GetTagPairsExample("2"))
	assert.Equal(t, ids, []int64{})

	// get ids of tag pairs (multiple ids)
	tagPairs := GetTagPairsExample("2")
	// incoming tagpairs: [a, 02][b, 12][c, 21][d, 32]
	// similar to previous one: [a, 01][b, 11][c, 21][d, 31] for c
	tagPairs[2].TagValue = "21"
	id = gi.InitNewSeriesID(tagPairs)
	assert.Equal(t, id, int64(2212))
	// when looking up with {c, 21}, should return multiple ids
	ids = gi.GetSeriesIDsWithTagPairs([]tsi2.TagPair{
		{
			TagKey:   "c",
			TagValue: "21",
		},
	})
	assert.Equal(t, Contains(ids, []int64{int64(2212), int64(1111)}), true)
}

func Contains(a, b []int64) bool {
	m := map[int64]struct{}{}
	for _, v := range a {
		m[v] = struct{}{}
	}
	for _, v := range b {
		if _, ok := m[v]; !ok {
			return false
		}
	}
	return true
}

// GetManyTagPairs: return many tag pair sets.
// For example, tagKeyNum = 2, tagValueNum = 5, then return
// [[{a 0} {b 0}] [{a 1} {b 1}] [{a 2} {b 2}] [{a 3} {b 3}] [{a 4} {b 4}]].
// Another example, tagKeyNum = 2, tagValueNum = 5, then return
// [[{a 0} {b 0} {c 0}] [{a 1} {b 1} {c 1}]]
func GetManyTagPairs(tagKeyNum, tagValueNum int) [][]tsi2.TagPair {
	manyTagPairs := [][]tsi2.TagPair{}
	for i := 0; i < tagValueNum; i++ {
		tagPairs := []tsi2.TagPair{}
		for j := 0; j < tagKeyNum; j++ {
			tagPairs = append(tagPairs, tsi2.TagPair{
				TagKey:   fmt.Sprintf("%c", 'a'+j),
				TagValue: fmt.Sprintf("%d", i),
			})
		}
		manyTagPairs = append(manyTagPairs, tagPairs)
	}
	return manyTagPairs
}

func TestMultiGrid(t *testing.T) {
	gi := tsi2.NewGridIndex(tsi2.NewMultiplierOptimizer(2, 1))
	manyTagPairs := GetManyTagPairs(2, 5)
	manyTagPairs2 := GetManyTagPairs(3, 2)
	manyTagPairs = append(manyTagPairs, manyTagPairs2...)
	wanted := []int64{0, 3, 4, 7, 8, 12, 19}
	for i, tagPairs := range manyTagPairs {
		id := gi.InitNewSeriesID(tagPairs)
		assert.Equal(t, wanted[i], id)
	}

	for i, tagPairs := range manyTagPairs {
		ids := gi.GetSeriesIDsWithTagPairs(tagPairs)
		assert.True(t, Contains(ids, []int64{wanted[i]}))
	}
}

func TestMultiGridWithMultiplier(t *testing.T) {
	gi := tsi2.NewGridIndex(tsi2.NewMultiplierOptimizer(2, 2))
	manyTagPairs := GetManyTagPairs(2, 5)
	manyTagPairs2 := GetManyTagPairs(3, 2)
	manyTagPairs = append(manyTagPairs, manyTagPairs2...)
	// tagKeyNum = 2, tagValueNum = 5
	// [[{a 0} {b 0}] [{a 1} {b 1}]     [{a 2} {b 2}] [{a 3} {b 3}] [{a 4} {b 4}]].
	//            2 * 2									4 * 4
	//        0             3                 4             9            14
	// tagKeyNum = 2, tagValueNum = 5
	// [[{a 0} {b 0} {c 0}] [{a 1} {b 1} {c 1}]]
	// 					4 * 4 * 2
	//           20		            31
	wanted := []int64{0, 3, 4, 9, 14, 20, 31}
	for i, tagPairs := range manyTagPairs {
		id := gi.InitNewSeriesID(tagPairs)
		assert.Equal(t, wanted[i], id)
	}

	for i, tagPairs := range manyTagPairs {
		ids := gi.GetSeriesIDsWithTagPairs(tagPairs)
		assert.True(t, Contains(ids, []int64{wanted[i]}))
	}
}

func TestMultiGridWithMultiplier2(t *testing.T) {
	gi := tsi2.NewGridIndex(tsi2.NewMultiplierOptimizer(2, 2))
	manyTagPairs := GenerateInsertTagPairs(4, 10)
	ids := make([]int64, 0, len(manyTagPairs))

	for _, tagPairs := range manyTagPairs {
		id := gi.InitNewSeriesID(tagPairs)
		ids = append(ids, id)
	}

	for i, tagPairs := range manyTagPairs {
		id := gi.GetSeriesIDsWithTagPairs(tagPairs)
		assert.True(t, Contains(id, []int64{ids[i]}))
	}
}
