package tsi2_test

import (
	"testing"

	"cycledb/pkg/tsdb/index/tsi2"

	"github.com/influxdata/influxdb/v2/pkg/testing/assert"
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
	gi := tsi2.NewGridIndex()
	// first series
	ok, id := gi.InitNewSeriesID(GetTagPairsExample("0"))
	assert.Equal(t, ok, true)
	assert.Equal(t, id, int64(0))
	// insert the same series twice, should return false and 0
	ok, id = gi.InitNewSeriesID(GetTagPairsExample("0"))
	assert.Equal(t, ok, false)
	assert.Equal(t, id, int64(0))
	// insert the second series
	ok, id = gi.InitNewSeriesID(GetTagPairsExample("1"))
	assert.Equal(t, ok, true)
	assert.Equal(t, id, int64(1111))
	// insert the same series twice, should return false and 1111
	ok, id = gi.InitNewSeriesID(GetTagPairsExample("1"))
	assert.Equal(t, ok, false)
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
	ok, id = gi.InitNewSeriesID(tagPairs)
	assert.Equal(t, ok, true)
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
