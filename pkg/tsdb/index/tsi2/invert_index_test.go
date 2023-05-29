package tsi2_test

import (
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
