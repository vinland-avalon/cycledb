package tsi2_test

import (
	"math/rand"
	"testing"
	"time"

	"cycledb/pkg/tsdb/index/tsi2"

	"github.com/stretchr/testify/assert"
)

func TestVariableBaseConvert(t *testing.T) {
	dimensions := [][]int{
		{-1, 2},
		{1, 3},
		{-1, 4},
	}
	prev := []int64{}
	if dimensions[0][0] != -1 {
		prev = append(prev, int64(dimensions[0][0]))
	} else {
		for i := 0; i < dimensions[0][1]; i++ {
			prev = append(prev, int64(i))
		}
	}
	res := tsi2.VariableBaseConvert(dimensions, 1, prev)
	assert.Equal(t, len(res), 8)
	assert.True(t, Contains(res, []int64{4, 5, 6, 7, 16, 17, 18, 19}))
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

func randomSelectTagPairSets(tagPairSets [][]tsi2.TagPair, queryNum int) [][]tsi2.TagPair {
	selectedSets := [][]tsi2.TagPair{}
	for i := 0; i < queryNum; i++ {
		rand.Seed(time.Now().UnixNano())
		selectedSets = append(selectedSets, tagPairSets[rand.Intn(len(tagPairSets))])
	}
	return selectedSets
}
