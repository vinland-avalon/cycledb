package tsi2_test

import (
	"testing"

	"cycledb/pkg/tsdb/index/tsi2"

	"github.com/stretchr/testify/assert"
)

func TestVariableBaseConvert(t *testing.T) {
	dimensions := [][]int{
		{-1, 2},
		{1, 3},
		{-1, 4},
	}
	prev := []uint64{}
	if dimensions[0][0] != -1 {
		prev = append(prev, uint64(dimensions[0][0]))
	} else {
		for i := 0; i < dimensions[0][1]; i++ {
			prev = append(prev, uint64(i))
		}
	}
	res := tsi2.VariableBaseConvert(dimensions, 1, prev)
	assert.Equal(t, len(res), 8)
	assert.True(t, ContainsUint64(res, []uint64{4, 5, 6, 7, 16, 17, 18, 19}))
}
