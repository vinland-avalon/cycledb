package tsi2_test

import (
	"testing"

	"cycledb/pkg/tsdb/index/tsi2"

	"github.com/stretchr/testify/assert"
)

func TestVariableBaseConvert(t *testing.T) {
	indexes := []int{-1, 1, -1}
	capabilities := []uint64{2, 3, 4}
	prev := []uint64{}
	if indexes[0] != -1 {
		prev = append(prev, uint64(indexes[0]))
	} else {
		for i := uint64(0); i < capabilities[0]; i++ {
			prev = append(prev, uint64(i))
		}
	}
	res := tsi2.VariableBaseConvert(indexes, capabilities, 1, prev)
	assert.Equal(t, len(res), 8)
	assert.True(t, ContainsUint64(res, []uint64{4, 5, 6, 7, 16, 17, 18, 19}))
}
