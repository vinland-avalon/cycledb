package tsi2_test

import (
	"testing"

	"cycledb/pkg/tsdb"
	"cycledb/pkg/tsdb/index/tsi2"

	"github.com/stretchr/testify/assert"
)

func TestInterface(t *testing.T) {
	var index tsdb.Index
	index = &tsi2.Index{}
	assert.NotNil(t, index)
}
