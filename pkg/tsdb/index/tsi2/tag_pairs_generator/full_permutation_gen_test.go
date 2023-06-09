package generator

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"cycledb/pkg/tsdb/index/tsi2"
)

var (
	tagKeyNum   int = 3
	tagValueNum int = 4
)

var g FullPermutationGen

func TestGenerateQuery(t *testing.T) {
	queries := g.GenerateQueryTagsSlice(tagKeyNum, tagValueNum)
	assert.Equal(t, tsi2.PowUint64((tagValueNum+1), tagKeyNum)-1, uint64(len(queries)))
	// fmt.Printf("%v\n", queries)
}

func TestGenerateInserts(t *testing.T) {
	inserts := g.GenerateInsertTagsSlice(tagKeyNum, tagValueNum)
	assert.Equal(t, tsi2.PowUint64(tagValueNum, tagKeyNum), uint64(len(inserts)))
	// fmt.Printf("%v\n", inserts)
}
