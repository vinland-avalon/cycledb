package generator

import (
	"cycledb/pkg/tsdb/index/tsi2"
	"testing"

	"github.com/stretchr/testify/assert"
)

var (
	tagKeyNum   int = 3
	tagValueNum int = 4
)

var g FullPermutationGen

func TestGenerateQuery(t *testing.T) {
	queries := g.GenerateQueryTagPairs(tagKeyNum, tagValueNum)
	assert.Equal(t, tsi2.PowInt((tagValueNum+1), tagKeyNum)-1, len(queries))
	// fmt.Printf("%v\n", queries)
}

func TestGenerateInserts(t *testing.T) {
	inserts := g.GenerateInsertTagPairs(tagKeyNum, tagValueNum)
	assert.Equal(t, tsi2.PowInt(tagValueNum, tagKeyNum), len(inserts))
	// fmt.Printf("%v\n", inserts)
}
