package generator

import (
	"testing"

	"github.com/influxdata/influxdb/v2/models"
	"github.com/stretchr/testify/assert"

	"cycledb/pkg/tsdb/index/tsi2"
)

var (
	tagKeyNum   int = 3
	tagValueNum int = 4
)

var g FullPermutationGen

// func TestGenerateQuery(t *testing.T) {
// 	queries := g.GenerateQueryTagsSlice(tagKeyNum, tagValueNum)
// 	assert.Equal(t, tsi2.PowUint64((tagValueNum+1), tagKeyNum)-1, uint64(len(queries)))
// 	// fmt.Printf("%v\n", queries)
// }

func TestGenerateInserts(t *testing.T) {
	inserts := g.GenerateInsertTagsSlice(tagKeyNum, tagValueNum)
	assert.Equal(t, tsi2.PowUint64(tagValueNum, tagKeyNum), uint64(len(inserts)))
	// fmt.Printf("%v\n", inserts)
}

func TestGenerateInsertsCnt(t *testing.T) {
	inserts := g.generate(4, 10)
	m := map[string]struct{}{}
	for _, insert := range inserts {
		key := models.MakeKey([]byte("new"), insert)
		m[string(key)] = struct{}{}
	}
	// fmt.Printf("%v\n", string(models.MakeKey([]byte("new"), inserts[2])))
	assert.Equal(t, 10000, len(m))
}
