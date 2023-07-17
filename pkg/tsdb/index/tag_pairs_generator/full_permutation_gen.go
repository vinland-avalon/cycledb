package generator

import (
	"fmt"

	"cycledb/pkg/tsdb/index/tsi2"

	"github.com/influxdata/influxdb/v2/models"
)

type TagPair struct {
	TagKey   string
	TagValue string
}

type FullPermutationGen struct{}

func (g *FullPermutationGen) GenerateInsertTagsSlice(tagKeyNum, tagValueNum int) []models.Tags {
	return g.generate(tagKeyNum, tagValueNum)
}

// GenerateQueryTagsSlice: responsible for formatting queries.
// 1. remove empty tag pairs, [[a:0],[],[c:1]]->[[a:0],[c:1]]
// 2. remove [[],[],[]] tatally, since grid index not support it
func (g *FullPermutationGen) GenerateQueryTagsSlice(tagKeyNum, tagValueNum int) []models.Tags {
	panic("unimplemented")
}

func (g *FullPermutationGen) generate(tagKeyNum, tagValueNum int) []models.Tags {
	tags := models.Tags{}
	for i := 0; i< tagKeyNum; i++ {
		tags = append(tags, models.NewTag([]byte(fmt.Sprintf("%c", 'a'+i)), []byte("0")))
	}
	tagsSlice := make([]models.Tags, 0, tsi2.PowUint64(tagValueNum, tagKeyNum))
	dfs(&tagsSlice, tagKeyNum, tagValueNum, 0, &tags)
	return tagsSlice
}

func dfs(tagsSlice *[]models.Tags, tagKeyNum, tagValueNum, idx int, tags *models.Tags) {
	if idx == tagKeyNum {
		*tagsSlice = append(*tagsSlice, tags.Clone())
		return
	}
	for i := 0; i<tagValueNum;i++ {
		(*tags)[idx].Value = []byte(fmt.Sprintf("%d", i))
		dfs(tagsSlice, tagKeyNum, tagValueNum, idx+1, tags)
	}
}