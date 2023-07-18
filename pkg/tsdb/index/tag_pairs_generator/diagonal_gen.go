package generator

import (
	"fmt"

	"github.com/influxdata/influxdb/v2/models"
)

type DiagonalGenerator struct{}

func (g *DiagonalGenerator) GenerateInsertTagsSlice(tagKeyNum, tagValueNum int) []models.Tags {
	return GetDiagonalTagsSlice(tagKeyNum, tagValueNum)
}
func (g *DiagonalGenerator) GenerateQueryTagsSlice(tagKeyNum, tagValueNum int) []models.Tags {
	fpGen := FullPermutationGen{}
	return fpGen.GenerateQueryTagsSlice(tagKeyNum, tagValueNum)
}

// GetDiagonalTagsSlices: return many tag pair sets.
// For example, tagKeyNum = 2, tagValueNum = 5, then return
// [[{a 0} {b 0}] [{a 1} {b 1}] [{a 2} {b 2}] [{a 3} {b 3}] [{a 4} {b 4}]].
// Another example, tagKeyNum = 2, tagValueNum = 5, then return
// [[{a 0} {b 0} {c 0}] [{a 1} {b 1} {c 1}]]
func GetDiagonalTagsSlice(tagKeyNum, tagValueNum int) []models.Tags {
	tagsSlice := []models.Tags{}
	for i := 0; i < tagValueNum; i++ {
		m := map[string]string{}
		for j := 0; j < tagKeyNum; j++ {
			m[fmt.Sprintf("%c", 'a'+j)] = fmt.Sprintf("%d", i)
		}
		tags := models.NewTags(m)
		tagsSlice = append(tagsSlice, tags)
	}
	return tagsSlice
}
