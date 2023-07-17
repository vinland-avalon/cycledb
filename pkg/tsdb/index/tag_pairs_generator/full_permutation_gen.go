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
	TagsSlice := g.generateFullPermutationTagsSlice(tagKeyNum, tagValueNum, true)
	for i := 0; i < len(TagsSlice); i++ {
		for j := 0; j < len(TagsSlice[i]); j++ {
			if len(TagsSlice[i][j].TagKey) == 0 {
				// it's safe to delete like this in Golang
				TagsSlice[i] = append(TagsSlice[i][:j], TagsSlice[i][j+1:]...)
				j--
			}
		}
		if len(TagsSlice[i]) == 0 {
			TagsSlice = append(TagsSlice[:i], TagsSlice[i+1:]...)
			i--
		}
	}
	return ConvertTagsSliceToModelTagsSlice(TagsSlice)
}

func ConvertTagsSliceToModelTagsSlice(tss [][]TagPair) []models.Tags {
	modelTagsSlice := make([]models.Tags, 0, len(tss))
	for _, ts := range tss {
		m := map[string]string{}
		for _, t := range ts {
			m[t.TagKey] = t.TagValue
		}
		modelTagsSlice = append(modelTagsSlice, models.NewTags(m))
	}
	return modelTagsSlice
}

// generateFullPermutationTagsSlice: returns all permutations of tag pairs.
// If allow empty, there will be empty tag pair, like [[a:0],[],[c:1]]
func (g *FullPermutationGen) generateFullPermutationTagsSlice(tagKeyNum, tagValueNum int, allowEmpty bool) [][]TagPair {
	if tagKeyNum == 1 {
		return g.generateTagPairsForOneTagKey(tagKeyNum-1, tagValueNum, allowEmpty)
	}

	currLayer := g.generateTagPairsForOneTagKey(tagKeyNum-1, tagValueNum, allowEmpty)
	prevLayers := g.generateFullPermutationTagsSlice(tagKeyNum-1, tagValueNum, allowEmpty)
	res := make([][]TagPair, 0, tsi2.PowUint64(tagValueNum, tagKeyNum))

	for _, curr := range currLayer {
		for _, prev := range prevLayers {
			if len(curr) > 0 {
				res = append(res, append(prev, curr[0]))
			} else {
				res = append(res, append(prev, TagPair{}))
			}
		}
	}
	return res
}

func (g *FullPermutationGen) generateTagPairsForOneTagKey(tagKeyIndex, tagValueNum int, allowEmpty bool) [][]TagPair {
	res := make([][]TagPair, 0, tagValueNum)
	key := fmt.Sprintf("%c", 'a'+tagKeyIndex)
	if allowEmpty {
		res = append(res, []TagPair{})
	}
	for i := 0; i < tagValueNum; i++ {
		res = append(res, []TagPair{
			{
				TagKey:   key,
				TagValue: fmt.Sprintf("%d", i),
			},
		})
	}
	return res
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