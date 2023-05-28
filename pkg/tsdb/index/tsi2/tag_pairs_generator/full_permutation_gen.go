package generator

import (
	"fmt"

	"cycledb/pkg/tsdb/index/tsi2"
)

type Full_Permutation_Gen struct{}

// generateFullPermutationTags: returns all permutations of tag pairs.
// If allow empty, there will be empty tag pair, like [[a:0],[],[c:1]]
func (g *Full_Permutation_Gen) generateFullPermutationTags(tagKeyNum, tagValueNum int, allowEmpty bool) [][]tsi2.TagPair {
	if tagKeyNum == 1 {
		return g.generateTagPairs(tagKeyNum-1, tagValueNum, allowEmpty)
	}

	currLayer := g.generateTagPairs(tagKeyNum-1, tagValueNum, allowEmpty)
	prevLayers := g.generateFullPermutationTags(tagKeyNum-1, tagValueNum, allowEmpty)
	res := make([][]tsi2.TagPair, 0, tsi2.PowInt(tagValueNum, tagKeyNum))

	for _, curr := range currLayer {
		for _, prev := range prevLayers {
			if len(curr) > 0 {
				res = append(res, append(prev, curr[0]))
			} else {
				res = append(res, append(prev, tsi2.TagPair{}))
			}
		}
	}
	return res
}

func (g *Full_Permutation_Gen) generateTagPairs(tagKeyIndex, tagValueNum int, allowEmpty bool) [][]tsi2.TagPair {
	res := make([][]tsi2.TagPair, 0, tagValueNum)
	key := fmt.Sprintf("%c", 'a'+tagKeyIndex)
	if allowEmpty {
		res = append(res, []tsi2.TagPair{})
	}
	for i := 0; i < tagValueNum; i++ {
		res = append(res, []tsi2.TagPair{
			{
				TagKey:   key,
				TagValue: fmt.Sprintf("%d", i),
			},
		})
	}
	return res
}

func (g *Full_Permutation_Gen) GenerateInsertTagPairs(tagKeyNum, tagValueNum int) [][]tsi2.TagPair {
	return g.generateFullPermutationTags(tagKeyNum, tagValueNum, false)
}

// GenerateQueryTagPairs: responsible for formatting queries.
// 1. remove empty tag pairs, [[a:0],[],[c:1]]->[[a:0],[c:1]]
// 2. remove [[],[],[]] tatally, since grid index not support it
func (g *Full_Permutation_Gen) GenerateQueryTagPairs(tagKeyNum, tagValueNum int) [][]tsi2.TagPair {
	manyTagPairs := g.generateFullPermutationTags(tagKeyNum, tagValueNum, true)
	for i := 0; i < len(manyTagPairs); i++ {
		for j := 0; j < len(manyTagPairs[i]); j++ {
			if len(manyTagPairs[i][j].TagKey) == 0 {
				// it's safe in Golang
				manyTagPairs[i] = append(manyTagPairs[i][:j], manyTagPairs[i][j+1:]...)
				j--
			}
		}
		if len(manyTagPairs[i]) == 0 {
			manyTagPairs = append(manyTagPairs[:i], manyTagPairs[i+1:]...)
			i--
		}
	}
	return manyTagPairs
}
