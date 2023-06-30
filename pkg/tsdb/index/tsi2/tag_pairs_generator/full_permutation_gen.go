package generator

import (
	"fmt"

	"cycledb/pkg/tsdb/index/tsi2"
)

type FullPermutationGen struct{}

func (g *FullPermutationGen) GenerateInsertTagPairSets(tagKeyNum, tagValueNum int) [][]tsi2.TagPair {
	return g.generateFullPermutationTagSets(tagKeyNum, tagValueNum, false)
}

// GenerateQueryTagPairSets: responsible for formatting queries.
// 1. remove empty tag pairs, [[a:0],[],[c:1]]->[[a:0],[c:1]]
// 2. remove [[],[],[]] tatally, since grid index not support it
func (g *FullPermutationGen) GenerateQueryTagPairSets(tagKeyNum, tagValueNum int) [][]tsi2.TagPair {
	tagPairSets := g.generateFullPermutationTagSets(tagKeyNum, tagValueNum, true)
	for i := 0; i < len(tagPairSets); i++ {
		for j := 0; j < len(tagPairSets[i]); j++ {
			if len(tagPairSets[i][j].TagKey) == 0 {
				// it's safe to delete like this in Golang
				tagPairSets[i] = append(tagPairSets[i][:j], tagPairSets[i][j+1:]...)
				j--
			}
		}
		if len(tagPairSets[i]) == 0 {
			tagPairSets = append(tagPairSets[:i], tagPairSets[i+1:]...)
			i--
		}
	}
	return tagPairSets
}

// generateFullPermutationTagSets: returns all permutations of tag pairs.
// If allow empty, there will be empty tag pair, like [[a:0],[],[c:1]]
func (g *FullPermutationGen) generateFullPermutationTagSets(tagKeyNum, tagValueNum int, allowEmpty bool) [][]tsi2.TagPair {
	if tagKeyNum == 1 {
		return g.generateTagPairsForOneTagKey(tagKeyNum-1, tagValueNum, allowEmpty)
	}

	currLayer := g.generateTagPairsForOneTagKey(tagKeyNum-1, tagValueNum, allowEmpty)
	prevLayers := g.generateFullPermutationTagSets(tagKeyNum-1, tagValueNum, allowEmpty)
	res := make([][]tsi2.TagPair, 0, tsi2.PowUint64(tagValueNum, tagKeyNum))

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

func (g *FullPermutationGen) generateTagPairsForOneTagKey(tagKeyIndex, tagValueNum int, allowEmpty bool) [][]tsi2.TagPair {
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
