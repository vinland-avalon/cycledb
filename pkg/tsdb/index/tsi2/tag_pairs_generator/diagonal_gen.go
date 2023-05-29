package generator

import (
	"fmt"

	"cycledb/pkg/tsdb/index/tsi2"
)

type DiagonalGenerator struct{}

func (g *DiagonalGenerator) GenerateInsertTagPairSets(tagKeyNum, tagValueNum int) [][]tsi2.TagPair {
	return GetDiagonalTagPairSetss(tagKeyNum, tagValueNum)
}
func (g *DiagonalGenerator) GenerateQueryTagPairSets(tagKeyNum, tagValueNum int) [][]tsi2.TagPair {
	fpGen := FullPermutationGen{}
	return fpGen.GenerateQueryTagPairSets(tagKeyNum, tagValueNum)
}

// GetDiagonalTagPairSetss: return many tag pair sets.
// For example, tagKeyNum = 2, tagValueNum = 5, then return
// [[{a 0} {b 0}] [{a 1} {b 1}] [{a 2} {b 2}] [{a 3} {b 3}] [{a 4} {b 4}]].
// Another example, tagKeyNum = 2, tagValueNum = 5, then return
// [[{a 0} {b 0} {c 0}] [{a 1} {b 1} {c 1}]]
func GetDiagonalTagPairSetss(tagKeyNum, tagValueNum int) [][]tsi2.TagPair {
	tagPairSets := [][]tsi2.TagPair{}
	for i := 0; i < tagValueNum; i++ {
		tagPairSet := []tsi2.TagPair{}
		for j := 0; j < tagKeyNum; j++ {
			tagPairSet = append(tagPairSet, tsi2.TagPair{
				TagKey:   fmt.Sprintf("%c", 'a'+j),
				TagValue: fmt.Sprintf("%d", i),
			})
		}
		tagPairSets = append(tagPairSets, tagPairSet)
	}
	return tagPairSets
}
