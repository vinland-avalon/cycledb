package generator

import (
	"fmt"

	"cycledb/pkg/tsdb/index/tsi2"
)

type DiagonalGenerator struct{}

func (g *DiagonalGenerator) GenerateInsertTagPairs(tagKeyNum, tagValueNum int) [][]tsi2.TagPair {
	return GetManyTagPairs(tagKeyNum, tagValueNum)
}
func (g *DiagonalGenerator) GenerateQueryTagPairs(tagKeyNum, tagValueNum int) [][]tsi2.TagPair {
	fpGen := FullPermutationGen{}
	return fpGen.GenerateQueryTagPairs(tagKeyNum, tagValueNum)
}

// GetManyTagPairs: return many tag pair sets.
// For example, tagKeyNum = 2, tagValueNum = 5, then return
// [[{a 0} {b 0}] [{a 1} {b 1}] [{a 2} {b 2}] [{a 3} {b 3}] [{a 4} {b 4}]].
// Another example, tagKeyNum = 2, tagValueNum = 5, then return
// [[{a 0} {b 0} {c 0}] [{a 1} {b 1} {c 1}]]
func GetManyTagPairs(tagKeyNum, tagValueNum int) [][]tsi2.TagPair {
	manyTagPairs := [][]tsi2.TagPair{}
	for i := 0; i < tagValueNum; i++ {
		tagPairs := []tsi2.TagPair{}
		for j := 0; j < tagKeyNum; j++ {
			tagPairs = append(tagPairs, tsi2.TagPair{
				TagKey:   fmt.Sprintf("%c", 'a'+j),
				TagValue: fmt.Sprintf("%d", i),
			})
		}
		manyTagPairs = append(manyTagPairs, tagPairs)
	}
	return manyTagPairs
}
