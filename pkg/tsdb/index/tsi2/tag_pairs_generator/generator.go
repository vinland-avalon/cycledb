// for test, genrate inserts and queries
package generator

import "cycledb/pkg/tsdb/index/tsi2"

type Generator interface {
	GenerateInsertTagPairs(tagKeyNum, tagValueNum int) [][]tsi2.TagPair
	GenerateQueryTagPairs(tagKeyNum, tagValueNum int) [][]tsi2.TagPair
}