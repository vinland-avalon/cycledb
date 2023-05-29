// for test, genrate inserts and queries
package generator

import "cycledb/pkg/tsdb/index/tsi2"

type Generator interface {
	GenerateInsertTagPairSets(tagKeyNum, tagValueNum int) [][]tsi2.TagPair
	// GenerateQueryTagPairSets(tagKeyNum, tagValueNum int) [][]tsi2.TagPair
}
