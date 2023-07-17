// for test, genrate inserts and queries
package generator

import "github.com/influxdata/influxdb/v2/models"

type Generator interface {
	GenerateInsertTagsSlice(tagKeyNum, tagValueNum int) []models.Tags
	// GenerateQueryTagPairSets(tagKeyNum, tagValueNum int) [][]tsi2.TagPair
}
