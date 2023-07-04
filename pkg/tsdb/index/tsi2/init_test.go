package tsi2_test

import (
	"flag"
	"fmt"
	"math/rand"
	"testing"
	"time"

	generator "cycledb/pkg/tsdb/index/tsi2/tag_pairs_generator"

	"github.com/influxdata/influxdb/v2/models"
)

var (
	tagKeyNum   int = 3
	tagValueNum int = 4
	// generators
	generators map[string]generator.Generator
	genID      string = FPGen
	gen        generator.Generator
)

const (
	FPGen       = "full_permutation_generator"
	DiagonalGen = "diagonal_generator"
	RandomGen   = "random_generator"
	queryNum    = 30
)

func init() {
	flag.IntVar(&tagKeyNum, "tagKeyNum", 3, "number of tag key")
	flag.IntVar(&tagValueNum, "tagValueNum", 4, "number of tag value for each tag Key")
	flag.StringVar(&genID, "seriesKeyGenerator", FPGen, "generator for tag pairs for benchmark, including full_permutation_generator, diagonal_generator and random_generator")
	testing.Init()
	flag.Parse()
	fmt.Printf("*************** tagKeyNum = %d, tagValueNum = %d, seriesKeyGenerator = %s *******************\n", tagKeyNum, tagValueNum, genID)

	// register generators
	generators = map[string]generator.Generator{}
	generators[FPGen] = &generator.FullPermutationGen{}
	generators[DiagonalGen] = &generator.DiagonalGenerator{}
	generators[RandomGen] = &generator.RandomGenerator{}
	gen = generators[genID]
}

func GetTagPairsExample(suffix string) models.Tags {
	m := map[string]string{
		"a": "0" + suffix,
		"b": "1" + suffix,
		"c": "2" + suffix,
		"d": "3" + suffix,
	}
	return models.NewTags(m)
}

func Contains(a, b []uint64) bool {
	m := map[uint64]struct{}{}
	for _, v := range a {
		m[v] = struct{}{}
	}
	for _, v := range b {
		if _, ok := m[v]; !ok {
			return false
		}
	}
	return true
}

func ContainsUint64(a, b []uint64) bool {
	m := map[uint64]struct{}{}
	for _, v := range a {
		m[v] = struct{}{}
	}
	for _, v := range b {
		if _, ok := m[v]; !ok {
			return false
		}
	}
	return true
}

func randomSelectTagPairSets(tagsSlice []models.Tags, queryNum int) []models.Tags {
	selectedSets := []models.Tags{}
	for i := 0; i < queryNum; i++ {
		rand.Seed(time.Now().UnixNano())
		selectedSets = append(selectedSets, tagsSlice[rand.Intn(len(tagsSlice))])
	}
	return selectedSets
}
