package tsi2_test

import (
	"flag"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"cycledb/pkg/tsdb/index/tsi2"
	generator "cycledb/pkg/tsdb/index/tsi2/tag_pairs_generator"
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

func GetTagPairsExample(suffix string) []tsi2.TagPair {
	tagPairs := []tsi2.TagPair{
		{
			TagKey:   "a",
			TagValue: "0" + suffix,
		},
		{
			TagKey:   "b",
			TagValue: "1" + suffix,
		},
		{
			TagKey:   "c",
			TagValue: "2" + suffix,
		},
		{
			TagKey:   "d",
			TagValue: "3" + suffix,
		},
	}
	return tagPairs
}

func Contains(a, b []int64) bool {
	m := map[int64]struct{}{}
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

func randomSelectTagPairSets(tagPairSets [][]tsi2.TagPair, queryNum int) [][]tsi2.TagPair {
	selectedSets := [][]tsi2.TagPair{}
	for i := 0; i < queryNum; i++ {
		rand.Seed(time.Now().UnixNano())
		selectedSets = append(selectedSets, tagPairSets[rand.Intn(len(tagPairSets))])
	}
	return selectedSets
}
