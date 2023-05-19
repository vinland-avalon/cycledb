package tsi2_test

import (
	"fmt"
	"testing"

	"cycledb/pkg/tsdb/index/tsi2"
)

func GenerateFullPermutationTags(tagKeyNum, tagValueNum int) [][]tsi2.TagPair {
	if tagKeyNum == 1 {
		return generateTagPairs(tagKeyNum - 1, tagValueNum)
	}

	currLayer := generateTagPairs(tagKeyNum - 1, tagValueNum)
	prevLayers := GenerateFullPermutationTags(tagKeyNum - 1, tagValueNum)
	res := make([][]tsi2.TagPair, 0, tsi2.PowInt(tagValueNum, tagKeyNum))

	for _, curr := range currLayer {
		for _, prev := range prevLayers {
			res = append(res, append(prev, curr[0]))
		}
	}
	return res
}

func generateTagPairs(tagKeyIndex, tagValueNum int) [][]tsi2.TagPair {
	res := make([][]tsi2.TagPair, 0, tagValueNum)
	key := fmt.Sprintf("%c", 'a'+tagKeyIndex)
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

// BenchmarkInvertIndex-16    	   16528	     74486 ns/op	   29747 B/op	     338 allocs/op
func BenchmarkInvertIndex(b *testing.B) {
	manyTagPairs := GenerateFullPermutationTags(3, 4)
	// fmt.Printf("%+v\n", manyTagPairs)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		index := tsi2.NewInvertIndex()
		for _, tagPairs := range manyTagPairs {
			index.InitNewSeriesID(tagPairs)
		}
	}
}

// BenchmarkGridIndex-16    	   54880	     21863 ns/op	    6424 B/op	      96 allocs/op
func BenchmarkGridIndex(b *testing.B) {
	manyTagPairs := GenerateFullPermutationTags(3, 4)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		gi := tsi2.NewGridIndex(tsi2.NewMultiplierOptimizer(2, 2))
		for _, tagPairs := range manyTagPairs {
			gi.InitNewSeriesID(tagPairs)
		}
	}
}
