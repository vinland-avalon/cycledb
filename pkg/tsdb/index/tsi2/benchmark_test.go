package tsi2_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"cycledb/pkg/tsdb/index/tsi2"
)

var (
	tagKeyNum   int = 3
	tagValueNum int = 10
)

func TestGenerateQuery(t *testing.T) {
	queries := GenerateQueryTagPairs(tagKeyNum, tagValueNum)
	assert.Equal(t, tsi2.PowInt((tagValueNum+1), tagKeyNum)-1, len(queries))
	fmt.Printf("%v\n", queries)
}

func TestGenerateInserts(t *testing.T) {
	inserts := GenerateFullPermutationTags(tagKeyNum, tagValueNum, false)
	assert.Equal(t, tsi2.PowInt(tagValueNum, tagKeyNum), len(inserts))
	fmt.Printf("%v\n", inserts)
}

// GenerateFullPermutationTags: returns all permutations of tag pairs.
// If allow empty, there will be empty tag pair, like [[a:0],[],[c:1]]
func GenerateFullPermutationTags(tagKeyNum, tagValueNum int, allowEmpty bool) [][]tsi2.TagPair {
	if tagKeyNum == 1 {
		return generateTagPairs(tagKeyNum-1, tagValueNum, allowEmpty)
	}

	currLayer := generateTagPairs(tagKeyNum-1, tagValueNum, allowEmpty)
	prevLayers := GenerateFullPermutationTags(tagKeyNum-1, tagValueNum, allowEmpty)
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

func generateTagPairs(tagKeyIndex, tagValueNum int, allowEmpty bool) [][]tsi2.TagPair {
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

// GenerateQueryTagPairs: responsible for formatting queries.
// 1. remove empty tag pairs, [[a:0],[],[c:1]]->[[a:0],[c:1]]
// 2. remove [[],[],[]] tatally, since grid index not support it
func GenerateQueryTagPairs(tagKeyNum, tagValueNum int) [][]tsi2.TagPair {
	manyTagPairs := GenerateFullPermutationTags(tagKeyNum, tagValueNum, true)
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

// 3,4	BenchmarkInvertIndex-16    	   16528	     74486 ns/op	   29747 B/op	     338 allocs/op
// 3,10	BenchmarkInvertIndex-16    	     138	   8737311 ns/op	 5415280 B/op	   28821 allocs/op
func BenchmarkInvertIndex(b *testing.B) {
	manyTagPairs := GenerateFullPermutationTags(tagKeyNum, tagValueNum, false)
	// fmt.Printf("%+v\n", manyTagPairs)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		index := tsi2.NewInvertIndex()
		for _, tagPairs := range manyTagPairs {
			index.InitNewSeriesID(tagPairs)
		}
	}
}

// 3,4	BenchmarkInvertIndexQuery-16    	    4126	    267746 ns/op	  167173 B/op	    1427 allocs/op
// 3,10	BenchmarkInvertIndexQuery-16    	      62	  19513102 ns/op	11177979 B/op	   65450 allocs/op
func BenchmarkInvertIndexQuery(b *testing.B) {
	manyTagPairs := GenerateFullPermutationTags(tagKeyNum, tagValueNum, false)

	index := tsi2.NewInvertIndex()
	for _, tagPairs := range manyTagPairs {
		index.InitNewSeriesID(tagPairs)
	}
	manyQueryTagPairs := GenerateQueryTagPairs(tagKeyNum, tagValueNum)
	// fmt.Printf("%+v\n", manyQueryTagPairs)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, query := range manyQueryTagPairs {
			index.GetSeriesIDsWithTagPairs(query)
		}
	}
}

// 3,4	BenchmarkGridIndex-16    	   54880	     21863 ns/op	    6424 B/op	      96 allocs/op
// 3,10	BenchmarkGridIndex-16    	    3253	    365903 ns/op	   11983 B/op	     156 allocs/op
func BenchmarkGridIndex(b *testing.B) {
	manyTagPairs := GenerateFullPermutationTags(tagKeyNum, tagValueNum, false)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		gi := tsi2.NewGridIndex(tsi2.NewMultiplierOptimizer(2, 2))
		for _, tagPairs := range manyTagPairs {
			gi.InitNewSeriesID(tagPairs)
		}
	}
}

// 3,4	BenchmarkGridIndexQuery-16    	     487	   2428514 ns/op	 2356831 B/op	   10371 allocs/op
// 3,10	BenchmarkGridIndexQuery-16    	       6	 187527690 ns/op	226902552 B/op	  277889 allocs/op
func BenchmarkGridIndexQuery(b *testing.B) {
	manyTagPairs := GenerateFullPermutationTags(tagKeyNum, tagValueNum, false)
	gi := tsi2.NewGridIndex(tsi2.NewMultiplierOptimizer(2, 2))
	for _, tagPairs := range manyTagPairs {
		gi.InitNewSeriesID(tagPairs)
	}
	manyQueryTagPairs := GenerateQueryTagPairs(tagKeyNum, tagValueNum)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, query := range manyQueryTagPairs {
			gi.GetSeriesIDsWithTagPairs(query)
		}
	}
}
