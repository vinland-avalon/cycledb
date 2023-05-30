package tsi2_test

import (
	"testing"

	"cycledb/pkg/tsdb/index/tsi2"
)

// 3,4	BenchmarkInvertIndex-16    	   16528	     74486 ns/op	   29747 B/op	     338 allocs/op
// 3,10	BenchmarkInvertIndex-16    	     138	   8737311 ns/op	 5415280 B/op	   28821 allocs/op
func BenchmarkInvertIndexInsert(b *testing.B) {
	tagPairSets := gen.GenerateInsertTagPairSets(tagKeyNum, tagValueNum)
	// fmt.Printf("%+v\n", manyTagPairs)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		index := tsi2.NewInvertIndex()
		for _, tagPairSet := range tagPairSets {
			index.InitNewSeriesID(tagPairSet)
		}
	}
}

// 3,4	BenchmarkInvertIndexQuery-16    	    4126	    267746 ns/op	  167173 B/op	    1427 allocs/op
// 3,10	BenchmarkInvertIndexQuery-16    	      62	  19513102 ns/op	11177979 B/op	   65450 allocs/op
func BenchmarkInvertIndexQuery(b *testing.B) {
	tagPairSets := gen.GenerateInsertTagPairSets(tagKeyNum, tagValueNum)

	index := tsi2.NewInvertIndex()
	for _, tagPairSet := range tagPairSets {
		index.InitNewSeriesID(tagPairSet)
	}

	queryTagPairSets := randomSelectTagPairSets(tagPairSets, queryNum)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, query := range queryTagPairSets {
			index.GetSeriesIDsWithTagPairs(query)
		}
	}
}

// 3,4	BenchmarkGridIndex-16    	   54880	     21863 ns/op	    6424 B/op	      96 allocs/op
// 3,10	BenchmarkGridIndex-16    	    3253	    365903 ns/op	   11983 B/op	     156 allocs/op
func BenchmarkGridIndexInsert(b *testing.B) {
	tagPairSets := gen.GenerateInsertTagPairSets(tagKeyNum, tagValueNum)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		gi := tsi2.NewGridIndex(tsi2.NewMultiplierOptimizer(2, 2))
		for _, tagPairSet := range tagPairSets {
			gi.SetTagPairSet(tagPairSet)
		}
	}
}

// 3,4	BenchmarkGridIndexQuery-16    	   20282	     58325 ns/op	   58608 B/op	    1200 allocs/op
// 3,10	BenchmarkGridIndexQuery-16    	    1542	    804862 ns/op	  822889 B/op	   10196 allocs/op
func BenchmarkGridIndexQuery(b *testing.B) {
	tagPairSets := gen.GenerateInsertTagPairSets(tagKeyNum, tagValueNum)
	gi := tsi2.NewGridIndex(tsi2.NewMultiplierOptimizer(2, 2))
	for _, tagPairSet := range tagPairSets {
		gi.SetTagPairSet(tagPairSet)
	}

	queryTagPairSets := randomSelectTagPairSets(tagPairSets, queryNum)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, query := range queryTagPairSets {
			gi.GetSeriesIDsWithTagPairSet(query)
		}
	}
}
