package tsi2

import "sync"

// for benchmark comparation
type InvertIndex struct {
	// tagKey -> tagValue -> series ids
	invertIndex map[string]map[string]map[int64]struct{}
	// id -> tag pairs
	idToTagPairSet map[int64][]TagPair
	idCnt          int64
	// TODO(vinland-avalon): for high performance, improvements include:
	// 1. partition
	// 2. limit the zone that lock protects
	mu sync.RWMutex
}

func NewInvertIndex() *InvertIndex {
	return &InvertIndex{
		invertIndex:    map[string]map[string]map[int64]struct{}{},
		idToTagPairSet: map[int64][]TagPair{},
		idCnt:          0,
		mu:             sync.RWMutex{},
	}
}

func (ii *InvertIndex) GetSeriesIDsWithTagPairSet(tagPairSet []TagPair) []int64 {
	ii.mu.RLock()
	defer ii.mu.RUnlock()

	return ii.getSeriesIDsWithTagPairSet(tagPairSet)
}

func (ii *InvertIndex) getSeriesIDsWithTagPairSet(tagPairSet []TagPair) []int64 {
	copyMapI64 := func(ids map[int64]struct{}) map[int64]struct{} {
		m := map[int64]struct{}{}
		for id := range ids {
			m[id] = struct{}{}
		}
		return m
	}

	ids := []int64{}
	// TODO(vinland-avalon): not support non-condition search so far
	if len(tagPairSet) == 0 {
		return ids
	}

	idSet := copyMapI64(ii.getSeriesIDsForSingleTagPair(tagPairSet[0]))
	for i := 1; i < len(tagPairSet); i++ {
		currIdsSet := ii.getSeriesIDsForSingleTagPair(tagPairSet[i])
		for k := range idSet {
			if _, ok := currIdsSet[k]; !ok {
				delete(idSet, k)
			}
		}
	}

	for k := range idSet {
		ids = append(ids, k)
	}
	return ids
}

func (ii *InvertIndex) getSeriesIDsForSingleTagPair(tagPair TagPair) map[int64]struct{} {
	return ii.invertIndex[tagPair.TagKey][tagPair.TagValue]
}

func (ii *InvertIndex) SetTagPairSet(tagPairSet []TagPair) (bool, int64) {
	// check if the tagPairs already exists in index
	ii.mu.RLock()
	if idFound := ii.getStrictlyMatchedSeriesIDForTagPairs(tagPairSet); idFound != -1 {
		defer ii.mu.RUnlock()
		return false, idFound
	}

	ii.mu.RUnlock()
	ii.mu.Lock()
	defer ii.mu.Unlock()
	// double check
	if idFound := ii.getStrictlyMatchedSeriesIDForTagPairs(tagPairSet); idFound != -1 {
		return false, idFound
	}
	// do the insert
	currId := ii.idCnt
	for _, tagPair := range tagPairSet {
		if _, ok := ii.invertIndex[tagPair.TagKey]; !ok {
			ii.invertIndex[tagPair.TagKey] = map[string]map[int64]struct{}{}
		}
		if _, ok := ii.invertIndex[tagPair.TagKey][tagPair.TagValue]; !ok {
			ii.invertIndex[tagPair.TagKey][tagPair.TagValue] = map[int64]struct{}{}
		}
		ii.invertIndex[tagPair.TagKey][tagPair.TagValue][currId] = struct{}{}
	}

	ii.idToTagPairSet[currId] = tagPairSet
	ii.idCnt++
	return true, currId
}

func (ii *InvertIndex) getStrictlyMatchedSeriesIDForTagPairs(tagPairs []TagPair) int64 {
	ids := ii.getSeriesIDsWithTagPairSet(tagPairs)
	for _, id := range ids {
		if IfTagPairsEqual(ii.idToTagPairSet[id], tagPairs) {
			return id
		}
	}
	return -1
}
