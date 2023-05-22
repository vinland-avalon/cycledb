package tsi2

// for benchmark comparation
type InvertIndex struct {
	// tagKey -> tagValue -> series ids
	invertIndex map[string]map[string][]int64
	// id -> tag pairs
	idToTagPairsMap map[int64][]TagPair
	// TODO(vinland-avalon): concurrency and lock
	idCnt int64
}

func NewInvertIndex() *InvertIndex {
	return &InvertIndex{
		invertIndex:     map[string]map[string][]int64{},
		idToTagPairsMap: map[int64][]TagPair{},
		idCnt:           0,
	}
}

func (ii *InvertIndex) GetSeriesIDsWithTagPairs(tagPairs []TagPair) []int64 {
	convertToMapI64 := func(ids []int64) map[int64]struct{} {
		m := map[int64]struct{}{}
		for _, id := range ids {
			m[id] = struct{}{}
		}
		return m
	}
	ids := []int64{}
	// TODO(vinland-avalon): not support non-condition search so far
	if len(tagPairs) == 0 {
		return ids
	}

	idsSet := convertToMapI64(ii.getSeriesIDsForSingleTagPair(tagPairs[0]))
	for i := 1; i < len(tagPairs); i++ {
		currIdsSet := convertToMapI64(ii.getSeriesIDsForSingleTagPair(tagPairs[i]))
		for k := range idsSet {
			if _, ok := currIdsSet[k]; !ok {
				delete(idsSet, k)
			}
		}
	}

	for k := range idsSet {
		ids = append(ids, k)
	}
	return ids
}

func (ii *InvertIndex) getSeriesIDsForSingleTagPair(tagPair TagPair) []int64 {
	return ii.invertIndex[tagPair.TagKey][tagPair.TagValue]
}

func (ii *InvertIndex) InitNewSeriesID(tagPairs []TagPair) (bool, int64) {
	currId := ii.idCnt

	// check if the tagPairs already exists in index
	if idFound := ii.getStrictlyMatchedSeriesIDForTagPairs(tagPairs); idFound != -1 {
		return false, idFound
	}

	for _, tagPair := range tagPairs {
		if _, ok := ii.invertIndex[tagPair.TagKey]; !ok {
			ii.invertIndex[tagPair.TagKey] = map[string][]int64{}
		}
		if _, ok := ii.invertIndex[tagPair.TagKey][tagPair.TagValue]; !ok {
			ii.invertIndex[tagPair.TagKey][tagPair.TagValue] = []int64{}
		}
		ii.invertIndex[tagPair.TagKey][tagPair.TagValue] = append(ii.invertIndex[tagPair.TagKey][tagPair.TagValue], currId)
	}

	ii.idCnt++
	return true, currId
}

func (ii *InvertIndex) getStrictlyMatchedSeriesIDForTagPairs(tagPairs []TagPair) int64 {
	ids := ii.GetSeriesIDsWithTagPairs(tagPairs)
	for _, id := range ids {
		if IfTagPairsEqual(ii.idToTagPairsMap[id], tagPairs) {
			return id
		}
	}
	return -1
}
