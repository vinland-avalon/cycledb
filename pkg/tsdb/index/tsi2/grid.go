package tsi2

type Grid struct {
	// the grids are linked, so id should skip
	offset int64
	// for each Grid, the size of tagValueSet(capacity) and size of element(capacity) within is pre-allocated
	capacity   int
	tagValuess []*TagValues
	tagKeys    []string
	// to accelerate search
	tagKeyToIndex map[string]int
}

func newGrid(offset int64, tagPairs []TagPair, tagValuess []*TagValues) *Grid {
	g := &Grid{
		offset:        offset,
		capacity:      len(tagPairs),
		tagValuess:    tagValuess,
		tagKeys:       []string{},
		tagKeyToIndex: map[string]int{},
	}
	for i, tagPair := range tagPairs {
		g.tagKeyToIndex[tagPair.TagKey] = i
		g.tagKeys = append(g.tagKeys, tagPair.TagKey)
	}
	return g
}

func (g *Grid) CalLength() int {
	length := 1
	for _, tagValues := range g.tagValuess {
		length *= tagValues.capacity
	}
	return length
}

func (g *Grid) IfTagPairExist(tagPair TagPair) bool {
	if tagValuesIndex, ok := g.tagKeyToIndex[tagPair.TagKey]; !ok {
		return false
	} else {
		if g.tagValuess[tagValuesIndex].GetValueIndex(tagPair.TagValue) == -1 {
			return false
		} else {
			return true
		}
	}
}

// GetIDsForSingleTagPair: return ids for a specific tag key and tag value
func (g *Grid) GetIDsForSingleTagPair(tagPair TagPair) []int64 {
	ids := []int64{}
	if !g.IfTagPairExist(tagPair) {
		return ids
	}

	tagKeyIndex := g.tagKeyToIndex[tagPair.TagKey]
	tagValues := g.tagValuess[tagKeyIndex]
	tagValueIndex := tagValues.GetValueIndex(tagPair.TagValue)

	duration := int64(1)
	for i := g.capacity - 1; i > tagKeyIndex; i-- {
		duration *= int64(g.tagValuess[i].capacity)
	}

	beginning := duration*int64(tagValueIndex) + g.offset
	cycle := duration * int64(g.tagValuess[tagKeyIndex].capacity)
	cycleCnt := int64(1)
	for i := 0; i < tagKeyIndex; i++ {
		cycleCnt *= int64(g.tagValuess[i].capacity)
	}

	for cycleCnt != 0 {
		for i := int64(0); i < duration; i++ {
			ids = append(ids, beginning+int64(i))
		}
		beginning += cycle
		cycleCnt--
	}

	return ids
}

// GetStrictlyMatchedIDForTagPairs: return -1 if not find it, or return id
func (g *Grid) GetStrictlyMatchedIDForTagPairs(tagPairs []TagPair) int64 {
	interval := 1
	id := 0
	tagPairsMap := map[string]string{}
	for _, tagPair := range tagPairs {
		tagPairsMap[tagPair.TagKey] = tagPair.TagValue
	}
	if len(tagPairsMap) != g.capacity {
		return int64(-1)
	}
	for i := g.capacity - 1; i >= 0; i-- {
		tagValue, ok := tagPairsMap[g.tagKeys[i]]
		if !ok {
			return int64(-1)
		}
		valueIndex, ok := g.tagValuess[i].valueToIndex[tagValue]
		if !ok {
			return int64(-1)
		}
		id += (valueIndex * interval)
		interval *= g.tagValuess[i].capacity
	}
	return int64(id) + g.offset
}

// InsertTagPairs: return whether the insert succeed and the corresponding id.
// If fails, return false, -1.
// Only when tags matches and all have free slot, the insert succeeds.
func (g *Grid) InsertTagPairs(tagPairs []TagPair) (bool, int64) {
	// get the tag pairs already exists, which means they don't need to be inserted
	existedTagPairIndex := map[int]struct{}{}

	// check each tag key matches and have free slot
	// the tag key does not match
	if len(tagPairs) != g.capacity {
		return false, -1
	}
	for i, tagPair := range tagPairs {
		index, ok := g.tagKeyToIndex[tagPair.TagKey]
		// the tag key does not match
		if !ok {
			return false, -1
		}

		// if value already exist, no need to insert
		tagValues := g.tagValuess[index]
		if tagValues.GetValueIndex(tagPair.TagValue) != -1 {
			existedTagPairIndex[i] = struct{}{}
			continue
		}

		// no free slot
		if g.IfTagKeyExistAndFilledUp(tagPair.TagKey) {
			return false, -1
		}
	}

	// do the insert
	for i, tagPair := range tagPairs {
		if _, ok := existedTagPairIndex[i]; ok {
			continue
		}
		index, _ := g.tagKeyToIndex[tagPair.TagKey]
		tagValues := g.tagValuess[index]
		tagValues.SetValue(tagPair.TagValue)
	}

	// calculate id
	return true, g.GetStrictlyMatchedIDForTagPairs(tagPairs)
}

func (g *Grid) IfTagKeyExistAndFilledUp(tagKey string) bool {
	index, ok := g.tagKeyToIndex[tagKey]
	// the tag key does not exist
	if !ok {
		return false
	}

	// filled up already
	tagValues := g.tagValuess[index]
	if tagValues.capacity == len(tagValues.values) {
		return true
	}
	return false
}

func (g *Grid) GetSeriesIDsWithTagPairs(tagPairs []TagPair) []int64 {
	// check if tag pairs match
	if len(tagPairs) > len(g.tagKeys) {
		return []int64{}
	}
	for _, tagPair := range tagPairs {
		if !g.IfTagPairExist(tagPair) {
			return []int64{}
		}
	}

	// TODO(vinland-avalon): not support non-condition search so far
	if len(tagPairs) == 0 {
		return []int64{}
	}

	// do search and intersection
	convertToMapI64 := func(ids []int64) map[int64]struct{} {
		m := map[int64]struct{}{}
		for _, id := range ids {
			m[id] = struct{}{}
		}
		return m
	}
	ids := []int64{}
	idsSet := convertToMapI64(g.GetIDsForSingleTagPair(tagPairs[0]))
	for i := 1; i < len(tagPairs); i++ {
		currIdsSet := convertToMapI64(g.GetIDsForSingleTagPair(tagPairs[i]))
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
