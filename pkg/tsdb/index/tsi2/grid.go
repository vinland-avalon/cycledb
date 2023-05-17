package tsi2

type TagValues struct {
	capacity int
	// TODO(vinland-avalon): any
	values       []string
	valueToIndex map[string]int
}

func newTagValues(cap int) *TagValues {
	return &TagValues{
		capacity:     cap,
		values:       []string{},
		valueToIndex: map[string]int{},
	}
}

// SetValue: return whether set succeed.
// If already exist or append new value to the end, return true.
// If reach capacity, return false.
func (tvs *TagValues) SetValue(v string) bool {
	if tvs.capacity == len(tvs.values) {
		return false
	}
	if _, ok := tvs.valueToIndex[v]; ok {
		return true
	}

	tvs.valueToIndex[v] = len(tvs.values)
	tvs.values = append(tvs.values, v)
	return true
}

// GetValueIndex: return -1 if not exist
func (tvs *TagValues) GetValueIndex(value string) int {
	if index, ok := tvs.valueToIndex[value]; !ok {
		return -1
	} else {
		return index
	}
}

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

func (g *Grid) IfTagPairExist(tagKey, tagValue string) bool {
	if tagValuesIndex, ok := g.tagKeyToIndex[tagKey]; !ok {
		return false
	} else {
		if g.tagValuess[tagValuesIndex].GetValueIndex(tagValue) == -1 {
			return false
		} else {
			return true
		}
	}
}

// GetIDsForSingleTagPair: return ids for a specific tag key and tag value
func (g *Grid) GetIDsForSingleTagPair(tagKey, tagValue string) []int64 {
	ids := []int64{}
	if !g.IfTagPairExist(tagKey, tagValue) {
		return ids
	}

	tagKeyIndex := g.tagKeyToIndex[tagKey]
	tagValues := g.tagValuess[tagKeyIndex]
	tagValueIndex := tagValues.GetValueIndex(tagValue)

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
