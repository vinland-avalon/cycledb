package tsi2

type Grid struct {
	// the grids are linked, so id should skip
	offset int64
	// for each Grid, the size of tag values array (tagValuess)
	// and size of element(capacity) within is pre-allocated
	tagValuess []*TagValues
	tagKeys    []string
	// to accelerate search
	tagKeyToIndex map[string]int
}

func initGrid(offset int64, tagPairSet []TagPair, tagValuess []*TagValues) *Grid {
	g := &Grid{
		offset:        offset,
		tagValuess:    tagValuess,
		tagKeys:       []string{},
		tagKeyToIndex: map[string]int{},
	}
	for i, tagPair := range tagPairSet {
		g.tagKeyToIndex[tagPair.TagKey] = i
		g.tagKeys = append(g.tagKeys, tagPair.TagKey)
	}
	return g
}

// getGridSize: return the number of tag keys inside
func (g *Grid) getGridSize() int {
	return len(g.tagKeys)
}

// getCapacityOfIDs: the number of ids in this grid
func (g *Grid) getCapacityOfIDs() int {
	length := 1
	for _, tagValues := range g.tagValuess {
		length *= tagValues.capacity
	}
	return length
}

func (g *Grid) tagPairExists(tagPair TagPair) bool {
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

// GetStrictlyMatchedIDForTagPairSet: return -1 if not find it, else return id
func (g *Grid) GetStrictlyMatchedIDForTagPairSet(tagPairSet []TagPair) int64 {
	if len(tagPairSet) != g.getGridSize() {
		return int64(-1)
	}

	ids := g.GetSeriesIDsWithTagPairSet(tagPairSet)
	if len(ids) == 0 {
		return int64(-1)
	}
	return ids[0]
}

// SetTagPairSet: return whether the insert succeed and the corresponding id.
// If fails, return false, -1.
// Only when tags matches and all have free slot, the insert succeeds.
func (g *Grid) SetTagPairSet(tagPairSet []TagPair) (bool, int64) {
	// get the tag pairs already exists, which means they don't need to be inserted
	existedTagPairIndex := map[int]struct{}{}

	// check each tag key matches and have free slot
	// the tag key does not match
	if len(tagPairSet) != g.getGridSize() {
		return false, -1
	}
	for i, tagPair := range tagPairSet {
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
		if g.tagKeyExistsAndFilledUp(tagPair.TagKey) {
			return false, -1
		}
	}

	// do the insert
	for i, tagPair := range tagPairSet {
		if _, ok := existedTagPairIndex[i]; ok {
			continue
		}
		index, _ := g.tagKeyToIndex[tagPair.TagKey]
		tagValues := g.tagValuess[index]
		tagValues.SetValue(tagPair.TagValue)
	}

	// calculate id
	return true, g.GetStrictlyMatchedIDForTagPairSet(tagPairSet)
}

func (g *Grid) tagKeyExistsAndFilledUp(tagKey string) bool {
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

func (g *Grid) GetSeriesIDsWithTagPairSet(tagPairSet []TagPair) []int64 {
	// check if tag pairs match
	if len(tagPairSet) > g.getGridSize() {
		return []int64{}
	}
	for _, tagPair := range tagPairSet {
		if !g.tagPairExists(tagPair) {
			return []int64{}
		}
	}

	// TODO(vinland-avalon): not support non-condition search so far
	if len(tagPairSet) == 0 {
		return []int64{}
	}

	dimensions := make([][]int, 0, g.getGridSize())
	for i := range g.tagKeys {
		dimensions = append(dimensions, []int{-1, g.tagValuess[i].capacity})
	}
	for _, tagPair := range tagPairSet {
		idx := g.tagKeyToIndex[tagPair.TagKey]
		valueIdx := g.tagValuess[idx].GetValueIndex(tagPair.TagValue)
		dimensions[idx][0] = valueIdx
	}

	prev := []int64{}
	if dimensions[0][0] != -1 {
		prev = append(prev, int64(dimensions[0][0]))
	} else {
		for i := 0; i < dimensions[0][1]; i++ {
			prev = append(prev, int64(i))
		}
	}
	ids := VariableBaseConvert(dimensions, 1, prev)
	for i := range ids {
		ids[i] += g.offset
	}
	return ids
}
