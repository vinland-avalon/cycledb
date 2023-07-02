package tsi2

import (
	"cycledb/pkg/tsdb"
)

type Grid struct {
	// the grids are linked, so id should skip
	offset uint64
	// for each Grid, the size of tag values array (tagValuess)
	// and size of element(capacity) within is pre-allocated
	tagValuess []*TagValues
	tagKeys    []string
	// to accelerate search
	tagKeyToIndex map[string]int

	// bitmap
	seriesIDSet *tsdb.SeriesIDSet
}

func initGrid(offset uint64, tagPairSet []TagPair, tagValuess []*TagValues) *Grid {
	g := &Grid{
		offset:        offset,
		tagValuess:    tagValuess,
		tagKeys:       []string{},
		tagKeyToIndex: map[string]int{},
		seriesIDSet:   tsdb.NewSeriesIDSet(),
	}
	for i, tagPair := range tagPairSet {
		g.tagKeyToIndex[tagPair.TagKey] = i
		g.tagKeys = append(g.tagKeys, tagPair.TagKey)
	}
	return g
}

func (g *Grid) HasTagKey(key string) bool {
	if _, ok := g.tagKeyToIndex[key]; ok {
		return true
	}
	return false
}

func (g *Grid) HasTagValue(key, value string) bool {
	if index, ok := g.tagKeyToIndex[key]; !ok {
		return false
	} else {
		values := g.tagValuess[index]
		if _, ok = values.valueToIndex[value]; !ok {
			return false
		} else {
			return true
		}
	}

}

// getGridSize: return the number of tag keys inside
func (g *Grid) getGridSize() int {
	return len(g.tagKeys)
}

// getCapacityOfIDs: the number of ids in this grid
func (g *Grid) getCapacityOfIDs() uint64 {
	capacity := uint64(1)
	for _, tagValues := range g.tagValuess {
		capacity *= tagValues.capacity
	}
	return capacity
}

func (g *Grid) tagValueExists(tagPair TagPair) bool {
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
func (g *Grid) GetStrictlyMatchedIDForTagPairSet(tagPairSet []TagPair) (uint64, bool) {
	id, ok := g.GetStrictlyMatchedIDForTagPairSetWithoutIDSet(tagPairSet)
	if !ok || !g.seriesIDSet.Contains(id) {
		return 0, false
	}
	return id, true
}

// GetStrictlyMatchedIDForTagPairSet: return -1 if not find it, else return id
func (g *Grid) GetStrictlyMatchedIDForTagPairSetWithoutIDSet(tagPairSet []TagPair) (uint64, bool) {
	if len(tagPairSet) != g.getGridSize() {
		return 0, false
	}

	ids := g.GetSeriesIDsWithTagPairSetWithoutIDSet(tagPairSet)
	if len(ids) == 0 {
		return 0, false
	}

	return ids[0], true
}

// SetTagPairSet: return whether the insert succeed and the corresponding id.
// If fails, return nil.
// Only when tags matches and all have free slot, the insert succeeds.
func (g *Grid) SetTagPairSet(tagPairSet []TagPair) (uint64, bool) {
	if !g.ableToSetTagPairSet(tagPairSet) {
		return 0, false
	}
	// get the tag pairs already exist, which means they don't need to be inserted
	existedTagPairIndex := map[int]struct{}{}
	for i, tagPair := range tagPairSet {
		index, ok := g.tagKeyToIndex[tagPair.TagKey]
		// the tag key does not match
		if !ok {
			return 0, false
		}

		// if value already exist, no need to insert
		tagValues := g.tagValuess[index]
		if tagValues.GetValueIndex(tagPair.TagValue) != -1 {
			existedTagPairIndex[i] = struct{}{}
		}
	}

	// do the insert
	for i, tagPair := range tagPairSet {
		if _, ok := existedTagPairIndex[i]; ok {
			continue
		}
		index := g.tagKeyToIndex[tagPair.TagKey]
		tagValues := g.tagValuess[index]
		tagValues.SetValue(tagPair.TagValue)
	}

	// calculate id

	id, _ := g.GetStrictlyMatchedIDForTagPairSetWithoutIDSet(tagPairSet)
	g.seriesIDSet.Add(id)
	return id, true
}

// TODO(vinland-avalon): also need to judge if whole tag pairs already exist
func (g *Grid) ableToSetTagPairSet(tagPairSet []TagPair) bool {
	// check each tag key matches and have free slot
	// the tag key does not match
	if len(tagPairSet) != g.getGridSize() {
		return false
	}
	for _, tagPair := range tagPairSet {
		index, ok := g.tagKeyToIndex[tagPair.TagKey]
		// the tag key does not match
		if !ok {
			return false
		}

		// if value already exist, no need to insert
		tagValues := g.tagValuess[index]
		if tagValues.GetValueIndex(tagPair.TagValue) != -1 {
			continue
		}

		// no free slot
		if g.tagKeyExistsAndFilledUp(tagPair.TagKey) {
			return false
		}
	}
	return true
}

func (g *Grid) tagKeyExistsAndFilledUp(tagKey string) bool {
	index, ok := g.tagKeyToIndex[tagKey]
	// the tag key does not exist
	if !ok {
		return false
	}

	// filled up already
	tagValues := g.tagValuess[index]
	return tagValues.capacity == uint64(len(tagValues.values))
}

func (g *Grid) GetSeriesIDsWithTagPairSet(tagPairSet []TagPair) *tsdb.SeriesIDSet {
	ids := g.GetSeriesIDsWithTagPairSetWithoutIDSet(tagPairSet)
	idsSet := tsdb.NewSeriesIDSet(ids...)
	return idsSet.And(g.seriesIDSet)
}

func (g *Grid) GetSeriesIDsWithTagPairSetWithoutIDSet(tagPairSet []TagPair) []uint64 {
	// check if tag pairs match
	ids := []uint64{}
	if len(tagPairSet) > g.getGridSize() {
		return ids
	}
	for _, tagPair := range tagPairSet {
		if !g.tagValueExists(tagPair) {
			return ids
		}
	}

	if len(tagPairSet) == 0 {
		g.seriesIDSet.ForEach(func(id uint64) {
			ids = append(ids, id+g.offset)
		})
		return ids
	}

	// [index, capacity]
	indexes := make([]int, 0, g.getGridSize())
	capacities := make([]uint64, 0, g.getGridSize())
	for i := range g.tagKeys {
		indexes = append(indexes, -1)
		capacities = append(capacities, g.tagValuess[i].capacity)
	}
	for _, tagPair := range tagPairSet {
		idx := g.tagKeyToIndex[tagPair.TagKey]
		valueIdx := g.tagValuess[idx].GetValueIndex(tagPair.TagValue)
		indexes[idx] = valueIdx
	}

	prev := []uint64{}
	if indexes[0] != -1 {
		prev = append(prev, uint64(indexes[0]))
	} else {
		for i := uint64(0); i < capacities[0]; i++ {
			prev = append(prev, i)
		}
	}
	ids = VariableBaseConvert(indexes, capacities, 1, prev)

	for i := range ids {
		ids[i] += g.offset
	}

	return ids
}
