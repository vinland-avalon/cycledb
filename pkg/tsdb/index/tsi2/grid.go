package tsi2

import (
	"cycledb/pkg/tsdb"

	"github.com/influxdata/influxdb/v2/models"
)

type Grid struct {
	// the grids are linked, so id should skip
	offset uint64
	// for each Grid, the size of tag values array (tagValuesSlice)
	// and size of element(capacity) within is pre-allocated
	tagValuesSlice []*TagValues
	tagKeys        []string
	// to accelerate search
	tagKeyToIndex map[string]int

	// bitmap
	seriesIDSet *tsdb.SeriesIDSet
}

func NewGridWithSingleTags(offset uint64, tags models.Tags, tagValuesSlice []*TagValues) *Grid {
	g := &Grid{
		offset:         offset,
		tagValuesSlice: tagValuesSlice,
		tagKeys:        []string{},
		tagKeyToIndex:  map[string]int{},
		seriesIDSet:    tsdb.NewSeriesIDSet(offset),
	}
	for i, tag := range tags {
		g.tagKeyToIndex[string(tag.Key)] = i
		g.tagKeys = append(g.tagKeys, string(tag.Key))
	}
	return g
}

func NewGridWithKeysAndValuesSlice(offset uint64, keys []string, tagValuesSlice []*TagValues, seriesIDSet *tsdb.SeriesIDSet) *Grid {
	g := &Grid{
		offset:         offset,
		tagValuesSlice: tagValuesSlice,
		tagKeys:        keys,
		tagKeyToIndex:  map[string]int{},
		seriesIDSet:    seriesIDSet,
	}
	for i, key := range keys {
		g.tagKeyToIndex[key] = i
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
		values := g.tagValuesSlice[index]
		if _, ok = values.valueToIndex[value]; !ok {
			return false
		} else {
			return true
		}
	}

}

// getNumOfDimensions: return the number of tag keys inside
func (g *Grid) getNumOfDimensions() int {
	return len(g.tagKeys)
}

// getCapacityOfIDs: the number of ids in this grid
func (g *Grid) getCapacityOfIDs() uint64 {
	capacity := uint64(1)
	for _, tagValues := range g.tagValuesSlice {
		capacity *= tagValues.capacity
	}
	return capacity
}

func (g *Grid) tagValueExists(tag models.Tag) bool {
	if tagValuesIndex, ok := g.tagKeyToIndex[string(tag.Key)]; !ok {
		return false
	} else {
		if g.tagValuesSlice[tagValuesIndex].GetValueIndex(string(tag.Value)) == -1 {
			return false
		} else {
			return true
		}
	}
}

// GetStrictlyMatchedIDForTags: return 0, false if not find it, else return id, true
func (g *Grid) GetStrictlyMatchedIDForTags(tags models.Tags) (uint64, bool) {
	id, ok := g.GetStrictlyMatchedIDForTagsNoIDSet(tags)
	if !ok || !g.seriesIDSet.Contains(id) {
		return 0, false
	}
	return id, true
}

// GetStrictlyMatchedIDForTagsNoIDSet: Only check grid, ignore SeriesIdsSet, return 0, false if not find it, else return id, true
func (g *Grid) GetStrictlyMatchedIDForTagsNoIDSet(tags models.Tags) (uint64, bool) {
	if len(tags) != g.getNumOfDimensions() {
		return 0, false
	}

	ids := g.GetSeriesIDsWithTagsNoIDSet(tags)
	if len(ids) == 0 {
		return 0, false
	}

	return ids[0], true
}

// SetTags: return whether the insert succeed and the corresponding id.
// The returned bool represents whether the id will exist after calling, which means, 1) already exist and 2) insert successfully both return true.
// Only when tags matches and all have free slot, the insert succeeds.
func (g *Grid) SetTags(tags models.Tags) (uint64, bool) {
	id, ok := g.GetStrictlyMatchedIDForTags(tags)
	if ok {
		return id, ok
	}
	if !g.ableToSetTags(tags) {
		return 0, false
	}
	// get the tag pairs already exist, which means they don't need to be inserted
	existedTagIndex := map[int]struct{}{}
	for i, tag := range tags {
		index, ok := g.tagKeyToIndex[string(tag.Key)]
		// the tag key does not match
		if !ok {
			return 0, false
		}

		// if value already exist, no need to insert
		tagValues := g.tagValuesSlice[index]
		if tagValues.GetValueIndex(string(tag.Value)) != -1 {
			existedTagIndex[i] = struct{}{}
		}
	}

	// do the insert
	for i, tag := range tags {
		if _, ok := existedTagIndex[i]; ok {
			continue
		}
		index := g.tagKeyToIndex[string(tag.Key)]
		tagValues := g.tagValuesSlice[index]
		tagValues.SetValue(string(tag.Value))
	}

	// calculate id
	id, _ = g.GetStrictlyMatchedIDForTagsNoIDSet(tags)
	g.seriesIDSet.Add(id)
	return id, true
}

// ableToSetTags: returns whether tags could be inserted in grid.
// For grids could represent tags already, also return true.
func (g *Grid) ableToSetTags(tags models.Tags) bool {
	// check each tag key matches and have free slot
	// the tag key does not match
	if len(tags) != g.getNumOfDimensions() {
		return false
	}
	for _, tag := range tags {
		index, ok := g.tagKeyToIndex[string(tag.Key)]
		// the tag key does not match
		if !ok {
			return false
		}

		// if value already exist, no need to insert
		tagValues := g.tagValuesSlice[index]
		if tagValues.GetValueIndex(string(tag.Value)) != -1 {
			continue
		}

		// no free slot
		if g.tagKeyExistsAndFilledUp(string(tag.Key)) {
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
	tagValues := g.tagValuesSlice[index]
	return tagValues.capacity == uint64(len(tagValues.values))
}

func (g *Grid) GetSeriesIDSetForTags(tags models.Tags) *tsdb.SeriesIDSet {
	ids := g.GetSeriesIDsWithTagsNoIDSet(tags)
	idsSet := tsdb.NewSeriesIDSet(ids...)
	return idsSet.And(g.seriesIDSet)
}

func (g *Grid) GetSeriesIDsWithTagsNoIDSet(tags models.Tags) []uint64 {
	ids := []uint64{}
	// check if tag pairs match
	if len(tags) > g.getNumOfDimensions() {
		return ids
	}
	for _, tag := range tags {
		if !g.tagValueExists(tag) {
			return ids
		}
	}

	if len(tags) == 0 {
		g.seriesIDSet.ForEach(func(id uint64) {
			ids = append(ids, id)
		})
		return ids
	}

	indexes := make([]int, 0, g.getNumOfDimensions())
	capacities := make([]uint64, 0, g.getNumOfDimensions())
	for i := range g.tagKeys {
		indexes = append(indexes, -1)
		capacities = append(capacities, g.tagValuesSlice[i].capacity)
	}
	for _, tag := range tags {
		idx := g.tagKeyToIndex[string(tag.Key)]
		valueIdx := g.tagValuesSlice[idx].GetValueIndex(string(tag.Value))
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
