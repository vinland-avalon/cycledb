package tsi2

import (
	"cycledb/pkg/tsdb"
	"sync"
)

type TagPair struct {
	TagKey   string
	TagValue string
}

type GridIndex struct {
	grids     []*Grid
	optimizer Optimizer

	mu sync.RWMutex
}

func NewGridIndex(optimizer *MultiplierOptimizer) *GridIndex {
	return &GridIndex{
		grids:     []*Grid{},
		optimizer: optimizer,
	}
}

func (gi *GridIndex) WithAnalyzer(analyzer *MultiplierOptimizer) {
	gi.optimizer = analyzer
}

// GetSeriesIDsWithTagPairSet:
func (gi *GridIndex) GetSeriesIDsWithTagPairSet(tagPairSet []TagPair) *tsdb.SeriesIDSet {
	ids := tsdb.NewSeriesIDSet()
	gi.mu.RLock()
	defer gi.mu.RUnlock()
	for _, grid := range gi.grids {
		idsForGrid := grid.GetSeriesIDsWithTagPairSet(tagPairSet)
		ids.MergeInPlace(idsForGrid)
	}
	return ids
}

// return -1 if not exist
func (gi *GridIndex) GetStrictlyMatchedSeriesIDForTagPairSet(tagPairSet []TagPair) *tsdb.SeriesIDSet {
	for _, grid := range gi.grids {
		idSet := grid.GetStrictlyMatchedIDForTagPairSet(tagPairSet)
		if idSet == nil{
			return idSet
		}
	}
	return nil
}

// SetTagPairSet: (insert series keys, then) return corresponding id
func (gi *GridIndex) SetTagPairSet(tagPairSet []TagPair) *tsdb.SeriesIDSet {
	// 1. if tag pair sets already exist
	gi.mu.RLock()
	idSet := gi.GetStrictlyMatchedSeriesIDForTagPairSet(tagPairSet)
	if idSet != nil {
		gi.mu.RUnlock()
		return idSet
	}
	gi.mu.RUnlock()

	// 2. try to do insert within existed grids
	// double check
	gi.mu.Lock()
	defer gi.mu.Unlock()
	idSet = gi.GetStrictlyMatchedSeriesIDForTagPairSet(tagPairSet)
	if idSet != nil {
		return idSet
	}

	for _, grid := range gi.grids {
		if idSet := grid.SetTagPairSet(tagPairSet); idSet != nil {
			return idSet
		}
	}

	// else create a new grid
	idSet = gi.initGridAndSetTagPairSet(tagPairSet)
	return idSet
}

func (gi *GridIndex) HasTagKey(key string) bool {
	gi.mu.RLock()
	for _, grid := range gi.grids {
		if _, ok := grid.tagKeyToIndex[key]; ok {
			gi.mu.RUnlock()
			return true
		}
	}
	return false
}

func (gi *GridIndex) HasTagValue(key, value string) bool {
	gi.mu.RLock()
	for _, grid := range gi.grids {
		if index, ok := grid.tagKeyToIndex[key]; ok {
			if _, ok := grid.tagValuess[index].valueToIndex[value]; ok {
				return true
			}
		}
	}
	return false
}

func (gi *GridIndex) initGridAndSetTagPairSet(tagPairSet []TagPair) *tsdb.SeriesIDSet {
	grid := gi.optimizer.NewOptimizedGrid(gi, tagPairSet)
	gi.grids = append(gi.grids, grid)

	return tsdb.NewSeriesIDSet(uint64(grid.offset))
}

func (gi *GridIndex) GetNumOfFilledUpGridForSingleTagKey(tagKey string) int {
	cnt := 0
	for _, g := range gi.grids {
		if g.tagKeyExistsAndFilledUp(tagKey) {
			cnt++
		}
	}
	return cnt
}

func (gi *GridIndex) NewTagKeyIterator() *TagKeyIterator {
	gi.mu.RLock()
	res := map[string]struct{}{}
	for _, g := range gi.grids {
		res = unionStringSets2(res, g.tagKeyToIndex)
	}
	gi.mu.RUnlock()

	slice := mapToSlice(res)
	return &TagKeyIterator{
		keys: slice,
	}
}

func (gi *GridIndex) NewTagValueIterator(key string) *TagValueIterator {
	gi.mu.RLock()
	res := map[string]struct{}{}
	for _, g := range gi.grids {
		if index, ok := g.tagKeyToIndex[key]; ok {
			res = unionStringSets2(res, g.tagValuess[index].valueToIndex)
		}
	}
	gi.mu.RUnlock()

	slice := mapToSlice(res)
	return &TagValueIterator{
		values: slice,
	}
}
