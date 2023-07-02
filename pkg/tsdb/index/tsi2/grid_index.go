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
		if idsForGrid != nil {
			ids.MergeInPlace(idsForGrid)
		}
	}
	return ids
}

// return -1 if not exist
func (gi *GridIndex) GetStrictlyMatchedSeriesIDForTagPairSet(tagPairSet []TagPair) (uint64, bool) {
	for _, grid := range gi.grids {
		id, ok := grid.GetStrictlyMatchedIDForTagPairSetWithoutIDSet(tagPairSet)
		if ok {
			return id, true
		}
	}
	return 0, false
}

// SetTagPairSet: (insert series keys, then) return corresponding id
func (gi *GridIndex) SetTagPairSet(tagPairSet []TagPair) (uint64, bool) {
	// 1. if tag pair sets already exist
	gi.mu.RLock()
	id, ok := gi.GetStrictlyMatchedSeriesIDForTagPairSet(tagPairSet)
	if ok {
		gi.mu.RUnlock()
		return id, false
	}
	gi.mu.RUnlock()

	// 2. try to do insert within existed grids
	// double check
	gi.mu.Lock()
	defer gi.mu.Unlock()
	id, ok = gi.GetStrictlyMatchedSeriesIDForTagPairSet(tagPairSet)
	if ok {
		return id, false
	}

	for _, grid := range gi.grids {
		if id, ok = grid.SetTagPairSet(tagPairSet); ok {
			return id, true
		}
	}

	// else create a new grid
	return gi.initGridAndSetTagPairSet(tagPairSet)
}

func (gi *GridIndex) HasTagKey(key string) bool {
	gi.mu.RLock()
	defer gi.mu.RUnlock()
	for _, grid := range gi.grids {
		if _, ok := grid.tagKeyToIndex[key]; ok {
			return true
		}
	}
	return false
}

func (gi *GridIndex) HasTagValue(key, value string) bool {
	gi.mu.RLock()
	defer gi.mu.Unlock()
	for _, grid := range gi.grids {
		if index, ok := grid.tagKeyToIndex[key]; ok {
			if _, ok := grid.tagValuess[index].valueToIndex[value]; ok {
				return true
			}
		}
	}
	return false
}

func (gi *GridIndex) initGridAndSetTagPairSet(tagPairSet []TagPair) (uint64, bool) {
	grid := gi.optimizer.NewOptimizedGrid(gi, tagPairSet)
	gi.grids = append(gi.grids, grid)
	grid.seriesIDSet.Add(grid.offset)

	return grid.offset, true
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

func (gi *GridIndex) SeriesIDSet() *tsdb.SeriesIDSet {
	gi.mu.RLock()
	defer gi.mu.Unlock()
	idsSet := tsdb.NewSeriesIDSet()
	for _, g := range gi.grids {
		idsSet.MergeInPlace(g.GetSeriesIDsWithTagPairSet([]TagPair{}))
	}
	return idsSet
}

func (gi *GridIndex) SeriesIDSetWithTagKey(key string) *tsdb.SeriesIDSet {
	gi.mu.RLock()
	defer gi.mu.Unlock()
	idsSet := tsdb.NewSeriesIDSet()
	for _, g := range gi.grids {
		if g.HasTagKey(key) {
			idsSet.MergeInPlace(g.GetSeriesIDsWithTagPairSet([]TagPair{}))
		}
	}
	return idsSet
}

func (gi *GridIndex) SeriesIDSetWithTagValue(key, value string) *tsdb.SeriesIDSet {
	gi.mu.RLock()
	defer gi.mu.RUnlock()
	idsSet := tsdb.NewSeriesIDSet()
	for _, g := range gi.grids {
		if g.HasTagValue(key, value) {
			idsSet.MergeInPlace(g.GetSeriesIDsWithTagPairSet([]TagPair{{TagKey: key, TagValue: value}}))
		}
	}
	// idsSet.ForEach(func (id uint64) {
	// 	fmt.Printf("SeriesIDSetWithTagValue returns idSet containing %d\n", id)
	// })
	return idsSet
}
