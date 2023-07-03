package tsi2

import (
	"cycledb/pkg/tsdb"
	"sync"

	"github.com/influxdata/influxdb/v2/models"
)

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

// GetSeriesIDsForTags:
func (gi *GridIndex) GetSeriesIDsForTags(tags models.Tags) *tsdb.SeriesIDSet {
	ids := tsdb.NewSeriesIDSet()
	gi.mu.RLock()
	defer gi.mu.RUnlock()
	for _, grid := range gi.grids {
		idsForGrid := grid.GetSeriesIDSetForTags(tags)
		if idsForGrid != nil {
			ids.MergeInPlace(idsForGrid)
		}
	}
	return ids
}

// GetStrictlyMatchedSeriesIDForTags: each dimension must match strictly, or return -1
func (gi *GridIndex) GetStrictlyMatchedSeriesIDForTags(tags models.Tags) (uint64, bool) {
	for _, grid := range gi.grids {
		id, ok := grid.GetStrictlyMatchedIDForTagsNoIDSet(tags)
		if ok {
			return id, true
		}
	}
	return 0, false
}

// SetTags: (insert series keys, then) return corresponding id
func (gi *GridIndex) SetTags(tags models.Tags) (uint64, bool) {
	// 1. if tag pair sets already exist
	gi.mu.RLock()
	id, ok := gi.GetStrictlyMatchedSeriesIDForTags(tags)
	if ok {
		gi.mu.RUnlock()
		return id, false
	}
	gi.mu.RUnlock()

	// 2. try to do insert within existed grids
	// double check
	gi.mu.Lock()
	defer gi.mu.Unlock()
	id, ok = gi.GetStrictlyMatchedSeriesIDForTags(tags)
	if ok {
		return id, false
	}

	for _, grid := range gi.grids {
		if id, ok = grid.SetTags(tags); ok {
			return id, true
		}
	}

	// else create a new grid
	return gi.initGridAndSetTags(tags)
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
			if _, ok := grid.tagValuesSlice[index].valueToIndex[value]; ok {
				return true
			}
		}
	}
	return false
}

func (gi *GridIndex) initGridAndSetTags(tags models.Tags) (uint64, bool) {
	grid := gi.optimizer.NewOptimizedGrid(gi, tags)
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
			res = unionStringSets2(res, g.tagValuesSlice[index].valueToIndex)
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
		idsSet.MergeInPlace(g.GetSeriesIDSetForTags(nil))
	}
	return idsSet
}

func (gi *GridIndex) SeriesIDSetForTagKey(key string) *tsdb.SeriesIDSet {
	gi.mu.RLock()
	defer gi.mu.Unlock()
	idsSet := tsdb.NewSeriesIDSet()
	for _, g := range gi.grids {
		if g.HasTagKey(key) {
			idsSet.MergeInPlace(g.GetSeriesIDSetForTags(nil))
		}
	}
	return idsSet
}

func (gi *GridIndex) SeriesIDSetForTagValue(key, value string) *tsdb.SeriesIDSet {
	gi.mu.RLock()
	defer gi.mu.RUnlock()
	idsSet := tsdb.NewSeriesIDSet()
	for _, g := range gi.grids {
		if g.HasTagValue(key, value) {
			idsSet.MergeInPlace(g.GetSeriesIDSetForTags(models.NewTags(
				map[string]string{
					key: value,
				},
			)))
		}
	}
	// idsSet.ForEach(func (id uint64) {
	// 	fmt.Printf("SeriesIDSetWithTagValue returns idSet containing %d\n", id)
	// })
	return idsSet
}
