package tsi2

import "sync"

type TagPair struct {
	TagKey   string
	TagValue string
}

type GridIndex struct {
	grids     []*Grid
	optimizer Optimizer
	mu        sync.RWMutex
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

// GetSeriesIDsWithTagPairSet: TODO(vinland-avalon): will return some fake ids
func (gi *GridIndex) GetSeriesIDsWithTagPairSet(tagPairSet []TagPair) []int64 {
	ids := []int64{}
	gi.mu.RLock()
	defer gi.mu.RUnlock()
	for _, grid := range gi.grids {
		idsForGrid := grid.GetSeriesIDsWithTagPairSet(tagPairSet)
		ids = append(ids, idsForGrid...)
	}
	return ids
}

// return -1 if not exist
func (gi *GridIndex) getStrictlyMatchedSeriesIDForTagPairSet(tagPairSet []TagPair) int64 {
	for _, grid := range gi.grids {
		id := grid.GetStrictlyMatchedIDForTagPairSet(tagPairSet)
		if id != -1 {
			return id
		}
	}
	return int64(-1)
}

// SetTagPairSet: (insert series keys, then) return corresponding id
func (gi *GridIndex) SetTagPairSet(tagPairSet []TagPair) int64 {
	// 1. if tag pair sets already exist
	gi.mu.RLock()
	id := gi.getStrictlyMatchedSeriesIDForTagPairSet(tagPairSet)
	if id != -1 {
		gi.mu.RUnlock()
		return id
	}
	gi.mu.RUnlock()

	// 2. try to do insert within existed grids
	// double check
	gi.mu.Lock()
	defer gi.mu.Unlock()
	id = gi.getStrictlyMatchedSeriesIDForTagPairSet(tagPairSet)
	if id != -1 {
		return id
	}

	for _, grid := range gi.grids {
		if id := grid.SetTagPairSet(tagPairSet); id != -1 {
			return id
		}
	}

	// else create a new grid
	id = gi.initGridAndSetTagPairSet(tagPairSet)
	return id
}

func (gi *GridIndex) initGridAndSetTagPairSet(tagPairSet []TagPair) int64 {
	grid := gi.optimizer.NewOptimizedGrid(gi, tagPairSet)
	gi.grids = append(gi.grids, grid)

	return grid.offset
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
