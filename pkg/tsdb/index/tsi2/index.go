package tsi2

type TagPair struct {
	TagKey   string
	TagValue string
}

type GridIndex struct {
	grids    []*Grid
	optimizer Optimizer
	// number of tag values of each tag key
	tagValueCnt map[string]int
}

func NewGridIndex(optimizer *MultiplierOptimizer) *GridIndex {
	return &GridIndex{
		grids:    []*Grid{},
		optimizer: optimizer,
	}
}

func (gi *GridIndex) WithAnalyzer(analyzer *MultiplierOptimizer) {
	gi.optimizer = analyzer
}

// GetSeriesIDsWithTagPairs: TODO(vinland-avalon): will return some fake ids
func (gi *GridIndex) GetSeriesIDsWithTagPairs(tagPairs []TagPair) []int64 {
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

	idsSet := convertToMapI64(gi.getSeriesIDsForSingleTagPair(tagPairs[0]))
	for i := 1; i < len(tagPairs); i++ {
		currIdsSet := convertToMapI64(gi.getSeriesIDsForSingleTagPair(tagPairs[i]))
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

func (gi *GridIndex) getSeriesIDsForSingleTagPair(p TagPair) []int64 {
	ids := []int64{}
	for _, grid := range gi.grids {
		ids = append(ids, grid.GetIDsForSingleTagPair(p.TagKey, p.TagValue)...)
	}
	return ids
}

// return -1 if not exist
func (gi *GridIndex) getStrictlyMatchedSeriesIDForTagPairs(tagPairs []TagPair) int64 {
	for _, grid := range gi.grids {
		id := grid.GetStrictlyMatchedIDForTagPairs(tagPairs)
		if id != -1 {
			return id
		}
	}
	return int64(-1)
}

// SetSeriesID: If the ID already exist, return false.
// Else init a new series id for the tag pairs.
// Both return corresponding id
func (gi *GridIndex) InitNewSeriesID(tagPairs []TagPair) (bool, int64) {
	// if already exist
	id := gi.getStrictlyMatchedSeriesIDForTagPairs(tagPairs)
	if id != -1 {
		return false, id
	}

	// if it can be represented in existed grids
	for _, grid := range gi.grids {
		if ok, ids := grid.InsertTagPairs(tagPairs); ok {
			return true, ids
		}
	}

	// else create a new grid
	id = gi.newGridAndSeriesIDWithTagPairs(tagPairs)
	return true, id
}

func (gi *GridIndex) newGridAndSeriesIDWithTagPairs(tagPairs []TagPair) int64 {
	grid := gi.optimizer.NewOptimizedGridWithInfo(gi, tagPairs)
	gi.grids = append(gi.grids, grid)

	return grid.offset
}

func (gi* GridIndex) GetNumOfFilledUpGridForSingleTagKey(tagKey string) int {
	cnt := 0
	for _, g := range gi.grids {
		if g.IfTagKeyExistAndFilledUp(tagKey) {
			cnt++;
		}
	}
	return cnt
}
