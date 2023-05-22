package tsi2

type TagPair struct {
	TagKey   string
	TagValue string
}

type GridIndex struct {
	grids     []*Grid
	optimizer Optimizer
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

// GetSeriesIDsWithTagPairs: TODO(vinland-avalon): will return some fake ids
func (gi *GridIndex) GetSeriesIDsWithTagPairs(tagPairs []TagPair) []int64 {
	ids := []int64{}
	for _, grid := range gi.grids {
		ids = append(ids, grid.GetSeriesIDsWithTagPairs(tagPairs)...)
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

// SetSeriesID: return corresponding id
func (gi *GridIndex) InitNewSeriesID(tagPairs []TagPair) int64 {
	// if already exist
	id := gi.getStrictlyMatchedSeriesIDForTagPairs(tagPairs)
	if id != -1 {
		return id
	}

	// if it can be represented in existed grids
	for _, grid := range gi.grids {
		if ok, id := grid.InsertTagPairs(tagPairs); ok {
			return id
		}
	}

	// else create a new grid
	id = gi.newGridAndSeriesIDWithTagPairs(tagPairs)
	return id
}

func (gi *GridIndex) newGridAndSeriesIDWithTagPairs(tagPairs []TagPair) int64 {
	grid := gi.optimizer.NewOptimizedGridWithInfo(gi, tagPairs)
	gi.grids = append(gi.grids, grid)

	return grid.offset
}

func (gi *GridIndex) GetNumOfFilledUpGridForSingleTagKey(tagKey string) int {
	cnt := 0
	for _, g := range gi.grids {
		if g.IfTagKeyExistAndFilledUp(tagKey) {
			cnt++
		}
	}
	return cnt
}
