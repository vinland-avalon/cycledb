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

// GetSeriesIDsWithTagPairSet: TODO(vinland-avalon): will return some fake ids
func (gi *GridIndex) GetSeriesIDsWithTagPairSet(tagPairSet []TagPair) []int64 {
	ids := []int64{}
	for _, grid := range gi.grids {
		ids = append(ids, grid.GetSeriesIDsWithTagPairSet(tagPairSet)...)
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
	// if already exist
	id := gi.getStrictlyMatchedSeriesIDForTagPairSet(tagPairSet)
	if id != -1 {
		return id
	}

	// if it can be represented in existed grids
	for _, grid := range gi.grids {
		if ok, id := grid.SetTagPairSet(tagPairSet); ok {
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
