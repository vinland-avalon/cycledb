package tsi2

type Optimizer interface {
	// To Generate a new grid with information of GridIndex
	NewOptimizedGridWithInfo(*GridIndex, []TagPair) *Grid
}

// If in the previous grids, the tag key `K` is filled up n times,
// it should be in size of pow(multiplier, n) * basicNum
type MultiplierOptimizer struct {
	basicNum   int
	multiplier int
}

// NewMultiplierOptimizer: return an Optimizer.
// If in the previous grids, the tag key `K` is filled up n times,
// it should be in size of pow(multiplier, n) * basicNum.
// Usually, to reach double pre-allocate, multiplier = 2.
// Or to reach fixed size, multiplier = 1.
func NewMultiplierOptimizer(basicNum, multiplier int) *MultiplierOptimizer {
	return &MultiplierOptimizer{
		basicNum:   basicNum,
		multiplier: multiplier,
	}
}

func (a *MultiplierOptimizer) NewOptimizedGridWithInfo(gi *GridIndex, tagPairs []TagPair) *Grid {
	offset := int64(0)
	if len(gi.grids) != 0 {
		lastGrid := gi.grids[len(gi.grids)-1]
		lastGridLength := lastGrid.CalLength()
		offset = lastGrid.offset + int64(lastGridLength)
	}

	tagValuess := make([]*TagValues, 0, len(tagPairs))
	for i := 0; i < len(tagPairs); i++ {
		n := gi.GetNumOfFilledUpGridForSingleTagKey(tagPairs[i].TagKey)
		tagValuess = append(tagValuess, newTagValues(PowInt(a.multiplier, n)*a.basicNum))
		tagValuess[i].SetValue(tagPairs[i].TagValue)
	}

	grid := newGrid(offset, tagPairs, tagValuess)
	return grid
}
