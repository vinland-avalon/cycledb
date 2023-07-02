package tsi2

type Optimizer interface {
	// To Generate a new grid with information of GridIndex
	NewOptimizedGrid(*GridIndex, []TagPair) *Grid
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

func (a *MultiplierOptimizer) NewOptimizedGrid(gi *GridIndex, tagPairSet []TagPair) *Grid {
	// so that the id begins at 1, not 0
	offset := uint64(1)
	if len(gi.grids) != 0 {
		lastGrid := gi.grids[len(gi.grids)-1]
		lastGridLength := lastGrid.getCapacityOfIDs()
		offset = lastGrid.offset + uint64(lastGridLength)
	}

	tagValuess := make([]*TagValues, 0, len(tagPairSet))
	for i := 0; i < len(tagPairSet); i++ {
		n := gi.GetNumOfFilledUpGridForSingleTagKey(tagPairSet[i].TagKey)
		tagValuess = append(tagValuess, newTagValues(PowUint64(a.multiplier, n)*uint64(a.basicNum)))
		tagValuess[i].SetValue(tagPairSet[i].TagValue)
	}

	grid := initGrid(offset, tagPairSet, tagValuess)
	return grid
}
