package tsi2

import "github.com/influxdata/influxdb/v2/models"

type Optimizer interface {
	// To Generate a new grid with information of GridIndex
	NewOptimizedGrid(*GridIndex, models.Tags) *Grid
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

func (a *MultiplierOptimizer) NewOptimizedGrid(gi *GridIndex, tags models.Tags) *Grid {
	// so that the id begins at 1, not 0
	offset := uint64(1)
	if len(gi.grids) != 0 {
		lastGrid := gi.grids[len(gi.grids)-1]
		lastGridLength := lastGrid.getCapacityOfIDs()
		offset = lastGrid.offset + uint64(lastGridLength)
	}

	tagValuess := make([]*TagValues, 0, len(tags))
	for i := 0; i < len(tags); i++ {
		n := gi.GetNumOfFilledUpGridForSingleTagKey(string(tags[i].Key))
		tagValuess = append(tagValuess, newTagValues(PowUint64(a.multiplier, n)*uint64(a.basicNum)))
		tagValuess[i].SetValue(string(tags[i].Value))
	}

	grid := NewGridWithSingleTags(offset, tags, tagValuess)
	return grid
}
