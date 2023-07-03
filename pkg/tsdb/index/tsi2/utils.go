package tsi2

import (
	"math"
)

func PowUint64(x, y int) uint64 {
	return uint64(math.Pow(float64(x), float64(y)))
}

// VariableBaseConvert: dimension: [[value,capacity]], if value==all, value==-1
func VariableBaseConvert(indexes []int, capacities []uint64, idx int, previous []uint64) []uint64 {
	if idx == len(indexes) {
		return previous
	}
	index := indexes[idx]
	capacity := capacities[idx]
	if index != -1 {
		for i := range previous {
			previous[i] = previous[i]*capacity + uint64(index)
		}
		return VariableBaseConvert(indexes, capacities, idx+1, previous)
	} else {
		curr := make([]uint64, 0, uint64(len(previous))*capacity)
		for i := uint64(0); i < capacity; i++ {
			for j := range previous {
				curr = append(curr, previous[j]*capacity+i)
			}
		}
		return VariableBaseConvert(indexes, capacities, idx+1, curr)
	}
}

// unionStringSets returns the union of two sets
func unionStringSets(a, b map[string]struct{}) map[string]struct{} {
	other := make(map[string]struct{})
	for k := range a {
		other[k] = struct{}{}
	}
	for k := range b {
		other[k] = struct{}{}
	}
	return other
}

// intersectStringSets returns the intersection of two sets.
func intersectStringSets(a, b map[string]struct{}) map[string]struct{} {
	if len(a) < len(b) {
		a, b = b, a
	}

	other := make(map[string]struct{})
	for k := range a {
		if _, ok := b[k]; ok {
			other[k] = struct{}{}
		}
	}
	return other
}

// unionStringSets returns the union of two sets
func unionStringSets2(a map[string]struct{}, b map[string]int) map[string]struct{} {
	other := make(map[string]struct{})
	for k := range a {
		other[k] = struct{}{}
	}
	for k := range b {
		other[k] = struct{}{}
	}
	return other
}

func mapToSlice(m map[string]struct{}) [][]byte {
	res := make([][]byte, 0, len(m))
	for key, _ := range m {
		res = append(res, []byte(key))
	}
	return res
}

func SeriesIdWithMeasurementId(measurementId, id uint64) uint64 {
	return measurementId<<32 | id
}
