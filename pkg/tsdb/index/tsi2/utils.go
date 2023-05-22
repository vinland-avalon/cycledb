package tsi2

import "math"

func PowInt(x, y int) int {
	return int(math.Pow(float64(x), float64(y)))
}

func IfTagPairsEqual(a, b []TagPair) bool {
	if len(a) != len(b) {
		return false
	}
	aMap := map[string]string{}
	for _, tagPair := range a {
		aMap[tagPair.TagKey] = tagPair.TagValue
	}
	for _, tagPair := range b {
		if v, ok := aMap[tagPair.TagKey]; !ok || v != tagPair.TagValue {
			return false
		}
	}
	return true
}

// VariableBaseConvert: dimension: [[value,capacity]], if value==all, value==-1
func VariableBaseConvert(dimensions [][]int, idx int, previous []int64) []int64 {
	if idx == len(dimensions) {
		return previous
	}
	d := dimensions[idx]
	if d[0] != -1 {
		for i := range previous {
			previous[i] = previous[i]*int64(d[1]) + int64(d[0])
		}
		return VariableBaseConvert(dimensions, idx+1, previous)
	} else {
		curr := make([]int64, 0, len(previous)*d[1])
		for i := 0; i < d[1]; i++ {
			for j := range previous {
				curr = append(curr, previous[j]*int64(d[1])+int64(i))
			}
		}
		return VariableBaseConvert(dimensions, idx+1, curr)
	}
}
