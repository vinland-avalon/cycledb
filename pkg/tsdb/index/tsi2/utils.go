package tsi2

import "math"

func powInt(x, y int) int {
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
