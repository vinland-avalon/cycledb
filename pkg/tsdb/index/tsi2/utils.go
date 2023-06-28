package tsi2

import (
	"math"

	"github.com/influxdata/influxdb/v2/models"
)

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
func VariableBaseConvert(dimensions [][]int, idx int, previous []uint64) []uint64 {
	if idx == len(dimensions) {
		return previous
	}
	d := dimensions[idx]
	if d[0] != -1 {
		for i := range previous {
			previous[i] = previous[i]*uint64(d[1]) + uint64(d[0])
		}
		return VariableBaseConvert(dimensions, idx+1, previous)
	} else {
		curr := make([]uint64, 0, len(previous)*d[1])
		for i := 0; i < d[1]; i++ {
			for j := range previous {
				curr = append(curr, previous[j]*uint64(d[1])+uint64(i))
			}
		}
		return VariableBaseConvert(dimensions, idx+1, curr)
	}
}

// func tagsConvert(tags []TagPair) (models.Tags){
// 	m := map[string]string{}
// 	for _, tag := range tags {
// 		m[tag.TagKey]=tag.TagValue
// 	}
// 	return models.NewTags(m)
// }

func tagsConvert(tags models.Tags) []TagPair {
	res := make([]TagPair, 0, tags.Len())
	for _, tag := range tags {
		res = append(res, TagPair{TagKey: string(tag.Key), TagValue: string(tag.Value)})
	}
	return res
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
