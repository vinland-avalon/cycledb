package tsi2

type TagValues struct {
	capacity uint64
	// TODO(vinland-avalon): any
	values       []string
	valueToIndex map[string]int
}

func newTagValues(cap uint64) *TagValues {
	return &TagValues{
		capacity:     cap,
		values:       []string{},
		valueToIndex: map[string]int{},
	}
}

// SetValue: return whether set succeed.
// If already exist or append new value to the end, return true.
// If reach capacity, return false.
func (tvs *TagValues) SetValue(v string) bool {
	if tvs.capacity == uint64(len(tvs.values)) {
		return false
	}
	if _, ok := tvs.valueToIndex[v]; ok {
		return true
	}

	tvs.valueToIndex[v] = len(tvs.values)
	tvs.values = append(tvs.values, v)
	return true
}

// GetValueIndex: return -1 if not exist
func (tvs *TagValues) GetValueIndex(value string) int {
	if index, ok := tvs.valueToIndex[value]; !ok {
		return -1
	} else {
		return index
	}
}
