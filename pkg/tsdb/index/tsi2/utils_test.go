package tsi2_test

import (
	"bytes"
	"encoding/gob"
	"math/rand"
	"reflect"
	"testing"

	"cycledb/pkg/tsdb/index/tsi2"

	"github.com/stretchr/testify/assert"
)

func TestVariableBaseConvert(t *testing.T) {
	indexes := []int{-1, 1, -1}
	capabilities := []uint64{2, 3, 4}
	prev := []uint64{}
	if indexes[0] != -1 {
		prev = append(prev, uint64(indexes[0]))
	} else {
		for i := uint64(0); i < capabilities[0]; i++ {
			prev = append(prev, uint64(i))
		}
	}
	res := tsi2.VariableBaseConvert(indexes, capabilities, 1, prev)
	assert.Equal(t, len(res), 8)
	assert.True(t, ContainsUint64(res, []uint64{4, 5, 6, 7, 16, 17, 18, 19}))
}

func generateRandomKVPairs(seed int64, count int) map[uint64]uint64 {
	// Set a fixed seed value to ensure reproducibility
	rand.Seed(seed) // You can change this seed value to get different sequences

	randomKeys := make([]uint64, count)
	for i := 0; i < count; i++ {
		randomKeys[i] = rand.Uint64()
	}
	randomValues := make([]uint64, count)
	for i := 0; i < count; i++ {
		randomValues[i] = rand.Uint64()
	}

	m := map[uint64]uint64{}
	for i, key := range randomKeys {
		m[key] = randomValues[i]
	}

	return m
}

func TestRandomNumbers(t *testing.T) {
	nums := generateRandomKVPairs(42, 10)
	assert.True(t, reflect.DeepEqual(nums, generateRandomKVPairs(42, 10)))
}

func TestFileHashMap(t *testing.T) {
	m := generateRandomKVPairs(42, 1000)

	var buf bytes.Buffer

	fhm := tsi2.NewFileHashMap()
	fhm.FlushTo(&buf, m)

	for k, v := range m {
		vfmp, ok := fhm.Get(buf.Bytes(), k)
		assert.Equal(t, v, vfmp)
		assert.True(t, ok)
	}

	qs := generateRandomKVPairs(21, 1000)
	for k := range qs {
		vfmp, ok := fhm.Get(buf.Bytes(), k)
		if wantedv, exist := m[k]; exist {
			assert.Equal(t, wantedv, vfmp)
			assert.True(t, ok)
		} else {
			assert.False(t, ok)
		}
	}
}

func TestGobEncoder(t *testing.T) {
	// Your map from uint64 to uint64
	myMap := make(map[uint64]uint64)
	myMap[42] = 123
	myMap[101] = 456

	// Create a file to write the encoded data
	var buf bytes.Buffer

	// Encode the map and write it to the file
	encoder := gob.NewEncoder(&buf)
	if err := encoder.Encode(myMap); err != nil {
		panic(err)
	}

	// Create an instance to decode the map
	decoder := gob.NewDecoder(&buf)

	// Create a map to hold the decoded data
	decodedMap := make(map[uint64]uint64)

	// Decode the map from the file
	if err := decoder.Decode(&decodedMap); err != nil {
		panic(err)
	}

	// Now you can access values using keys directly from decodedMap
	key := uint64(42)
	value := decodedMap[key]
	assert.Equal(t, value, uint64(123))
}
