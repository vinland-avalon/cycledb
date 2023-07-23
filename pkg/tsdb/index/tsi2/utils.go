package tsi2

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"io"
	"math"

	"github.com/influxdata/influxdb/pkg/rhh"
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

// // unionStringSets returns the union of two sets
// func unionStringSets(a, b map[string]struct{}) map[string]struct{} {
// 	other := make(map[string]struct{})
// 	for k := range a {
// 		other[k] = struct{}{}
// 	}
// 	for k := range b {
// 		other[k] = struct{}{}
// 	}
// 	return other
// }

// // intersectStringSets returns the intersection of two sets.
// func intersectStringSets(a, b map[string]struct{}) map[string]struct{} {
// 	if len(a) < len(b) {
// 		a, b = b, a
// 	}

// 	other := make(map[string]struct{})
// 	for k := range a {
// 		if _, ok := b[k]; ok {
// 			other[k] = struct{}{}
// 		}
// 	}
// 	return other
// }

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

type FileHashMap struct {
	// data []byte
}

func NewFileHashMap() FileHashMap {
	return FileHashMap{}
}

func (fh *FileHashMap) FlushTo(w io.Writer, m map[uint64]uint64) error {
	n := int64(0)

	// to avoid offset of zero
	writeUint64To(w, uint64(0), &n)

	// Build key hash map
	rhhm := rhh.NewHashMap(rhh.Options{
		Capacity:   int64(len(m)),
		LoadFactor: LoadFactor,
	})

	for k, v := range m {
		// fmt.Printf("put k:%v, v:%v at %v\n", k, v, n)
		rhhm.Put(itob(k), n)
		writeUint64To(w, k, &n)
		writeUint64To(w, v, &n)
	}

	indexOffset := n

	// Encode hash map length.
	if err := writeUint64To(w, uint64(rhhm.Cap()), &n); err != nil {
		return err
	}

	// Encode hash map offset entries.
	for i := int64(0); i < rhhm.Cap(); i++ {
		_, v := rhhm.Elem(i)

		var offset int64
		if tmpOffset, ok := v.(int64); ok {
			offset = tmpOffset
		}
		// fmt.Printf("set elem: %v at %v\n", v, n)
		if err := writeUint64To(w, uint64(offset), &n); err != nil {
			return err
		}
	}

	writeUint64To(w, uint64(indexOffset), &n)

	return nil
}

func itob(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

// func (fh *FHash) ReadDataFrom(buf []byte) error {

// }

func (fh *FileHashMap) Get(buf []byte, k uint64) (uint64, bool) {
	indexOffset := int64(binary.BigEndian.Uint64(buf[len(buf)-8:]))

	n := int64(binary.BigEndian.Uint64(buf[indexOffset : indexOffset+8]))
	indexOffset += 8
	hash := rhh.HashKey(itob(k))
	pos := hash % n
	// fmt.Printf("k: %v, hashKey: %v, pos: %v\n", k, hash, pos)

	// Track current distance
	var d int64
	for {
		// Find offset of k/v pair.
		offset := binary.BigEndian.Uint64(buf[indexOffset+(pos*8):])
		// fmt.Printf("find %v at %v\n", k, offset)
		if offset == 0 {
			return 0, false
		}

		// Evaluate key if offset is not empty.
		if offset > 0 {
			// Parse into element.
			tmpK := binary.BigEndian.Uint64(buf[offset : offset+8])
			// Return if key match.
			if k == tmpK {
				return binary.BigEndian.Uint64(buf[offset+8 : offset+16]), true
			}

			// Check if we've exceeded the probe distance.
			if d > rhh.Dist(rhh.HashKey(itob(tmpK)), pos, n) {
				return 0, false
			}
		}

		// Move position forward.
		pos = (pos + 1) % n
		d++

		if d > n {
			return 0, false
		}
	}
}

type IdMap struct {
	m map[uint64]uint64
}

func NewIdMap() *IdMap {
	return &IdMap{}
}

func (idm *IdMap) FlushTo(w io.Writer, m map[uint64]uint64) error {
	// Encode the map and write it to the file
	encoder := gob.NewEncoder(w)
	if err := encoder.Encode(m); err != nil {
		return err
	}
	return nil
}

func (idm *IdMap) ReadFrom(buf []byte) {
	// Create an instance to decode the map
	decoder := gob.NewDecoder(bytes.NewReader(buf))

	// Create a map to hold the decoded data
	idm.m = make(map[uint64]uint64)

	// Decode the map from the file
	if err := decoder.Decode(&idm.m); err != nil {
		panic(err)
	}
}

func (idm *IdMap) Get(k uint64) (uint64, bool) {
	v, ok := idm.m[k]
	return v, ok
}
