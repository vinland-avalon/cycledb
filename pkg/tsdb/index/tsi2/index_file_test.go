package tsi2_test

import (
	"bytes"
	"cycledb/pkg/tsdb"
	"cycledb/pkg/tsdb/index/tsi2"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/influxdata/influxdb/v2/models"
	"github.com/stretchr/testify/assert"
)

func BenchmarkIndexFile_WriteTo_Full_Permutation(b *testing.B) {
	tmp_gen := generators[FPGen]
	tagKeyNum := 4
	for _, tagValueNum := range []int{8, 11, 14} {
		name := fmt.Sprintf("tagKeyNum=%d, tagValueNum=%d", tagKeyNum, tagValueNum)
		b.Run(name, func(b *testing.B) {
			idx := MustOpenDefaultIndex(b)
			defer idx.Close()

			tagsSlice := tmp_gen.GenerateInsertTagsSlice(tagKeyNum, tagValueNum)

			// Initialize log file with series data.
			for _, tags := range tagsSlice {
				// if _, err := idx.AddSeriesList(
				// 	seriesSet,
				// 	[][]byte{[]byte("cpu")},
				// 	[]models.Tags{{
				// 		{Key: []byte("host"), Value: []byte(fmt.Sprintf("server-%d", i))},
				// 		{Key: []byte("location"), Value: []byte("us-west")},
				// 	}},
				// ); err != nil {
				// 	b.Fatal(err)
				// }
				key := models.MakeKey([]byte("test"), tags)
				err := idx.CreateSeriesIfNotExists(key, []byte("test"), tags)
				assert.Nil(b, err)
			}
			b.ResetTimer()

			// fmt.Printf("len of idx: %v\ndupli cnt:%v\nset tags:%v\n", len(idx.IndexIdToSeriesFileId), tsi2.Duplicnt, tsi2.SetTags)
			// fmt.Printf("wantToSetTags: %v\n", tsi2.WantToSetTags)
			// Compact log file.
			for i := 0; i < b.N; i++ {
				buf := bytes.NewBuffer(make([]byte, 0, 4*len(tagsSlice)))
				if _, err := idx.CompactTo(buf); err != nil {
					b.Fatal(err)
				}
				if i == 0 {
					b.Logf("sz=%db", buf.Len())
				}
			}
		})
	}
}

func BenchmarkIndexFile_ReadFrom_Full_Permutation(b *testing.B) {
	tmp_gen := generators[FPGen]
	tagKeyNum := 4
	for _, tagValueNum := range []int{4, 5, 6, 7, 8, 9, 10} {
		name := fmt.Sprintf("tagKeyNum=%d, tagValueNum=%d", tagKeyNum, tagValueNum)
		idx := MustOpenDefaultIndex(b)
		defer idx.Close()

		tagsSlice := tmp_gen.GenerateInsertTagsSlice(tagKeyNum, tagValueNum)

		// Initialize log file with series data.
		for _, tags := range tagsSlice {
			// if _, err := idx.AddSeriesList(
			// 	seriesSet,
			// 	[][]byte{[]byte("cpu")},
			// 	[]models.Tags{{
			// 		{Key: []byte("host"), Value: []byte(fmt.Sprintf("server-%d", i))},
			// 		{Key: []byte("location"), Value: []byte("us-west")},
			// 	}},
			// ); err != nil {
			// 	b.Fatal(err)
			// }
			key := models.MakeKey([]byte("test"), tags)
			err := idx.CreateSeriesIfNotExists(key, []byte("test"), tags)
			assert.Nil(b, err)
		}

		// Compact log file.
		id := time.Now().Nanosecond()
		if err := idx.Compact(id); err != nil {
			b.Fatal(err)
		}

		// read
		filename := filepath.Join(tsi2.IndexFilePath, tsi2.FormatIndexFileName(id, 1))
		defer os.Remove(filename)

		b.ResetTimer()

		b.Run(name, func(b *testing.B) {
			// fmt.Printf("len of idx: %v\ndupli cnt:%v\nset tags:%v\n", len(idx.IndexIdToSeriesFileId), tsi2.Duplicnt, tsi2.SetTags)
			// fmt.Printf("wantToSetTags: %v\n", tsi2.WantToSetTags)
			// Compact log file.
			for i := 0; i < b.N; i++ {
				ifile := tsi2.NewIndexFile(filename)
				err := ifile.Restore()
				if err != nil {
					b.Fatal(err)
				}

				rand.Seed(time.Now().UnixNano())
				index := rand.Intn(len(tagsSlice))
				keyIndex := rand.Intn(tagKeyNum)
				idsSet := ifile.SeriesIDSetForTagValue([]byte("test"), []byte(tagsSlice[index][keyIndex].Key), []byte(tagsSlice[index][keyIndex].Value))
				if idsSet.Cardinality() != tsi2.PowUint64(tagValueNum, tagKeyNum-1) {
					b.Fatal()
				}
			}
		})
	}
}

func TestIndexFile_Num(t *testing.T) {
	tmp_gen := generators[FPGen]
	tagKeyNum := 4
	tagValueNum := 5

	idx := MustOpenDefaultIndex(t)
	defer idx.Close()

	tagsSlice := tmp_gen.GenerateInsertTagsSlice(tagKeyNum, tagValueNum)

	// Initialize log file with series data.
	for _, tags := range tagsSlice {
		key := models.MakeKey([]byte("test"), tags)
		err := idx.CreateSeriesIfNotExists(key, []byte("test"), tags)
		assert.Nil(t, err)
	}

	// fmt.Printf("len of idx: %v\ndupli cnt:%v\nset tags:%v\n", len(idx.IndexIdToSeriesFileId), tsi2.Duplicnt, tsi2.SetTags)
	// fmt.Printf("wantToSetTags: %v\n", tsi2.WantToSetTags)
	// Compact log file.
	id := time.Now().Nanosecond()
	if err := idx.Compact(id); err != nil {
		t.Fatal(err)
	}

	// read
	filename := filepath.Join(tsi2.IndexFilePath, tsi2.FormatIndexFileName(id, 1))
	defer os.Remove(filename)

	ifile := tsi2.NewIndexFile(filename)
	err := ifile.Restore()
	assert.Nil(t, err)

	rand.Seed(time.Now().UnixNano())
	index := rand.Intn(len(tagsSlice))
	keyIndex := rand.Intn(tagKeyNum)
	idsSet := ifile.SeriesIDSetForTagValue([]byte("test"), []byte(tagsSlice[index][keyIndex].Key), []byte(tagsSlice[index][keyIndex].Value))
	assert.Equal(t, tsi2.PowUint64(tagValueNum, tagKeyNum-1), idsSet.Cardinality())

	ifile = tsi2.NewIndexFile(filename)
	err = ifile.Restore()
	assert.Nil(t, err)

	rand.Seed(time.Now().UnixNano())
	index = rand.Intn(len(tagsSlice))
	keyIndex = rand.Intn(tagKeyNum)
	idsSet = ifile.SeriesIDSetForTagValue([]byte("test"), []byte(tagsSlice[index][keyIndex].Key), []byte(tagsSlice[index][keyIndex].Value))
	assert.Equal(t, tsi2.PowUint64(tagValueNum, tagKeyNum-1), idsSet.Cardinality())

}

func TestIndexFile_Get(t *testing.T) {
	idx := MustOpenDefaultIndex(t) // Uses the batch series creation method CreateSeriesListIfNotExists
	defer idx.Close()

	var batchNames [][]byte
	var batchTags []models.Tags
	mmNames := []string{"cpu", "mem", "disk"}

	for i := 0; i < 40; i++ {
		for _, name := range mmNames {
			batchNames = append(batchNames, []byte(name))
			m := map[string]string{"region": fmt.Sprintf("region_%d", i), "server": fmt.Sprintf("server_%d", i)}
			batchTags = append(batchTags, models.NewTags(m))
		}
	}

	batchKeys := tsdb.GenerateSeriesKeys(batchNames, batchTags)

	if err := idx.CreateSeriesListIfNotExists(batchKeys, batchNames, batchTags); err != nil {
		t.Fatal(err)
	}

	// fmt.Printf("idx: %+v\n", len(idx.IndexIdToSeriesFileId))

	names := idx.MeasurementNames()
	assert.Equal(t, []string{"cpu", "disk", "mem"}, names)

	id := time.Now().Nanosecond()

	err := idx.Compact(id)
	assert.Nil(t, err)

	filename := filepath.Join(tsi2.IndexFilePath, tsi2.FormatIndexFileName(id, 1))
	defer os.Remove(filename)

	indexFile := tsi2.NewIndexFile(filename)
	indexFile.Restore()
	// SeriesIDSet
	idsSet := indexFile.SeriesIDSet([]byte("disk"))
	assert.Equal(t, uint64(40), idsSet.Cardinality())
	// SeriesIDSetForTagKey
	idsSet = indexFile.SeriesIDSetForTagKey([]byte("disk"), []byte("region"))
	assert.Equal(t, uint64(40), idsSet.Cardinality())
	idsSet = indexFile.SeriesIDSetForTagKey([]byte("disk"), []byte("wrong_key"))
	assert.Equal(t, uint64(0), idsSet.Cardinality())
	// SeriesIDSetForTagValue
	idsSet = indexFile.SeriesIDSetForTagValue([]byte("disk"), []byte("region"), []byte("region_1"))
	assert.Equal(t, uint64(1), idsSet.Cardinality())
	idsSet = indexFile.SeriesIDSetForTagValue([]byte("disk"), []byte("region"), []byte("wrong_value"))
	assert.Equal(t, uint64(0), idsSet.Cardinality())
}
