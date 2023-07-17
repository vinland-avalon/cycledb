package tsi2_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/influxdata/influxdb/pkg/bloom"
	"github.com/influxdata/influxdb/v2/models"
	"github.com/stretchr/testify/assert"
)

func BenchmarkLogFile_WriteTo(b *testing.B) {
	tmp_gen := generators[FPGen]
	tagKeyNum := 4
	for _, tagValueNum := range []int{11} {
		name := fmt.Sprintf("tagKeyNum=%d, tagValueNum=%d", tagKeyNum, tagValueNum)
		b.Run(name, func(b *testing.B) {
			idx := MustOpenDefaultIndex(b)
			defer idx.Close()

			tagsSlice := tmp_gen.GenerateInsertTagsSlice(tagKeyNum, tagValueNum)

			// Estimate bloom filter size.
			m, k := bloom.Estimate(uint64(len(tagsSlice)), 0.02)

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
				if _, err := idx.CompactTo(buf, m, k); err != nil {
					b.Fatal(err)
				}
				if i == 0 {
					b.Logf("sz=%db", buf.Len())
				}
			}
		})
	}
}
