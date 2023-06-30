package tsi2_test

import (
	// "reflect"
	"testing"

	"cycledb/pkg/tsdb"
	"cycledb/pkg/tsdb/index/tsi2"

	"github.com/influxdata/influxdb/v2/models"
	"github.com/stretchr/testify/assert"
)

// SeriesFile is a test wrapper for tsdb.SeriesFile.
type SeriesFile struct {
	*tsdb.SeriesFile
}

// Index is a test wrapper for tsi1.Index.
type Index struct {
	*tsi2.Index
	SeriesFile *SeriesFile
}

// MustOpenIndex returns a new, open index. Panic on error.
func MustOpenIndex(tb testing.TB) *Index {
	idx := NewIndex(tb)
	if err := idx.Open(); err != nil {
		panic(err)
	}
	return idx
}

// NewIndex returns a new instance of Index at a temporary path.
func NewIndex(tb testing.TB) *Index {
	idx := &Index{SeriesFile: NewSeriesFile(tb)}
	idx.Index = tsi2.NewIndex(idx.SeriesFile.SeriesFile, "db0", tsi2.WithPath(tb.TempDir()))
	return idx
}

// NewSeriesFile returns a new instance of SeriesFile with a temporary file path.
func NewSeriesFile(tb testing.TB) *SeriesFile {
	return &SeriesFile{SeriesFile: tsdb.NewSeriesFile(tb.TempDir())}
}

func TestInterface(t *testing.T) {
	var index tsdb.Index
	index = &tsi2.Index{}
	assert.NotNil(t, index)
}

// Series represents name/tagset pairs that are used in testing.
type Series struct {
	Name    []byte
	Tags    models.Tags
	Deleted bool
}

// CreateSeriesSliceIfNotExists creates multiple series at a time.
func (idx *Index) CreateSeriesSliceIfNotExists(a []Series) error {
	keys := make([][]byte, 0, len(a))
	names := make([][]byte, 0, len(a))
	tags := make([]models.Tags, 0, len(a))
	for _, s := range a {
		keys = append(keys, models.MakeKey(s.Name, s.Tags))
		names = append(names, s.Name)
		tags = append(tags, s.Tags)
	}
	return idx.CreateSeriesListIfNotExists(keys, names, tags)
}

// Run is the same is RunStateAware but for tests that do not depend on compaction state
func (idx *Index) Run(t *testing.T, fn func(t *testing.T)) {
	idx.RunStateAware(t, func(t *testing.T, _ int) {
		fn(t)
	})
}

const (
	Initial = iota
	Reopen
	PostCompaction
	PostCompactionReopen
)

// Run executes a subtest for each of several different states:
//
// - Immediately
// - After reopen
// - After compaction
// - After reopen again
//
// The index should always respond in the same fashion regardless of
// how data is stored. This helper allows the index to be easily tested
// in all major states.
func (idx *Index) RunStateAware(t *testing.T, fn func(t *testing.T, state int)) {
	// Invoke immediately.
	t.Run("state=initial", curryState(Initial, fn))

	// // Reopen and invoke again.
	// if err := idx.Reopen(tsdb.DefaultMaxIndexLogFileSize); err != nil {
	// 	t.Fatalf("reopen error: %s", err)
	// }
	// t.Run("state=reopen", curryState(Reopen, fn))

	// // Reopen requiring a full compaction of the TSL files and invoke again.
	// idx.Reopen(1)
	// for {
	// 	needsCompaction := false
	// 	for i := 0; i < int(idx.PartitionN); i++ {
	// 		needsCompaction = needsCompaction || idx.PartitionAt(i).NeedsCompaction(false)
	// 	}
	// 	if !needsCompaction {
	// 		break
	// 	}
	// 	time.Sleep(10 * time.Millisecond)
	// }
	// t.Run("state=post-compaction", curryState(PostCompaction, fn))

	// // Reopen and invoke again.
	// if err := idx.Reopen(tsdb.DefaultMaxIndexLogFileSize); err != nil {
	// 	t.Fatalf("post-compaction reopen error: %s", err)
	// }
	// t.Run("state=post-compaction-reopen", curryState(PostCompactionReopen, fn))
}

func curryState(state int, f func(t *testing.T, state int)) func(t *testing.T) {
	return func(t *testing.T) {
		f(t, state)
	}
}

// Ensure index can iterate over all measurement names.
func TestIndex_ForEachMeasurementName(t *testing.T) {
	// idx := MustOpenIndex(t)
	// defer idx.Close()

	// // Add series to index.
	// if err := idx.CreateSeriesSliceIfNotExists([]Series{
	// 	{Name: []byte("cpu"), Tags: models.NewTags(map[string]string{"region": "east"})},
	// 	{Name: []byte("cpu"), Tags: models.NewTags(map[string]string{"region": "west"})},
	// 	{Name: []byte("mem"), Tags: models.NewTags(map[string]string{"region": "east"})},
	// }); err != nil {
	// 	t.Fatal(err)
	// }

	// // Verify measurements are returned.
	// idx.Run(t, func(t *testing.T) {
	// 	var names []string
	// 	if err := idx.ForEachMeasurementName(func(name []byte) error {
	// 		names = append(names, string(name))
	// 		return nil
	// 	}); err != nil {
	// 		t.Fatal(err)
	// 	}

	// 	if !reflect.DeepEqual(names, []string{"cpu", "mem"}) {
	// 		t.Fatalf("unexpected names: %#v", names)
	// 	}
	// })

	// // Add more series.
	// if err := idx.CreateSeriesSliceIfNotExists([]Series{
	// 	{Name: []byte("disk")},
	// 	{Name: []byte("mem")},
	// }); err != nil {
	// 	t.Fatal(err)
	// }

	// // Verify new measurements.
	// idx.Run(t, func(t *testing.T) {
	// 	var names []string
	// 	if err := idx.ForEachMeasurementName(func(name []byte) error {
	// 		names = append(names, string(name))
	// 		return nil
	// 	}); err != nil {
	// 		t.Fatal(err)
	// 	}

	// 	if !reflect.DeepEqual(names, []string{"cpu", "disk", "mem"}) {
	// 		t.Fatalf("unexpected names: %#v", names)
	// 	}
	// })
}
