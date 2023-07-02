package tsi2_test

import (
	"reflect"
	"regexp"
	"sort"
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

// Open opens the underlying tsi1.Index and tsdb.SeriesFile
func (idx Index) Open() error {
	if err := idx.SeriesFile.Open(); err != nil {
		return err
	}
	return idx.Index.Open()
}

// Close closes and removes the index directory.
func (idx *Index) Close() error {
	// Series file is opened first and must be closed last
	if err := idx.Index.Close(); err != nil {
		return err
	}
	if err := idx.SeriesFile.Close(); err != nil {
		return err
	}
	return nil
}

// MustOpenIndex returns a new, open index. Panic on error.
func MustOpenDefaultIndex(tb testing.TB) *Index {
	idx := NewIndex(tb)
	if err := idx.SeriesFile.Open(); err != nil {
		panic(err)
	}
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

func TestSeriesFile(t *testing.T) {

}

// Ensure index can iterate over all measurement names.
func TestIndex_ForEachMeasurementName(t *testing.T) {
	idx := MustOpenDefaultIndex(t)
	defer idx.Close()
	// Add series to index.
	if err := idx.CreateSeriesSliceIfNotExists([]Series{
		{Name: []byte("cpu"), Tags: models.NewTags(map[string]string{"region": "east"})},
		{Name: []byte("cpu"), Tags: models.NewTags(map[string]string{"region": "west"})},
		{Name: []byte("mem"), Tags: models.NewTags(map[string]string{"region": "east"})},
	}); err != nil {
		t.Fatal(err)
	}

	// Verify measurements are returned.
	idx.Run(t, func(t *testing.T) {
		var names []string
		if err := idx.ForEachMeasurementName(func(name []byte) error {
			names = append(names, string(name))
			return nil
		}); err != nil {
			t.Fatal(err)
		}

		if !reflect.DeepEqual(names, []string{"cpu", "mem"}) {
			t.Fatalf("unexpected names: %#v", names)
		}
	})

	// Add more series.
	if err := idx.CreateSeriesSliceIfNotExists([]Series{
		{Name: []byte("disk")},
		{Name: []byte("mem")},
	}); err != nil {
		t.Fatal(err)
	}

	// Verify new measurements.
	idx.Run(t, func(t *testing.T) {
		var names []string
		if err := idx.ForEachMeasurementName(func(name []byte) error {
			names = append(names, string(name))
			return nil
		}); err != nil {
			t.Fatal(err)
		}

		// todo(vinland): check if it is necessary to return names in order
		sort.Strings(names)

		if !reflect.DeepEqual(names, []string{"cpu", "disk", "mem"}) {
			t.Fatalf("unexpected names: %#v", names)
		}
	})
}

// Ensure index can return whether a measurement exists.
func TestIndex_MeasurementExists(t *testing.T) {
	idx := MustOpenDefaultIndex(t)
	defer idx.Close()

	// Add series to index.
	if err := idx.CreateSeriesSliceIfNotExists([]Series{
		{Name: []byte("cpu"), Tags: models.NewTags(map[string]string{"region": "east"})},
		{Name: []byte("cpu"), Tags: models.NewTags(map[string]string{"region": "west"})},
	}); err != nil {
		t.Fatal(err)
	}

	// Verify measurement exists.
	idx.Run(t, func(t *testing.T) {
		if v, err := idx.MeasurementExists([]byte("cpu")); err != nil {
			t.Fatal(err)
		} else if !v {
			t.Fatal("expected measurement to exist")
		}
	})

	// todo(vinland): in origin tsi and series file, id == 0 means id not exists, while in grid index not
	name, tags := []byte("cpu"), models.NewTags(map[string]string{"region": "west"})
	sid := idx.Index.SeriesFile().SeriesID(name, tags, nil)
	if sid == 0 {
		t.Fatalf("got 0 series id for %s/%v", name, tags)
	}

	// // todo(vinland): drop series is unimplemented so far
	// // Delete one series.
	// if err := idx.DropSeries(sid, models.MakeKey(name, tags), true); err != nil {
	// 	t.Fatal(err)
	// }

	// // Verify measurement still exists.
	// idx.Run(t, func(t *testing.T) {
	// 	if v, err := idx.MeasurementExists([]byte("cpu")); err != nil {
	// 		t.Fatal(err)
	// 	} else if !v {
	// 		t.Fatal("expected measurement to still exist")
	// 	}
	// })

	// // Delete second series.
	// tags.Set([]byte("region"), []byte("west"))
	// sid = idx.Index.SeriesFile().SeriesID(name, tags, nil)
	// if sid == 0 {
	// 	t.Fatalf("got 0 series id for %s/%v", name, tags)
	// }
	// if err := idx.DropSeries(sid, models.MakeKey(name, tags), true); err != nil {
	// 	t.Fatal(err)
	// }

	// // Verify measurement is now deleted.
	// idx.Run(t, func(t *testing.T) {
	// 	if v, err := idx.MeasurementExists([]byte("cpu")); err != nil {
	// 		t.Fatal(err)
	// 	} else if v {
	// 		t.Fatal("expected measurement to be deleted")
	// 	}
	// })
}

// Ensure index can return a list of matching measurements.
func TestIndex_MeasurementNamesByRegex(t *testing.T) {
	idx := MustOpenDefaultIndex(t)
	defer idx.Close()

	// Add series to index.
	if err := idx.CreateSeriesSliceIfNotExists([]Series{
		{Name: []byte("cpu")},
		{Name: []byte("disk")},
		{Name: []byte("mem")},
	}); err != nil {
		t.Fatal(err)
	}

	// Retrieve measurements by regex.
	idx.Run(t, func(t *testing.T) {
		names, err := idx.MeasurementNamesByRegex(regexp.MustCompile(`cpu|mem`))
		if err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(names, [][]byte{[]byte("cpu"), []byte("mem")}) {
			t.Fatalf("unexpected names: %v", names)
		}
	})
}

// Ensure index can delete a measurement and all related keys, values, & series.
func TestIndex_DropMeasurement(t *testing.T) {
	idx := MustOpenDefaultIndex(t)
	defer idx.Close()

	// Add series to index.
	if err := idx.CreateSeriesSliceIfNotExists([]Series{
		{Name: []byte("cpu"), Tags: models.NewTags(map[string]string{"region": "east"})},
		{Name: []byte("cpu"), Tags: models.NewTags(map[string]string{"region": "west"})},
		{Name: []byte("disk"), Tags: models.NewTags(map[string]string{"region": "north"})},
		{Name: []byte("mem"), Tags: models.NewTags(map[string]string{"region": "west", "country": "us"})},
	}); err != nil {
		t.Fatal(err)
	}

	// Drop measurement.
	if err := idx.DropMeasurement([]byte("cpu")); err != nil {
		t.Fatal(err)
	}

	// Verify data is gone in each stage.
	idx.Run(t, func(t *testing.T) {
		// Verify measurement is gone.
		if v, err := idx.MeasurementExists([]byte("cpu")); err != nil {
			t.Fatal(err)
		} else if v {
			t.Fatal("expected no measurement")
		}

		// // todo(vinland): Drop Measurement should also do something in series files
		// // Obtain file set to perform lower level checks.
		// fs, err := idx.PartitionAt(0).RetainFileSet()
		// if err != nil {
		// 	t.Fatal(err)
		// }
		// defer fs.Release()

		// // Verify tags & values are gone.
		// if e := fs.TagKeyIterator([]byte("cpu")).Next(); e != nil && !e.Deleted() {
		// 	t.Fatal("expected deleted tag key")
		// }
		// if itr := fs.TagValueIterator([]byte("cpu"), []byte("region")); itr != nil {
		// 	t.Fatal("expected nil tag value iterator")
		// }

	})
}

func TestIndex_Open(t *testing.T) {
	t.Run("open new index", func(t *testing.T) {
		// Opening a fresh index should set the MANIFEST version to current version.
		idx := MustOpenDefaultIndex(t)
		t.Cleanup(func() { assert.NoError(t, idx.Close()) })

		// 	// Check version set appropriately.
		// 	for i := 0; uint64(i) < tsi1.DefaultPartitionN; i++ {
		// 		partition := idx.PartitionAt(i)

		// 		if got, exp := partition.Manifest().Version, 1; got != exp {
		// 			t.Fatalf("got index version %d, expected %d", got, exp)
		// 		}
		// 	}

		// 	for i := 0; i < int(idx.PartitionN); i++ {
		// 		p := idx.PartitionAt(i)

		// 		if got, exp := p.NeedsCompaction(false), false; got != exp {
		// 			t.Fatalf("got needs compaction %v, expected %v", got, exp)
		// 		}
		// 	}
	})

	// Reopening an open index should return an error.
	t.Run("reopen open index", func(t *testing.T) {
		idx := MustOpenDefaultIndex(t)
		t.Cleanup(func() { assert.NoError(t, idx.Close()) })

		// Manually closing the existing SeriesFile so that it won't be left
		// opened after idx.Open(), which calls another idx.SeriesFile.Open().
		//
		// This is required for t.TempDir() to be cleaned-up successfully on
		// Windows.
		assert.NoError(t, idx.SeriesFile.Close())

		err := idx.Open()
		if err == nil {
			t.Fatal("didn't get an error on reopen, but expected one")
		}
	})

	// // Opening an incompatible index should return an error.
	// incompatibleVersions := []int{-1, 0, 2}
	// for _, v := range incompatibleVersions {
	// 	t.Run(fmt.Sprintf("incompatible index version: %d", v), func(t *testing.T) {
	// 		idx := NewDefaultIndex(t)

	// 		// Manually create a MANIFEST file for an incompatible index version.
	// 		// under one of the partitions.
	// 		partitionPath := filepath.Join(idx.Path(), "2")
	// 		os.MkdirAll(partitionPath, 0777)

	// 		mpath := filepath.Join(partitionPath, tsi1.ManifestFileName)
	// 		m := tsi1.NewManifest(mpath)
	// 		m.Levels = nil
	// 		m.Version = v // Set example MANIFEST version.
	// 		if _, err := m.Write(); err != nil {
	// 			t.Fatal(err)
	// 		}

	// 		// Log the MANIFEST file.
	// 		data, err := os.ReadFile(mpath)
	// 		if err != nil {
	// 			panic(err)
	// 		}
	// 		t.Logf("Incompatible MANIFEST: %s", data)

	// 		// Opening this index should return an error because the MANIFEST has an
	// 		// incompatible version.
	// 		err = idx.Open()
	// 		t.Cleanup(func() { assert.NoError(t, idx.Close()) })
	// 		if !errors.Is(err, tsi1.ErrIncompatibleVersion) {
	// 			t.Fatalf("got error %v, expected %v", err, tsi1.ErrIncompatibleVersion)
	// 		}
	// 	})
	// }
}

func TestIndex_TagValueSeriesIDIterator(t *testing.T) {
	// idx1 := MustOpenDefaultIndex(t) // Uses the single series creation method CreateSeriesIfNotExists
	// defer idx1.Close()
	idx2 := MustOpenDefaultIndex(t) // Uses the batch series creation method CreateSeriesListIfNotExists
	defer idx2.Close()

	// Add some series.
	data := []struct {
		Key  string
		Name string
		Tags map[string]string
	}{
		{"cpu,region=west,server=a", "cpu", map[string]string{"region": "west", "server": "a"}},
		{"cpu,region=west,server=b", "cpu", map[string]string{"region": "west", "server": "b"}},
		{"cpu,region=east,server=a", "cpu", map[string]string{"region": "east", "server": "a"}},
		{"cpu,region=north,server=c", "cpu", map[string]string{"region": "north", "server": "c"}},
		{"cpu,region=south,server=s", "cpu", map[string]string{"region": "south", "server": "s"}},
		{"mem,region=west,server=a", "mem", map[string]string{"region": "west", "server": "a"}},
		{"mem,region=west,server=b", "mem", map[string]string{"region": "west", "server": "b"}},
		{"mem,region=west,server=c", "mem", map[string]string{"region": "west", "server": "c"}},
		{"disk,region=east,server=a", "disk", map[string]string{"region": "east", "server": "a"}},
		{"disk,region=east,server=a", "disk", map[string]string{"region": "east", "server": "a"}},
		{"disk,region=north,server=c", "disk", map[string]string{"region": "north", "server": "c"}},
	}

	var batchKeys [][]byte
	var batchNames [][]byte
	var batchTags []models.Tags
	for _, pt := range data {
		// if err := idx1.CreateSeriesIfNotExists([]byte(pt.Key), []byte(pt.Name), models.NewTags(pt.Tags)); err != nil {
		// 	t.Fatal(err)
		// }

		batchKeys = append(batchKeys, []byte(pt.Key))
		batchNames = append(batchNames, []byte(pt.Name))
		batchTags = append(batchTags, models.NewTags(pt.Tags))
	}

	if err := idx2.CreateSeriesListIfNotExists(batchKeys, batchNames, batchTags); err != nil {
		t.Fatal(err)
	}

	// fmt.Printf("idx2.indexToSeriesFile: %v\n", idx2.IndexIdToSeriesFileId)

	testTagValueSeriesIDIterator := func(t *testing.T, name, key, value string, expKeys []string) {
		// for i, idx := range []*Index{idx1, idx2} {
		for i, idx := range []*Index{idx2} {
			sitr, err := idx.TagValueSeriesIDIterator([]byte(name), []byte(key), []byte(value))
			if err != nil {
				t.Fatalf("[index %d] %v", i, err)
			} else if sitr == nil {
				t.Fatalf("[index %d] series id iterater nil", i)
			}

			// fmt.Printf("sitr: %+v", sitr)
			// for e, err := sitr.Next(); err == nil; e, err = sitr.Next() {
			// 	if e == nil {
			// 		break
			// 	}
			// 	fmt.Printf("e: %+v, %+v", e.SeriesID, e.Expr)
			// }

			// Convert series ids to series keys.
			itr := tsdb.NewSeriesIteratorAdapter(idx.SeriesFile.SeriesFile, sitr)
			if itr == nil {
				t.Fatalf("[index %d] got nil iterator", i)
			}
			defer itr.Close()

			var keys []string
			for e, err := itr.Next(); err == nil; e, err = itr.Next() {
				if e == nil {
					break
				}
				keys = append(keys, string(models.MakeKey(e.Name(), e.Tags())))
			}

			if err != nil {
				t.Fatal(err)
			}

			// Iterator was in series id order, which may not be series key order.
			sort.Strings(keys)
			if got, exp := keys, expKeys; !reflect.DeepEqual(got, exp) {
				t.Fatalf("[index %d] got %v, expected %v", i, got, exp)
			}
		}
	}

	// Test that correct series are initially returned
	t.Run("initial", func(t *testing.T) {
		testTagValueSeriesIDIterator(t, "cpu", "region", "west", []string{
			"cpu,region=west,server=a",
			"cpu,region=west,server=b",
		})
	})

	// Test that correct series are initially returned
	t.Run("initial", func(t *testing.T) {
		testTagValueSeriesIDIterator(t, "mem", "region", "west", []string{
			"mem,region=west,server=a",
			"mem,region=west,server=b",
			"mem,region=west,server=c",
		})
	})

	// // The result should now be cached, and the same result should be returned.
	// t.Run("cached", func(t *testing.T) {
	// 	testTagValueSeriesIDIterator(t, "mem", "region", "west", []string{
	// 		"mem,region=west,server=a",
	// 		"mem,region=west,server=b",
	// 		"mem,region=west,server=c",
	// 	})
	// })

	// // // Adding a new series that would be referenced by some cached bitsets (in this case
	// // // the bitsets for mem->region->west and mem->server->c) should cause the cached
	// // // bitsets to be updated.
	// // if err := idx1.CreateSeriesIfNotExists(
	// // 	[]byte("mem,region=west,root=x,server=c"),
	// // 	[]byte("mem"),
	// // 	models.NewTags(map[string]string{"region": "west", "root": "x", "server": "c"}),
	// // ); err != nil {
	// // 	t.Fatal(err)
	// // }

	// if err := idx2.CreateSeriesListIfNotExists(
	// 	[][]byte{[]byte("mem,region=west,root=x,server=c")},
	// 	[][]byte{[]byte("mem")},
	// 	[]models.Tags{models.NewTags(map[string]string{"region": "west", "root": "x", "server": "c"})},
	// ); err != nil {
	// 	t.Fatal(err)
	// }

	// t.Run("insert series", func(t *testing.T) {
	// 	testTagValueSeriesIDIterator(t, "mem", "region", "west", []string{
	// 		"mem,region=west,root=x,server=c",
	// 		"mem,region=west,server=a",
	// 		"mem,region=west,server=b",
	// 		"mem,region=west,server=c",
	// 	})
	// })

	// // if err := idx1.CreateSeriesIfNotExists(
	// // 	[]byte("mem,region=west,root=x,server=c"),
	// // 	[]byte("mem"),
	// // 	models.NewTags(map[string]string{"region": "west", "root": "x", "server": "c"}),
	// // ); err != nil {
	// // 	t.Fatal(err)
	// // }

	// if err := idx2.CreateSeriesListIfNotExists(
	// 	[][]byte{[]byte("mem,region=west,root=x,server=c")},
	// 	[][]byte{[]byte("mem")},
	// 	[]models.Tags{models.NewTags(map[string]string{"region": "west", "root": "x", "server": "c"})},
	// ); err != nil {
	// 	t.Fatal(err)
	// }

	// t.Run("insert same series", func(t *testing.T) {
	// 	testTagValueSeriesIDIterator(t, "mem", "region", "west", []string{
	// 		"mem,region=west,root=x,server=c",
	// 		"mem,region=west,server=a",
	// 		"mem,region=west,server=b",
	// 		"mem,region=west,server=c",
	// 	})
	// })

	// t.Run("no matching series", func(t *testing.T) {
	// 	testTagValueSeriesIDIterator(t, "foo", "bar", "zoo", nil)
	// })
}
