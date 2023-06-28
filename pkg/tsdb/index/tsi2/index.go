package tsi2

import (
	"fmt"
	"regexp"

	"github.com/influxdata/influxdb/pkg/bytesutil"
	"github.com/influxdata/influxdb/pkg/estimator"
	"github.com/influxdata/influxdb/v2/models"
	"github.com/influxdata/influxql"
	"go.uber.org/zap"

	"cycledb/pkg/tsdb"
)

var Version = 1

type Index struct {
	// todo(vinland): partition

	measurements *Measurements

	logger *zap.Logger // Index's logger.

	// The following must be set when initializing an Index.
	sfile    *tsdb.SeriesFile // series lookup file
	database string           // Name of database.

	// todo(vinland): to estimate bytes
	// need to be protected by mutex
	// // Cached sketches.
	// mSketch, mTSketch estimator.Sketch // Measurement sketches, add, delete?
	// sSketch, sTSketch estimator.Sketch // Series sketches

	// Fast series lookup of series IDs in the series file that have been present
	// in this partition. This set tracks both insertions and deletions of a series.
	// whithin partition for tsi
	// not actually used
	seriesIDSet *tsdb.SeriesIDSet

	// Index's version.
	version int
}

// NewIndex returns a new instance of Index.
func NewIndex(sfile *tsdb.SeriesFile, database string) *Index {
	return &Index{
		logger:   zap.NewNop(),
		version:  Version,
		sfile:    sfile,
		database: database,
	}
}

func (i *Index) Open() error {
	i.measurements = NewMeasurements()
	i.seriesIDSet = tsdb.NewSeriesIDSet()
	return nil
}

func (i *Index) Close() error {
	return nil
}

func (i *Index) WithLogger(*zap.Logger) {}

func (i *Index) Database() string {
	return i.database
}

func (i *Index) MeasurementExists(name []byte) (bool, error) {
	// if _, ok := i.measurementToGIndexes[string(name[:])]; !ok {
	// 	return false, nil
	// } else {
	// 	return true, nil
	// }
	gIndex, err := i.measurements.MeasurementByName(name)
	if err != nil {
		return false, err
	}
	if gIndex == nil {
		return false, nil
	}
	return true, nil
}

func (i *Index) MeasurementNamesByRegex(re *regexp.Regexp) ([][]byte, error) {
	// return [][]byte{[]byte("measurement_test")}, nil
	var res [][]byte
	for m, _ := range i.measurements.measurementId {
		if re.MatchString(m) {
			// Clone bytes since they will be used after the fileset is released.
			res = append(res, bytesutil.Clone([]byte(m)))
		}
	}
	return res, nil
}

func (i *Index) DropMeasurement(name []byte) error {
	return i.measurements.DropMeasurement(name)
}

func (i *Index) ForEachMeasurementName(fn func(name []byte) error) error {
	for m, _ := range i.measurements.measurementId {
		if err := fn([]byte(m)); err != nil {
			return err
		}
	}
	return nil
}

func (i *Index) CreateSeriesListIfNotExists(keys, names [][]byte, tagsSlice []models.Tags) error {
	if len(names) == 0 {
		return nil
	} else if len(names) != len(tagsSlice) {
		return fmt.Errorf("uneven batch, sent %d names and %d tags", len(names), len(tagsSlice))
	}
	newIDs := make([]uint64, 0)
	newNames := make([][]byte, 0)
	newTagsSlice := make([]models.Tags, 0)
	for index := range names {
		buf := make([]byte, 1024)
		// 1. check if this seriesKey already exists in seriesFile
		// todo(vinland): `key`` is a combined []byte, order of tags make a difference
		// todo(vinland): series file and index could have inconsistent series
		if exist := i.sfile.HasSeries(names[index], tagsSlice[index], buf); !exist {
			newTagsSlice = append(newTagsSlice, tagsSlice[index])
			newNames = append(newNames, names[index])

			// 2. if not. add to grid index
			exist, err := i.MeasurementExists(names[index])
			if err != nil {
				return err
			}
			if !exist {
				i.measurements.AppendMeasurement(names[index])
			}
			// todo(vinland): use model.tags for set function directly
			id := i.measurements.SetTagPairSet(names[index], tagsSlice[index])
			newIDs = append(newIDs, uint64(id))
		}
	}

	// 3. add to seriesFile
	ids, err := i.sfile.CreateSeriesListIfNotExists(newNames, newTagsSlice, newIDs)
	if err != nil {
		return err
	}

	// todo(vinland): drop the checks for production env
	if len(ids) != len(newIDs) {
		return fmt.Errorf("CreateSeriesListIfNotExist: ids in grid index is not same as ids in series file, %d to %d", len(ids), len(newIDs))
	}
	for i, id := range newIDs {
		if id != ids[i] {
			return fmt.Errorf("CreateSeriesListIfNotExist: ids in grid index is not same as ids in series file, %d to %d at %d", id, ids[i], i)
		}
	}
	return nil
}

func (i *Index) CreateSeriesIfNotExists(key, name []byte, tags models.Tags) error {
	return i.CreateSeriesListIfNotExists([][]byte{name}, [][]byte{name}, []models.Tags{tags})
}

func (i *Index) DropSeries(seriesID uint64, key []byte, cascade bool) error {
	panic("unimplemented")
}
func (i *Index) DropMeasurementIfSeriesNotExist(name []byte) (bool, error) {
	panic("unimplemented")
}

// MeasurementsSketches returns the two measurement sketches for the index.
func (i *Index) MeasurementsSketches() (estimator.Sketch, estimator.Sketch, error) {
	// i.mu.RLock()
	// defer i.mu.RUnlock()
	// return i.mSketch.Clone(), i.mTSketch.Clone(), nil
	panic("not implemented")
}

// SeriesSketches returns the two series sketches for the index.
func (i *Index) SeriesSketches() (estimator.Sketch, estimator.Sketch, error) {
	// i.mu.RLock()
	// defer i.mu.RUnlock()
	// return i.sSketch.Clone(), i.sTSketch.Clone(), nil
	panic("not implemented")
}

func (i *Index) SeriesIDSet() *tsdb.SeriesIDSet {
	return i.seriesIDSet
}

func (i *Index) SeriesN() int64 {
	return int64(i.SeriesIDSet().Cardinality())
}

func (i *Index) HasTagKey(name, key []byte) (bool, error) {
	return i.measurements.HasTagKey(name, key)
}
func (i *Index) HasTagValue(name, key, value []byte) (bool, error) {
	return i.measurements.HasTagValue(name, key, value)
}

// MeasurementTagKeysByExpr extracts the tag keys wanted by the expression.
func (i *Index) MeasurementTagKeysByExpr(name []byte, expr influxql.Expr) (map[string]struct{}, error) {
	// Return all keys if no condition was passed in.
	// if expr == nil {
	// 	m := make(map[string]struct{})
	// 	if itr := i.TagKeyIterator(name); itr != nil {
	// 		for e := itr.Next(); e != nil; e = itr.Next() {
	// 			m[string(e.Key())] = struct{}{}
	// 		}
	// 	}
	// 	return m, nil
	// }

	// switch e := expr.(type) {
	// case *influxql.BinaryExpr:
	// 	switch e.Op {
	// 	case influxql.EQ, influxql.NEQ, influxql.EQREGEX, influxql.NEQREGEX:
	// 		tag, ok := e.LHS.(*influxql.VarRef)
	// 		if !ok {
	// 			return nil, fmt.Errorf("left side of '%s' must be a tag key", e.Op.String())
	// 		} else if tag.Val != "_tagKey" {
	// 			return nil, nil
	// 		}

	// 		if influxql.IsRegexOp(e.Op) {
	// 			re, ok := e.RHS.(*influxql.RegexLiteral)
	// 			if !ok {
	// 				return nil, fmt.Errorf("right side of '%s' must be a regular expression", e.Op.String())
	// 			}
	// 			return i.tagKeysByFilter(name, e.Op, nil, re.Val), nil
	// 		}

	// 		s, ok := e.RHS.(*influxql.StringLiteral)
	// 		if !ok {
	// 			return nil, fmt.Errorf("right side of '%s' must be a tag value string", e.Op.String())
	// 		}
	// 		return i.tagKeysByFilter(name, e.Op, []byte(s.Val), nil), nil

	// 	case influxql.AND, influxql.OR:
	// 		lhs, err := i.MeasurementTagKeysByExpr(name, e.LHS)
	// 		if err != nil {
	// 			return nil, err
	// 		}

	// 		rhs, err := i.MeasurementTagKeysByExpr(name, e.RHS)
	// 		if err != nil {
	// 			return nil, err
	// 		}

	// 		if lhs != nil && rhs != nil {
	// 			if e.Op == influxql.OR {
	// 				return unionStringSets(lhs, rhs), nil
	// 			}
	// 			return intersectStringSets(lhs, rhs), nil
	// 		} else if lhs != nil {
	// 			return lhs, nil
	// 		} else if rhs != nil {
	// 			return rhs, nil
	// 		}
	// 		return nil, nil
	// 	default:
	// 		return nil, fmt.Errorf("invalid operator for tag keys by expression")
	// 	}

	// case *influxql.ParenExpr:
	// 	return i.MeasurementTagKeysByExpr(name, e.Expr)
	// }

	// return nil, fmt.Errorf("invalid measurement tag keys expression: %#v", expr)
	panic("not implemented")
}

// TagKeyCardinality always returns zero.
// It is not possible to determine cardinality of tags across index files, and
// thus it cannot be done across partitions.
func (i *Index) TagKeyCardinality(name, key []byte) int {
	return 0
}

func (i *Index) MeasurementIterator() (tsdb.MeasurementIterator, error) {
	return NewMeasurementsIterator(i.measurements), nil
}

func (i *Index) TagKeyIterator(name []byte) (tsdb.TagKeyIterator, error) {
	m, err := i.measurements.MeasurementByName(name)
	if err != nil || m == nil {
		return nil, err
	}
	return NewTagKeyIterator(m.gIndex), nil
}

func (i *Index) TagValueIterator(name, key []byte) (tsdb.TagValueIterator, error) {
	m, err := i.measurements.MeasurementByName(name)
	if err != nil || m == nil {
		return nil, err
	}
	return NewTagValueIterator(m.gIndex, key), nil
}

// todo: iterators for series ids should be filtered with bit map
func (i *Index) MeasurementSeriesIDIterator(name []byte) (tsdb.SeriesIDIterator, error) {
	ok, err := i.MeasurementExists(name)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}
	return i.measurements.MeasurementSeriesIDIterator(name)
}
