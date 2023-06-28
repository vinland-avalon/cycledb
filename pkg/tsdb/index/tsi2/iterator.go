package tsi2

import "cycledb/pkg/tsdb"

type MeasurementIterator struct {
	ms *Measurements
	curr int
}

func NewMeasurementsIterator (m *Measurements) tsdb.MeasurementIterator {
	return &MeasurementIterator{
		ms: m,
		curr: 0,
	}
}

func (itr *MeasurementIterator) Close() (err error) { return nil }

func (itr *MeasurementIterator) Next() ([]byte, error) {
	for itr.curr < len(itr.ms.measurements) {
		if itr.ms.measurements[itr.curr] != nil {
			itr.curr++;
			return []byte(itr.ms.measurements[itr.curr].name), nil
		}
		itr.curr++
	}
	return nil, nil
}

type TagKeyIterator struct {
	keys [][]byte
}

func NewTagKeyIterator (gi *GridIndex) tsdb.TagKeyIterator {
	return gi.NewTagKeyIterator()
}

func (itr *TagKeyIterator) Close() (err error) { return nil }

func (itr *TagKeyIterator) Next() (name []byte, err error) {
	if len(itr.keys) == 0 {
		return nil, nil
	}
	name, itr.keys = itr.keys[0], itr.keys[1:]
	return name, nil
}

type TagValueIterator struct {
	values [][]byte
}

func NewTagValueIterator (gi *GridIndex, key []byte) tsdb.TagValueIterator {
	return gi.NewTagValueIterator(string(key))
}

func (itr *TagValueIterator) Close() (err error) { return nil }

func (itr *TagValueIterator) Next() (name []byte, err error) {
	if len(itr.values) == 0 {
		return nil, nil
	}
	name, itr.values = itr.values[0], itr.values[1:]
	return name, nil
}

type seriesIDSetIterator struct {
	ss  *tsdb.SeriesIDSet
	itr tsdb.SeriesIDSetIterable
}

func NewSeriesIDSetIterator(ss *tsdb.SeriesIDSet) tsdb.SeriesIDSetIterator {
	if ss == nil {
		return nil
	}
	return &seriesIDSetIterator{ss: ss, itr: ss.Iterator()}
}

func (itr *seriesIDSetIterator) Next() (tsdb.SeriesIDElem, error) {
	if !itr.itr.HasNext() {
		return tsdb.SeriesIDElem{}, nil
	}
	return tsdb.SeriesIDElem{SeriesID: uint64(itr.itr.Next())}, nil
}

func (itr *seriesIDSetIterator) Close() error { return nil }

func (itr *seriesIDSetIterator) SeriesIDSet() *tsdb.SeriesIDSet { return itr.ss }