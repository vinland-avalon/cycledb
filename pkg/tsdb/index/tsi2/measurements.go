package tsi2

import (
	"cycledb/pkg/tsdb"
	"fmt"

	"github.com/influxdata/influxdb/v2/models"
)

type Measurement struct {
	name          string
	gIndex        *GridIndex
	measurementId uint64
}

func NewMeasurement(i *GridIndex, name string, measurementId uint64) *Measurement {
	return &Measurement{
		measurementId: measurementId,
		gIndex:        i,
		name:          name,
	}
}

func (m *Measurement) SeriesIDSet() *tsdb.SeriesIDSet {

	return m.gIndex.SeriesIDSet()
}

// one measurement map to one grid index
// 2-byte to address measurement, then 4-byte to address id in gIndex within, combined as series id
type Measurements struct {
	measurementId map[string]uint64
	measurements  []*Measurement
}

func NewMeasurements() *Measurements {
	return &Measurements{
		measurementId: map[string]uint64{},
		measurements:  []*Measurement{},
	}
}

func (ms *Measurements) MeasurementByName(name []byte) (*Measurement, error) {
	id, exist := ms.measurementId[string(name)]
	if !exist {
		return nil, nil
	}
	if len(ms.measurements) <= int(id) {
		return nil, fmt.Errorf("inconsistent between measurementId and measurements")
	}
	return ms.measurements[id], nil
}

func (ms *Measurements) DropMeasurement(name []byte) error {
	if id, ok := ms.measurementId[string(name)]; ok {
		delete(ms.measurementId, string(name))
		ms.measurements[id] = nil
	}
	return nil
}

func (ms *Measurements) AppendMeasurement(name []byte) error {
	measurementId := uint64(len(ms.measurements))
	m := NewMeasurement(NewGridIndex(NewMultiplierOptimizer(5, 2)), string(name), measurementId)
	ms.measurementId[string(name)] = measurementId
	ms.measurements = append(ms.measurements, m)
	return nil
}

func (ms *Measurements) SetTagPairSet(name []byte, tags models.Tags) (uint64, bool) {
	measurementId, ok := ms.measurementId[string(name)]
	if !ok {
		return 0, false
	}
	m := ms.measurements[measurementId]
	id, success := m.gIndex.SetTagPairSet(tagsConvert(tags))
	if !success {
		return id, false
	}
	// return (int64(measurementId) << 32) | id
	return ms.SeriesIdWithMeasurementId(m.measurementId, id), true
}

func (ms *Measurements) SeriesIdWithMeasurementId(measurementId, id uint64) uint64 {
	return measurementId << 32 | id
}

func (ms *Measurements) HasTagKey(name, key []byte) (bool, error) {
	m, err := ms.MeasurementByName(name)
	if err != nil || m == nil {
		return false, err
	}
	return m.gIndex.HasTagKey(string(key)), nil
}

func (ms *Measurements) HasTagValue(name, key, value []byte) (bool, error) {
	m, err := ms.MeasurementByName(name)
	if err != nil || m == nil {
		return false, err
	}
	return m.gIndex.HasTagValue(string(key), string(value)), nil
}

func (ms *Measurements) MeasurementSeriesIDIterator(name []byte) (tsdb.SeriesIDIterator, error) {
	m, err := ms.MeasurementByName(name)
	if err != nil || m == nil {
		return nil, err
	}
	return NewSeriesIDSetIterator(m.SeriesIDSet()), nil
}

// type MeasurementsIterator struct {
// 	curr uint
// 	ms *Measurements
// }

// func NewMeasurementsIterator(ms *Measurements) *MeasurementsIterator{
// 	return &MeasurementsIterator{
// 		curr: 0,

// 	}
// }

func (ms *Measurements) TagKeySeriesIDIterator(name, key []byte) (tsdb.SeriesIDSetIterator, error) {
	m, err := ms.MeasurementByName(name)
	if err != nil || m == nil {
		return nil, err
	}
	return NewSeriesIDSetIterator(m.gIndex.SeriesIDSetWithTagKey(string(key))), nil
}

func (ms *Measurements) TagValueSeriesIDIterator(name, key, value []byte) (tsdb.SeriesIDSetIterator, error) {
	m, err := ms.MeasurementByName(name)
	if err != nil || m == nil {
		return nil, err
	}
	return NewSeriesIDSetIterator(m.gIndex.SeriesIDSetWithTagValue(string(key), string(value))), nil
}
