package tsi2

import (
	"cycledb/pkg/tsdb"
	"fmt"

	"github.com/influxdata/influxdb/v2/models"
)

type Measurement struct {
	name   string
	gIndex *GridIndex
}

func NewMeasurement(i *GridIndex, name string) *Measurement {
	return &Measurement {
		gIndex: i,
		name: name,
	}
}

func (m *Measurement) SeriesIDSet() *tsdb.SeriesIDSet {
	panic("unimplemented")
}

// one measurement map to one grid index
// 2-byte to address measurement, then 4-byte to address id in gIndex within, combined as series id
type Measurements struct {
	measurementId map[string]uint16
	measurements  []*Measurement
}

func NewMeasurements() *Measurements {
	return &Measurements{
		measurementId: map[string]uint16{},
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
	ms.measurementId[string(name)] = uint16(len(ms.measurements))
	ms.measurements = append(ms.measurements, NewMeasurement(NewGridIndex(NewMultiplierOptimizer(5, 2)), string(name)))
	return nil
}

func (ms *Measurements) SetTagPairSet(name []byte, tags models.Tags) int64 {
	measurementId, ok := ms.measurementId[string(name)]
	if !ok {
		return -1
	}
	m := ms.measurements[measurementId]
	idSet := m.gIndex.SetTagPairSet(tagsConvert(tags))
	// return (int64(measurementId) << 32) | id
	return ms.idSetWithMeasurementId(idSet)
}

func (ms *Measurements) idSetWithMeasurementId(idSet *tsdb.SeriesIDSet) int64 {
	panic("unimplemented")
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
