package tsi2

import "errors"

var (
	ErrFailToSetSeriesKey = errors.New("fail to set series key")
	ErrMeasurementNotFound = errors.New("fail to find measurement")
)
