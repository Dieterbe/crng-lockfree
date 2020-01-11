package router

import "math"

type metric struct {
	Key uint32
	Ts  uint32
	Val float64
}

var metricEndMarker = metric{
	Key: math.MaxUint32,
	Ts:  math.MaxUint32,
	Val: math.MaxFloat64,
}
