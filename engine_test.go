package router

import (
	"fmt"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

// important things to try: insert batch that is, or is around a multiple of segment size
// everything above but concurrently
// everything above but under a read workload
func TestAddAndCompare(t *testing.T) {
	insertBatchSizes := []int{2, 3, 7, 8, 50, 100, 500, 1000, 5000, 10000, 20000}
	numMetricss := []int{0, 1, 2, 3, 31, 127, 128, 129, 4094, 4095, 4096, 4097, 4098, 65534, 65535, 65536, 65537, (2 * 65536) - 1, 2 * 65536, (2 * 65536) + 2}
	for _, numMetrics := range numMetricss {
		for _, insertBatchSize := range insertBatchSizes {
			name := fmt.Sprintf("numMetrics %d insertBatchSize %d", numMetrics, insertBatchSize)
			t.Run(name, func(t *testing.T) {
				table := NewTable()
				metrics := make([]metric, 0, numMetrics)
				for i := 0; i < numMetrics; i++ {
					metrics = append(metrics, metric{
						Key: uint32(i),
						Ts:  uint32(i),
					})
				}
				// the table may mangle our input if it's buggy, so to check the result
				// we need a safe copy that no one can tamper with
				want := make([]metric, numMetrics)
				copy(want, metrics)

				// split up the input in batches
				var i, j int
				for j < len(metrics) {
					j = i + insertBatchSize
					if j > len(metrics) {
						j = len(metrics)
					}
					batch := metrics[i:j]
					table.Add(batch)
					i = j
				}
				got := make([]metric, 0, numMetrics)
				cb := func(m metric) {
					got = append(got, m)
				}
				table.Consume(0, uint64(numMetrics), cb)
				if diff := cmp.Diff(want, got); diff != "" {
					t.Errorf("testAdd() mismatch (-want +got):\n%s", diff)
				}
			})
		}
	}
}

func BenchmarkAdd(b *testing.B) {
	insertBatchSizes := []int{32, 64, 100, 128, 500, 1000, 5000}
	for _, insertBatchSize := range insertBatchSizes {
		name := fmt.Sprintf("insertBatchSize %d", insertBatchSize)
		b.Run(name, func(b *testing.B) {
			a := time.Now()
			table := NewTable()
			metrics := make([]metric, 0, b.N)
			for i := 0; i < b.N; i++ {
				metrics = append(metrics, metric{
					Key: uint32(i),
					Ts:  uint32(i),
				})
			}
			b.ResetTimer()
			// split up the input in batches
			var i, j int
			for j < len(metrics) {
				j = i + insertBatchSize
				if j > len(metrics) {
					j = len(metrics)
				}
				batch := metrics[i:j]
				table.Add(batch)
				i = j
			}
			dur := time.Since(a)
			b.ReportMetric(float64(1e9*int64(b.N)/dur.Nanoseconds()), "metrics/s")
		})
	}
}
