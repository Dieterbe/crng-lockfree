package router

import (
	"fmt"
	"runtime"
	"sync/atomic"
)

type segment struct {
	metrics [65536]metric // 2^16
}

// TODO: pool segments that have been read from, so we don't have to allocate them when we need them
// TODO: garbage collect old segments?

type table struct {
	// note : math.MaxUint64/1e7/(60*60*24*365) = 58k
	// so even at a constant rate of 10M, uint64 allows us to count 58k years worth of metrics
	// so we will never run out during the lifetime of this process.
	// note that you may read metricEndMarker which means there is no more data
	offsetNext uint64 // where to write to next
	offsetDone uint64 // until where have we succesfully written in slice [:end] syntax (possibly 0)

	curSegment  uint64
	numSegments uint64       // the number of segments that the segments slice is guaranteed to have (at least)
	segments    atomic.Value // holds a []*segment

	batchSize uint64
}

// batchSize should probably be >= 128 to amortize various costs
func NewTable(batchSize int) table {
	t := table{
		numSegments: 1,
		batchSize:   uint64(batchSize),
	}
	(&t.segments).Store([]*segment{&segment{}})
	return t
}

// end is the "Next one" you don't want
func (t *table) Consume(start, end uint64, callback func(m metric)) {
	done := atomic.LoadUint64(&t.offsetDone)
	if end > done {
		panic(fmt.Sprintf("requested end %d greater then offsetDone %d", end, done))
	}
	pos := start
	segments := t.segments.Load().([]*segment)

	for pos < end {
		segmentPos := pos / 65536
		metric := segments[segmentPos].metrics[pos%65536]
		if metric == metricEndMarker {
			return
		}
		callback(metric)
		pos++
	}
}

func (t *table) Add(metrics []metric) {
	i, j := 0, int(t.batchSize)
	for j <= len(metrics) {
		t.addBatch(metrics[i:j])
		i += int(t.batchSize)
		j += int(t.batchSize)
	}

	// if there's any remainders, create a new batch
	// that ends on a metric with special key that we know to mark the end
	// are we allowed to append to metrics? what if it's huge? may cause expensive copy
	// better allocate new slice and copy into it
	// this needs good code review. don't use this for now.
	if j > len(metrics) && len(metrics) > 0 {
		remainders := make([]metric, t.batchSize)
		num := copy(remainders, metrics[i:])

		// if you read in batches of a full batch size, only 1 end marker (followed by empty data) suffices
		// but our read api allows reading anywhere you like, so we have to fill all slots
		for num < int(t.batchSize) {
			remainders[num] = metricEndMarker
			num++
		}
		t.addBatch(remainders)
	}
}

// must have len of exactly the batch size!
func (t *table) addBatch(metrics []metric) {
	pos := atomic.AddUint64(&t.offsetNext, t.batchSize) - t.batchSize

	segmentPos := pos / 65536

	// if we need the first segment, it has already been created during table creation time
	// if we need a subsequent one, we must check if it still needs to be created
	if segmentPos > 0 {
		prevSegmentPos := segmentPos - 1
		// if we are the unlucky one to successfully update curSegment, it means
		// we are responsible for allocating it
		if atomic.CompareAndSwapUint64(&t.curSegment, prevSegmentPos, segmentPos) {
			// Note that under very high ingest rate, we may have multiple routines concurrently entering a new segment.
			// thus, we wait for any concurrently executing goroutine to finish their update to segments first.
			// let's say we update pos 2->3. we want to change numSegments 3 -> 4
			// someone else updates pos 1->2. they have to update numSegments 2 -> 3 first
			for atomic.LoadUint64(&t.numSegments) < segmentPos {
				runtime.Gosched()
			}
			// note that because we linearize all updates to numSegments and segments,
			// we don't have to check the current value or worry about concurrent updates (until we bump numSegments)
			segments := t.segments.Load().([]*segment)
			segments = append(segments, &segment{})
			(&t.segments).Store(segments)

			// make others aware that t.sgements is now safe for reading the new segment or doing further updates to it
			atomic.StoreUint64(&t.numSegments, uint64(len(segments)))
		}
	}

	// if multiple routines concurrently "enter" into a new segment, we may have to wait
	// until the other one has updated the segments slice
	for atomic.LoadUint64(&t.numSegments) < segmentPos+1 {
		runtime.Gosched()
	}

	// now we know that t.segments contains the segment we need. if someone is adding a segment it's possible that they're creating
	// a new slice, but that's fine.
	segments := t.segments.Load().([]*segment)
	segment := segments[segmentPos]

	// copy data into the live segment
	// note that different routines may copy into different sections of the segment concurrently
	copy(segment.metrics[(pos%65536):], metrics)

	// make sure other earlier adds have finished and updated done accordingly,
	// before we mark the current position as done
	for !atomic.CompareAndSwapUint64(&t.offsetDone, pos, pos+t.batchSize) {
		runtime.Gosched()
	}
}
