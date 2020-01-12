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
	// writers update offsetNext, then execute their updates, then update offsetDone to mark completion
	// note : math.MaxUint64/1e7/(60*60*24*365) = 58k
	// so even at a constant rate of 10M, uint64 allows us to count 58k years worth of metrics
	// so we will never run out during the lifetime of this process.
	offsetNext uint64 // where to write to next
	offsetDone uint64 // amount of entries written

	// writers update curSegment, create segments as needed, then update numSegments to mark completion
	curSegment  uint64       // the number of the segment that we're about to write into, are writing into or have last written into
	numSegments uint64       // the number of segments that the segments slice is guaranteed to have (at least)
	segments    atomic.Value // holds a []*segment
}

func NewTable() table {
	t := table{
		numSegments: 1,
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
		callback(metric)
		pos++
	}
}

func (t *table) Add(metrics []metric) {
	size := uint64(len(metrics))

	// with this update, we "own" the range from what t.offsetNext (inclusive) was up until t.offsetNext+size (not inclusive)
	posN := atomic.AddUint64(&t.offsetNext, size) - 1 // post of the last element
	pos0 := posN + 1 - size                           // pos of first element

	segmentPos0 := pos0 / 65536 // segment that first element will go into
	segmentPosN := posN / 65536 // segment that last element will go into (potentially the sam)

	// see what the highest segment is that we'll need.
	// if we need the first segment, it has already been created during table creation time
	// if we need a subsequent one, we need to create it, and any intermediate ones, unless someone else beats us to it
	if segmentPosN > 0 {
		// our batch may fit within the current live segment, or it may require one or more new segments
		numSeg := atomic.LoadUint64(&t.numSegments)

		// cur: index of segment we want to create (unless someone else beats us to it)
		// prev: index of segment that we know for sure to exist (later segments may also exist if someone else is fast)
		for cur := numSeg; cur <= segmentPosN; cur++ {
			prev := cur - 1

			// only one routine will succeed at executing this concurrently.
			// might another one may succeed at cur to cur+1 right after we execute this line?
			// no, because for them to do that, their cur would be our cur+1 which means they would see t.numSegments of our cur
			// but we only update t.numSegments when we're done
			if atomic.CompareAndSwapUint64(&t.curSegment, prev, cur) {
				// note that because we linearize all updates to numSegments and segments,
				// we don't have to check the current value or worry about concurrent updates (until we bump numSegments)
				segments := t.segments.Load().([]*segment)
				segments = append(segments, &segment{})
				(&t.segments).Store(segments)

				// make others aware that t.sgements is now safe for reading the new segment or doing further updates to it
				atomic.StoreUint64(&t.numSegments, uint64(len(segments)))
			} else {
				// if we are not. someone else will be updating numSegments to let us know the segment has been created
				// all we have to do is wait for it
				// e.g. prev =1, cur =2
				// if someone else updates curSegment from 1 to 2, it also means they're on the hook for updating numSegments to 3 when they're done
				// we wait until numSegments is 3 aka cur+1, so as long as wait < cur+1
				for atomic.LoadUint64(&t.numSegments) <= cur {
					runtime.Gosched()
				}
				// next run we will bump cur to 3, and numSegments will be 3
			}
		}

		// consider we are moving from curSegment 1 to 3
		// somebody else wants to go from curSegment 1 to 2, they are updating curSegment
		// we still need to wait until they're done and retry 2->3 unless somebody beat us to that too
		// if that didn't wonrk
	}

	// if multiple routines concurrently "enter" into a new segment, we may have to wait
	// until the other one has updated the segments slice
	for atomic.LoadUint64(&t.numSegments) < segmentPosN+1 {
		runtime.Gosched()
	}

	// now we know that t.segments contains the segment(s) we need. if someone is adding a segment it's possible that they're creating
	// a new slice, but that's fine.
	segments := t.segments.Load().([]*segment)
	var i, j int
	pos := int((pos0 % 65536))
	posMax := 65536
	for segmentPos := segmentPos0; segmentPos <= segmentPosN; segmentPos++ {
		segment := segments[segmentPos]

		// copy data into the live segment
		// note that different routines may copy into different sections of the segment concurrently
		// TODO: is instruction reordering a concern here? this is not atomic

		availableSlots := posMax - pos
		j = len(metrics)
		if (j - i) > availableSlots {
			j = i + availableSlots
		}

		copy(segment.metrics[pos:posMax], metrics[i:j])
		pos = 0
		i = j
	}

	// make sure other earlier adds have finished and updated done accordingly,
	// before we mark the current position as done
	for !atomic.CompareAndSwapUint64(&t.offsetDone, pos0, pos0+size) {
		runtime.Gosched()
	}
}
