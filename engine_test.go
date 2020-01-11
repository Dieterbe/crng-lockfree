package router

import (
	"math/rand"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestAdd0(t *testing.T) {
	testAdd(t, 0)
}
func TestAdd1(t *testing.T) {
	testAdd(t, 1)
}
func TestAdd5(t *testing.T) {
	testAdd(t, 5)
}
func TestAdd127(t *testing.T) {
	testAdd(t, 127)
}
func TestAdd128(t *testing.T) {
	testAdd(t, 128)
}
func TestAdd250(t *testing.T) {
	testAdd(t, 250)
}
func TestAdd260(t *testing.T) {
	testAdd(t, 260)
}
func TestAdd60k(t *testing.T) {
	testAdd(t, 60000)
}
func TestAdd64kminus2(t *testing.T) {
	testAdd(t, 65534)
}
func TestAdd64kminus1(t *testing.T) {
	testAdd(t, 65534)
}
func TestAdd64(t *testing.T) {
	testAdd(t, 65536)
}
func TestAdd64plus1(t *testing.T) {
	testAdd(t, 65537)
}
func TestAdd256k(t *testing.T) {
	testAdd(t, 4*65535)
}

// TODO do our inserts in batches of varying sizes?
// and in variying segment batch sizes.
// we then don't need all these hardcoded variants anymore either

func testAdd(t *testing.T, num int) {
	table := NewTable(4)
	metrics := make([]metric, 0, num)
	for i := 0; i < num; i++ {
		metrics = append(metrics, metric{
			Key: uint32(i),
			Ts:  uint32(i),
			Val: rand.Float64(),
		})
	}
	// the table may mangle our input if it's buggy, so to check the result
	// we need a safe copy that no one can tamper with
	want := make([]metric, num)
	copy(want, metrics)
	table.Add(metrics)
	got := make([]metric, 0, num)
	cb := func(m metric) {
		got = append(got, m)
	}
	table.Consume(0, uint64(num), cb)

	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("testAdd() mismatch (-want +got):\n%s", diff)
	}
}

func BenchmarkRouting(b *testing.B) {
}
