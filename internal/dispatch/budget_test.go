package dispatch

import (
	"testing"
	"time"
)

func TestBudgetNoBurstOrAccumulation(t *testing.T) {
	var b budget
	now := time.Now()
	if !b.Ready(now, 10) {
		t.Fatal("initial permit")
	}
	b.Charge(now, 10)
	if b.Ready(now, 10) {
		t.Fatal("burst")
	}
	future := now.Add(time.Hour)
	if !b.Ready(future, 10) {
		t.Fatal("idle release")
	}
	b.Charge(future, 10)
	if b.Ready(future, 10) {
		t.Fatal("idle accumulated quota")
	}
}
func TestSlidingWindowAndRateDecrease(t *testing.T) {
	var b budget
	now := time.Now()
	for i := 0; i < 10; i++ {
		at := now.Add(time.Duration(i) * 200 * time.Millisecond)
		if !b.Ready(at, 10) {
			t.Fatal("unexpected limit", i)
		}
		b.Charge(at, 10)
	}
	if b.Ready(now.Add(2*time.Second), 1) {
		t.Fatal("rate reduction discarded previous issued permits")
	}
}
