package model

import (
	"encoding/json"
	"math"
	"testing"
)

func TestRanges(t *testing.T) {
	tests := []struct {
		in   string
		from uint64
		to   *uint64
	}{{"", 0, nil}, {"[100,]", 100, nil}, {"12", 0, ptr(12)}, {"[12,12]", 12, ptr(12)}, {" [ 0, 18446744073709551615 ] ", 0, ptr(math.MaxUint64)}, {"18446744073709551615", 0, ptr(math.MaxUint64)}}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			r, e := ParseRange(tt.in)
			if e != nil {
				t.Fatal(e)
			}
			if uint64(r.From) != tt.from || (tt.to == nil) != (r.To == nil) || tt.to != nil && uint64(*r.To) != *tt.to {
				t.Fatalf("unexpected range %#v", r)
			}
		})
	}
	for _, s := range []string{"-1", "+1", "[2,1]", "[1]", "[1,,]", "[,5]", "1.5", "18446744073709551616", "[0,18446744073709551616]", "null", "[0,1] trailing"} {
		if _, e := ParseRange(s); e == nil {
			t.Errorf("accepted invalid range %q", s)
		}
	}
}
func ptr(n uint64) *uint64 { return &n }
func TestBlockBoundaries(t *testing.T) {
	to := Number(104)
	if got := BlockEnd(100, 1000, &to); got != 104 {
		t.Fatal(got)
	}
	if got := BlockEnd(Number(math.MaxUint64)-2, 1000, nil); got != Number(math.MaxUint64) {
		t.Fatal(got)
	}
	next := Number(0)
	covered := map[Number]bool{}
	limit := Number(10007)
	for next <= limit {
		end := BlockEnd(next, 1000, &limit)
		for n := next; ; n++ {
			if covered[n] {
				t.Fatal("overlap", n)
			}
			covered[n] = true
			if n == end {
				break
			}
		}
		next = end + 1
	}
	if len(covered) != 10008 {
		t.Fatal("gap")
	}
}
func TestNumberJSON(t *testing.T) {
	n := Number(math.MaxUint64)
	b, e := json.Marshal(n)
	if e != nil || string(b) != "\"18446744073709551615\"" {
		t.Fatal(string(b), e)
	}
	for _, raw := range []string{"18446744073709551615", "\"18446744073709551615\""} {
		var decoded Number
		if e = json.Unmarshal([]byte(raw), &decoded); e != nil || decoded != n {
			t.Fatal(decoded, e)
		}
	}
	for _, raw := range []string{"1e3", "-1", "1.0", "null", "\"\""} {
		var n Number
		if json.Unmarshal([]byte(raw), &n) == nil {
			t.Error("accepted", raw)
		}
	}
}
func TestStates(t *testing.T) {
	for _, x := range []struct{ from, action, to string }{{"created", "start", "validating"}, {"running", "pause", "pausing"}, {"running", "stop", "stopping"}, {"paused", "resume", "running"}, {"stopped", "resume", "running"}, {"failed", "resume", "validating"}} {
		got, e := Transition(x.from, x.action)
		if e != nil || got != x.to {
			t.Fatal(x, got, e)
		}
	}
	for _, state := range []string{"completed", "running", "created"} {
		if _, e := Transition(state, "resume"); e == nil {
			t.Error("invalid resume", state)
		}
	}
	if Final("cancelled") || Final("retry_wait") || Final("rate_limited") {
		t.Fatal("non-final result closes a gap")
	}
}
func TestConfigValidation(t *testing.T) {
	text := "[10,]"
	c := JobConfig{RangeText: &text, WorkerIDs: []string{"w"}, Threads: 40, ThreadsByWorker: map[string]int{"w": 8}}
	r, e := c.Normalize()
	if e != nil || r.From != 10 || c.ThreadsFor("w", 20) != 8 || c.ThreadsFor("other", 20) != 20 {
		t.Fatal(r, c, e)
	}
	from := Number(10)
	c.From = &from
	if _, e = c.Normalize(); e == nil {
		t.Fatal("conflicting range accepted")
	}
}
func FuzzParseRange(f *testing.F) {
	for _, s := range []string{"", "[1,2]", "5", "[9,]", "18446744073709551615"} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		r, e := ParseRange(s)
		if e == nil && r.To != nil && r.From > *r.To {
			t.Fatal("inverted range")
		}
	})
}
