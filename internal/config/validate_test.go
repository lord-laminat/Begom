package config

import (
	"github.com/rychkin/Begom/internal/model"
	"math"
	"testing"
)

func TestDonorQuotaAndTransportBounds(t *testing.T) {
	valid := Source{ID: "kinescope", PageTemplate: "https://kinescope.io/{id}", AllowedHosts: []string{"kinescope.io"}, Limits: model.Limits{RPS: 1, Connections: 4}}
	if e := valid.Validate(); e != nil {
		t.Fatal(e)
	}
	for _, rate := range []float64{-1, .00001, math.NaN(), math.Inf(1)} {
		s := valid
		s.Limits.RPS = rate
		if s.Validate() == nil {
			t.Fatal("invalid rate accepted", rate)
		}
	}
	invalid := valid
	invalid.PageTemplate = "http://kinescope.io/{id}"
	if invalid.Validate() == nil {
		t.Fatal("cleartext real donor")
	}
	invalid = valid
	invalid.MockCIDRs = []string{"0.0.0.0/0"}
	if invalid.Validate() == nil {
		t.Fatal("mock policy on real source")
	}
}
