package webui

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http/httptest"
	"testing"
)

func TestTerminalAssetsIntegrity(t *testing.T) {
	data, e := assets.ReadFile("assets/vendor/manifest.json")
	if e != nil {
		t.Fatal(e)
	}
	var packages []struct {
		Name, Version string
		Files         map[string]string
	}
	if e = json.Unmarshal(data, &packages); e != nil {
		t.Fatal(e)
	}
	if len(packages) != 2 {
		t.Fatal("terminal packages missing")
	}
	for _, pkg := range packages {
		for name, want := range pkg.Files {
			data, e := assets.ReadFile("assets/vendor/" + name)
			if e != nil {
				t.Fatal(e)
			}
			hash := sha256.Sum256(data)
			if hex.EncodeToString(hash[:]) != want {
				t.Fatal("vendored asset changed", pkg.Name, pkg.Version, name)
			}
		}
	}
}
func TestDeepLinksAndPrivatePaths(t *testing.T) {
	h := Handler()
	for _, p := range []string{"/catalog", "/gallery", "/watch/file-1", "/workers/worker-1/terminal"} {
		r := httptest.NewRecorder()
		h.ServeHTTP(r, httptest.NewRequest("GET", p, nil))
		if r.Code != 200 {
			t.Fatal("deep link unavailable", p, r.Code)
		}
	}
	for _, p := range []string{"/data/staging/worker-1/video.part", "/assets/../../go.mod", "/watch/../foo", "/workers/w1/../terminal"} {
		r := httptest.NewRecorder()
		h.ServeHTTP(r, httptest.NewRequest("GET", p, nil))
		if r.Code != 404 {
			t.Fatal("unexpected path served", p, r.Code)
		}
	}
}
