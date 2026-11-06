package bootstrap

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestBaoInitWaitsForActiveNode(t *testing.T) {
	for _, resume := range []bool{false, true} {
		name := "first_initialization"
		if resume {
			name = "resume_after_failed_seed"
		}
		t.Run(name, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "profile")
			if err := InitSource(dir, "kinescope"); err != nil {
				t.Fatal(err)
			}
			keys := BaoKeys{Keys: []string{"test-unseal-key"}, RootToken: "test-root-token"}
			keyPath := filepath.Join(dir, "bootstrap/bao-init.json")
			if resume {
				if err := jsonFile(keyPath, keys); err != nil {
					t.Fatal(err)
				}
			}
			var initialized atomic.Bool
			initialized.Store(resume)
			var ready atomic.Bool
			var initCalls, unsealCalls, healthCalls, writes atomic.Int32
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch r.URL.Path {
				case "/v1/sys/seal-status":
					json.NewEncoder(w).Encode(map[string]bool{"initialized": initialized.Load(), "sealed": !initialized.Load()})
				case "/v1/sys/init":
					initCalls.Add(1)
					initialized.Store(true)
					json.NewEncoder(w).Encode(keys)
				case "/v1/sys/unseal":
					unsealCalls.Add(1)
					json.NewEncoder(w).Encode(map[string]bool{"sealed": false})
				case "/v1/sys/health":
					n := healthCalls.Add(1)
					if n == 1 {
						w.WriteHeader(http.StatusTooManyRequests)
						json.NewEncoder(w).Encode(map[string]bool{"initialized": true, "sealed": false, "standby": true})
					} else if n == 2 {
						// A successful response must also describe an active node.
						json.NewEncoder(w).Encode(map[string]bool{"initialized": true, "sealed": false, "standby": true})
					} else {
						ready.Store(true)
						json.NewEncoder(w).Encode(map[string]bool{"initialized": true, "sealed": false, "standby": false})
					}
				default:
					if !ready.Load() {
						t.Error("seeding began while storage was still read-only")
						http.Error(w, "cannot write to readonly storage", http.StatusInternalServerError)
						return
					}
					if r.Method != "GET" {
						writes.Add(1)
					}
					if strings.HasSuffix(r.URL.Path, "/role-id") {
						json.NewEncoder(w).Encode(map[string]any{"data": map[string]string{"role_id": "test-role"}})
					} else if strings.HasSuffix(r.URL.Path, "/secret-id") {
						json.NewEncoder(w).Encode(map[string]any{"data": map[string]string{"secret_id": "test-secret"}})
					} else {
						json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{}})
					}
				}
			}))
			defer server.Close()
			ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
			if err := os.WriteFile(filepath.Join(dir, "secrets/ca.crt"), ca, 0600); err != nil {
				t.Fatal(err)
			}
			if err := BaoInit(t.Context(), dir, server.URL); err != nil {
				t.Fatal(err)
			}
			wantInit := int32(1)
			if resume {
				wantInit = 0
			}
			if initCalls.Load() != wantInit || unsealCalls.Load() != wantInit || healthCalls.Load() < 3 || writes.Load() == 0 {
				t.Fatal("unexpected initialization or readiness sequence", initCalls.Load(), unsealCalls.Load(), healthCalls.Load(), writes.Load())
			}
			before, err := os.ReadFile(keyPath)
			if err != nil {
				t.Fatal(err)
			}
			seedWrites := writes.Load()
			if err = BaoInit(t.Context(), dir, server.URL); err != nil {
				t.Fatal(err)
			}
			after, err := os.ReadFile(keyPath)
			if err != nil || string(after) != string(before) || writes.Load() != seedWrites || initCalls.Load() != wantInit {
				t.Fatal("restart repeated initialization or rewrote recovery material", err)
			}
			if _, err = os.Stat(filepath.Join(dir, "bootstrap/bao.seeded")); err != nil {
				t.Fatal("seed marker missing", err)
			}
		})
	}
}

func TestBaoInitStandbyDeadlineDoesNotSeed(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "secrets"), 0700); err != nil {
		t.Fatal(err)
	}
	keys := BaoKeys{Keys: []string{"test-unseal-key"}, RootToken: "test-root-token"}
	if err := jsonFile(filepath.Join(dir, "bootstrap/bao-init.json"), keys); err != nil {
		t.Fatal(err)
	}
	var writes atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			writes.Add(1)
		}
		json.NewEncoder(w).Encode(map[string]bool{"initialized": true, "sealed": false, "standby": true})
	}))
	defer server.Close()
	ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
	if err := os.WriteFile(filepath.Join(dir, "secrets/ca.crt"), ca, 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	err := BaoInit(ctx, dir, server.URL)
	if !errors.Is(err, context.DeadlineExceeded) || writes.Load() != 0 {
		t.Fatal("standby readiness ignored cancellation or started seeding", err, writes.Load())
	}
	if _, err = os.Stat(filepath.Join(dir, "bootstrap/bao.seeded")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("failed bootstrap was marked successful", err)
	}
}
