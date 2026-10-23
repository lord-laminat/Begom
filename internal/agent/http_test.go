package agent

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/rychkin/Begom/internal/config"
	"github.com/rychkin/Begom/internal/logging"
	"github.com/rychkin/Begom/internal/model"
	"github.com/rychkin/Begom/internal/storage"
	"github.com/rychkin/Begom/internal/target"
	"github.com/rychkin/Begom/internal/wire"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type fixture struct {
	runner   *Runner
	permits  atomic.Int64
	requests atomic.Int64
	server   *httptest.Server
}

func setupHTTP(t *testing.T, handler http.HandlerFunc) *fixture {
	t.Helper()
	f := &fixture{}
	f.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { f.requests.Add(1); handler(w, r) }))
	t.Cleanup(f.server.Close)
	control := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasSuffix(r.URL.Path, "/permits"):
			f.permits.Add(1)
			now := time.Now().UTC()
			json.NewEncoder(w).Encode(map[string]any{"state": "issued", "permit": model.Permit{ID: model.NewID(), Epoch: "test", NotBefore: now, ExpiresAt: now.Add(500 * time.Millisecond), ServerTime: now, ConnectionID: model.NewID(), ConnectionExpiresAt: now.Add(10 * time.Second)}})
		case strings.HasSuffix(r.URL.Path, "/renew"):
			now := time.Now().UTC()
			json.NewEncoder(w).Encode(map[string]any{"connection_expires_at": now.Add(10 * time.Second), "server_time": now, "epoch": "test"})
		case strings.HasSuffix(r.URL.Path, "/progress"):
			json.NewEncoder(w).Encode(map[string]any{"accepted": []any{map[string]string{"status": "downloading"}}})
		default:
			json.NewEncoder(w).Encode(map[string]bool{"ok": true})
		}
	}))
	t.Cleanup(control.Close)
	client := &wire.Client{Base: control.URL, HTTP: control.Client()}
	u, _ := url.Parse(f.server.URL)
	source := config.Source{AllowedHosts: []string{u.Hostname()}, Mock: true, MockCIDRs: []string{"127.0.0.0/8"}}
	root := t.TempDir()
	os.WriteFile(filepath.Join(root, ".begom-volume"), []byte("test"), 0444)
	v := &storage.Volume{Config: config.Storage{Root: root, VolumeID: "test", Mode: "test"}}
	f.runner = &Runner{Client: client, HTTP: NewHTTP(client, "w", target.Policy{Source: source}), Config: config.Agent{WorkerID: "w"}, Volume: v, Log: logging.New("test"), downloads: map[string]model.ActiveDownload{}}
	return f
}
func TestEveryRedirectObtainsPermit(t *testing.T) {
	f := setupHTTP(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/first":
			http.Redirect(w, r, "/second", 302)
		case "/second":
			http.Redirect(w, r, "/third", 307)
		default:
			io.WriteString(w, "done")
		}
	})
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	r, e := f.runner.HTTP.Get(ctx, model.LeaseRef{}, "check", f.server.URL+"/first", nil, time.Second)
	if e != nil {
		t.Fatal(e)
	}
	b, e := io.ReadAll(r.Body)
	r.Body.Close()
	if e != nil || string(b) != "done" || f.permits.Load() != 3 || f.requests.Load() != 3 {
		t.Fatal(string(b), f.permits.Load(), f.requests.Load(), e)
	}
}
func TestDownloadResumeAndSafeRestart(t *testing.T) {
	data := []byte(strings.Repeat("synthetic-video-data", 8000))
	for _, mode := range []string{"range", "ignored", "etag-changed", "invalid-range", "range-416"} {
		t.Run(mode, func(t *testing.T) {
			var calls atomic.Int64
			f := setupHTTP(t, func(w http.ResponseWriter, r *http.Request) {
				call := calls.Add(1)
				w.Header().Set("Content-Type", "video/mp4")
				tag := "\"v1\""
				body := data
				if mode == "etag-changed" && call > 1 {
					tag = "\"v2\""
					body = append([]byte(nil), data...)
					body[len(body)-1] ^= 1
				}
				w.Header().Set("ETag", tag)
				if call == 1 {
					w.Header().Set("Content-Length", fmt.Sprint(len(body)))
					w.WriteHeader(200)
					w.Write(body[:len(body)/2])
					w.(http.Flusher).Flush()
					conn, _, e := w.(http.Hijacker).Hijack()
					if e == nil {
						conn.Close()
					}
					return
				}
				if mode == "ignored" {
					r.Header.Del("Range")
				}
				if mode == "invalid-range" && r.Header.Get("Range") != "" {
					w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", len(body)/2+1, len(body)-1, len(body)))
					w.WriteHeader(206)
					w.Write(body[len(body)/2:])
					return
				}
				if mode == "range-416" && r.Header.Get("Range") != "" {
					w.WriteHeader(416)
					return
				}
				http.ServeContent(w, r, "video.mp4", time.Now(), bytes.NewReader(body))
			})
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			x := &execution{ctx: ctx, lease: model.Lease{AttemptID: "attempt"}}
			result := model.ItemResult{ExternalID: 1, Metadata: &model.Metadata{CanonicalID: "video", MIME: "video/mp4", Size: uint64(len(data))}, DownloadID: "download", StageID: "stage", ReserveBytes: uint64(len(data))}
			cfg := model.JobConfig{MaxFileBytes: 1 << 20, HTTPTimeoutMS: 3000, Retry: model.RetryPolicy{MaxRetries: 4, BaseMS: 1, MaxMS: 5}}
			n, hash, e := f.runner.download(x, result, f.server.URL, cfg)
			if e != nil {
				t.Fatal(e)
			}
			want := append([]byte(nil), data...)
			if mode == "etag-changed" {
				want[len(want)-1] ^= 1
			}
			digest := sha256.Sum256(want)
			if n != uint64(len(data)) || hash != hex.EncodeToString(digest[:]) {
				t.Fatal(n, hash)
			}
			if f.permits.Load() != f.requests.Load() || f.requests.Load() < 2 {
				t.Fatal("request escaped C6", f.permits.Load(), f.requests.Load())
			}
		})
	}
}
func TestCorruptPartialStartsFromZero(t *testing.T) {
	var usedRange atomic.Bool
	data := []byte("complete-video")
	f := setupHTTP(t, func(w http.ResponseWriter, r *http.Request) {
		usedRange.Store(r.Header.Get("Range") != "")
		w.Header().Set("Content-Type", "video/mp4")
		w.Header().Set("ETag", "\"v1\"")
		w.Write(data)
	})
	root, e := f.runner.Volume.OpenRoot()
	if e != nil {
		t.Fatal(e)
	}
	storage.MkdirAll(root, "staging/w/attempt/stage", 0770)
	file, _ := root.Create("staging/w/attempt/stage/video.part")
	file.Write([]byte("bad"))
	file.Close()
	storage.WriteJSON(root, "staging/w/attempt/stage/partial.json", partial{Offset: 3, Checksum: strings.Repeat("0", 64), Validator: "\"v1\"", Total: uint64(len(data))})
	root.Close()
	x := &execution{ctx: t.Context(), lease: model.Lease{AttemptID: "attempt"}}
	r := model.ItemResult{Metadata: &model.Metadata{CanonicalID: "v", Size: uint64(len(data))}, StageID: "stage", DownloadID: "d", ReserveBytes: 100}
	_, _, e = f.runner.download(x, r, f.server.URL, model.JobConfig{HTTPTimeoutMS: 2000, MaxFileBytes: 100, Retry: model.RetryPolicy{MaxRetries: 1}})
	if e != nil || usedRange.Load() {
		t.Fatal("corrupt prefix was resumed", e)
	}
}
func TestContentRange(t *testing.T) {
	a, b, total, e := ParseContentRange("bytes 10-19/20")
	if e != nil || a != 10 || b != 19 || total != 20 {
		t.Fatal(a, b, total, e)
	}
	for _, s := range []string{"bytes */20", "bytes 10-20/20", "bytes 20-10/30", "bytes 0-9/10 extra", "bytes 0-9/*"} {
		if _, _, _, e := ParseContentRange(s); e == nil {
			t.Error("accepted", s)
		}
	}
}
