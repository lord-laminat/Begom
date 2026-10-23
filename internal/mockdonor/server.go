// Package mockdonor provides controllable public Kinescope-shaped responses.
package mockdonor

import (
	"bytes"
	"crypto/subtle"
	"embed"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

//go:embed fixtures/video.mp4
var fixture embed.FS

type Case struct {
	Title       string `json:"title"`
	Status      int    `json:"status"`
	DelayMS     int    `json:"delay_ms"`
	DropAfter   int    `json:"drop_after"`
	ETag        string `json:"etag"`
	ChangeETag  bool   `json:"change_etag"`
	IgnoreRange bool   `json:"ignore_range"`
	NoDownload  bool   `json:"no_download"`
	RetryAfter  string `json:"retry_after"`
	FileBytes   int    `json:"file_bytes"`
}
type Config struct {
	DelayMS int             `json:"delay_ms"`
	Cases   map[string]Case `json:"cases"`
	Demo    bool            `json:"demo"`
	Reset   bool            `json:"reset"`
}
type Record struct {
	At     time.Time `json:"at"`
	Worker string    `json:"worker"`
	Class  string    `json:"class"`
	Status int       `json:"status"`
}
type Stats struct {
	Active   int      `json:"active"`
	Peak     int      `json:"peak"`
	Requests uint64   `json:"requests"`
	Records  []Record `json:"records"`
	Bytes    uint64   `json:"bytes"`
}
type Server struct {
	Token  string
	mu     sync.Mutex
	config Config
	stats  Stats
	calls  map[string]int
	video  []byte
}

func New(token string) *Server {
	b, e := fixture.ReadFile("fixtures/video.mp4")
	if e != nil {
		panic(e)
	}
	return &Server{Token: token, config: Config{DelayMS: 4000, Cases: map[string]Case{}, Demo: true}, calls: map[string]int{}, video: b}
}
func (s *Server) Handler() http.Handler {
	m := http.NewServeMux()
	m.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("ok")) })
	m.HandleFunc("GET /stats", s.statsHandler)
	m.HandleFunc("PUT /control", s.control)
	m.HandleFunc("GET /downloads/{id}", s.assets)
	m.HandleFunc("GET /files/{id}", s.file)
	m.HandleFunc("GET /{id}", s.page)
	return m
}
func (s *Server) begin(r *http.Request, class string) (Case, int, func(int, int)) {
	started := time.Now().UTC()
	id := r.PathValue("id")
	s.mu.Lock()
	c, ok := s.config.Cases[id]
	if !ok {
		c.DelayMS = s.config.DelayMS
		if s.config.Demo {
			n, _ := strconv.ParseUint(id, 10, 64)
			switch n % 10 {
			case 1:
				c.Status = 404
			case 2:
				c.Status = 403
			case 3:
				c.Status = 429
				c.RetryAfter = "1"
			case 4:
				c.Status = 500
			case 5:
				c.NoDownload = true
			case 6:
				c.IgnoreRange = true
			case 7:
				c.DropAfter = len(s.video) / 2
			case 8:
				c.ChangeETag = true
			}
		}
	}
	key := id + ":" + class
	s.calls[key]++
	call := s.calls[key]
	if !ok && call > 1 && (c.Status == 429 || c.Status == 500) {
		c.Status = 200
	}
	s.stats.Active++
	if s.stats.Active > s.stats.Peak {
		s.stats.Peak = s.stats.Active
	}
	s.stats.Requests++
	s.mu.Unlock()
	return c, call, func(status, n int) {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.stats.Active--
		s.stats.Bytes += uint64(max(n, 0))
		s.stats.Records = append(s.stats.Records, Record{At: started, Worker: r.Header.Get("X-Begom-Worker"), Class: class, Status: status})
		if len(s.stats.Records) > 10000 {
			s.stats.Records = s.stats.Records[len(s.stats.Records)-10000:]
		}
	}
}
func wait(r *http.Request, ms int) bool {
	t := time.NewTimer(time.Duration(ms) * time.Millisecond)
	defer t.Stop()
	select {
	case <-r.Context().Done():
		return false
	case <-t.C:
		return true
	}
}
func base(r *http.Request) string { return "http://" + r.Host }
func (s *Server) page(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if _, e := strconv.ParseUint(id, 10, 64); e != nil {
		http.NotFound(w, r)
		return
	}
	c, _, finish := s.begin(r, "check")
	status := c.Status
	if status == 0 {
		status = 200
	}
	defer func() { finish(status, 0) }()
	if !wait(r, c.DelayMS) {
		return
	}
	if status != 200 {
		if c.RetryAfter != "" {
			w.Header().Set("Retry-After", c.RetryAfter)
		}
		http.Error(w, http.StatusText(status), status)
		return
	}
	item := map[string]any{"id": "mock-" + id, "title": "Тестовое видео " + id}
	if c.Title != "" {
		item["title"] = c.Title
	}
	if !c.NoDownload {
		item["downloadLink"] = base(r) + "/downloads/" + id
	}
	b, _ := json.Marshal(map[string]any{"playlist": []any{item}})
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	fmt.Fprintf(w, "<!doctype html><html><head><title>Begom mock</title></head><body><script>const playerOptions = %s;</script></body></html>", b)
}
func (s *Server) assets(w http.ResponseWriter, r *http.Request) {
	c, _, finish := s.begin(r, "catalog")
	defer finish(200, 0)
	size := len(s.video)
	if c.FileBytes > size && c.FileBytes <= 128<<20 {
		size = c.FileBytes
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{"assets": []any{map[string]any{"quality": "720p", "size": size, "url": base(r) + "/files/" + r.PathValue("id"), "download_filename": "video.mp4"}}, "transcripts": []any{}})
}
func (s *Server) file(w http.ResponseWriter, r *http.Request) {
	c, call, finish := s.begin(r, "download")
	defer finish(200, 0)
	data := s.video
	if c.FileBytes > len(data) && c.FileBytes <= 128<<20 {
		data = make([]byte, c.FileBytes)
		copy(data, s.video)
		padding := len(data) - len(s.video)
		if padding >= 8 {
			p := data[len(s.video):]
			p[0] = byte(padding >> 24)
			p[1] = byte(padding >> 16)
			p[2] = byte(padding >> 8)
			p[3] = byte(padding)
			copy(p[4:8], "free")
		}
	}
	tag := c.ETag
	if tag == "" {
		tag = "\"begom-v1\""
	}
	if c.ChangeETag && call > 1 {
		tag = "\"begom-v2\""
		data = bytes.Clone(data)
		data[len(data)-1] ^= 1
	}
	w.Header().Set("ETag", tag)
	w.Header().Set("Content-Type", "video/mp4")
	if c.DropAfter > 0 && call == 1 && c.DropAfter < len(data) {
		w.Header().Set("Content-Length", strconv.Itoa(len(data)))
		w.WriteHeader(200)
		w.Write(data[:c.DropAfter])
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		if h, ok := w.(http.Hijacker); ok {
			conn, _, e := h.Hijack()
			if e == nil {
				conn.Close()
			}
		}
		return
	}
	if c.IgnoreRange {
		r = r.Clone(r.Context())
		r.Header = r.Header.Clone()
		r.Header.Del("Range")
		r.Header.Del("If-Range")
	}
	http.ServeContent(w, r, "video.mp4", time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), bytes.NewReader(data))
}
func (s *Server) statsHandler(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(s.stats)
}
func (s *Server) control(w http.ResponseWriter, r *http.Request) {
	token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	if s.Token == "" || subtle.ConstantTimeCompare([]byte(token), []byte(s.Token)) != 1 {
		http.Error(w, "forbidden", 403)
		return
	}
	var c Config
	d := json.NewDecoder(io.LimitReader(r.Body, 65536))
	d.DisallowUnknownFields()
	if d.Decode(&c) != nil || c.DelayMS < 0 || c.DelayMS > 30000 || len(c.Cases) > 1000 {
		http.Error(w, "invalid control", 400)
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if c.Reset && s.stats.Active != 0 {
		http.Error(w, "requests still active", 409)
		return
	}
	if c.Reset {
		s.stats = Stats{Records: []Record{}}
		s.calls = map[string]int{}
	}
	s.config = c
	json.NewEncoder(w).Encode(map[string]bool{"ok": true})
}
