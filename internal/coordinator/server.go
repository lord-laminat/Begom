package coordinator

import (
	"context"
	"crypto/x509"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/rychkin/Begom/internal/auth"
	"github.com/rychkin/Begom/internal/config"
	"github.com/rychkin/Begom/internal/db"
	"github.com/rychkin/Begom/internal/dispatch"
	"github.com/rychkin/Begom/internal/hostmetrics"
	"github.com/rychkin/Begom/internal/model"
	"github.com/rychkin/Begom/internal/publication"
	"github.com/rychkin/Begom/internal/secrets"
	"github.com/rychkin/Begom/internal/sshcontrol"
	"github.com/rychkin/Begom/internal/storage"
	"github.com/rychkin/Begom/internal/webui"
	"github.com/rychkin/Begom/internal/wire"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Server struct {
	Store         *db.Store
	Config        config.Coordinator
	Auth          *auth.Service
	Dispatch      *dispatch.Dispatcher
	Publisher     *publication.Service
	Volume        *storage.Volume
	SSH           *sshcontrol.Service
	Secrets       *secrets.Bao
	Log           *slog.Logger
	loginMu       sync.Mutex
	loginAttempts map[string][]time.Time
}

func (s *Server) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("API-Version", model.APIVersion)
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Referrer-Policy", "no-referrer")
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("X-Frame-Options", "DENY")
		w.Header().Set("Content-Security-Policy", "default-src 'self'; script-src 'self'; style-src 'self'; connect-src 'self'; media-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'self'")
		r.Header.Set("X-Request-ID", model.NewID())
		w.Header().Set("X-Request-ID", r.Header.Get("X-Request-ID"))
		if version := r.Header.Get("API-Version"); version != "" && version != model.APIVersion {
			s.fail(w, r, &db.Error{Status: 426, Code: "api_version_unsupported", Message: "Unsupported API version"})
			return
		}
		if s.Dispatch != nil && r.Method != "GET" && r.Method != "HEAD" && strings.HasPrefix(r.URL.Path, "/api/") && !s.Dispatch.Epoch.Owns() {
			s.fail(w, r, &db.Error{Status: 503, Code: "epoch_lost", Message: "Coordinator authority unavailable"})
			return
		}
		if s.Secrets != nil && r.Method != "GET" && r.Method != "HEAD" && strings.HasPrefix(r.URL.Path, "/api/") {
			if _, e := s.Secrets.Get(r.Context(), s.Config.Bao.Prefix+"/runtime"); e != nil {
				s.fail(w, r, e)
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}
func (s *Server) Human() http.Handler {
	m := http.NewServeMux()
	m.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { s.json(w, 200, map[string]string{"status": "alive"}) })
	m.HandleFunc("GET /readyz", s.ready)
	m.HandleFunc("POST /api/auth/login", s.login)
	m.HandleFunc("GET /api/auth/session", s.user(false, func(w http.ResponseWriter, r *http.Request, u auth.User) {
		csrf := ""
		if c, e := r.Cookie(auth.CSRFCookie); e == nil {
			csrf = c.Value
		}
		s.json(w, 200, map[string]any{"user": u, "csrf_token": csrf})
	}))
	m.HandleFunc("POST /api/auth/logout", s.user(true, func(w http.ResponseWriter, r *http.Request, u auth.User) {
		c, _ := r.Cookie(auth.SessionCookie)
		if e := s.Auth.Logout(r.Context(), c.Value); e != nil {
			s.fail(w, r, e)
			return
		}
		auth.Cookies(w, "", "")
		s.json(w, 200, map[string]bool{"ok": true})
	}))
	m.HandleFunc("GET /api/jobs", s.user(false, s.jobs))
	m.HandleFunc("POST /api/jobs", s.user(true, s.createJob))
	m.HandleFunc("GET /api/jobs/{jobId}", s.user(false, s.job))
	for _, action := range []string{"start", "pause", "resume", "stop"} {
		m.HandleFunc("POST /api/jobs/{jobId}/"+action, s.user(true, s.act))
	}
	m.HandleFunc("PATCH /api/jobs/{jobId}/config", s.user(true, s.configure))
	m.HandleFunc("POST /api/jobs/{jobId}/items/retry", s.user(true, s.retryItems))
	m.HandleFunc("GET /api/jobs/{jobId}/items", s.user(false, s.items))
	m.HandleFunc("GET /api/jobs/{jobId}/events", s.user(false, s.events))
	m.HandleFunc("GET /api/jobs/{jobId}/export", s.user(false, s.export))
	m.HandleFunc("GET /api/jobs/{jobId}/access", s.user(false, s.access))
	m.HandleFunc("PUT /api/jobs/{jobId}/access", s.user(true, s.setAccess))
	m.HandleFunc("GET /api/workers", s.user(false, s.workers))
	m.HandleFunc("POST /api/workers", s.user(true, s.saveWorker))
	m.HandleFunc("GET /api/workers/{workerId}", s.user(false, s.worker))
	m.HandleFunc("PATCH /api/workers/{workerId}", s.user(true, s.saveWorker))
	m.HandleFunc("GET /api/workers/{workerId}/metrics", s.user(false, s.workerMetrics))
	m.HandleFunc("GET /api/workers/{workerId}/actions", s.user(false, s.actions))
	m.HandleFunc("POST /api/workers/{workerId}/actions", s.user(true, s.action))
	m.HandleFunc("POST /api/workers/{workerId}/certificates/revoke", s.user(true, s.revoke))
	m.HandleFunc("POST /api/workers/{workerId}/certificates/rotate", s.user(true, s.rotate))
	m.HandleFunc("GET /api/users", s.user(false, s.users))
	m.HandleFunc("POST /api/users", s.user(true, s.saveUser))
	m.HandleFunc("PATCH /api/users/{userId}", s.user(true, s.saveUser))
	m.HandleFunc("GET /api/audit", s.user(false, s.audit))
	m.HandleFunc("GET /api/snapshot", s.user(false, s.snapshot))
	m.HandleFunc("GET /api/events/stream", s.user(false, s.eventStream))
	m.HandleFunc("POST /api/workers/{workerId}/ssh/sessions", s.user(true, s.sshSession))
	m.HandleFunc("GET /api/workers/{workerId}/ssh/stream", s.user(false, s.sshStream))
	m.HandleFunc("GET /api/files/{fileId}/content", s.user(false, s.content))
	m.HandleFunc("GET /internal/grafana-auth", s.user(false, func(w http.ResponseWriter, r *http.Request, u auth.User) {
		if !u.Admin() {
			s.denied(w, r)
			return
		}
		w.Header().Set("X-WEBAUTH-USER", u.Username)
		w.WriteHeader(204)
	}))
	m.Handle("GET /", webui.Handler())
	return s.Middleware(m)
}
func (s *Server) Agents() http.Handler {
	m := http.NewServeMux()
	m.HandleFunc("POST /api/workers/register", s.agent(s.register))
	m.HandleFunc("POST /api/workers/{workerId}/heartbeat", s.agent(s.heartbeat))
	m.HandleFunc("POST /api/workers/{workerId}/lease", s.agent(s.lease))
	m.HandleFunc("POST /api/workers/{workerId}/progress", s.agent(s.progress))
	m.HandleFunc("POST /api/workers/{workerId}/complete", s.agent(s.complete))
	m.HandleFunc("POST /api/workers/{workerId}/fail", s.agent(s.complete))
	m.HandleFunc("POST /api/workers/{workerId}/permits", s.agent(s.permit))
	m.HandleFunc("GET /api/workers/{workerId}/permits/{uid}", s.agent(s.permitGet))
	m.HandleFunc("DELETE /api/workers/{workerId}/permits/{uid}", s.agent(s.permitCancel))
	m.HandleFunc("POST /api/workers/{workerId}/connections/{connectionId}/renew", s.agent(s.connectionRenew))
	m.HandleFunc("POST /api/workers/{workerId}/connections/{connectionId}/release", s.agent(s.connectionRelease))
	m.HandleFunc("POST /api/workers/{workerId}/downloads/{downloadId}/commit", s.agent(s.commit))
	m.HandleFunc("GET /api/workers/{workerId}/publications/{publicationId}", s.agent(s.publication))
	m.HandleFunc("GET /api/workers/{workerId}/control", s.agent(s.controlStream))
	m.HandleFunc("GET /metrics", s.metrics)
	m.HandleFunc("GET /readyz", s.ready)
	return s.Middleware(m)
}
func (s *Server) user(mutate bool, next func(http.ResponseWriter, *http.Request, auth.User)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		u, e := s.Auth.Authenticate(r, mutate)
		if e != nil {
			s.fail(w, r, e)
			return
		}
		next(w, r, u)
	}
}
func (s *Server) agent(next func(http.ResponseWriter, *http.Request, string)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.TLS == nil || len(r.TLS.VerifiedChains) == 0 || len(r.TLS.PeerCertificates) == 0 {
			s.denied(w, r)
			return
		}
		cert := r.TLS.PeerCertificates[0]
		if time.Now().Before(cert.NotBefore) || !time.Now().Before(cert.NotAfter) {
			s.denied(w, r)
			return
		}
		worker, e := WorkerIdentity(cert)
		if e != nil {
			s.denied(w, r)
			return
		}
		if id := r.PathValue("workerId"); id != "" && id != worker {
			s.denied(w, r)
			return
		}
		var revoked bool
		if e = s.Store.Pool.QueryRow(r.Context(), "SELECT EXISTS(SELECT 1 FROM revoked_certificates WHERE serial=$1)", cert.SerialNumber.String()).Scan(&revoked); e != nil {
			s.fail(w, r, e)
			return
		}
		if revoked {
			s.denied(w, r)
			return
		}
		if r.URL.Path != "/api/workers/register" {
			var valid bool
			if e = s.Store.Pool.QueryRow(r.Context(), "SELECT session_id=$2 AND session_id<>'' FROM workers WHERE id=$1", worker, r.Header.Get("Agent-Session")).Scan(&valid); e != nil {
				s.fail(w, r, e)
				return
			}
			if !valid {
				s.fail(w, r, db.Conflict("agent_session_lost"))
				return
			}
		}
		next(w, r, worker)
	}
}
func WorkerIdentity(cert *x509.Certificate) (string, error) {
	for _, u := range cert.URIs {
		if u.Scheme == "spiffe" && u.Host == "begom" && strings.HasPrefix(u.Path, "/worker/") {
			id := strings.TrimPrefix(u.Path, "/worker/")
			if model.ValidID(id) {
				return id, nil
			}
		}
	}
	return "", errors.New("invalid worker identity")
}
func (s *Server) decode(w http.ResponseWriter, r *http.Request, out any) ([]byte, error) {
	b, e := io.ReadAll(http.MaxBytesReader(w, r.Body, 2<<20))
	if e != nil {
		return nil, &db.Error{Status: 413, Code: "body_too_large", Message: "Request body exceeds limit"}
	}
	if len(b) == 0 {
		b = []byte("{}")
	}
	d := json.NewDecoder(strings.NewReader(string(b)))
	d.DisallowUnknownFields()
	if e = d.Decode(out); e != nil {
		return nil, &db.Error{Status: 400, Code: "invalid_json", Message: "Invalid request fields or JSON"}
	}
	if d.Decode(new(any)) != io.EOF {
		return nil, &db.Error{Status: 400, Code: "invalid_json", Message: "Trailing JSON data"}
	}
	return b, nil
}
func (s *Server) mutate(w http.ResponseWriter, r *http.Request, actor string, body []byte, fn db.Mutation) bool {
	v, e := s.Store.Mutate(r.Context(), actor, r.Method+" "+r.URL.Path, r.Header.Get("Idempotency-Key"), body, fn)
	if e != nil {
		s.fail(w, r, e)
		return false
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(v.Status)
	w.Write(v.Body)
	return true
}
func (s *Server) json(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}
func (s *Server) fail(w http.ResponseWriter, r *http.Request, e error) {
	status, code, message := 503, "dependency_unavailable", "Required dependency unavailable"
	var d *db.Error
	var pg *pgconn.PgError
	if errors.As(e, &d) {
		status, code, message = d.Status, d.Code, d.Message
	} else if errors.Is(e, pgx.ErrNoRows) {
		status, code, message = 404, "not_found", "Object not found"
	} else if errors.As(e, &pg) && (pg.Code == "23505" || pg.Code == "23503") {
		status, code, message = 409, "data_conflict", "Operation conflicts with stored data"
	}
	s.Log.Warn("API request rejected", "operation", r.Method+" "+r.Pattern, "result", "error", "error_code", code, "request_id", r.Header.Get("X-Request-ID"))
	s.json(w, status, model.APIError{Code: code, Message: message, RequestID: r.Header.Get("X-Request-ID")})
}
func (s *Server) denied(w http.ResponseWriter, r *http.Request) {
	s.fail(w, r, &db.Error{Status: 403, Code: "forbidden", Message: "Insufficient privileges"})
}
func (s *Server) ready(w http.ResponseWriter, r *http.Request) {
	if s.Secrets != nil {
		if _, e := s.Secrets.Get(r.Context(), s.Config.Bao.Prefix+"/runtime"); e != nil {
			s.fail(w, r, e)
			return
		}
	}
	if e := s.Store.Pool.Ping(r.Context()); e != nil {
		s.fail(w, r, e)
		return
	}
	if _, e := s.Volume.Check(); e != nil {
		s.fail(w, r, e)
		return
	}
	if e := s.Dispatch.Epoch.Check(r.Context()); e != nil {
		s.fail(w, r, e)
		return
	}
	s.json(w, 200, map[string]string{"status": "ready", "epoch": s.Dispatch.Epoch.ID})
}
func (s *Server) login(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}
	if _, e := s.decode(w, r, &in); e != nil {
		s.fail(w, r, e)
		return
	}
	peer, _, _ := net.SplitHostPort(r.RemoteAddr)
	key := peer + ":" + in.Username
	s.loginMu.Lock()
	if s.loginAttempts == nil {
		s.loginAttempts = map[string][]time.Time{}
	}
	if len(s.loginAttempts) > 2048 {
		for k, times := range s.loginAttempts {
			if len(times) == 0 || time.Since(times[len(times)-1]) > time.Minute {
				delete(s.loginAttempts, k)
			}
		}
	}
	times := s.loginAttempts[key]
	i := 0
	for i < len(times) && time.Since(times[i]) > time.Minute {
		i++
	}
	times = times[i:]
	if len(times) >= 10 {
		s.loginMu.Unlock()
		s.fail(w, r, &db.Error{Status: 429, Code: "login_throttled", Message: "Too many login attempts"})
		return
	}
	s.loginAttempts[key] = append(times, time.Now())
	s.loginMu.Unlock()
	u, token, csrf, e := s.Auth.Login(r.Context(), in.Username, in.Password)
	if e != nil {
		s.fail(w, r, e)
		return
	}
	auth.Cookies(w, token, csrf)
	s.json(w, 200, map[string]any{"user": u, "csrf_token": csrf})
}
func (s *Server) jobAccess(w http.ResponseWriter, r *http.Request, u auth.User, manage bool) bool {
	if manage && !u.Operator() {
		s.denied(w, r)
		return false
	}
	e := s.Store.CheckAccess(r.Context(), u.ID, r.PathValue("jobId"), u.Admin(), manage)
	if e != nil {
		s.fail(w, r, e)
		return false
	}
	return true
}
func (s *Server) jobs(w http.ResponseWriter, r *http.Request, u auth.User) {
	limit := min(200, auth.ParseLimit(r.URL.Query().Get("limit"), 200))
	jobs, e := s.Store.ListJobsPage(r.Context(), u.ID, u.Admin(), r.URL.Query().Get("before"), limit)
	if e != nil {
		s.fail(w, r, e)
		return
	}
	next := ""
	if len(jobs) == limit {
		next = jobs[len(jobs)-1].ID
	}
	s.json(w, 200, map[string]any{"jobs": jobs, "next_before": next})
}
func (s *Server) createJob(w http.ResponseWriter, r *http.Request, u auth.User) {
	if !u.Operator() {
		s.denied(w, r)
		return
	}
	var c model.JobConfig
	b, e := s.decode(w, r, &c)
	if e != nil {
		s.fail(w, r, e)
		return
	}
	s.mutate(w, r, u.ID, b, func(tx pgx.Tx) (any, int, error) {
		return s.Store.CreateJob(r.Context(), tx, u.ID, r.Header.Get("X-Request-ID"), c)
	})
}
func (s *Server) job(w http.ResponseWriter, r *http.Request, u auth.User) {
	if !s.jobAccess(w, r, u, false) {
		return
	}
	j, e := s.Store.GetJob(r.Context(), r.PathValue("jobId"))
	if e != nil {
		s.fail(w, r, e)
		return
	}
	s.json(w, 200, j)
}
func (s *Server) act(w http.ResponseWriter, r *http.Request, u auth.User) {
	if !s.jobAccess(w, r, u, true) {
		return
	}
	var empty struct{}
	b, e := s.decode(w, r, &empty)
	if e != nil {
		s.fail(w, r, e)
		return
	}
	action := path.Base(r.URL.Path)
	s.mutate(w, r, u.ID, b, func(tx pgx.Tx) (any, int, error) {
		return s.Store.Act(r.Context(), tx, u.ID, r.Header.Get("X-Request-ID"), r.PathValue("jobId"), action)
	})
}
func (s *Server) configure(w http.ResponseWriter, r *http.Request, u auth.User) {
	if !s.jobAccess(w, r, u, true) {
		return
	}
	var c model.JobConfig
	b, e := s.decode(w, r, &c)
	if e != nil {
		s.fail(w, r, e)
		return
	}
	s.mutate(w, r, u.ID, b, func(tx pgx.Tx) (any, int, error) {
		return s.Store.UpdateConfig(r.Context(), tx, u.ID, r.Header.Get("X-Request-ID"), r.PathValue("jobId"), c)
	})
}
func (s *Server) retryItems(w http.ResponseWriter, r *http.Request, u auth.User) {
	if !s.jobAccess(w, r, u, true) {
		return
	}
	var in struct {
		IDs []model.Number `json:"external_ids"`
	}
	b, e := s.decode(w, r, &in)
	if e != nil {
		s.fail(w, r, e)
		return
	}
	s.mutate(w, r, u.ID, b, func(tx pgx.Tx) (any, int, error) {
		return s.Store.RetryItems(r.Context(), tx, u.ID, r.Header.Get("X-Request-ID"), r.PathValue("jobId"), in.IDs)
	})
}
func (s *Server) items(w http.ResponseWriter, r *http.Request, u auth.User) {
	if !s.jobAccess(w, r, u, false) {
		return
	}
	q := r.URL.Query()
	items, e := s.Store.Items(r.Context(), r.PathValue("jobId"), q.Get("status"), q.Get("after"), auth.ParseLimit(q.Get("limit"), 500))
	if e != nil {
		s.fail(w, r, e)
		return
	}
	s.json(w, 200, map[string]any{"items": items})
}
func csvSafe(value any) string {
	switch v := value.(type) {
	case nil:
		return ""
	case *string:
		if v == nil {
			return ""
		}
		value = *v
	case *time.Time:
		if v == nil {
			return ""
		}
		value = v.UTC().Format(time.RFC3339)
	}
	text := fmt.Sprint(value)
	trimmed := strings.TrimLeft(text, " \t\r\n")
	if len(trimmed) > 0 && strings.ContainsAny(trimmed[:1], "=+-@") {
		return "'" + text
	}
	return text
}
func (s *Server) export(w http.ResponseWriter, r *http.Request, u auth.User) {
	if !s.jobAccess(w, r, u, false) {
		return
	}
	format := r.URL.Query().Get("format")
	if format != "csv" && format != "json" {
		s.fail(w, r, db.Bad("invalid_format", "Choose csv or json"))
		return
	}
	cursor := r.URL.Query().Get("after")
	rows, e := s.Store.Items(r.Context(), r.PathValue("jobId"), r.URL.Query().Get("status"), cursor, 1000)
	if e != nil {
		s.fail(w, r, e)
		return
	}
	w.Header().Set("Content-Disposition", "attachment; filename=results."+format)
	w.Header().Set("Trailer", "X-Export-Complete")
	var cw *csv.Writer
	if format == "json" {
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"items":[`)
	} else {
		w.Header().Set("Content-Type", "text/csv; charset=utf-8")
		cw = csv.NewWriter(w)
		cw.Write([]string{"external_id", "status", "error_code", "title", "file_id", "size_bytes", "mime_type", "checksum", "downloaded_at", "worker_id", "storage_path"})
	}
	count, complete := 0, true
	for {
		for _, row := range rows {
			if format == "json" {
				if count > 0 {
					io.WriteString(w, ",")
				}
				if e = json.NewEncoder(w).Encode(row); e != nil {
					return
				}
			} else {
				title, mime := "", ""
				if md, ok := row["metadata"].(model.Metadata); ok {
					title, mime = md.Title, md.MIME
				}
				values := []any{row["external_id"], row["status"], row["error_code"], title, row["file_id"], row["size_bytes"], mime, row["checksum"], row["downloaded_at"], row["worker_id"], row["storage_path"]}
				cells := make([]string, len(values))
				for i, value := range values {
					cells[i] = csvSafe(value)
				}
				if cw.Write(cells) != nil {
					return
				}
			}
			count++
			cursor = fmt.Sprint(row["external_id"])
		}
		if len(rows) < 1000 {
			break
		}
		if _, e = s.Auth.Authenticate(r, false); e != nil {
			complete = false
			break
		}
		if e = s.Store.CheckAccess(r.Context(), u.ID, r.PathValue("jobId"), u.Admin(), false); e != nil {
			complete = false
			break
		}
		rows, e = s.Store.Items(r.Context(), r.PathValue("jobId"), r.URL.Query().Get("status"), cursor, 1000)
		if e != nil {
			complete = false
			break
		}
	}
	if format == "json" {
		fmt.Fprintf(w, `],"complete":%t,"count":%d}`, complete, count)
	} else {
		if !complete {
			cw.Write([]string{"", "", "incomplete_export", "", "", "", "", "", "", "", ""})
		}
		cw.Flush()
	}
	w.Header().Set("X-Export-Complete", strconv.FormatBool(complete))
}
func (s *Server) events(w http.ResponseWriter, r *http.Request, u auth.User) {
	if !s.jobAccess(w, r, u, false) {
		return
	}
	q := r.URL.Query()
	var after int64
	if v := q.Get("after"); v != "" {
		n, e := strconv.ParseInt(v, 10, 64)
		if e != nil || n < 0 {
			s.fail(w, r, db.Bad("invalid_cursor", "Invalid event cursor"))
			return
		}
		after = n
	}
	var from, to any
	for key, dest := range map[string]*any{"from": &from, "to": &to} {
		if q.Get(key) != "" {
			t, e := time.Parse(time.RFC3339, q.Get(key))
			if e != nil {
				s.fail(w, r, db.Bad("invalid_time", "Use RFC3339 timestamps"))
				return
			}
			*dest = t
		}
	}
	rows, e := s.Store.Pool.Query(r.Context(), "SELECT id,timestamp,component,worker_id,chunk_id,attempt_id,external_id::text,operation,result,error_code FROM events WHERE job_id=$1 AND id>$2 AND ($3='' OR worker_id=$3) AND ($4='' OR result=$4) AND ($5::timestamptz IS NULL OR timestamp>=$5) AND ($6::timestamptz IS NULL OR timestamp<=$6) ORDER BY id LIMIT $7", r.PathValue("jobId"), after, q.Get("worker_id"), q.Get("status"), from, to, auth.ParseLimit(q.Get("limit"), 500))
	if e != nil {
		s.fail(w, r, e)
		return
	}
	defer rows.Close()
	out := []map[string]any{}
	for rows.Next() {
		var id int64
		var at time.Time
		var component, op, result, code string
		var worker, chunk, attempt, external *string
		if e = rows.Scan(&id, &at, &component, &worker, &chunk, &attempt, &external, &op, &result, &code); e != nil {
			s.fail(w, r, e)
			return
		}
		out = append(out, map[string]any{"id": id, "timestamp": at, "component": component, "worker_id": worker, "chunk_id": chunk, "attempt_id": attempt, "external_id": external, "operation": op, "result": result, "error_code": code})
	}
	if e = rows.Err(); e != nil {
		s.fail(w, r, e)
		return
	}
	s.json(w, 200, map[string]any{"events": out})
}
func (s *Server) access(w http.ResponseWriter, r *http.Request, u auth.User) {
	if !s.jobAccess(w, r, u, true) {
		return
	}
	rows, e := s.Store.Pool.Query(r.Context(), "SELECT a.user_id,u.username,a.permission FROM job_access a JOIN users u ON u.id=a.user_id WHERE a.job_id=$1 ORDER BY u.username", r.PathValue("jobId"))
	if e != nil {
		s.fail(w, r, e)
		return
	}
	defer rows.Close()
	out := []map[string]string{}
	for rows.Next() {
		var id, name, p string
		if e = rows.Scan(&id, &name, &p); e != nil {
			s.fail(w, r, e)
			return
		}
		out = append(out, map[string]string{"user_id": id, "username": name, "permission": p})
	}
	s.json(w, 200, map[string]any{"access": out})
}
func (s *Server) setAccess(w http.ResponseWriter, r *http.Request, u auth.User) {
	if !s.jobAccess(w, r, u, true) {
		return
	}
	var in struct {
		UserID     string `json:"user_id"`
		Permission string `json:"permission"`
	}
	b, e := s.decode(w, r, &in)
	if e != nil {
		s.fail(w, r, e)
		return
	}
	if in.Permission != "view" && in.Permission != "manage" && in.Permission != "remove" {
		s.fail(w, r, db.Bad("invalid_permission", "Choose view, manage or remove"))
		return
	}
	s.mutate(w, r, u.ID, b, func(tx pgx.Tx) (any, int, error) {
		var owner string
		if e := tx.QueryRow(r.Context(), "SELECT owner_id FROM jobs WHERE id=$1 FOR UPDATE", r.PathValue("jobId")).Scan(&owner); e != nil {
			return nil, 0, e
		}
		if !u.Admin() && owner != u.ID {
			return nil, 0, db.Forbidden()
		}
		if owner == in.UserID && in.Permission != "manage" {
			return nil, 0, db.Conflict("owner_access")
		}
		if in.Permission == "remove" {
			_, e = tx.Exec(r.Context(), "DELETE FROM job_access WHERE job_id=$1 AND user_id=$2", r.PathValue("jobId"), in.UserID)
		} else {
			_, e = tx.Exec(r.Context(), "INSERT INTO job_access VALUES($1,$2,$3) ON CONFLICT(job_id,user_id) DO UPDATE SET permission=EXCLUDED.permission", r.PathValue("jobId"), in.UserID, in.Permission)
		}
		if e != nil {
			return nil, 0, e
		}
		e = db.Audit(r.Context(), tx, u.ID, "job.access", r.PathValue("jobId"), r.Header.Get("X-Request-ID"), in)
		return map[string]bool{"ok": true}, 200, e
	})
}
func (s *Server) workerAllowed(ctx context.Context, u auth.User, id string) error {
	if u.Operator() {
		return nil
	}
	var ok bool
	e := s.Store.Pool.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM job_workers jw JOIN job_access a ON a.job_id=jw.job_id WHERE jw.worker_id=$1 AND a.user_id=$2)", id, u.ID).Scan(&ok)
	if e != nil {
		return e
	}
	if !ok {
		return db.Forbidden()
	}
	return nil
}
func (s *Server) allowedWorkers(ctx context.Context, u auth.User) ([]model.Worker, error) {
	all, e := s.Store.Workers(ctx)
	if e != nil {
		return nil, e
	}
	out := []model.Worker{}
	for _, w := range all {
		if e = s.workerAllowed(ctx, u, w.ID); e == nil {
			w.SSHSecret = ""
			if e = s.filterWorker(ctx, u, &w); e != nil {
				return nil, e
			}
			out = append(out, w)
		} else {
			var de *db.Error
			if !errors.As(e, &de) {
				return nil, e
			}
		}
	}
	return out, nil
}
func (s *Server) workers(w http.ResponseWriter, r *http.Request, u auth.User) {
	out, e := s.allowedWorkers(r.Context(), u)
	if e != nil {
		s.fail(w, r, e)
		return
	}
	s.json(w, 200, map[string]any{"workers": out})
}
func (s *Server) worker(w http.ResponseWriter, r *http.Request, u auth.User) {
	if e := s.workerAllowed(r.Context(), u, r.PathValue("workerId")); e != nil {
		s.fail(w, r, e)
		return
	}
	v, e := s.Store.GetWorker(r.Context(), r.PathValue("workerId"))
	if e != nil {
		s.fail(w, r, e)
		return
	}
	v.SSHSecret = ""
	if e = s.filterWorker(r.Context(), u, &v); e != nil {
		s.fail(w, r, e)
		return
	}
	s.json(w, 200, v)
}
func (s *Server) filterWorker(ctx context.Context, u auth.User, w *model.Worker) error {
	if u.Admin() {
		return nil
	}
	leases := []model.Lease{}
	for _, l := range w.Leases {
		if e := s.Store.CheckAccess(ctx, u.ID, l.JobID, false, false); e == nil {
			leases = append(leases, l)
		} else {
			var de *db.Error
			if !errors.As(e, &de) {
				return e
			}
		}
	}
	downloads := []model.ActiveDownload{}
	for _, d := range w.Downloads {
		if e := s.Store.CheckAccess(ctx, u.ID, d.JobID, false, false); e == nil {
			downloads = append(downloads, d)
		} else {
			var de *db.Error
			if !errors.As(e, &de) {
				return e
			}
		}
	}
	w.Leases = leases
	w.Downloads = downloads
	if len(leases) == 0 {
		w.Metrics.ActiveChecks = 0
	}
	w.Metrics.ActiveDownloads = len(downloads)
	var ownedBytes string
	var errorCount uint64
	if e := s.Store.Pool.QueryRow(ctx, "SELECT COALESCE(sum(d.offset_bytes),0)::text FROM downloads d JOIN job_access a ON a.job_id=d.job_id WHERE a.user_id=$1 AND d.worker_id=$2", u.ID, w.ID).Scan(&ownedBytes); e != nil {
		return e
	}
	w.Metrics.Bytes = uint64(db.Number(ownedBytes))
	if e := s.Store.Pool.QueryRow(ctx, "SELECT count(*) FROM events e JOIN job_access a ON a.job_id=e.job_id WHERE a.user_id=$1 AND e.worker_id=$2 AND e.operation='item.result' AND e.result='failed'", u.ID, w.ID).Scan(&errorCount); e != nil {
		return e
	}
	w.Metrics.Errors = errorCount
	return nil
}
func (s *Server) workerMetrics(w http.ResponseWriter, r *http.Request, u auth.User) {
	if e := s.workerAllowed(r.Context(), u, r.PathValue("workerId")); e != nil {
		s.fail(w, r, e)
		return
	}
	v, e := s.Store.GetWorker(r.Context(), r.PathValue("workerId"))
	if e != nil {
		s.fail(w, r, e)
		return
	}
	if e = s.filterWorker(r.Context(), u, &v); e != nil {
		s.fail(w, r, e)
		return
	}
	s.json(w, 200, v.Metrics)
}
func (s *Server) saveWorker(w http.ResponseWriter, r *http.Request, u auth.User) {
	if !u.Admin() {
		s.denied(w, r)
		return
	}
	var in model.WorkerConfig
	b, e := s.decode(w, r, &in)
	if e != nil {
		s.fail(w, r, e)
		return
	}
	if id := r.PathValue("workerId"); id != "" {
		in.ID = id
	}
	if in.Validate() != nil || in.StoragePath != "" && (!filepath.IsAbs(in.StoragePath) || filepath.Clean(in.StoragePath) != in.StoragePath) {
		s.fail(w, r, db.Bad("invalid_worker", "Invalid worker configuration"))
		return
	}
	s.mutate(w, r, u.ID, b, func(tx pgx.Tx) (any, int, error) {
		var previous []byte
		e := tx.QueryRow(r.Context(), "SELECT config FROM workers WHERE id=$1 FOR UPDATE", in.ID).Scan(&previous)
		if e != nil && !errors.Is(e, pgx.ErrNoRows) {
			return nil, 0, e
		}
		if in.SSHSecret == "" && previous != nil {
			var old model.WorkerConfig
			if e = json.Unmarshal(previous, &old); e != nil {
				return nil, 0, e
			}
			in.SSHSecret = old.SSHSecret
			if in.StoragePath == "" {
				in.StoragePath = old.StoragePath
			}
		}
		if previous != nil {
			var old model.WorkerConfig
			if e = json.Unmarshal(previous, &old); e != nil {
				return nil, 0, e
			}
			if old.StoragePath != in.StoragePath {
				if in.Enabled {
					return nil, 0, db.Bad("disable_before_storage_change", "Disable the worker before changing its mounted NAS path")
				}
				if _, e = tx.Exec(r.Context(), "UPDATE workers SET session_id='',last_heartbeat=NULL WHERE id=$1", in.ID); e != nil {
					return nil, 0, e
				}
			}
		}
		_, e = tx.Exec(r.Context(), "INSERT INTO workers(id,config,enabled) VALUES($1,$2,$3) ON CONFLICT(id) DO UPDATE SET config=EXCLUDED.config,enabled=EXCLUDED.enabled", in.ID, db.JSON(in), in.Enabled)
		if e != nil {
			return nil, 0, e
		}
		if e = s.Store.RecordQuotaHorizon(r.Context(), tx, model.Limits{WorkerRPS: in.RPS}); e != nil {
			return nil, 0, e
		}
		e = db.Audit(r.Context(), tx, u.ID, "worker.configure", in.ID, r.Header.Get("X-Request-ID"), map[string]any{"enabled": in.Enabled, "threads": in.Threads})
		out := in
		out.SSHSecret = ""
		return out, 200, e
	})
}
func (s *Server) users(w http.ResponseWriter, r *http.Request, u auth.User) {
	if !u.Admin() {
		s.denied(w, r)
		return
	}
	rows, e := s.Store.Pool.Query(r.Context(), "SELECT id FROM users ORDER BY username")
	if e != nil {
		s.fail(w, r, e)
		return
	}
	ids := []string{}
	for rows.Next() {
		var id string
		if e = rows.Scan(&id); e != nil {
			rows.Close()
			s.fail(w, r, e)
			return
		}
		ids = append(ids, id)
	}
	rows.Close()
	out := []auth.User{}
	for _, id := range ids {
		v, e := s.Auth.User(r.Context(), id)
		if e != nil {
			s.fail(w, r, e)
			return
		}
		out = append(out, v)
	}
	s.json(w, 200, map[string]any{"users": out})
}
func (s *Server) saveUser(w http.ResponseWriter, r *http.Request, u auth.User) {
	if !u.Admin() {
		s.denied(w, r)
		return
	}
	var in auth.UserInput
	b, e := s.decode(w, r, &in)
	if e != nil {
		s.fail(w, r, e)
		return
	}
	s.mutate(w, r, u.ID, b, func(tx pgx.Tx) (any, int, error) {
		return s.Auth.SaveUser(r.Context(), tx, u.ID, r.Header.Get("X-Request-ID"), r.PathValue("userId"), in)
	})
}
func (s *Server) audit(w http.ResponseWriter, r *http.Request, u auth.User) {
	if !u.Admin() {
		s.denied(w, r)
		return
	}
	rows, e := s.Store.Pool.Query(r.Context(), "SELECT id,timestamp,actor_id,action,object_id,request_id,context FROM audit_log ORDER BY id DESC LIMIT $1", auth.ParseLimit(r.URL.Query().Get("limit"), 500))
	if e != nil {
		s.fail(w, r, e)
		return
	}
	defer rows.Close()
	out := []map[string]any{}
	for rows.Next() {
		var id int64
		var at time.Time
		var actor, action, object, request string
		var raw json.RawMessage
		if e = rows.Scan(&id, &at, &actor, &action, &object, &request, &raw); e != nil {
			s.fail(w, r, e)
			return
		}
		out = append(out, map[string]any{"id": id, "timestamp": at, "actor_id": actor, "action": action, "object_id": object, "request_id": request, "context": raw})
	}
	s.json(w, 200, map[string]any{"audit": out})
}
func (s *Server) content(w http.ResponseWriter, r *http.Request, u auth.User) {
	var rel, mime string
	var size string
	e := s.Store.Pool.QueryRow(r.Context(), "SELECT f.storage_path,f.mime_type,f.size_bytes::text FROM files f WHERE f.id=$1 AND f.ready AND ($3 OR EXISTS(SELECT 1 FROM items i JOIN job_access a ON a.job_id=i.job_id WHERE i.media_id=f.media_id AND i.status='completed' AND a.user_id=$2))", r.PathValue("fileId"), u.ID, u.Admin()).Scan(&rel, &mime, &size)
	if e != nil {
		s.fail(w, r, e)
		return
	}
	if !storage.ValidPath(rel) {
		s.fail(w, r, db.Conflict("invalid_catalog_path"))
		return
	}
	root, e := s.Volume.OpenRoot()
	if e != nil {
		s.fail(w, r, e)
		return
	}
	defer root.Close()
	st, e := storage.StatRegular(root, path.Join("published", rel))
	if e != nil || !st.Mode().IsRegular() || uint64(st.Size()) != uint64(db.Number(size)) {
		s.fail(w, r, db.Conflict("file_unavailable"))
		return
	}
	encoded := []string{}
	for _, part := range strings.Split(rel, "/") {
		encoded = append(encoded, url.PathEscape(part))
	}
	w.Header().Set("X-Accel-Redirect", "/_published/"+strings.Join(encoded, "/"))
	w.Header().Set("Content-Type", mime)
	w.WriteHeader(200)
}
func (s *Server) register(w http.ResponseWriter, r *http.Request, id string) {
	var in model.Register
	b, e := s.decode(w, r, &in)
	if e != nil {
		s.fail(w, r, e)
		return
	}
	s.mutate(w, r, id, b, func(tx pgx.Tx) (any, int, error) { return s.Store.Register(r.Context(), tx, id, in) })
}
func (s *Server) heartbeat(w http.ResponseWriter, r *http.Request, id string) {
	var in model.Heartbeat
	b, e := s.decode(w, r, &in)
	if e != nil {
		s.fail(w, r, e)
		return
	}
	s.mutate(w, r, id, b, func(tx pgx.Tx) (any, int, error) {
		out, status, e := s.Store.HeartbeatTx(r.Context(), tx, id, in)
		if reply, ok := out.(model.HeartbeatReply); ok {
			reply.Epoch = s.Dispatch.Epoch.ID
			out = reply
		}
		return out, status, e
	})
}
func (s *Server) lease(w http.ResponseWriter, r *http.Request, id string) {
	var empty struct{}
	b, e := s.decode(w, r, &empty)
	if e != nil {
		s.fail(w, r, e)
		return
	}
	if e = s.Dispatch.Epoch.Check(r.Context()); e != nil {
		s.fail(w, r, e)
		return
	}
	s.mutate(w, r, id, b, func(tx pgx.Tx) (any, int, error) {
		free, e := s.Volume.Check()
		if e != nil {
			return nil, 0, e
		}
		return s.Store.LeaseResourceTx(r.Context(), tx, id, free)
	})
}
func (s *Server) progress(w http.ResponseWriter, r *http.Request, id string) {
	var in model.Progress
	b, e := s.decode(w, r, &in)
	if e != nil {
		s.fail(w, r, e)
		return
	}
	s.mutate(w, r, id, b, func(tx pgx.Tx) (any, int, error) {
		free, e := s.Volume.Check()
		if e != nil {
			return nil, 0, e
		}
		return s.Store.ProgressTx(r.Context(), tx, id, in, free, s.Config.Storage.MinFreeBytes)
	})
}
func (s *Server) complete(w http.ResponseWriter, r *http.Request, id string) {
	var in model.LeaseRef
	b, e := s.decode(w, r, &in)
	if e != nil {
		s.fail(w, r, e)
		return
	}
	s.mutate(w, r, id, b, func(tx pgx.Tx) (any, int, error) {
		return s.Store.CompleteTx(r.Context(), tx, id, in, strings.HasSuffix(r.URL.Path, "/fail"))
	})
}
func (s *Server) permit(w http.ResponseWriter, r *http.Request, id string) {
	var in model.PermitRequest
	b, e := s.decode(w, r, &in)
	if e != nil {
		s.fail(w, r, e)
		return
	}
	s.mutate(w, r, id, b, func(tx pgx.Tx) (any, int, error) { return s.Dispatch.RequestTx(r.Context(), tx, id, in) })
}
func (s *Server) permitGet(w http.ResponseWriter, r *http.Request, id string) {
	if e := s.Dispatch.Epoch.Check(r.Context()); e != nil {
		s.fail(w, r, e)
		return
	}
	v, status, e := s.Dispatch.Get(id, r.PathValue("uid"))
	if e != nil {
		s.fail(w, r, e)
		return
	}
	s.json(w, status, v)
}
func (s *Server) permitCancel(w http.ResponseWriter, r *http.Request, id string) {
	s.mutate(w, r, id, []byte("{}"), func(tx pgx.Tx) (any, int, error) {
		s.Dispatch.Cancel(id, r.PathValue("uid"))
		return map[string]bool{"cancelled": true}, 200, nil
	})
}
func (s *Server) connectionRenew(w http.ResponseWriter, r *http.Request, id string) {
	var in struct{}
	b, e := s.decode(w, r, &in)
	if e != nil {
		s.fail(w, r, e)
		return
	}
	s.mutate(w, r, id, b, func(tx pgx.Tx) (any, int, error) {
		return s.Dispatch.RenewTx(r.Context(), tx, id, r.PathValue("connectionId"))
	})
}
func (s *Server) connectionRelease(w http.ResponseWriter, r *http.Request, id string) {
	var in model.Feedback
	b, e := s.decode(w, r, &in)
	if e != nil {
		s.fail(w, r, e)
		return
	}
	confirmed := s.mutate(w, r, id, b, func(tx pgx.Tx) (any, int, error) {
		e := s.Dispatch.ReleaseTx(r.Context(), tx, id, r.PathValue("connectionId"), in)
		return map[string]bool{"released": true}, 200, e
	})
	if confirmed {
		s.Dispatch.FinalizeRelease(id, r.PathValue("connectionId"))
	}
}
func (s *Server) commit(w http.ResponseWriter, r *http.Request, id string) {
	var in model.Commit
	b, e := s.decode(w, r, &in)
	if e != nil {
		s.fail(w, r, e)
		return
	}
	s.mutate(w, r, id, b, func(tx pgx.Tx) (any, int, error) {
		return s.Store.PreparePublication(r.Context(), tx, id, r.PathValue("downloadId"), in)
	})
}
func (s *Server) publication(w http.ResponseWriter, r *http.Request, id string) {
	p, e := s.Store.Publication(r.Context(), r.PathValue("publicationId"))
	if e != nil {
		s.fail(w, r, e)
		return
	}
	if p.WorkerID != id {
		s.denied(w, r)
		return
	}
	s.json(w, 200, p)
}
func (s *Server) metrics(w http.ResponseWriter, r *http.Request) {
	if !wire.MetricsIdentity(r) {
		s.denied(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	io.WriteString(w, s.Dispatch.Prometheus())
	workers, e := s.Store.Workers(r.Context())
	if e != nil {
		return
	}
	for _, v := range workers {
		hostmetrics.Prometheus(w, "begom_worker_", fmt.Sprintf("{worker=%q}", v.ID), v.Metrics)
		fmt.Fprintf(w, "begom_worker_version{worker=%q,version=%q} 1\n", v.ID, v.Version)
		fmt.Fprintf(w, "begom_worker_heartbeat_timestamp_seconds{worker=%q} %g\nbegom_worker_storage_free_bytes{worker=%q} %d\n", v.ID, heartbeatTime(v.LastHeartbeat), v.ID, v.Metrics.DiskFree)
	}
	var active, reassign uint64
	s.Store.Pool.QueryRow(r.Context(), "SELECT count(*) FROM range_chunks WHERE status='leased'").Scan(&active)
	s.Store.Pool.QueryRow(r.Context(), "SELECT COALESCE(sum(attempt-1),0) FROM range_chunks WHERE attempt>1").Scan(&reassign)
	fmt.Fprintf(w, "begom_chunks_active %d\nbegom_lease_reassignments_total %d\n", active, reassign)
	rows, e := s.Store.Pool.Query(r.Context(), "SELECT j.id,j.state,count(i.*) FILTER(WHERE i.final),count(i.*) FILTER(WHERE i.status='completed'),count(i.*) FILTER(WHERE i.status='failed'),COALESCE(EXTRACT(EPOCH FROM max(i.updated_at)),0) FROM jobs j LEFT JOIN items i ON i.job_id=j.id GROUP BY j.id")
	if e == nil {
		defer rows.Close()
		for rows.Next() {
			var id, state string
			var done, completed, failed uint64
			var last float64
			if rows.Scan(&id, &state, &done, &completed, &failed, &last) != nil {
				return
			}
			fmt.Fprintf(w, "begom_job_state{job=%q,state=%q} 1\nbegom_job_processed{job=%q} %d\nbegom_job_completed{job=%q} %d\nbegom_job_failed{job=%q} %d\nbegom_job_last_progress_timestamp_seconds{job=%q} %g\n", id, state, id, done, id, completed, id, failed, id, last)
		}
	}
	for _, table := range []string{"publications", "storage_reservations", "downloads"} {
		var n int
		s.Store.Pool.QueryRow(r.Context(), "SELECT count(*) FROM "+table).Scan(&n)
		fmt.Fprintf(w, "begom_database_rows{table=%q} %d\n", table, n)
	}
	pool := s.Store.Pool.Stat()
	var oldest, retries float64
	s.Store.Pool.QueryRow(r.Context(), "SELECT COALESCE(max(EXTRACT(EPOCH FROM clock_timestamp()-created_at)),0) FROM jobs WHERE state IN ('created','validating') OR state='running' AND EXISTS(SELECT 1 FROM range_chunks c WHERE c.job_id=jobs.id AND c.status='pending')").Scan(&oldest)
	s.Store.Pool.QueryRow(r.Context(), "SELECT count(*)::float8 FROM events WHERE operation='item.result' AND result IN ('retry_wait','rate_limited')").Scan(&retries)
	fmt.Fprintf(w, "begom_oldest_queued_job_seconds %g\nbegom_item_retries_total %g\n", oldest, retries)
	fmt.Fprintf(w, "begom_sql_connections %d\nbegom_sql_acquired_connections %d\nbegom_up 1\n", pool.TotalConns(), pool.AcquiredConns())
}
func heartbeatTime(t *time.Time) float64 {
	if t == nil {
		return 0
	}
	return float64(t.UnixMilli()) / 1000
}
func (s *Server) Run(ctx context.Context) {
	ticker := time.NewTicker(time.Duration(s.Config.SweepMS) * time.Millisecond)
	nextRotation := time.Now().Add(time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !s.Dispatch.Epoch.Owns() {
				continue
			}
			if s.Secrets != nil && time.Now().After(nextRotation) {
				nextRotation = time.Now().Add(time.Hour)
				go func() {
					rctx, cancel := context.WithTimeout(ctx, 30*time.Second)
					defer cancel()
					s.renewIdentities(rctx)
				}()
			}
			if e := s.Store.Sweep(ctx); e != nil {
				s.Log.Warn("sweep unavailable", "operation", "lease.sweep", "result", "retry", "error_code", "database_unavailable")
			}
			if e := s.Store.EnforceStops(ctx); e != nil {
				s.Log.Warn("stop evaluation unavailable", "operation", "job.stops", "result", "retry", "error_code", "database_unavailable")
			}
			if e := s.recoverActions(ctx); e != nil {
				s.Log.Warn("SSH recovery unavailable", "operation", "ssh.recovery", "result", "retry", "error_code", "database_unavailable")
			}
		}
	}
}
