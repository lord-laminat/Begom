//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rychkin/Begom/internal/auth"
	"github.com/rychkin/Begom/internal/db"
	"github.com/rychkin/Begom/internal/model"
	"os"
	"sync"
	"testing"
	"time"
)

type stand struct {
	s        *db.Store
	user     string
	workers  []string
	sessions map[string]string
}

func setup(t *testing.T) *stand {
	t.Helper()
	raw := os.Getenv("BEGOM_TEST_DATABASE_URL")
	if raw == "" {
		t.Fatal("BEGOM_TEST_DATABASE_URL must point to a disposable PostgreSQL 18 database")
	}
	ctx := t.Context()
	admin, e := pgxpool.New(ctx, raw)
	if e != nil {
		t.Fatal(e)
	}
	schema := "begom_test_" + model.NewID()
	if _, e = admin.Exec(ctx, "CREATE SCHEMA "+pgx.Identifier{schema}.Sanitize()); e != nil {
		t.Fatal(e)
	}
	cfg, e := pgxpool.ParseConfig(raw)
	if e != nil {
		t.Fatal(e)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, e := pgxpool.NewWithConfig(ctx, cfg)
	if e != nil {
		t.Fatal(e)
	}
	s := &db.Store{Pool: pool, Lease: 2 * time.Second, Heartbeat: time.Second, SourceID: "mock", MetadataKey: make([]byte, 32)}
	t.Cleanup(func() {
		pool.Close()
		qctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		admin.Exec(qctx, "DROP SCHEMA "+pgx.Identifier{schema}.Sanitize()+" CASCADE")
		admin.Close()
	})
	if e = s.Migrate(ctx); e != nil {
		t.Fatal("migration", e)
	}
	if e = s.Migrate(ctx); e != nil {
		t.Fatal("idempotent migration", e)
	}
	a := &auth.Service{Store: s}
	if e = a.Bootstrap(ctx, "test-admin", "integration-only-password"); e != nil {
		t.Fatal(e)
	}
	x := &stand{s: s, sessions: map[string]string{}}
	if e = pool.QueryRow(ctx, "SELECT id FROM users WHERE username='test-admin'").Scan(&x.user); e != nil {
		t.Fatal(e)
	}
	for _, worker := range []string{"w1", "w2", "w3"} {
		if e = s.SeedWorkers(ctx, []model.WorkerConfig{{ID: worker, Threads: 40, Enabled: true}}); e != nil {
			t.Fatal(e)
		}
		x.workers = append(x.workers, worker)
		session := model.NewID()
		x.sessions[worker] = session
		mutate(t, s, worker, "register", model.Register{WorkerID: worker, SessionID: session, Version: model.Version, APIVersion: model.APIVersion}, func(tx pgx.Tx) (any, int, error) {
			return s.Register(ctx, tx, worker, model.Register{WorkerID: worker, SessionID: session, Version: model.Version, APIVersion: model.APIVersion})
		})
		h := model.Heartbeat{SessionID: session, Version: model.Version, Metrics: model.Metrics{MeasuredAt: time.Now().UTC(), DiskFree: 1 << 40, DiskTotal: 2 << 40, StorageHealthy: true}}
		mutate(t, s, worker, "heartbeat", h, func(tx pgx.Tx) (any, int, error) { return s.HeartbeatTx(ctx, tx, worker, h) })
	}
	return x
}
func mutate(t *testing.T, s *db.Store, actor, op string, body any, fn db.Mutation) db.Result {
	t.Helper()
	result, e := s.Mutate(t.Context(), actor, op, model.NewID(), db.JSON(body), fn)
	if e != nil {
		t.Fatal(op, e)
	}
	return result
}
func (x *stand) job(t *testing.T, text string, block uint64) string {
	t.Helper()
	c := model.JobConfig{RangeText: &text, WorkerIDs: x.workers, Threads: 40, BlockSize: block}
	r := mutate(t, x.s, x.user, "create", c, func(tx pgx.Tx) (any, int, error) { return x.s.CreateJob(t.Context(), tx, x.user, "req", c) })
	var reply struct {
		ID string `json:"job_id"`
	}
	json.Unmarshal(r.Body, &reply)
	mutate(t, x.s, x.user, "start", reply.ID, func(tx pgx.Tx) (any, int, error) { return x.s.Act(t.Context(), tx, x.user, "req", reply.ID, "start") })
	return reply.ID
}
func (x *stand) lease(t *testing.T, worker string) *model.Lease {
	t.Helper()
	r := mutate(t, x.s, worker, "lease", nil, func(tx pgx.Tx) (any, int, error) { return x.s.LeaseTx(t.Context(), tx, worker) })
	var reply struct {
		Lease *model.Lease `json:"lease"`
	}
	if e := json.Unmarshal(r.Body, &reply); e != nil {
		t.Fatal(e)
	}
	return reply.Lease
}
func TestConcurrentLeaseOwnership(t *testing.T) {
	x := setup(t)
	x.job(t, "[0,299]", 100)
	var wg sync.WaitGroup
	var mu sync.Mutex
	leases := []model.Lease{}
	errs := make(chan error, 60)
	for i := 0; i < 60; i++ {
		worker := x.workers[i%3]
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, e := x.s.Mutate(t.Context(), worker, "lease", model.NewID(), []byte("{}"), func(tx pgx.Tx) (any, int, error) { return x.s.LeaseTx(t.Context(), tx, worker) })
			if e != nil {
				errs <- e
				return
			}
			var out struct {
				Lease *model.Lease `json:"lease"`
			}
			if e = json.Unmarshal(r.Body, &out); e != nil {
				errs <- e
				return
			}
			if out.Lease != nil {
				mu.Lock()
				leases = append(leases, *out.Lease)
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Error(e)
	}
	unique := map[string]bool{}
	for _, l := range leases {
		if unique[l.ChunkID] {
			t.Fatal("double active lease", l.ChunkID)
		}
		unique[l.ChunkID] = true
	}
	for _, w := range x.workers {
		if l := x.lease(t, w); l != nil {
			unique[l.ChunkID] = true
		}
	}
	if len(unique) != 3 {
		t.Fatalf("expected three distinct leases, got %d", len(unique))
	}
	var active int
	if e := x.s.Pool.QueryRow(t.Context(), "SELECT count(*) FROM range_chunks WHERE status='leased'").Scan(&active); e != nil || active != 3 {
		t.Fatal(active, e)
	}
}
func TestGapCheckpointIdempotencyAndStaleAttempt(t *testing.T) {
	x := setup(t)
	job := x.job(t, "[10,12]", 3)
	l := x.lease(t, "w1")
	if l == nil {
		t.Fatal("no lease")
	}
	p := model.Progress{LeaseRef: l.Ref(), OperationID: model.NewID(), Results: []model.ItemResult{{ExternalID: 10, Status: "not_found"}, {ExternalID: 12, Status: "forbidden"}}}
	key := model.NewID()
	body := db.JSON(p)
	r, e := x.s.Mutate(t.Context(), "w1", "progress", key, body, func(tx pgx.Tx) (any, int, error) { return x.s.ProgressTx(t.Context(), tx, "w1", p, 1<<40, 0) })
	if e != nil {
		t.Fatal(e)
	}
	j, e := x.s.GetJob(t.Context(), job)
	if e != nil || j.NextID == nil || *j.NextID != 11 {
		t.Fatal("gap hidden", j.NextID, e)
	}
	var eventsBefore int
	x.s.Pool.QueryRow(t.Context(), "SELECT count(*) FROM events").Scan(&eventsBefore)
	_, e = x.s.Pool.Exec(t.Context(), "UPDATE range_chunks SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE id=$1", l.ChunkID)
	if e != nil {
		t.Fatal(e)
	}
	again, e := x.s.Mutate(t.Context(), "w1", "progress", key, body, func(tx pgx.Tx) (any, int, error) { t.Error("committed operation executed twice"); return nil, 0, nil })
	if e != nil || string(r.Body) != string(again.Body) {
		t.Fatal("lost confirmed response", e)
	}
	var eventsAfter int
	x.s.Pool.QueryRow(t.Context(), "SELECT count(*) FROM events").Scan(&eventsAfter)
	if eventsBefore != eventsAfter {
		t.Fatal("duplicate side effects")
	}
	if _, e = x.s.Mutate(t.Context(), "w1", "progress", key, []byte("different"), func(pgx.Tx) (any, int, error) { return nil, 0, nil }); e == nil {
		t.Fatal("idempotency body conflict accepted")
	}
	if e = x.s.Sweep(t.Context()); e != nil {
		t.Fatal(e)
	}
	next := x.lease(t, "w2")
	if next == nil || next.AttemptID == l.AttemptID || next.NextID == nil || *next.NextID != 11 {
		t.Fatal("recovery", next)
	}
	_, e = x.s.Mutate(t.Context(), "w1", "progress", model.NewID(), body, func(tx pgx.Tx) (any, int, error) { return x.s.ProgressTx(t.Context(), tx, "w1", p, 1<<40, 0) })
	var conflict *db.Error
	if !errors.As(e, &conflict) || conflict.Status != 409 {
		t.Fatal("stale attempt accepted", e)
	}
	p = model.Progress{LeaseRef: next.Ref(), OperationID: model.NewID(), Results: []model.ItemResult{{ExternalID: 11, Status: "not_found"}}}
	mutate(t, x.s, "w2", "progress", p, func(tx pgx.Tx) (any, int, error) { return x.s.ProgressTx(t.Context(), tx, "w2", p, 1<<40, 0) })
	mutate(t, x.s, "w2", "complete", next.Ref(), func(tx pgx.Tx) (any, int, error) { return x.s.CompleteTx(t.Context(), tx, "w2", next.Ref(), false) })
	if e = x.s.Sweep(t.Context()); e != nil {
		t.Fatal(e)
	}
	j, e = x.s.GetJob(t.Context(), job)
	if e != nil || j.State != "completed" || j.Counts["not_found"] != 2 {
		t.Fatal(j, e)
	}
}
func TestCheckpointCrossesDecimalDigitBoundary(t *testing.T) {
	x := setup(t)
	x.job(t, "[0,120]", 121)
	l := x.lease(t, "w1")
	results := []model.ItemResult{}
	for i := 120; i >= 0; i-- {
		results = append(results, model.ItemResult{ExternalID: model.Number(i), Status: "not_found"})
	}
	p := model.Progress{LeaseRef: l.Ref(), OperationID: model.NewID(), Results: results}
	mutate(t, x.s, "w1", "progress", p, func(tx pgx.Tx) (any, int, error) { return x.s.ProgressTx(t.Context(), tx, "w1", p, 1<<40, 0) })
	j, e := x.s.GetJob(t.Context(), l.JobID)
	if e != nil || j.NextID != nil {
		t.Fatal("checkpoint sorted decimal strings instead of numeric IDs", j.NextID, e)
	}
}
func TestMaximumIdentifierAndCancelledGap(t *testing.T) {
	x := setup(t)
	job := x.job(t, "[18446744073709551615,18446744073709551615]", 1000)
	l := x.lease(t, "w1")
	if l == nil || l.From != model.Number(^uint64(0)) || l.To != l.From {
		t.Fatal(l)
	}
	mutate(t, x.s, x.user, "stop", nil, func(tx pgx.Tx) (any, int, error) { return x.s.Act(t.Context(), tx, x.user, "req", job, "stop") })
	mutate(t, x.s, x.user, "resume", nil, func(tx pgx.Tx) (any, int, error) { return x.s.Act(t.Context(), tx, x.user, "req", job, "resume") })
	again := x.lease(t, "w2")
	if again == nil || again.NextID == nil || *again.NextID != l.From || again.RunID == l.RunID {
		t.Fatal("cancelled item skipped", again)
	}
	p := model.Progress{LeaseRef: again.Ref(), OperationID: model.NewID(), Results: []model.ItemResult{{ExternalID: again.From, Status: "not_found"}}}
	mutate(t, x.s, "w2", "progress", p, func(tx pgx.Tx) (any, int, error) { return x.s.ProgressTx(t.Context(), tx, "w2", p, 1<<40, 0) })
	mutate(t, x.s, "w2", "complete", nil, func(tx pgx.Tx) (any, int, error) { return x.s.CompleteTx(t.Context(), tx, "w2", again.Ref(), false) })
	if e := x.s.Sweep(t.Context()); e != nil {
		t.Fatal(e)
	}
	j, e := x.s.GetJob(t.Context(), job)
	if e != nil || j.State != "completed" || j.NextID != nil {
		t.Fatal("overflow at domain end", j, e)
	}
}

// downloadJob seeds confirmed, accessible discovery results and creates a real
// immutable download selection. Network parsing is covered by the stand tests.
func (x *stand) downloadJob(t *testing.T, text string, block uint64, canonical func(model.Number) string) string {
	t.Helper()
	r, e := model.ParseRange(text)
	if e != nil || r.To == nil {
		t.Fatal("finite fixture range required", e)
	}
	scan := x.job(t, text, 10000)
	chunk := model.NewID()
	if _, e = x.s.Pool.Exec(t.Context(), "INSERT INTO range_chunks(id,job_id,range_start,range_end,status) VALUES($1,$2,$3,$4,'completed')", chunk, scan, r.From.String(), r.To.String()); e != nil {
		t.Fatal(e)
	}
	ids := []string{}
	for n := r.From; ; n++ {
		name := "video-" + n.String()
		if canonical != nil {
			name = canonical(n)
		}
		meta := model.Metadata{CanonicalID: name, Title: "Video " + n.String(), MIME: "video/mp4", Size: 30, Qualities: []model.Quality{{Name: "720p", Size: 30}}}
		var mid string
		if e = x.s.Pool.QueryRow(t.Context(), "INSERT INTO media_metadata(id,source_id,external_id,canonical_id,title,page_url,data,downloadable) VALUES($1,'mock',$2,$3,$4,'https://example.com',$5,true) ON CONFLICT(source_id,canonical_id) DO UPDATE SET downloadable=true RETURNING id", model.NewID(), n.String(), name, meta.Title, db.JSON(meta)).Scan(&mid); e != nil {
			t.Fatal(e)
		}
		if _, e = x.s.Pool.Exec(t.Context(), "INSERT INTO items(job_id,external_id,run_id,chunk_id,attempt_id,status,final,media_id) VALUES($1,$2,'fixture',$3,'fixture','available',true,$4)", scan, n.String(), chunk, mid); e != nil {
			t.Fatal(e)
		}
		if _, e = x.s.Pool.Exec(t.Context(), "INSERT INTO media_aliases(source_id,external_id,media_id) VALUES('mock',$1,$2) ON CONFLICT(source_id,external_id) DO UPDATE SET media_id=EXCLUDED.media_id", n.String(), mid); e != nil {
			t.Fatal(e)
		}
		ids = append(ids, mid)
		if n == *r.To {
			break
		}
	}
	if _, e = x.s.Pool.Exec(t.Context(), "UPDATE jobs SET state='completed',frontier=NULL WHERE id=$1", scan); e != nil {
		t.Fatal(e)
	}
	// Seeding large catalogs can exceed the deliberately short liveness window.
	// Submit real fixture heartbeats before validating the download job.
	for _, worker := range x.workers {
		h := model.Heartbeat{SessionID: x.sessions[worker], Version: model.Version, Metrics: model.Metrics{MeasuredAt: time.Now().UTC(), DiskFree: 1 << 40, DiskTotal: 2 << 40, StorageHealthy: true}}
		mutate(t, x.s, worker, "fixture.heartbeat", h, func(tx pgx.Tx) (any, int, error) { return x.s.HeartbeatTx(t.Context(), tx, worker, h) })
	}
	// A canonical material is selected once even when several numeric aliases exist.
	seen := map[string]bool{}
	unique := []string{}
	for _, id := range ids {
		if !seen[id] {
			seen[id] = true
			unique = append(unique, id)
		}
	}
	in := model.CreateJob{Kind: model.Download, Config: model.JobConfig{WorkerIDs: x.workers, Threads: 40, BlockSize: block}, MediaIDs: unique}
	result := mutate(t, x.s, x.user, "download.create", in, func(tx pgx.Tx) (any, int, error) {
		return x.s.CreateRequestedJob(t.Context(), tx, x.user, "fixture", in)
	})
	var out struct {
		ID string `json:"job_id"`
	}
	if e = json.Unmarshal(result.Body, &out); e != nil {
		t.Fatal(e)
	}
	mutate(t, x.s, x.user, "start", out.ID, func(tx pgx.Tx) (any, int, error) { return x.s.Act(t.Context(), tx, x.user, "fixture", out.ID, "start") })
	return out.ID
}
