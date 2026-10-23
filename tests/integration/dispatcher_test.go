//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rychkin/Begom/internal/db"
	"github.com/rychkin/Begom/internal/dispatch"
	"github.com/rychkin/Begom/internal/model"
	"sync"
	"testing"
	"time"
)

func TestDispatcherConcurrentJournalAndRecovery(t *testing.T) {
	x := setup(t)
	x.s.Lease = 30 * time.Second
	x.job(t, "[0,239]", 80)
	leases := []*model.Lease{}
	for _, worker := range x.workers {
		leases = append(leases, x.lease(t, worker))
	}
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	ep, e := x.s.AcquireEpoch(ctx, dispatch.DrainInterval)
	if e != nil {
		t.Fatal(e)
	}
	ep.GuardUntil = time.Now().Add(-time.Second)
	go ep.Run(ctx)
	canceled, stop := context.WithCancel(ctx)
	stop()
	if e = ep.Check(canceled); e == nil || !ep.Owns() {
		t.Fatal("canceled request revoked live coordinator", e)
	}
	cfg := x.s.Pool.Config()
	cfg.MaxConns = 4
	control, e := pgxpool.NewWithConfig(ctx, cfg)
	if e != nil {
		t.Fatal(e)
	}
	defer control.Close()
	store := &db.Store{Pool: control, SourceID: x.s.SourceID, Lease: x.s.Lease, Heartbeat: x.s.Heartbeat}
	d := dispatch.New(store, ep, model.Limits{RPS: 200, Connections: 120})
	if e = d.Restore(ctx); e != nil {
		t.Fatal(e)
	}
	type request struct{ worker, uid string }
	requests := make([]request, 120)
	var wg sync.WaitGroup
	errs := make(chan error, 120)
	for i := range requests {
		w := i / 40
		uid := model.NewID()
		requests[i] = request{x.workers[w], uid}
		class := "check"
		if i%2 == 1 {
			class = "download"
		}
		p := model.PermitRequest{LeaseRef: leases[w].Ref(), RequestUID: uid, Class: class}
		wg.Go(func() {
			_, e := x.s.Mutate(ctx, x.workers[w], "permit", uid, db.JSON(p), func(tx pgx.Tx) (any, int, error) { return d.RequestTx(ctx, tx, x.workers[w], p) })
			errs <- e
		})
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("nested SQL acquisition deadlocked under 120 requests")
	}
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	go d.Run(ctx)
	first := requests[0]
	var permit *model.Permit
	for permit == nil && ctx.Err() == nil {
		v, _, e := d.Get(first.worker, first.uid)
		if e != nil {
			t.Fatal(e)
		}
		b, _ := json.Marshal(v)
		var out struct {
			Permit *model.Permit `json:"permit"`
		}
		json.Unmarshal(b, &out)
		permit = out.Permit
		if permit == nil {
			time.Sleep(5 * time.Millisecond)
		}
	}
	if permit == nil {
		t.Fatal("dispatcher did not issue permits")
	}
	fair := d.Stats()
	lo, hi := uint64(120), uint64(0)
	for _, worker := range x.workers {
		lo = min(lo, fair.IssuedByWorker[worker])
		hi = max(hi, fair.IssuedByWorker[worker])
	}
	checks, downloads := fair.IssuedByClass["check"], fair.IssuedByClass["download"]
	if hi-lo > 1 || max(checks, downloads)-min(checks, downloads) > 3 {
		t.Fatal("pending workers or request classes were starved", fair)
	}
	feedback := model.Feedback{Status: 429, RetryAfterMS: 60000}
	tx, e := x.s.Pool.Begin(ctx)
	if e != nil {
		t.Fatal(e)
	}
	if e = d.ReleaseTx(ctx, tx, first.worker, permit.ConnectionID, feedback); e != nil {
		t.Fatal(e)
	}
	tx.Rollback(ctx)
	mutate(t, x.s, first.worker, "release", feedback, func(tx pgx.Tx) (any, int, error) {
		return map[string]bool{"released": true}, 200, d.ReleaseTx(ctx, tx, first.worker, permit.ConnectionID, feedback)
	})
	d.FinalizeRelease(first.worker, permit.ConnectionID)
	state, e := store.QuotaState(ctx)
	if e != nil || state.Factor != .5 || time.Until(state.Cooldown) < 50*time.Second {
		t.Fatal("feedback did not survive rollback/retry", state, e)
	}
	restored := dispatch.New(store, ep, model.Limits{RPS: 200})
	if e = restored.Restore(ctx); e != nil {
		t.Fatal(e)
	}
	stats := restored.Stats()
	if stats.Factor != .5 || time.Until(stats.Cooldown) < 50*time.Second {
		t.Fatal("restart forgot Retry-After", stats)
	}
	cancel()
}
