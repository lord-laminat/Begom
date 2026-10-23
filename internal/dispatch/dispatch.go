// Package dispatch implements C6: demand queues, fairness, paced sliding windows
// and expiring slots. SQL remains authoritative for attempts and generations.
package dispatch

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/rychkin/Begom/internal/db"
	"github.com/rychkin/Begom/internal/model"
	"math"
	"strings"
	"sync"
	"time"
)

const PermitTTL = 500 * time.Millisecond
const ConnectionLease = 10 * time.Second
const ClockMargin = 100 * time.Millisecond
const DrainInterval = time.Second + PermitTTL + ConnectionLease + 2*ClockMargin

type entry struct {
	request model.PermitRequest
	worker  string
	state   string
	permit  *model.Permit
	at      time.Time
	job     model.JobConfig
	config  model.WorkerConfig
}
type slot struct {
	id, worker, job string
	ref             model.LeaseRef
	expires         time.Time
	class           string
	feedback        *model.Feedback
	cancelled       bool
}
type duration struct {
	sum   float64
	count uint64
}
type workerQueue struct {
	check, download []*entry
	nextClass       int
}
type budget struct {
	times []time.Time
	next  time.Time
}

// Charging W+TTL+2*skew bounds starts as well as issuance for compliant agents.
func (b *budget) Ready(now time.Time, rate float64) bool {
	if rate <= 0 {
		return true
	}
	window := time.Second + PermitTTL + 2*ClockMargin
	n := int(math.Floor(rate))
	if n < 1 {
		n = 1
		window = time.Duration(float64(window) / rate)
	}
	i := 0
	for i < len(b.times) && !b.times[i].After(now.Add(-window)) {
		i++
	}
	b.times = b.times[i:]
	return !now.Before(b.next) && len(b.times) < n
}
func (b *budget) Charge(now time.Time, rate float64) {
	if rate <= 0 {
		return
	}
	window := time.Second + PermitTTL + 2*ClockMargin
	n := math.Max(1, math.Floor(rate))
	if rate < 1 {
		window = time.Duration(float64(window) / rate)
	}
	b.times = append(b.times, now)
	b.next = now.Add(time.Duration(math.Ceil(float64(window) / n)))
}

type Stats struct {
	Pending        int
	Active         int
	Issued         uint64
	Requests       map[string]uint64
	Factor         float64
	Cooldown       time.Time
	WaitSeconds    float64
	IssuedByWorker map[string]uint64
	IssuedByClass  map[string]uint64
}
type Dispatcher struct {
	Store         *db.Store
	Epoch         *db.Epoch
	Limits        model.Limits
	StorageCheck  func() error
	storageNext   time.Time
	storageReady  bool
	mu            sync.Mutex
	queues        map[string]*workerQueue
	workers       []string
	cursor        int
	entries       map[string]*entry
	slots         map[string]*slot
	budgets       map[string]*budget
	factor        float64
	cooldown      time.Time
	issued        uint64
	requests      map[string]uint64
	waitSeconds   float64
	lastIncrease  time.Time
	errors        int
	horizon       time.Duration
	waitBuckets   [10]uint64
	durationSum   float64
	durationCount uint64
	bytes         uint64
	durations     map[string]duration
	issuedWorker  map[string]uint64
	issuedClass   map[string]uint64
}

func New(s *db.Store, epoch *db.Epoch, l model.Limits) *Dispatcher {
	return &Dispatcher{Store: s, Epoch: epoch, Limits: l, queues: map[string]*workerQueue{}, entries: map[string]*entry{}, slots: map[string]*slot{}, budgets: map[string]*budget{}, factor: 1, requests: map[string]uint64{}, durations: map[string]duration{}, issuedWorker: map[string]uint64{}, issuedClass: map[string]uint64{}, lastIncrease: time.Now()}
}
func (d *Dispatcher) Restore(ctx context.Context) error {
	q, e := d.Store.QuotaState(ctx)
	if e != nil {
		return e
	}
	d.factor = q.Factor
	d.cooldown = q.Cooldown
	d.lastIncrease = q.LastIncrease
	d.errors = q.Errors
	d.horizon = q.Horizon
	return nil
}
func quotaWindow(rate float64) time.Duration {
	w := time.Second + PermitTTL + 2*ClockMargin
	if rate > 0 && rate < 1 {
		return time.Duration(float64(w) / rate)
	}
	return w
}
func key(worker, uid string) string { return worker + ":" + uid }
func (d *Dispatcher) Request(ctx context.Context, worker string, r model.PermitRequest) (any, int, error) {
	return d.request(ctx, d.Store.Pool, worker, r)
}
func (d *Dispatcher) RequestTx(ctx context.Context, tx pgx.Tx, worker string, r model.PermitRequest) (any, int, error) {
	return d.request(ctx, tx, worker, r)
}
func (d *Dispatcher) request(ctx context.Context, q db.Queryer, worker string, r model.PermitRequest) (any, int, error) {
	if r.Class != "check" && r.Class != "download" || !model.ValidID(r.RequestUID) {
		return nil, 0, db.Bad("invalid_permit_request", "Invalid class or request UID")
	}
	if e := d.Epoch.CheckWith(ctx, q); e != nil {
		return nil, 0, e
	}
	job, e := d.Store.CheckLeaseWith(ctx, q, worker, r.LeaseRef)
	if e != nil {
		return nil, 0, e
	}
	w, e := d.Store.WorkerConfigWith(ctx, q, worker)
	if e != nil {
		return nil, 0, e
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	k := key(worker, r.RequestUID)
	if old := d.entries[k]; old != nil {
		if old.request != r {
			return nil, 0, db.Conflict("request_uid_conflict")
		}
		return d.reply(old)
	}
	queue := d.queues[worker]
	if queue == nil {
		queue = &workerQueue{}
		d.queues[worker] = queue
		d.workers = append(d.workers, worker)
	}
	if len(queue.check)+len(queue.download) >= w.Threads*2+2 {
		return nil, 0, &db.Error{Status: 429, Code: "permit_queue_full", Message: "Worker demand queue is full"}
	}
	x := &entry{request: r, worker: worker, state: "pending", at: time.Now(), job: job, config: w}
	d.entries[k] = x
	if r.Class == "check" {
		queue.check = append(queue.check, x)
	} else {
		queue.download = append(queue.download, x)
	}
	return d.reply(x)
}
func (d *Dispatcher) reply(e *entry) (any, int, error) {
	if e.state == "issued" && e.permit != nil {
		if time.Now().After(e.permit.ExpiresAt) {
			return map[string]any{"state": "expired"}, 200, nil
		}
		return map[string]any{"state": "issued", "permit": e.permit}, 200, nil
	}
	status := 200
	if e.state == "pending" {
		status = 202
	}
	return map[string]any{"state": e.state, "retry_after_ms": 20}, status, nil
}
func (d *Dispatcher) Get(worker, uid string) (any, int, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	e := d.entries[key(worker, uid)]
	if e == nil {
		return nil, 0, db.Forbidden()
	}
	return d.reply(e)
}
func (d *Dispatcher) Cancel(worker, uid string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if e := d.entries[key(worker, uid)]; e != nil && e.state == "pending" {
		e.state = "cancelled"
	}
}
func (d *Dispatcher) Renew(ctx context.Context, worker, id string) (any, int, error) {
	return d.renew(ctx, d.Store.Pool, worker, id)
}
func (d *Dispatcher) RenewTx(ctx context.Context, tx pgx.Tx, worker, id string) (any, int, error) {
	return d.renew(ctx, tx, worker, id)
}
func (d *Dispatcher) renew(ctx context.Context, q db.Queryer, worker, id string) (any, int, error) {
	d.mu.Lock()
	s := d.slots[id]
	if s == nil || s.worker != worker {
		d.mu.Unlock()
		return nil, 0, db.Conflict("connection_expired")
	}
	ref := s.ref
	d.mu.Unlock()
	if e := d.Epoch.CheckWith(ctx, q); e != nil {
		return nil, 0, e
	}
	if _, e := d.Store.CheckLeaseWith(ctx, q, worker, ref); e != nil {
		return nil, 0, e
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	s = d.slots[id]
	if s == nil || s.worker != worker || !s.expires.After(time.Now()) {
		return nil, 0, db.Conflict("connection_expired")
	}
	s.expires = time.Now().Add(ConnectionLease)
	return map[string]any{"connection_expires_at": s.expires, "server_time": time.Now().UTC(), "epoch": d.Epoch.ID}, 200, nil
}
func (d *Dispatcher) Release(ctx context.Context, worker, id string, f model.Feedback) error {
	tx, e := d.Store.Pool.Begin(ctx)
	if e != nil {
		return e
	}
	defer tx.Rollback(ctx)
	if e = d.release(ctx, tx, worker, id, f); e != nil {
		return e
	}
	if e = tx.Commit(ctx); e == nil {
		d.FinalizeRelease(worker, id)
	}
	return e
}
func (d *Dispatcher) ReleaseTx(ctx context.Context, tx pgx.Tx, worker, id string, f model.Feedback) error {
	return d.release(ctx, tx, worker, id, f)
}
func (d *Dispatcher) release(ctx context.Context, q db.Queryer, worker, id string, f model.Feedback) error {
	if f.Status < 0 || f.Status > 599 || f.RetryAfterMS < 0 || f.RetryAfterMS > (time.Duration(1<<63-1)).Milliseconds() || f.DurationMS < 0 || f.Bytes > 1<<60 {
		return db.Bad("invalid_feedback", "Invalid HTTP feedback")
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	s := d.slots[id]
	if s == nil {
		return nil
	}
	if s.worker != worker {
		return db.Forbidden()
	}
	// An interrupted request from a stopped job, expired attempt or disabled
	// worker does not describe donor health. Keep journaling the release while
	// retaining the backoff caused by genuine donor failures.
	cancelled := false
	if f.Status == 0 {
		if _, err := d.Store.CheckLeaseWith(ctx, q, worker, s.ref); err != nil {
			var de *db.Error
			if !errors.As(err, &de) || de.Code != "stale_attempt" {
				return err
			}
			cancelled = true
		}
	}
	digest := fmt.Sprintf("%x", sha256.Sum256(db.JSON(f)))
	var inserted string
	err := q.QueryRow(ctx, "INSERT INTO donor_feedback(connection_id,source_id,worker_id,body_hash) VALUES($1,$2,$3,$4) ON CONFLICT DO NOTHING RETURNING connection_id", id, d.Store.SourceID, worker, digest).Scan(&inserted)
	if errors.Is(err, pgx.ErrNoRows) {
		var owner, hash string
		if e := q.QueryRow(ctx, "SELECT worker_id,body_hash FROM donor_feedback WHERE connection_id=$1", id).Scan(&owner, &hash); e != nil {
			return e
		}
		if owner != worker || hash != digest {
			return db.Conflict("feedback_conflict")
		}
		return nil
	}
	if err != nil {
		return err
	}
	if err = q.QueryRow(ctx, "SELECT factor,GREATEST(cooldown,'2000-01-01'::timestamptz),last_increase,error_count FROM donor_quota_state WHERE source_id=$1 FOR UPDATE", d.Store.SourceID).Scan(&d.factor, &d.cooldown, &d.lastIncrease, &d.errors); err != nil {
		return err
	}
	// Commit adaptation before acknowledging feedback or freeing the slot.
	previousFactor, previousCooldown, previousIncrease, previousErrors := d.factor, d.cooldown, d.lastIncrease, d.errors
	if f.Status == 429 {
		delay := time.Duration(f.RetryAfterMS) * time.Millisecond
		if delay < time.Second {
			delay = time.Second
		}
		until := time.Now().Add(delay)
		if until.After(d.cooldown) {
			d.cooldown = until
		}
		d.factor = math.Max(.1, d.factor*.5)
		d.lastIncrease = time.Now()
	} else if f.Status >= 500 || f.Status == 0 && !cancelled {
		d.errors++
		if d.errors >= 3 {
			d.factor = math.Max(.1, d.factor*.7)
			d.errors = 0
			d.lastIncrease = time.Now()
		}
	}
	horizon := max(d.horizon, quotaWindow(d.Limits.RPS*d.factor))
	_, err = q.Exec(ctx, "UPDATE donor_quota_state SET factor=$2,cooldown=$3,last_increase=$4,error_count=$5,horizon_ms=GREATEST(horizon_ms,$6) WHERE source_id=$1", d.Store.SourceID, d.factor, d.cooldown, d.lastIncrease, d.errors, horizon.Milliseconds()+1)
	if err != nil {
		d.factor = previousFactor
		d.cooldown = previousCooldown
		d.lastIncrease = previousIncrease
		d.errors = previousErrors
		return err
	}
	d.horizon = horizon
	s.feedback = &f
	s.cancelled = cancelled
	return nil
}
func (d *Dispatcher) FinalizeRelease(worker, id string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if s := d.slots[id]; s != nil && s.worker == worker {
		if f := s.feedback; f != nil {
			class := "network"
			switch {
			case s.cancelled:
				class = "cancelled"
			case f.Status == 429:
				class = "429"
			case f.Status >= 500:
				class = "5xx"
			case f.Status >= 400:
				class = "4xx"
			case f.Status >= 300:
				class = "3xx"
			case f.Status >= 200:
				class = "2xx"
			}
			d.requests[s.class+":"+class]++
			seconds := float64(max(int64(0), f.DurationMS)) / 1000
			d.durationSum += seconds
			d.durationCount++
			d.bytes += f.Bytes
			dur := d.durations[s.class]
			dur.sum += seconds
			dur.count++
			d.durations[s.class] = dur
		}
		delete(d.slots, id)
	}
}
func minRate(vs ...float64) float64 {
	r := float64(0)
	for _, v := range vs {
		if v > 0 && (r == 0 || v < r) {
			r = v
		}
	}
	return r
}
func minInt(vs ...int) int {
	r := int(0)
	for _, v := range vs {
		if v > 0 && (r == 0 || v < r) {
			r = v
		}
	}
	return r
}
func (d *Dispatcher) dispatch(ctx context.Context) {
	d.mu.Lock()
	defer d.mu.Unlock()
	now := time.Now()
	for id, s := range d.slots {
		if !s.expires.After(now) {
			delete(d.slots, id)
		}
	}
	for k, e := range d.entries {
		if e.state != "pending" && now.Sub(e.at) > 2*time.Minute {
			delete(d.entries, k)
		}
	}
	if now.Sub(d.lastIncrease) > 30*time.Second && d.factor < 1 && now.After(d.cooldown) {
		factor := math.Min(1, d.factor+.1)
		if _, e := d.Store.Pool.Exec(ctx, "UPDATE donor_quota_state SET factor=$2,last_increase=$3 WHERE source_id=$1", d.Store.SourceID, factor, now); e != nil {
			return
		}
		d.factor = factor
		d.lastIncrease = now
	}
	if !d.Epoch.Ready() || now.Before(d.cooldown) || len(d.workers) == 0 {
		return
	}
	if d.StorageCheck != nil {
		if now.After(d.storageNext) {
			d.storageReady = d.StorageCheck() == nil
			d.storageNext = now.Add(200 * time.Millisecond)
		}
		if !d.storageReady {
			return
		}
	}
	if b := d.budgets["global"]; b != nil && !b.Ready(now, d.Limits.RPS*d.factor) {
		return
	}
	for n := 0; n < len(d.workers); n++ {
		d.cursor %= len(d.workers)
		worker := d.workers[d.cursor]
		d.cursor++
		q := d.queues[worker]
		for c := 0; c < 2; c++ {
			classIndex := (q.nextClass + c) % 2
			list := &q.check
			if classIndex == 1 {
				list = &q.download
			}
			for len(*list) > 0 && (*list)[0].state != "pending" {
				*list = (*list)[1:]
			}
			if len(*list) == 0 {
				continue
			}
			e := (*list)[0]
			qctx, stop := context.WithTimeout(ctx, 500*time.Millisecond)
			latest, checkErr := d.Store.CheckLease(qctx, worker, e.request.LeaseRef)
			var current model.WorkerConfig
			if checkErr == nil {
				current, checkErr = d.Store.WorkerConfigWith(qctx, d.Store.Pool, worker)
			}
			stop()
			if checkErr != nil {
				var de *db.Error
				if errors.As(checkErr, &de) && de.Status == 409 {
					e.state = "cancelled"
				}
				return
			}
			e.job = latest
			e.config = current
			workerSlots, jobSlots := 0, 0
			for _, s := range d.slots {
				if s.worker == worker {
					workerSlots++
				}
				if s.job == e.request.JobID {
					jobSlots++
				}
			}
			if cap := d.Limits.Connections; cap > 0 && len(d.slots) >= cap {
				continue
			}
			if cap := minInt(d.Limits.WorkerConnections, e.config.Connections, e.job.Limits.WorkerConnections, e.job.ThreadsFor(worker, e.config.Threads)); cap > 0 && workerSlots >= cap {
				continue
			}
			if e.job.Limits.Connections > 0 && jobSlots >= e.job.Limits.Connections {
				continue
			}
			classRate := d.Limits.CheckRPS
			jobClass := e.job.Limits.CheckRPS
			if classIndex == 1 {
				classRate = d.Limits.DownloadRPS
				jobClass = e.job.Limits.DownloadRPS
			}
			rates := map[string]float64{"global": d.Limits.RPS * d.factor, "worker:" + worker: minRate(d.Limits.WorkerRPS, e.config.RPS, e.job.Limits.WorkerRPS), "class:" + e.request.Class: classRate, "job:" + e.request.JobID: e.job.Limits.RPS, "jobclass:" + e.request.JobID + ":" + e.request.Class: jobClass}
			horizon := d.horizon
			for _, rate := range rates {
				horizon = max(horizon, quotaWindow(rate))
			}
			if horizon > d.horizon {
				if _, err := d.Store.Pool.Exec(ctx, "UPDATE donor_quota_state SET horizon_ms=GREATEST(horizon_ms,$2) WHERE source_id=$1", d.Store.SourceID, horizon.Milliseconds()+1); err != nil {
					return
				}
				d.horizon = horizon
			}
			allowed := true
			for k, rate := range rates {
				b := d.budgets[k]
				if b == nil {
					b = &budget{}
					d.budgets[k] = b
				}
				if !b.Ready(now, rate) {
					allowed = false
					break
				}
			}
			if !allowed {
				continue
			}
			qctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
			err := d.Epoch.CheckWith(qctx, d.Store.Pool)
			if err == nil {
				_, err = d.Store.CheckLease(qctx, worker, e.request.LeaseRef)
			}
			cancel()
			if err != nil {
				var de *db.Error
				if errors.As(err, &de) && de.Status == 409 {
					e.state = "cancelled"
				}
				return
			}
			now = time.Now()
			for k, rate := range rates {
				d.budgets[k].Charge(now, rate)
			}
			connectionID := model.NewID()
			p := &model.Permit{ID: model.NewID(), Epoch: d.Epoch.ID, RequestUID: e.request.RequestUID, NotBefore: now.UTC(), ExpiresAt: now.Add(PermitTTL).UTC(), ConnectionID: connectionID, ConnectionExpiresAt: now.Add(ConnectionLease).UTC(), ServerTime: now.UTC()}
			d.slots[connectionID] = &slot{id: connectionID, worker: worker, job: e.request.JobID, ref: e.request.LeaseRef, expires: p.ConnectionExpiresAt, class: e.request.Class}
			e.permit = p
			e.state = "issued"
			*list = (*list)[1:]
			q.nextClass = 1 - classIndex
			d.issued++
			d.issuedWorker[worker]++
			d.issuedClass[e.request.Class]++
			d.waitSeconds += now.Sub(e.at).Seconds()
			for i, bound := range []float64{.01, .05, .1, .25, .5, 1, 2, 5, 10, 30} {
				if now.Sub(e.at).Seconds() <= bound {
					d.waitBuckets[i]++
				}
			}
			return
		}
	}
}
func (d *Dispatcher) Run(ctx context.Context) {
	t := time.NewTicker(2 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			d.dispatch(ctx)
		}
	}
}
func (d *Dispatcher) Stats() Stats {
	d.mu.Lock()
	defer d.mu.Unlock()
	s := Stats{Active: len(d.slots), Issued: d.issued, Requests: map[string]uint64{}, Factor: d.factor, Cooldown: d.cooldown, WaitSeconds: d.waitSeconds, IssuedByWorker: map[string]uint64{}, IssuedByClass: map[string]uint64{}}
	for k, v := range d.issuedWorker {
		s.IssuedByWorker[k] = v
	}
	for k, v := range d.issuedClass {
		s.IssuedByClass[k] = v
	}
	for _, e := range d.entries {
		if e.state == "pending" {
			s.Pending++
		}
	}
	for k, v := range d.requests {
		s.Requests[k] = v
	}
	return s
}
func (d *Dispatcher) Prometheus() string {
	s := d.Stats()
	out := fmt.Sprintf("begom_active_connections %d\nbegom_permits_pending %d\nbegom_permits_issued_total %d\nbegom_permit_wait_seconds_sum %g\nbegom_donor_rate_factor %g\n", s.Active, s.Pending, s.Issued, s.WaitSeconds, s.Factor)
	for key, n := range s.Requests {
		parts := strings.SplitN(key, ":", 2)
		out += fmt.Sprintf("begom_http_requests_total{request_class=%q,status_class=%q} %d\n", parts[0], parts[1], n)
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	for class, duration := range d.durations {
		out += fmt.Sprintf("begom_donor_request_duration_seconds_sum{request_class=%q} %g\nbegom_donor_request_duration_seconds_count{request_class=%q} %d\n", class, duration.sum, class, duration.count)
	}
	for i, bound := range []float64{.01, .05, .1, .25, .5, 1, 2, 5, 10, 30} {
		out += fmt.Sprintf("begom_permit_wait_seconds_bucket{le=%q} %d\n", fmt.Sprint(bound), d.waitBuckets[i])
	}
	out += fmt.Sprintf("begom_permit_wait_seconds_bucket{le=\"+Inf\"} %d\nbegom_permit_wait_seconds_count %d\nbegom_http_duration_seconds_sum %g\nbegom_http_duration_seconds_count %d\nbegom_http_bytes_total %d\n", d.issued, d.issued, d.durationSum, d.durationCount, d.bytes)
	return out
}
