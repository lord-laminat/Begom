package db

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/jackc/pgx/v5"
	"github.com/rychkin/Begom/internal/model"
	"time"
)

func (s *Store) Register(ctx context.Context, tx pgx.Tx, worker string, r model.Register) (any, int, error) {
	if r.SourceID != "" && r.SourceID != s.SourceID {
		return nil, 0, Conflict("source_profile_mismatch")
	}
	var expected []byte
	if e := tx.QueryRow(ctx, "SELECT config FROM workers WHERE id=$1", worker).Scan(&expected); e != nil {
		return nil, 0, e
	}
	var configured model.WorkerConfig
	if e := json.Unmarshal(expected, &configured); e != nil {
		return nil, 0, e
	}
	if configured.StoragePath != "" && configured.StoragePath != r.StorageRoot {
		return nil, 0, Conflict("storage_path_mismatch")
	}
	if r.WorkerID != worker || r.APIVersion != model.APIVersion || r.Version != model.Version || !model.ValidID(r.SessionID) {
		return nil, 0, Conflict("incompatible_agent")
	}
	var old string
	var recent bool
	e := tx.QueryRow(ctx, "SELECT session_id,COALESCE(last_heartbeat>clock_timestamp()-$2::interval,false) FROM workers WHERE id=$1 FOR UPDATE", worker, (2*s.Heartbeat).String()).Scan(&old, &recent)
	if errors.Is(e, pgx.ErrNoRows) {
		return nil, 0, Forbidden()
	}
	if e != nil {
		return nil, 0, e
	}
	if recent && old != "" && old != r.SessionID {
		return nil, 0, Conflict("agent_already_active")
	}
	_, e = tx.Exec(ctx, "UPDATE workers SET session_id=$2,version=$3,last_heartbeat=clock_timestamp() WHERE id=$1", worker, r.SessionID, r.Version)
	if e != nil {
		return nil, 0, e
	}
	return map[string]any{"worker_id": worker, "api_version": model.APIVersion, "server_time": time.Now().UTC(), "heartbeat_interval_ms": s.Heartbeat.Milliseconds()}, 200, Audit(ctx, tx, worker, "worker.register", worker, "", nil)
}
func (s *Store) HeartbeatTx(ctx context.Context, tx pgx.Tx, worker string, h model.Heartbeat) (any, int, error) {
	if h.Version != model.Version || len(h.Leases) > 32 || len(h.Downloads) > 1024 {
		return nil, 0, Bad("invalid_heartbeat", "Invalid agent heartbeat")
	}
	var cfg []byte
	tag, e := tx.Exec(ctx, "UPDATE workers SET last_heartbeat=clock_timestamp(),active_downloads=$3 WHERE id=$1 AND session_id=$2", worker, h.SessionID, JSON(h.Downloads))
	if e != nil {
		return nil, 0, e
	}
	if tag.RowsAffected() != 1 {
		return nil, 0, Conflict("agent_session_lost")
	}
	if e = tx.QueryRow(ctx, "SELECT config FROM workers WHERE id=$1", worker).Scan(&cfg); e != nil {
		return nil, 0, e
	}
	var w model.WorkerConfig
	if e = json.Unmarshal(cfg, &w); e != nil {
		return nil, 0, e
	}
	if e = tx.QueryRow(ctx, "SELECT enabled FROM workers WHERE id=$1", worker).Scan(&w.Enabled); e != nil {
		return nil, 0, e
	}
	if h.Metrics.MeasuredAt.IsZero() || time.Since(h.Metrics.MeasuredAt) > time.Minute || time.Until(h.Metrics.MeasuredAt) > time.Second {
		return nil, 0, Bad("invalid_metric_time", "Metric timestamp outside accepted interval")
	}
	if _, e = tx.Exec(ctx, "INSERT INTO worker_metrics(worker_id,measured_at,data) VALUES($1,$2,$3) ON CONFLICT DO NOTHING", worker, h.Metrics.MeasuredAt, JSON(h.Metrics)); e != nil {
		return nil, 0, e
	}
	reply := model.HeartbeatReply{ServerTime: time.Now().UTC(), Worker: w, Leases: []model.Renewal{}}
	for _, ref := range h.Leases {
		var r model.Renewal
		r.ChunkID = ref.ChunkID
		var b []byte
		var expires *time.Time
		e = tx.QueryRow(ctx, "SELECT j.state,j.config,j.config_version,c.lease_expires_at FROM range_chunks c JOIN jobs j ON j.id=c.job_id WHERE c.id=$1 AND c.job_id=$2", ref.ChunkID, ref.JobID).Scan(&r.State, &b, &r.ConfigVersion, &expires)
		if errors.Is(e, pgx.ErrNoRows) {
			reply.Leases = append(reply.Leases, r)
			continue
		}
		if e != nil {
			return nil, 0, e
		}
		if e = json.Unmarshal(b, &r.Config); e != nil {
			return nil, 0, e
		}
		if w.Enabled {
			e = tx.QueryRow(ctx, "UPDATE range_chunks SET lease_expires_at=clock_timestamp()+$6::interval,updated_at=clock_timestamp() WHERE id=$1 AND job_id=$2 AND run_id=$3 AND attempt_id=$4 AND leased_by=$5 AND status='leased' AND lease_expires_at>clock_timestamp() AND EXISTS(SELECT 1 FROM jobs j WHERE j.id=$2 AND j.run_id=$3 AND j.state IN('running','pausing')) RETURNING lease_expires_at", ref.ChunkID, ref.JobID, ref.RunID, ref.AttemptID, worker, s.Lease.String()).Scan(&r.ExpiresAt)
			if e == nil {
				r.Valid = true
				_, e = tx.Exec(ctx, "UPDATE storage_reservations SET expires_at=$2 WHERE attempt_id=$1", ref.AttemptID, r.ExpiresAt)
			}
			if e != nil && !errors.Is(e, pgx.ErrNoRows) {
				return nil, 0, e
			}
		}
		reply.Leases = append(reply.Leases, r)
	}
	return reply, 200, nil
}
func (s *Store) GetWorker(ctx context.Context, id string) (model.Worker, error) {
	var w model.Worker
	var b []byte
	var active []byte
	e := s.Pool.QueryRow(ctx, "SELECT config,enabled,version,last_heartbeat,active_downloads FROM workers WHERE id=$1", id).Scan(&b, &w.Enabled, &w.Version, &w.LastHeartbeat, &active)
	if e != nil {
		return w, e
	}
	enabled := w.Enabled
	if e = json.Unmarshal(b, &w.WorkerConfig); e != nil {
		return w, e
	}
	w.Enabled = enabled
	if e = json.Unmarshal(active, &w.Downloads); e != nil {
		return w, e
	}
	w.Online = w.LastHeartbeat != nil && time.Since(*w.LastHeartbeat) < 2*s.Heartbeat
	e = s.Pool.QueryRow(ctx, "SELECT data FROM worker_metrics WHERE worker_id=$1 ORDER BY measured_at DESC LIMIT 1", id).Scan(&b)
	if e == nil {
		if e = json.Unmarshal(b, &w.Metrics); e != nil {
			return w, e
		}
	} else if !errors.Is(e, pgx.ErrNoRows) {
		return w, e
	}
	rows, e := s.Pool.Query(ctx, "SELECT id,job_id,run_id,attempt_id,range_start::text,range_end::text,next_id::text,lease_expires_at FROM range_chunks WHERE leased_by=$1 AND status='leased'", id)
	if e != nil {
		return w, e
	}
	defer rows.Close()
	w.Leases = []model.Lease{}
	for rows.Next() {
		var l model.Lease
		var start, end string
		var next *string
		if e = rows.Scan(&l.ChunkID, &l.JobID, &l.RunID, &l.AttemptID, &start, &end, &next, &l.ExpiresAt); e != nil {
			return w, e
		}
		l.From = Number(start)
		l.To = Number(end)
		if next != nil {
			n := Number(*next)
			l.NextID = &n
		}
		w.Leases = append(w.Leases, l)
	}
	return w, rows.Err()
}
func (s *Store) Workers(ctx context.Context) ([]model.Worker, error) {
	rows, e := s.Pool.Query(ctx, "SELECT id FROM workers ORDER BY id")
	if e != nil {
		return nil, e
	}
	var ids []string
	for rows.Next() {
		var id string
		if e = rows.Scan(&id); e != nil {
			rows.Close()
			return nil, e
		}
		ids = append(ids, id)
	}
	rows.Close()
	if e = rows.Err(); e != nil {
		return nil, e
	}
	out := []model.Worker{}
	for _, id := range ids {
		w, e := s.GetWorker(ctx, id)
		if e != nil {
			return nil, e
		}
		out = append(out, w)
	}
	return out, nil
}
func (s *Store) LeaseTx(ctx context.Context, tx pgx.Tx, worker string) (any, int, error) {
	return s.LeaseResourceTx(ctx, tx, worker, ^uint64(0))
}
func (s *Store) LeaseResourceTx(ctx context.Context, tx pgx.Tx, worker string, actualFree uint64) (any, int, error) {
	var cfg, metrics []byte
	var enabled, recent bool
	var version string
	e := tx.QueryRow(ctx, "SELECT w.config,w.enabled,COALESCE(w.last_heartbeat>clock_timestamp()-$2::interval,false),w.version,COALESCE((SELECT data FROM worker_metrics WHERE worker_id=w.id ORDER BY measured_at DESC LIMIT 1),'{}') FROM workers w WHERE w.id=$1 FOR UPDATE", worker, (2*s.Heartbeat).String()).Scan(&cfg, &enabled, &recent, &version, &metrics)
	if e != nil {
		return nil, 0, e
	}
	var w model.WorkerConfig
	var m model.Metrics
	if e = json.Unmarshal(cfg, &w); e != nil {
		return nil, 0, e
	}
	if e = json.Unmarshal(metrics, &m); e != nil {
		return nil, 0, e
	}
	empty := map[string]any{"lease": nil, "retry_after_ms": 250}
	m.DiskFree = min(m.DiskFree, actualFree)
	if !enabled || !recent || version != model.Version || !m.StorageHealthy || m.DiskFree < w.MinFreeBytes || m.CPU > 85 || m.RAMTotal > 0 && float64(m.RAMUsed)/float64(m.RAMTotal) > .9 {
		return empty, 200, nil
	}
	if m.RecentErrorsPerSecond >= 10 || w.MaxNetworkBPS > 0 && m.NetworkBPS >= float64(w.MaxNetworkBPS) || w.MaxDiskBPS > 0 && m.DiskBPS >= float64(w.MaxDiskBPS) {
		return empty, 200, nil
	}
	var occupied bool
	if e = tx.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM range_chunks WHERE leased_by=$1 AND status='leased' AND lease_expires_at>clock_timestamp())", worker).Scan(&occupied); e != nil {
		return nil, 0, e
	}
	if occupied {
		return empty, 200, nil
	}
	var job, run string
	var frontier, end *string
	var config []byte
	var cv int64
	e = tx.QueryRow(ctx, "SELECT j.id,j.run_id,j.frontier::text,j.range_end::text,j.config,j.config_version FROM jobs j JOIN job_workers jw ON jw.job_id=j.id WHERE jw.worker_id=$1 AND j.state='running' AND (j.frontier IS NOT NULL OR EXISTS(SELECT 1 FROM range_chunks c WHERE c.job_id=j.id AND c.status='pending')) ORDER BY j.updated_at,j.created_at FOR UPDATE OF j SKIP LOCKED LIMIT 1", worker).Scan(&job, &run, &frontier, &end, &config, &cv)
	if errors.Is(e, pgx.ErrNoRows) {
		return empty, 200, nil
	}
	if e != nil {
		return nil, 0, e
	}
	var jc model.JobConfig
	if e = json.Unmarshal(config, &jc); e != nil {
		return nil, 0, e
	}
	if m.DiskFree < jc.MinFreeBytes {
		return empty, 200, nil
	}
	var reserved string
	if e = tx.QueryRow(ctx, "SELECT COALESCE(sum(GREATEST(r.bytes-d.offset_bytes,0)),0)::text FROM storage_reservations r JOIN downloads d ON d.id=r.download_id WHERE r.expires_at>clock_timestamp()").Scan(&reserved); e != nil {
		return nil, 0, e
	}
	if uint64(Number(reserved)) >= m.DiskFree-max(w.MinFreeBytes, jc.MinFreeBytes) {
		return empty, 200, nil
	}
	var l model.Lease
	var start, stop string
	var next *string
	l.JobID = job
	l.RunID = run
	l.Config = jc
	l.ConfigVersion = cv
	l.HeartbeatMS = int(s.Heartbeat.Milliseconds())
	e = tx.QueryRow(ctx, "SELECT id,range_start::text,range_end::text,next_id::text FROM range_chunks WHERE job_id=$1 AND status='pending' ORDER BY range_start FOR UPDATE SKIP LOCKED LIMIT 1", job).Scan(&l.ChunkID, &start, &stop, &next)
	if errors.Is(e, pgx.ErrNoRows) {
		if frontier == nil {
			return empty, 200, nil
		}
		a := Number(*frontier)
		var to *model.Number
		if end != nil {
			n := Number(*end)
			to = &n
		}
		b := model.BlockEnd(a, jc.BlockSize, to)
		l.ChunkID = model.NewID()
		start, stop = a.String(), b.String()
		n := start
		next = &n
		if _, e = tx.Exec(ctx, "INSERT INTO range_chunks(id,job_id,range_start,range_end,next_id,status) VALUES($1,$2,$3,$4,$3,'pending')", l.ChunkID, job, start, stop); e != nil {
			return nil, 0, e
		}
		var following any
		if b != model.Number(^uint64(0)) && (to == nil || b < *to) {
			following = (b + 1).String()
		}
		if _, e = tx.Exec(ctx, "UPDATE jobs SET frontier=$2 WHERE id=$1", job, following); e != nil {
			return nil, 0, e
		}
	} else if e != nil {
		return nil, 0, e
	}
	l.From = Number(start)
	l.To = Number(stop)
	if next != nil {
		n := Number(*next)
		l.NextID = &n
	}
	l.AttemptID = model.NewID()
	e = tx.QueryRow(ctx, "UPDATE range_chunks SET status='leased',leased_by=$2,attempt_id=$3,run_id=$4,attempt=attempt+1,lease_expires_at=clock_timestamp()+$5::interval,updated_at=clock_timestamp() WHERE id=$1 RETURNING lease_expires_at,clock_timestamp()", l.ChunkID, worker, l.AttemptID, run, s.Lease.String()).Scan(&l.ExpiresAt, &l.ServerTime)
	if e != nil {
		return nil, 0, e
	}
	rows, e := tx.Query(ctx, "SELECT external_id::text FROM items WHERE job_id=$1 AND external_id BETWEEN $2 AND $3 AND final ORDER BY external_id", job, start, stop)
	if e != nil {
		return nil, 0, e
	}
	for rows.Next() {
		var n string
		if e = rows.Scan(&n); e != nil {
			rows.Close()
			return nil, 0, e
		}
		l.Confirmed = append(l.Confirmed, Number(n))
	}
	rows.Close()
	if e = rows.Err(); e != nil {
		return nil, 0, e
	}
	if _, e = tx.Exec(ctx, "UPDATE jobs SET updated_at=clock_timestamp() WHERE id=$1", job); e != nil {
		return nil, 0, e
	}
	if e = Event(ctx, tx, job, worker, l.ChunkID, l.AttemptID, "lease.granted", "leased", "", nil); e != nil {
		return nil, 0, e
	}
	e = Audit(ctx, tx, worker, "lease.granted", l.ChunkID, "", map[string]string{"attempt_id": l.AttemptID})
	return map[string]any{"lease": l}, 200, e
}
func (s *Store) CheckLease(ctx context.Context, worker string, r model.LeaseRef) (model.JobConfig, error) {
	return s.CheckLeaseWith(ctx, s.Pool, worker, r)
}
func (s *Store) CheckLeaseWith(ctx context.Context, q Queryer, worker string, r model.LeaseRef) (model.JobConfig, error) {
	var b []byte
	e := q.QueryRow(ctx, "SELECT j.config FROM jobs j JOIN range_chunks c ON c.job_id=j.id JOIN workers w ON w.id=c.leased_by WHERE j.id=$1 AND j.run_id=$2 AND c.id=$3 AND c.attempt_id=$4 AND c.leased_by=$5 AND c.status='leased' AND c.lease_expires_at>clock_timestamp() AND j.state IN('running','pausing') AND w.enabled", r.JobID, r.RunID, r.ChunkID, r.AttemptID, worker).Scan(&b)
	if errors.Is(e, pgx.ErrNoRows) {
		return model.JobConfig{}, Conflict("stale_attempt")
	}
	var c model.JobConfig
	if e == nil {
		e = json.Unmarshal(b, &c)
	}
	return c, e
}
func (s *Store) WorkerConfigWith(ctx context.Context, q Queryer, id string) (w model.WorkerConfig, e error) {
	var b []byte
	var enabled bool
	e = q.QueryRow(ctx, "SELECT config,enabled FROM workers WHERE id=$1", id).Scan(&b, &enabled)
	if e == nil {
		e = json.Unmarshal(b, &w)
	}
	w.Enabled = enabled
	return
}
func (s *Store) ValidateLeaseTx(ctx context.Context, tx pgx.Tx, worker string, r model.LeaseRef) (model.Number, model.Number, error) {
	var start, end string
	e := tx.QueryRow(ctx, "SELECT c.range_start::text,c.range_end::text FROM jobs j JOIN range_chunks c ON c.job_id=j.id JOIN workers w ON w.id=c.leased_by WHERE j.id=$1 AND j.run_id=$2 AND c.id=$3 AND c.attempt_id=$4 AND c.leased_by=$5 AND c.status='leased' AND c.lease_expires_at>clock_timestamp() AND j.state IN('running','pausing') AND w.enabled FOR UPDATE OF j,c", r.JobID, r.RunID, r.ChunkID, r.AttemptID, worker).Scan(&start, &end)
	if errors.Is(e, pgx.ErrNoRows) {
		return 0, 0, Conflict("stale_attempt")
	}
	return Number(start), Number(end), e
}
func (s *Store) Advance(ctx context.Context, tx pgx.Tx, job, chunk string) error {
	var next *string
	var end string
	if e := tx.QueryRow(ctx, "SELECT next_id::text,range_end::text FROM range_chunks WHERE id=$1 FOR UPDATE", chunk).Scan(&next, &end); e != nil {
		return e
	}
	if next == nil {
		return nil
	}
	n, last := Number(*next), Number(end)
	rows, e := tx.Query(ctx, "SELECT external_id::text FROM items WHERE job_id=$1 AND external_id BETWEEN $2 AND $3 AND final ORDER BY items.external_id", job, *next, end)
	if e != nil {
		return e
	}
	done := false
	for rows.Next() {
		var id string
		if e = rows.Scan(&id); e != nil {
			rows.Close()
			return e
		}
		x := Number(id)
		if x != n {
			break
		}
		if n == last {
			done = true
			break
		}
		n++
	}
	rows.Close()
	if e = rows.Err(); e != nil {
		return e
	}
	var value any = n.String()
	if done {
		value = nil
	}
	_, e = tx.Exec(ctx, "UPDATE range_chunks SET next_id=$2,updated_at=clock_timestamp() WHERE id=$1", chunk, value)
	return e
}
func (s *Store) CompleteTx(ctx context.Context, tx pgx.Tx, worker string, r model.LeaseRef, failed bool) (any, int, error) {
	if _, _, e := s.ValidateLeaseTx(ctx, tx, worker, r); e != nil {
		return nil, 0, e
	}
	if e := s.Advance(ctx, tx, r.JobID, r.ChunkID); e != nil {
		return nil, 0, e
	}
	var next *string
	if e := tx.QueryRow(ctx, "SELECT next_id::text FROM range_chunks WHERE id=$1", r.ChunkID).Scan(&next); e != nil {
		return nil, 0, e
	}
	state := "completed"
	if next != nil {
		if !failed {
			return nil, 0, Conflict("incomplete_chunk")
		}
		state = "pending"
	}
	if _, e := tx.Exec(ctx, "UPDATE range_chunks SET status=$2,leased_by=NULL,lease_expires_at=NULL WHERE id=$1", r.ChunkID, state); e != nil {
		return nil, 0, e
	}
	e := Event(ctx, tx, r.JobID, worker, r.ChunkID, r.AttemptID, "chunk."+state, state, "", nil)
	return map[string]any{"state": state, "next_id": next}, 200, e
}
