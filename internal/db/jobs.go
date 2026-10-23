package db

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/jackc/pgx/v5"
	"github.com/rychkin/Begom/internal/model"
	"math"
	"time"
)

func (s *Store) CreateJob(ctx context.Context, tx pgx.Tx, actor, request string, c model.JobConfig) (any, int, error) {
	r, e := c.Normalize()
	if e != nil {
		return nil, 0, Bad("invalid_job", e.Error())
	}
	if e = s.RecordQuotaHorizon(ctx, tx, c.Limits); e != nil {
		return nil, 0, e
	}
	id, run := model.NewID(), model.NewID()
	var to any
	if r.To != nil {
		to = r.To.String()
	}
	for _, w := range c.WorkerIDs {
		var ok bool
		if e = tx.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM workers WHERE id=$1)", w).Scan(&ok); e != nil {
			return nil, 0, e
		}
		if !ok {
			return nil, 0, Bad("unknown_worker", "Worker is not registered")
		}
	}
	_, e = tx.Exec(ctx, "INSERT INTO jobs(id,owner_id,run_id,state,range_start,range_end,frontier,config) VALUES($1,$2,$3,'created',$4,$5,$4,$6)", id, actor, run, r.From.String(), to, JSON(c))
	if e != nil {
		return nil, 0, e
	}
	if _, e = tx.Exec(ctx, "INSERT INTO job_access VALUES($1,$2,'manage')", id, actor); e != nil {
		return nil, 0, e
	}
	for _, w := range c.WorkerIDs {
		if _, e = tx.Exec(ctx, "INSERT INTO job_workers VALUES($1,$2)", id, w); e != nil {
			return nil, 0, e
		}
	}
	e = Audit(ctx, tx, actor, "job.create", id, request, map[string]any{"range": r})
	return map[string]any{"job_id": id, "run_id": run, "config_version": 1, "state": "created", "range": r, "config": c}, 201, e
}
func (s *Store) Act(ctx context.Context, tx pgx.Tx, actor, request, id, action string) (any, int, error) {
	var state, run string
	if e := tx.QueryRow(ctx, "SELECT state,run_id FROM jobs WHERE id=$1 FOR UPDATE", id).Scan(&state, &run); e != nil {
		return nil, 0, e
	}
	next, e := model.Transition(state, action)
	if e != nil {
		return nil, 0, Conflict("invalid_transition")
	}
	if action == "start" || next == "validating" {
		var available bool
		e = tx.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM job_workers jw JOIN workers w ON w.id=jw.worker_id WHERE jw.job_id=$1 AND w.enabled AND w.version=$2 AND w.last_heartbeat>clock_timestamp()-$3::interval)", id, model.Version, (2 * s.Heartbeat).String()).Scan(&available)
		if e != nil {
			return nil, 0, e
		}
		if available {
			next = "running"
		} else {
			next = "failed"
		}
	}
	if action == "resume" {
		run = model.NewID()
	}
	reason := ""
	if next == "failed" {
		reason = "no_healthy_workers"
	}
	if _, e = tx.Exec(ctx, "UPDATE jobs SET state=$2,run_id=$3,reason=$4,state_since=clock_timestamp(),updated_at=clock_timestamp(),no_workers_since=NULL,started_at=CASE WHEN $2='running' THEN COALESCE(started_at,clock_timestamp()) ELSE started_at END WHERE id=$1", id, next, run, reason); e != nil {
		return nil, 0, e
	}
	if next == "stopping" {
		if _, e = tx.Exec(ctx, "UPDATE range_chunks SET status='pending',leased_by=NULL,lease_expires_at=NULL,updated_at=clock_timestamp() WHERE job_id=$1 AND status='leased'", id); e != nil {
			return nil, 0, e
		}
		if _, e = tx.Exec(ctx, "UPDATE items SET status='cancelled',updated_at=clock_timestamp() WHERE job_id=$1 AND NOT final", id); e != nil {
			return nil, 0, e
		}
		if _, e = tx.Exec(ctx, "DELETE FROM storage_reservations WHERE download_id IN(SELECT id FROM downloads WHERE job_id=$1)", id); e != nil {
			return nil, 0, e
		}
		next = "stopped"
		if _, e = tx.Exec(ctx, "UPDATE jobs SET state='stopped' WHERE id=$1", id); e != nil {
			return nil, 0, e
		}
	}
	if action == "resume" {
		if _, e = tx.Exec(ctx, "UPDATE items SET status='pending' WHERE job_id=$1 AND NOT final", id); e != nil {
			return nil, 0, e
		}
	}
	if e = Audit(ctx, tx, actor, "job."+action, id, request, map[string]string{"from": state, "to": next}); e != nil {
		return nil, 0, e
	}
	e = Event(ctx, tx, id, "", "", "", "job."+action, next, reason, nil)
	return map[string]any{"job_id": id, "state": next, "run_id": run, "reason": reason}, 200, e
}
func (s *Store) UpdateConfig(ctx context.Context, tx pgx.Tx, actor, request, id string, c model.JobConfig) (any, int, error) {
	var old model.JobConfig
	var b []byte
	var version int64
	if e := tx.QueryRow(ctx, "SELECT config,config_version FROM jobs WHERE id=$1 FOR UPDATE", id).Scan(&b, &version); e != nil {
		return nil, 0, e
	}
	if e := json.Unmarshal(b, &old); e != nil {
		return nil, 0, e
	}
	// Full replacement of hot parameters; the original range cannot change.
	c.RangeText = old.RangeText
	c.From = old.From
	c.To = old.To
	if (c.Stop.MaxItems == nil) != (old.Stop.MaxItems == nil) || c.Stop.MaxItems != nil && *c.Stop.MaxItems != *old.Stop.MaxItems {
		return nil, 0, Bad("immutable_effective_range", "max_items defines the original range; use a deadline or stop for an earlier finish")
	}
	if _, e := c.Normalize(); e != nil {
		return nil, 0, Bad("invalid_config", e.Error())
	}
	if e := s.RecordQuotaHorizon(ctx, tx, c.Limits); e != nil {
		return nil, 0, e
	}
	if _, e := tx.Exec(ctx, "DELETE FROM job_workers WHERE job_id=$1", id); e != nil {
		return nil, 0, e
	}
	for _, w := range c.WorkerIDs {
		if _, e := tx.Exec(ctx, "INSERT INTO job_workers(job_id,worker_id) SELECT $1,id FROM workers WHERE id=$2", id, w); e != nil {
			return nil, 0, e
		}
		var ok bool
		if e := tx.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM job_workers WHERE job_id=$1 AND worker_id=$2)", id, w).Scan(&ok); e != nil || !ok {
			if e != nil {
				return nil, 0, e
			}
			return nil, 0, Bad("unknown_worker", "Worker is not registered")
		}
	}
	version++
	if _, e := tx.Exec(ctx, "UPDATE jobs SET config=$2,config_version=$3,updated_at=clock_timestamp() WHERE id=$1", id, JSON(c), version); e != nil {
		return nil, 0, e
	}
	if _, e := tx.Exec(ctx, "UPDATE range_chunks SET status='pending',leased_by=NULL,lease_expires_at=NULL,updated_at=clock_timestamp() WHERE job_id=$1 AND status='leased' AND NOT EXISTS(SELECT 1 FROM job_workers jw WHERE jw.job_id=$1 AND jw.worker_id=range_chunks.leased_by)", id); e != nil {
		return nil, 0, e
	}
	e := Audit(ctx, tx, actor, "job.configure", id, request, map[string]any{"config_version": version})
	return map[string]any{"config_version": version, "config": c}, 200, e
}
func (s *Store) GetJob(ctx context.Context, id string) (model.Job, error) {
	var j model.Job
	var start string
	var end, next *string
	var b []byte
	var started *time.Time
	e := s.Pool.QueryRow(ctx, "SELECT id,run_id,state,range_start::text,range_end::text,config,config_version,reason,created_at,updated_at,frontier::text,started_at FROM jobs WHERE id=$1", id).Scan(&j.ID, &j.RunID, &j.State, &start, &end, &b, &j.ConfigVersion, &j.Reason, &j.CreatedAt, &j.UpdatedAt, &next, &started)
	if e != nil {
		return j, e
	}
	j.Range.From = Number(start)
	if end != nil {
		n := Number(*end)
		j.Range.To = &n
	}
	if next != nil {
		n := Number(*next)
		j.NextID = &n
	}
	if e = json.Unmarshal(b, &j.Config); e != nil {
		return j, e
	}
	j.Counts = map[string]uint64{}
	rows, e := s.Pool.Query(ctx, "SELECT status,count(*) FROM items WHERE job_id=$1 GROUP BY status", id)
	if e != nil {
		return j, e
	}
	for rows.Next() {
		var st string
		var n uint64
		if e = rows.Scan(&st, &n); e != nil {
			rows.Close()
			return j, e
		}
		j.Counts[st] = n
	}
	rows.Close()
	if e = rows.Err(); e != nil {
		return j, e
	}
	var bytes string
	e = s.Pool.QueryRow(ctx, "SELECT COALESCE(sum(f.size_bytes),0)::text FROM files f JOIN downloads d ON d.id=f.download_id WHERE d.job_id=$1 AND f.ready", id).Scan(&bytes)
	if e != nil {
		return j, e
	}
	j.Bytes = uint64(Number(bytes))
	var recent uint64
	var transferred string
	if e = s.Pool.QueryRow(ctx, "SELECT count(*) FILTER(WHERE final AND updated_at>clock_timestamp()-interval '5 seconds'),count(*) FILTER(WHERE media_id IS NOT NULL) FROM items WHERE job_id=$1", id).Scan(&recent, &j.FoundCount); e != nil {
		return j, e
	}
	j.CurrentRate = float64(recent) / 5
	if e = s.Pool.QueryRow(ctx, "SELECT COALESCE(sum(offset_bytes),0)::text FROM downloads WHERE job_id=$1", id).Scan(&transferred); e != nil {
		return j, e
	}
	var count uint64
	for st, n := range j.Counts {
		if model.Final(st) {
			count += n
		}
	}
	if started != nil {
		elapsed := time.Since(*started).Seconds()
		if elapsed > 0 {
			j.Rate = float64(count) / elapsed
			j.AverageDownloadRate = float64(uint64(Number(transferred))) / elapsed
		}
	}
	if j.Range.To != nil && j.Rate > 0 {
		total := float64(uint64(*j.Range.To)-uint64(j.Range.From)) + 1
		eta := math.Max(0, (total-float64(count))/j.Rate)
		j.ETASeconds = &eta
	}
	// Global checkpoint is the earliest uncovered chunk, then the creation frontier.
	var gap *string
	if e = s.Pool.QueryRow(ctx, "SELECT min(next_id)::text FROM range_chunks WHERE job_id=$1 AND status<>'completed'", id).Scan(&gap); e != nil {
		return j, e
	}
	if gap != nil {
		n := Number(*gap)
		j.NextID = &n
	}
	return j, nil
}
func (s *Store) ListJobs(ctx context.Context, user string, admin bool) ([]model.Job, error) {
	return s.ListJobsPage(ctx, user, admin, "", 200)
}

func (s *Store) ListJobsPage(ctx context.Context, user string, admin bool, before string, limit int) ([]model.Job, error) {
	if limit < 1 || limit > 200 {
		return nil, Bad("invalid_limit", "Choose 1..200 jobs")
	}
	rows, e := s.Pool.Query(ctx, "SELECT id FROM jobs WHERE ($2 OR EXISTS(SELECT 1 FROM job_access a WHERE a.job_id=jobs.id AND a.user_id=$1)) AND ($3='' OR (created_at,id)<(SELECT created_at,id FROM jobs WHERE id=$3)) ORDER BY created_at DESC,id DESC LIMIT $4", user, admin, before, limit)
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
	jobs := make([]model.Job, 0, len(ids))
	for _, id := range ids {
		j, e := s.GetJob(ctx, id)
		if e != nil {
			return nil, e
		}
		jobs = append(jobs, j)
	}
	return jobs, nil
}
func (s *Store) RetryItems(ctx context.Context, tx pgx.Tx, actor, request, job string, ids []model.Number) (any, int, error) {
	if len(ids) == 0 || len(ids) > 1000 {
		return nil, 0, Bad("invalid_retry", "Select between 1 and 1000 failed items")
	}
	var state string
	if e := tx.QueryRow(ctx, "SELECT state FROM jobs WHERE id=$1 FOR UPDATE", job).Scan(&state); e != nil {
		return nil, 0, e
	}
	if state == "stopping" || state == "pausing" {
		return nil, 0, Conflict("invalid_transition")
	}
	for _, id := range ids {
		var chunk, status string
		if e := tx.QueryRow(ctx, "SELECT chunk_id,status FROM items WHERE job_id=$1 AND external_id=$2 FOR UPDATE", job, id.String()).Scan(&chunk, &status); e != nil {
			return nil, 0, e
		}
		if status != "failed" {
			return nil, 0, Bad("not_retryable", "Only failed items may be retried")
		}
		var leased bool
		if e := tx.QueryRow(ctx, "SELECT status='leased' AND lease_expires_at>clock_timestamp() FROM range_chunks WHERE id=$1 FOR UPDATE", chunk).Scan(&leased); e != nil {
			return nil, 0, e
		}
		if leased {
			return nil, 0, Conflict("active_chunk")
		}
		if _, e := tx.Exec(ctx, "UPDATE items SET final=false,status='pending',error_code='' WHERE job_id=$1 AND external_id=$2", job, id.String()); e != nil {
			return nil, 0, e
		}
		if _, e := tx.Exec(ctx, "UPDATE range_chunks SET status='pending',next_id=LEAST(COALESCE(next_id,$2),$2),leased_by=NULL,lease_expires_at=NULL WHERE id=$1", chunk, id.String()); e != nil {
			return nil, 0, e
		}
	}
	if state == "completed" {
		if _, e := tx.Exec(ctx, "UPDATE jobs SET state='stopped',reason='retry_requested' WHERE id=$1", job); e != nil {
			return nil, 0, e
		}
	}
	e := Audit(ctx, tx, actor, "items.retry", job, request, map[string]any{"count": len(ids)})
	return map[string]any{"accepted": len(ids)}, 200, e
}
func (s *Store) Sweep(ctx context.Context) error {
	tx, e := s.Pool.Begin(ctx)
	if e != nil {
		return e
	}
	defer tx.Rollback(ctx)
	rows, e := tx.Query(ctx, "SELECT c.id,c.job_id,COALESCE(c.leased_by,''),c.attempt_id FROM range_chunks c JOIN jobs j ON j.id=c.job_id WHERE c.status='leased' AND c.lease_expires_at<=clock_timestamp() FOR UPDATE OF j,c SKIP LOCKED")
	if e != nil {
		return e
	}
	type expired struct{ id, job, worker, attempt string }
	var expiredChunks []expired
	for rows.Next() {
		var x expired
		if e = rows.Scan(&x.id, &x.job, &x.worker, &x.attempt); e != nil {
			rows.Close()
			return e
		}
		expiredChunks = append(expiredChunks, x)
	}
	rows.Close()
	if e = rows.Err(); e != nil {
		return e
	}
	for _, x := range expiredChunks {
		if _, e = tx.Exec(ctx, "UPDATE range_chunks SET status='pending',leased_by=NULL,lease_expires_at=NULL,updated_at=clock_timestamp() WHERE id=$1", x.id); e != nil {
			return e
		}
		if _, e = tx.Exec(ctx, "UPDATE items SET status='pending' WHERE chunk_id=$1 AND NOT final", x.id); e != nil {
			return e
		}
		if e = Event(ctx, tx, x.job, x.worker, x.id, x.attempt, "lease.expired", "pending", "", nil); e != nil {
			return e
		}
	}
	if _, e = tx.Exec(ctx, "DELETE FROM storage_reservations WHERE expires_at<=clock_timestamp() OR attempt_id IN(SELECT attempt_id FROM range_chunks WHERE status<>'leased')"); e != nil {
		return e
	}
	if _, e = tx.Exec(ctx, "UPDATE jobs j SET state='paused',state_since=clock_timestamp(),updated_at=clock_timestamp() WHERE state='pausing' AND NOT EXISTS(SELECT 1 FROM range_chunks c WHERE c.job_id=j.id AND c.status='leased')"); e != nil {
		return e
	}
	if _, e = tx.Exec(ctx, "UPDATE jobs j SET state='completed',state_since=clock_timestamp(),updated_at=clock_timestamp() WHERE state='running' AND frontier IS NULL AND NOT EXISTS(SELECT 1 FROM range_chunks c WHERE c.job_id=j.id AND c.status<>'completed')"); e != nil {
		return e
	}
	if _, e = tx.Exec(ctx, "DELETE FROM worker_metrics WHERE measured_at<clock_timestamp()-interval '7 days'; DELETE FROM sessions WHERE expires_at<clock_timestamp(); DELETE FROM ssh_sessions WHERE expires_at<clock_timestamp();"); e != nil {
		return e
	}
	return tx.Commit(ctx)
}
func (s *Store) EnforceStops(ctx context.Context) error {
	rows, e := s.Pool.Query(ctx, "SELECT id,config FROM jobs WHERE state IN ('running','pausing')")
	if e != nil {
		return e
	}
	type candidate struct {
		id string
		c  model.JobConfig
	}
	var jobs []candidate
	for rows.Next() {
		var x candidate
		var b []byte
		if e = rows.Scan(&x.id, &b); e != nil {
			rows.Close()
			return e
		}
		if e = json.Unmarshal(b, &x.c); e != nil {
			rows.Close()
			return e
		}
		jobs = append(jobs, x)
	}
	rows.Close()
	if e = rows.Err(); e != nil {
		return e
	}
	for _, j := range jobs {
		reason := ""
		if j.c.Stop.At != nil && !time.Now().Before(*j.c.Stop.At) {
			reason = "deadline"
		}
		var healthy bool
		e = s.Pool.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM job_workers jw JOIN workers w ON w.id=jw.worker_id WHERE jw.job_id=$1 AND w.enabled AND w.last_heartbeat>clock_timestamp()-$2::interval)", j.id, (2 * s.Heartbeat).String()).Scan(&healthy)
		if e != nil {
			return e
		}
		if !healthy {
			if _, e = s.Pool.Exec(ctx, "UPDATE jobs SET no_workers_since=COALESCE(no_workers_since,clock_timestamp()) WHERE id=$1", j.id); e != nil {
				return e
			}
			var elapsed bool
			if e = s.Pool.QueryRow(ctx, "SELECT no_workers_since<clock_timestamp()-$2::interval FROM jobs WHERE id=$1", j.id, (time.Duration(j.c.Stop.NoWorkersSeconds) * time.Second).String()).Scan(&elapsed); e != nil {
				return e
			}
			if elapsed {
				reason = "no_healthy_workers"
			}
		} else {
			if _, e = s.Pool.Exec(ctx, "UPDATE jobs SET no_workers_since=NULL WHERE id=$1", j.id); e != nil {
				return e
			}
		}
		if j.c.Stop.ConsecutiveMissing > 0 {
			var n uint64
			var checkpoint *string
			if e = s.Pool.QueryRow(ctx, "SELECT COALESCE(min(next_id),(SELECT frontier FROM jobs WHERE id=$1))::text FROM range_chunks WHERE job_id=$1 AND status<>'completed'", j.id).Scan(&checkpoint); e != nil {
				return e
			}
			if checkpoint != nil {
				r, e := s.Pool.Query(ctx, "SELECT status FROM items WHERE job_id=$1 AND external_id<$2 ORDER BY external_id DESC LIMIT $3", j.id, *checkpoint, j.c.Stop.ConsecutiveMissing)
				if e != nil {
					return e
				}
				for r.Next() {
					var st string
					if e = r.Scan(&st); e != nil {
						r.Close()
						return e
					}
					if st != "not_found" {
						break
					}
					n++
				}
				r.Close()
				if n >= j.c.Stop.ConsecutiveMissing {
					reason = "consecutive_missing"
				}
			}
		}
		if reason != "" {
			_, e = s.Mutate(ctx, "system", "stop:"+j.id, model.NewID(), []byte(reason), func(tx pgx.Tx) (any, int, error) {
				v, status, e := s.Act(ctx, tx, "system", "", j.id, "stop")
				if e != nil {
					var de *Error
					if errors.As(e, &de) && de.Status == 409 {
						return map[string]bool{"stopped": true}, 200, nil
					}
					return nil, 0, e
				}
				_, e = tx.Exec(ctx, "UPDATE jobs SET reason=$2 WHERE id=$1", j.id, reason)
				return v, status, e
			})
			if e != nil {
				return e
			}
		}
	}
	return nil
}
