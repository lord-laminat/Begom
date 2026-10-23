package db

import (
	"context"
	"errors"
	"github.com/rychkin/Begom/internal/model"
	"math"
	"time"
)

func (s *Store) RecordQuotaHorizon(ctx context.Context, q Queryer, l model.Limits) error {
	ms := int64(1700)
	for _, rate := range []float64{l.RPS, l.WorkerRPS, l.CheckRPS, l.DownloadRPS} {
		if rate > 0 && rate < 1 {
			ms = max(ms, int64(math.Ceil(1700/rate)))
		}
	}
	_, e := q.Exec(ctx, "INSERT INTO donor_quota_state(source_id,horizon_ms) VALUES($1,$2) ON CONFLICT(source_id) DO UPDATE SET horizon_ms=GREATEST(donor_quota_state.horizon_ms,EXCLUDED.horizon_ms)", s.SourceID, ms)
	return e
}

// BindProfile prevents numeric mock identifiers and production identifiers from
// silently sharing a catalog. A donor change requires a separate database.
func (s *Store) BindProfile(ctx context.Context, mock bool) error {
	_, e := s.Pool.Exec(ctx, "INSERT INTO system_profile(singleton,source_id,mock) VALUES(true,$1,$2) ON CONFLICT DO NOTHING", s.SourceID, mock)
	if e != nil {
		return e
	}
	var source string
	var saved bool
	if e = s.Pool.QueryRow(ctx, "SELECT source_id,mock FROM system_profile WHERE singleton").Scan(&source, &saved); e != nil {
		return e
	}
	if source != s.SourceID || saved != mock {
		return errors.New("database donor profile differs from configuration")
	}
	return nil
}

type QuotaState struct {
	Factor                 float64
	Cooldown, LastIncrease time.Time
	Errors                 int
	Horizon                time.Duration
}

func (s *Store) QuotaState(ctx context.Context) (q QuotaState, e error) {
	_, e = s.Pool.Exec(ctx, "INSERT INTO donor_quota_state(source_id) VALUES($1) ON CONFLICT DO NOTHING", s.SourceID)
	if e != nil {
		return
	}
	var ms int64
	// PostgreSQL infinity cannot be represented as time.Time.
	e = s.Pool.QueryRow(ctx, "SELECT factor,GREATEST(cooldown,'2000-01-01'::timestamptz),last_increase,error_count,horizon_ms FROM donor_quota_state WHERE source_id=$1", s.SourceID).Scan(&q.Factor, &q.Cooldown, &q.LastIncrease, &q.Errors, &ms)
	q.Horizon = time.Duration(ms) * time.Millisecond
	return
}
