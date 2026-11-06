package db

import (
	"context"
	"errors"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rychkin/Begom/internal/model"
	"sync/atomic"
	"time"
)

// Epoch holds a dedicated PostgreSQL advisory lock and a renewable DB-time fence.
type Epoch struct {
	Store      *Store
	ID         string
	GuardUntil time.Time
	conn       *pgxpool.Conn
	healthy    atomic.Bool
}

func (s *Store) AcquireEpoch(ctx context.Context, drain time.Duration) (*Epoch, error) {
	c, e := s.Pool.Acquire(ctx)
	if e != nil {
		return nil, e
	}
	var locked bool
	if e = c.QueryRow(ctx, "SELECT pg_try_advisory_lock(82939100)").Scan(&locked); e != nil || !locked {
		c.Release()
		return nil, errors.New("another coordinator holds the dispatcher lock")
	}
	fail := func(e error) (*Epoch, error) {
		c.Exec(context.Background(), "SELECT pg_advisory_unlock(82939100)")
		c.Release()
		return nil, e
	}
	tx, e := c.Begin(ctx)
	if e != nil {
		return fail(e)
	}
	defer tx.Rollback(ctx)
	var previous *time.Time
	var now time.Time
	if e = tx.QueryRow(ctx, "SELECT max(lease_expires_at),clock_timestamp() FROM coordinator_epochs").Scan(&previous, &now); e != nil {
		return fail(e)
	}
	if _, e = tx.Exec(ctx, "UPDATE coordinator_epochs SET active=false WHERE active"); e != nil {
		return fail(e)
	}
	ep := &Epoch{Store: s, ID: model.NewID(), conn: c, GuardUntil: now.Add(time.Second)}
	if previous != nil {
		if previous.After(now) {
			ep.GuardUntil = *previous
		}
		ep.GuardUntil = ep.GuardUntil.Add(drain)
	}
	if _, e = tx.Exec(ctx, "INSERT INTO coordinator_epochs(id,lease_expires_at) VALUES($1,clock_timestamp()+interval '4 seconds')", ep.ID); e != nil {
		return fail(e)
	}
	if e = tx.Commit(ctx); e != nil {
		return fail(e)
	}
	ep.healthy.Store(true)
	return ep, nil
}
func (e *Epoch) Ready() bool { return e.healthy.Load() && time.Now().After(e.GuardUntil) }
func (e *Epoch) Owns() bool  { return e.healthy.Load() }
func (e *Epoch) Check(ctx context.Context) error {
	return e.CheckWith(ctx, e.Store.Pool)
}
func (e *Epoch) CheckWith(ctx context.Context, q Queryer) error {
	if !e.Ready() {
		return &Error{503, "dispatcher_recovering", "Dispatcher is draining its previous generation"}
	}
	var active bool
	err := q.QueryRow(ctx, "SELECT active AND lease_expires_at>clock_timestamp() FROM coordinator_epochs WHERE id=$1", e.ID).Scan(&active)
	if err != nil {
		return &Error{503, "epoch_check_unavailable", "Dispatcher authority check unavailable"}
	}
	if !active {
		e.healthy.Store(false)
		return &Error{503, "epoch_lost", "Dispatcher authority unavailable"}
	}
	return nil
}
func (e *Epoch) Run(ctx context.Context) {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	defer func() {
		e.healthy.Store(false)
		cctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		e.conn.Exec(cctx, "SELECT pg_advisory_unlock(82939100)")
		e.conn.Release()
	}()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if !e.healthy.Load() {
				return
			}
			qctx, cancel := context.WithTimeout(ctx, 700*time.Millisecond)
			tag, err := e.conn.Exec(qctx, "UPDATE coordinator_epochs SET lease_expires_at=clock_timestamp()+interval '4 seconds' WHERE id=$1 AND active AND lease_expires_at>clock_timestamp()", e.ID)
			cancel()
			if err != nil || tag.RowsAffected() != 1 {
				e.healthy.Store(false)
				return
			}
		}
	}
}
