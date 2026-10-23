//go:build integration

package integration

import (
	"github.com/jackc/pgx/v5"
	"github.com/rychkin/Begom/internal/model"
	"math"
	"testing"
)

func TestRateETAAndGap(t *testing.T) {
	x := setup(t)
	job := x.job(t, "[0,2]", 3)
	l := x.lease(t, "w1")
	p := model.Progress{LeaseRef: l.Ref(), OperationID: model.NewID(), Results: []model.ItemResult{{ExternalID: 0, Status: "not_found"}, {ExternalID: 2, Status: "not_found"}}}
	mutate(t, x.s, "w1", "progress", p, func(tx pgx.Tx) (any, int, error) { return x.s.ProgressTx(t.Context(), tx, "w1", p, 1<<40, 0) })
	if _, e := x.s.Pool.Exec(t.Context(), "UPDATE jobs SET started_at=clock_timestamp()-interval '100 seconds' WHERE id=$1", job); e != nil {
		t.Fatal(e)
	}
	j, e := x.s.GetJob(t.Context(), job)
	if e != nil || j.NextID == nil || *j.NextID != 1 || j.Rate < .019 || j.Rate > .021 || j.ETASeconds == nil || *j.ETASeconds < 49 || *j.ETASeconds > 51 || math.IsInf(*j.ETASeconds, 0) {
		t.Fatal("rate, ETA or confirmed gap mismatch", j, e)
	}
}

func TestHistoryCursorKeepsACLAndTimestampTies(t *testing.T) {
	x := setup(t)
	viewer := model.NewID()
	if _, e := x.s.Pool.Exec(t.Context(), "INSERT INTO users(id,username,password_hash) VALUES($1,'history-viewer','unusable-test-hash')", viewer); e != nil {
		t.Fatal(e)
	}
	for i := 0; i < 5; i++ {
		job := x.job(t, "[0,0]", 1)
		if _, e := x.s.Pool.Exec(t.Context(), "UPDATE jobs SET created_at='2026-01-01'::timestamptz WHERE id=$1", job); e != nil {
			t.Fatal(e)
		}
		if i%2 == 0 {
			if _, e := x.s.Pool.Exec(t.Context(), "INSERT INTO job_access VALUES($1,$2,'view')", job, viewer); e != nil {
				t.Fatal(e)
			}
		}
	}
	seen, cursor := map[string]bool{}, ""
	for {
		page, e := x.s.ListJobsPage(t.Context(), viewer, false, cursor, 1)
		if e != nil {
			t.Fatal(e)
		}
		if len(page) == 0 {
			break
		}
		cursor = page[0].ID
		if seen[cursor] {
			t.Fatal("duplicate history entry")
		}
		seen[cursor] = true
	}
	if len(seen) != 3 {
		t.Fatal("history skipped an authorized job or exposed an unassigned one", len(seen))
	}
}
