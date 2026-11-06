//go:build integration

package integration

import (
	"encoding/json"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/rychkin/Begom/internal/db"
	"github.com/rychkin/Begom/internal/model"
)

func TestDiscoveryCatalogSparseSelectionAndCheckpoint(t *testing.T) {
	x := setup(t)
	ids := []string{}
	for _, number := range []string{"0", "18446744073709551615"} {
		job := x.job(t, "["+number+","+number+"]", 1)
		lease := x.lease(t, "w1")
		if lease == nil || lease.JobID != job {
			t.Fatal("discovery lease missing", lease)
		}
		meta := model.Metadata{CanonicalID: "canonical-" + number, Title: "Разрешённое видео " + number, PageURL: "https://example.com/" + number, DownloadURL: "https://example.com/video?secret=never-exposed", Qualities: []model.Quality{{Name: "720p", Size: 30}}}
		p := model.Progress{LeaseRef: lease.Ref(), OperationID: model.NewID(), Results: []model.ItemResult{{ExternalID: lease.From, Status: "available", Metadata: &meta}}}
		mutate(t, x.s, "w1", "discovery.result", p, func(tx pgx.Tx) (any, int, error) { return x.s.ProgressTx(t.Context(), tx, "w1", p, 1<<40, 0) })
		mutate(t, x.s, "w1", "complete", lease.Ref(), func(tx pgx.Tx) (any, int, error) { return x.s.CompleteTx(t.Context(), tx, "w1", lease.Ref(), false) })
		if e := x.s.Sweep(t.Context()); e != nil {
			t.Fatal(e)
		}
		catalog, _, e := x.s.Catalog(t.Context(), x.user, true, db.ListQuery{Job: job, Limit: 100})
		if e != nil || len(catalog) != 1 {
			t.Fatal(catalog, e)
		}
		if catalog[0].Metadata.DownloadURL != "" {
			t.Fatal("protected link exposed")
		}
		ids = append(ids, catalog[0].ID)
	}
	for _, table := range []string{"downloads", "storage_reservations", "publications", "files"} {
		var n int
		if e := x.s.Pool.QueryRow(t.Context(), "SELECT count(*) FROM "+table).Scan(&n); e != nil || n != 0 {
			t.Fatal("discovery side effect", table, n, e)
		}
	}
	page, next, e := x.s.Catalog(t.Context(), x.user, true, db.ListQuery{Limit: 1})
	if e != nil || len(page) != 1 || next == "" {
		t.Fatal("catalog pagination", e)
	}
	second, _, e := x.s.Catalog(t.Context(), x.user, true, db.ListQuery{Limit: 1, After: next})
	if e != nil || len(second) != 1 || second[0].ID == page[0].ID {
		t.Fatal("catalog pagination repeats", second, e)
	}
	if hidden, _, e := x.s.Catalog(t.Context(), "outsider", false, db.ListQuery{Limit: 100}); e != nil || len(hidden) != 0 {
		t.Fatal("catalog ACL", hidden, e)
	}
	in := model.CreateJob{Kind: model.Download, Config: model.JobConfig{WorkerIDs: x.workers, BlockSize: 10000}, MediaIDs: append(ids, ids[0])}
	r := mutate(t, x.s, x.user, "download.create", in, func(tx pgx.Tx) (any, int, error) { return x.s.CreateRequestedJob(t.Context(), tx, x.user, "", in) })
	var out struct {
		ID string `json:"job_id"`
	}
	json.Unmarshal(r.Body, &out)
	mutate(t, x.s, x.user, "start", out.ID, func(tx pgx.Tx) (any, int, error) { return x.s.Act(t.Context(), tx, x.user, "", out.ID, "start") })
	lease := x.lease(t, "w2")
	if lease == nil || lease.Kind != model.Download || len(lease.Targets) != 2 || lease.From != 0 || lease.To != model.Number(^uint64(0)) {
		t.Fatal("sparse lease expanded or truncated", lease)
	}
	report := func(id model.Number) error {
		p := model.Progress{LeaseRef: lease.Ref(), OperationID: model.NewID(), Results: []model.ItemResult{{ExternalID: id, Status: "failed", ErrorCode: "fixture_failure"}}}
		_, e := x.s.Mutate(t.Context(), "w2", "progress", model.NewID(), db.JSON(p), func(tx pgx.Tx) (any, int, error) { return x.s.ProgressTx(t.Context(), tx, "w2", p, 1<<40, 0) })
		return e
	}
	if e = report(model.Number(^uint64(0))); e != nil {
		t.Fatal(e)
	}
	j, e := x.s.GetJob(t.Context(), out.ID)
	if e != nil || j.SelectionCount != 2 || j.NextID == nil || *j.NextID != 0 {
		t.Fatal("sparse gap hidden", j, e)
	}
	if e = report(1); e == nil {
		t.Fatal("unselected identifier accepted")
	}
	manyDuplicates := model.CreateJob{Kind: model.Download, Config: model.JobConfig{WorkerIDs: x.workers}, MediaIDs: make([]string, 10001)}
	for i := range manyDuplicates.MediaIDs {
		manyDuplicates.MediaIDs[i] = ids[0]
	}
	duplicateResponse := mutate(t, x.s, x.user, "download.deduplicate", manyDuplicates, func(tx pgx.Tx) (any, int, error) {
		return x.s.CreateRequestedJob(t.Context(), tx, x.user, "", manyDuplicates)
	})
	var deduplicated struct {
		Count int `json:"selection_count"`
	}
	if e = json.Unmarshal(duplicateResponse.Body, &deduplicated); e != nil || deduplicated.Count != 1 {
		t.Fatal("selection limit must count unique materials", deduplicated, e)
	}
	if e = report(0); e != nil {
		t.Fatal(e)
	}
	j, e = x.s.GetJob(t.Context(), out.ID)
	if e != nil || j.NextID != nil {
		t.Fatal("sparse checkpoint overflow", j, e)
	}
}

func TestDiscoveryRejectsDownloadAndSelectionChecksAccess(t *testing.T) {
	x := setup(t)
	x.job(t, "[0,0]", 1)
	lease := x.lease(t, "w1")
	p := model.Progress{LeaseRef: lease.Ref(), OperationID: model.NewID(), Results: []model.ItemResult{{ExternalID: 0, Status: "download_queued", DownloadID: "forbidden", StageID: "stage"}}}
	if _, e := x.s.Mutate(t.Context(), "w1", "progress", model.NewID(), db.JSON(p), func(tx pgx.Tx) (any, int, error) { return x.s.ProgressTx(t.Context(), tx, "w1", p, 1<<40, 0) }); e == nil {
		t.Fatal("discovery accepted a download")
	}
	in := model.CreateJob{Kind: model.Download, Config: model.JobConfig{WorkerIDs: x.workers}, MediaIDs: []string{"unknown"}}
	if _, e := x.s.Mutate(t.Context(), x.user, "create", model.NewID(), db.JSON(in), func(tx pgx.Tx) (any, int, error) { return x.s.CreateRequestedJob(t.Context(), tx, x.user, "", in) }); e == nil {
		t.Fatal("unknown selection accepted")
	}
	text := "[0,1]"
	in.Config.RangeText = &text
	if _, e := x.s.Mutate(t.Context(), x.user, "create", model.NewID(), db.JSON(in), func(tx pgx.Tx) (any, int, error) { return x.s.CreateRequestedJob(t.Context(), tx, x.user, "", in) }); e == nil {
		t.Fatal("download accepted numeric range")
	}
}

func TestCanonicalReplacementKeepsHistoryAndUpdatesCatalog(t *testing.T) {
	x := setup(t)
	oldJob := x.downloadJob(t, "[10,10]", 1, func(model.Number) string { return "old-canonical" })
	mutate(t, x.s, x.user, "stop", oldJob, func(tx pgx.Tx) (any, int, error) { return x.s.Act(t.Context(), tx, x.user, "", oldJob, "stop") })
	old, _, e := x.s.Catalog(t.Context(), x.user, true, db.ListQuery{Limit: 10})
	if e != nil || len(old) != 1 {
		t.Fatal(old, e)
	}
	scan := x.job(t, "[10,10]", 1)
	lease := x.lease(t, "w1")
	if lease == nil || lease.JobID != scan {
		t.Fatal("scan lease missing")
	}
	meta := model.Metadata{CanonicalID: "new-canonical", Title: "Updated material", PageURL: "https://example.com/10", DownloadURL: "https://example.com/new", Qualities: []model.Quality{{Name: "720p", Size: 30}}}
	p := model.Progress{LeaseRef: lease.Ref(), OperationID: model.NewID(), Results: []model.ItemResult{{ExternalID: 10, Status: "available", Metadata: &meta}}}
	mutate(t, x.s, "w1", "progress", p, func(tx pgx.Tx) (any, int, error) { return x.s.ProgressTx(t.Context(), tx, "w1", p, 1<<40, 0) })
	current, _, e := x.s.Catalog(t.Context(), x.user, true, db.ListQuery{Limit: 10})
	if e != nil || len(current) != 1 || current[0].Metadata.CanonicalID != "new-canonical" || current[0].ID == old[0].ID {
		t.Fatal("stale alias in catalog", current, e)
	}
	var materials int
	if e = x.s.Pool.QueryRow(t.Context(), "SELECT count(*) FROM media_metadata WHERE external_id=10").Scan(&materials); e != nil || materials != 2 {
		t.Fatal("canonical history overwritten", materials, e)
	}
	in := model.CreateJob{Kind: model.Download, Config: model.JobConfig{WorkerIDs: x.workers}, MediaIDs: []string{old[0].ID}}
	if _, e = x.s.Mutate(t.Context(), x.user, "create", model.NewID(), db.JSON(in), func(tx pgx.Tx) (any, int, error) { return x.s.CreateRequestedJob(t.Context(), tx, x.user, "", in) }); e == nil {
		t.Fatal("old canonical alias selectable")
	}
}
