//go:build integration

package integration

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"github.com/jackc/pgx/v5"
	"github.com/rychkin/Begom/internal/config"
	"github.com/rychkin/Begom/internal/db"
	"github.com/rychkin/Begom/internal/model"
	"github.com/rychkin/Begom/internal/publication"
	"github.com/rychkin/Begom/internal/storage"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
)

func TestPublicationRecoveryAndInodeIsolation(t *testing.T) {
	for _, step := range []string{"part", "renamed_part", "renamed_directory", "corrupt"} {
		t.Run(step, func(t *testing.T) {
			x := setup(t)
			job := x.job(t, "[10,10]", 1)
			l := x.lease(t, "w1")
			if l == nil {
				t.Fatal("lease missing")
			}
			x.s.Pool.Exec(t.Context(), "UPDATE range_chunks SET lease_expires_at=clock_timestamp()+interval '30 seconds' WHERE id=$1", l.ChunkID)
			data := []byte("authorized synthetic video content")
			sum := sha256.Sum256(data)
			hash := hex.EncodeToString(sum[:])
			stageID, downloadID := model.NewID(), model.NewID()
			meta := model.Metadata{CanonicalID: "canonical-one", Title: "Video", MIME: "video/mp4", Size: uint64(len(data)), PageURL: "https://example.com/10", DownloadURL: "https://example.com/video?token=secret"}
			r := model.ItemResult{ExternalID: 10, Status: "download_queued", Metadata: &meta, DownloadID: downloadID, StageID: stageID, ReserveBytes: uint64(2 * len(data))}
			p := model.Progress{LeaseRef: l.Ref(), OperationID: model.NewID(), Sequence: 1, Results: []model.ItemResult{r}}
			mutate(t, x.s, "w1", "progress", p, func(tx pgx.Tx) (any, int, error) { return x.s.ProgressTx(t.Context(), tx, "w1", p, 1<<40, 0) })
			commit := model.Commit{LeaseRef: l.Ref(), StageID: stageID, Size: uint64(len(data)), Checksum: hash}
			answer := mutate(t, x.s, "w1", "commit", commit, func(tx pgx.Tx) (any, int, error) {
				return x.s.PreparePublication(t.Context(), tx, "w1", downloadID, commit)
			})
			var reply struct {
				ID string `json:"publication_id"`
			}
			json.Unmarshal(answer.Body, &reply)
			pub, e := x.s.Publication(t.Context(), reply.ID)
			if e != nil {
				t.Fatal(e)
			}
			dir := t.TempDir()
			os.WriteFile(filepath.Join(dir, ".begom-volume"), []byte("test-volume"), 0444)
			stage := filepath.Join(dir, storage.Stage("w1", l.AttemptID, stageID))
			os.MkdirAll(stage, 0770)
			root, _ := os.OpenRoot(dir)
			defer root.Close()
			if e = storage.WriteJSON(root, storage.Stage("w1", l.AttemptID, stageID)+"/metadata.json", meta); e != nil {
				t.Fatal(e)
			}
			filename := filepath.Join(stage, "video.part")
			if step == "renamed_part" {
				filename = filepath.Join(stage, "video")
			}
			if step == "corrupt" {
				data[0] ^= 1
			}
			os.WriteFile(filename, data, 0640)
			held, e := os.OpenFile(filename, os.O_RDWR, 0)
			if e != nil {
				t.Fatal(e)
			}
			defer held.Close()
			if step == "renamed_directory" {
				dest := filepath.Join(dir, "published", filepath.Dir(pub.Path))
				os.MkdirAll(filepath.Dir(dest), 0750)
				os.Rename(filename, filepath.Join(stage, "video"))
				if e = os.Rename(stage, dest); e != nil {
					t.Fatal(e)
				}
			}
			svc := &publication.Service{Store: x.s, Volume: &storage.Volume{Config: config.Storage{Root: dir, Mode: "test", VolumeID: "test-volume"}}, Log: slog.New(slog.NewJSONHandler(io.Discard, nil))}
			if e = svc.Publish(t.Context(), pub.ID); e != nil {
				t.Fatal(e)
			}
			got, e := x.s.Publication(t.Context(), pub.ID)
			if e != nil {
				t.Fatal(e)
			}
			if step == "corrupt" {
				if got.State != "failed" {
					t.Fatal("corrupt publication accepted", got.State)
				}
				return
			}
			if got.State != "ready" {
				t.Fatal(got.State)
			}
			// Only the new copy may be published. The renamed_directory case simulates
			// the older implementation's already-completed filesystem transaction.
			if step != "renamed_directory" {
				held.WriteAt([]byte("CORRUPT"), 0)
			}
			size, actual, e := storage.Hash(root, "published/"+pub.Path)
			if e != nil || size != pub.Size || actual != hash {
				t.Fatal("published inode changed through agent FD", actual, e)
			}
			if e = svc.Publish(t.Context(), pub.ID); e != nil {
				t.Fatal("publication replay", e)
			}
			var files int
			x.s.Pool.QueryRow(t.Context(), "SELECT count(*) FROM files WHERE ready").Scan(&files)
			if files != 1 {
				t.Fatal("duplicate files", files)
			}
			j, e := x.s.GetJob(t.Context(), job)
			if e != nil || j.NextID != nil {
				t.Fatal("publication checkpoint", j.NextID, e)
			}
			again := x.job(t, "[11,11]", 1)
			_ = again
			mutate(t, x.s, "w1", "complete", l.Ref(), func(tx pgx.Tx) (any, int, error) { return x.s.CompleteTx(t.Context(), tx, "w1", l.Ref(), false) })
			next := x.lease(t, "w2")
			if next == nil {
				t.Fatal("next lease")
			}
			r.ExternalID = 11
			r.DownloadID = model.NewID()
			r.StageID = model.NewID()
			p = model.Progress{LeaseRef: next.Ref(), OperationID: model.NewID(), Sequence: 1, Results: []model.ItemResult{r}}
			mutate(t, x.s, "w2", "deduplicate", p, func(tx pgx.Tx) (any, int, error) { return x.s.ProgressTx(t.Context(), tx, "w2", p, 1<<40, 0) })
			var status string
			x.s.Pool.QueryRow(t.Context(), "SELECT status FROM items WHERE job_id=$1", next.JobID).Scan(&status)
			if status != "completed" {
				t.Fatal("canonical file not reused", status)
			}
			if step == "part" {
				x.job(t, "[12,12]", 1)
				denied := x.lease(t, "w3")
				if denied == nil {
					t.Fatal("denied job lease")
				}
				noGrant := model.Progress{LeaseRef: denied.Ref(), OperationID: model.NewID(), Results: []model.ItemResult{{ExternalID: 12, Status: "skipped", Metadata: &meta, ErrorCode: "download_not_allowed"}}}
				mutate(t, x.s, "w3", "no-grant", noGrant, func(tx pgx.Tx) (any, int, error) { return x.s.ProgressTx(t.Context(), tx, "w3", noGrant, 1<<40, 0) })
				items, e := x.s.Items(t.Context(), denied.JobID, "", "", 100)
				if e != nil || len(items) != 1 || items[0]["status"] != "skipped" || items[0]["file_id"] != nil {
					t.Fatal("catalog reuse bypassed current permission", items, e)
				}
				fd, e := root.OpenFile("published/"+pub.Path, os.O_WRONLY, 0)
				if e != nil {
					t.Fatal(e)
				}
				fd.WriteAt([]byte("BAD"), 0)
				fd.Close()
				if e = svc.Scrub(t.Context(), 100); e != nil {
					t.Fatal(e)
				}
				x.s.Pool.QueryRow(t.Context(), "SELECT count(*) FROM files WHERE ready").Scan(&files)
				if files != 0 {
					t.Fatal("corrupt catalog remains published")
				}
				var failed int
				x.s.Pool.QueryRow(t.Context(), "SELECT count(*) FROM items WHERE status='failed' AND error_code='catalog_checksum_mismatch'").Scan(&failed)
				if failed != 2 {
					t.Fatal("dependent items not withdrawn", failed)
				}
				if _, e = root.Stat("published/" + pub.Path); !os.IsNotExist(e) {
					t.Fatal("corrupt directory not quarantined", e)
				}
			}
		})
	}
}

func TestProgressSequenceAndSharedDiskReservation(t *testing.T) {
	x := setup(t)
	x.job(t, "[10,11]", 1)
	a, b := x.lease(t, "w1"), x.lease(t, "w2")
	if a == nil || b == nil {
		t.Fatal("missing leases")
	}
	report := func(l *model.Lease, worker string, seq uint64, status string, reserve uint64) error {
		r := model.ItemResult{ExternalID: l.From, Status: status, Metadata: &model.Metadata{CanonicalID: "video-" + l.From.String(), Title: "video", MIME: "video/mp4", Size: 30}, DownloadID: "download" + worker, StageID: "stage" + worker, ReserveBytes: reserve}
		p := model.Progress{LeaseRef: l.Ref(), OperationID: model.NewID(), Sequence: seq, Results: []model.ItemResult{r}}
		_, e := x.s.Mutate(t.Context(), worker, "progress", model.NewID(), db.JSON(p), func(tx pgx.Tx) (any, int, error) { return x.s.ProgressTx(t.Context(), tx, worker, p, 100, 10) })
		return e
	}
	if e := report(a, "w1", 2, "downloading", 60); e != nil {
		t.Fatal(e)
	}
	if e := report(a, "w1", 1, "checking", 0); e != nil {
		t.Fatal(e)
	}
	var state string
	x.s.Pool.QueryRow(t.Context(), "SELECT status FROM items WHERE job_id=$1 AND external_id=$2", a.JobID, a.From.String()).Scan(&state)
	if state != "downloading" {
		t.Fatal("out of order report regressed state", state)
	}
	if e := report(b, "w2", 1, "download_queued", 60); e == nil {
		t.Fatal("shared disk overcommitted")
	}
	var count int
	x.s.Pool.QueryRow(t.Context(), "SELECT count(*) FROM storage_reservations").Scan(&count)
	if count != 1 {
		t.Fatal("failed report leaked reservation", count)
	}
}
