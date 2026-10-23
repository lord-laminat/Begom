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

func TestSimultaneousCanonicalDownloadsLeaveOnePublishedCopy(t *testing.T) {
	x := setup(t)
	x.job(t, "[0,1]", 1)
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, ".begom-volume"), []byte("dedup-volume"), 0444)
	root, e := os.OpenRoot(dir)
	if e != nil {
		t.Fatal(e)
	}
	defer root.Close()
	data := []byte("same authorized video")
	hash := sha256.Sum256(data)
	sum := hex.EncodeToString(hash[:])
	meta := model.Metadata{CanonicalID: "same-canonical", Title: "Video", MIME: "video/mp4", Size: uint64(len(data))}
	var pubs []db.Publication
	for _, worker := range []string{"w1", "w2"} {
		lease := x.lease(t, worker)
		if lease == nil {
			t.Fatal("missing lease")
		}
		x.s.Pool.Exec(t.Context(), "UPDATE range_chunks SET lease_expires_at=clock_timestamp()+interval '30 seconds' WHERE id=$1", lease.ChunkID)
		download, stage := model.NewID(), model.NewID()
		p := model.Progress{LeaseRef: lease.Ref(), OperationID: model.NewID(), Results: []model.ItemResult{{ExternalID: lease.From, Status: "download_queued", Metadata: &meta, DownloadID: download, StageID: stage, ReserveBytes: uint64(2 * len(data))}}}
		mutate(t, x.s, worker, "progress", p, func(tx pgx.Tx) (any, int, error) { return x.s.ProgressTx(t.Context(), tx, worker, p, 1<<40, 0) })
		stagePath := storage.Stage(worker, lease.AttemptID, stage)
		if e = storage.MkdirAll(root, stagePath, 0770); e != nil {
			t.Fatal(e)
		}
		if e = storage.WriteJSON(root, stagePath+"/metadata.json", meta); e != nil {
			t.Fatal(e)
		}
		if e = os.WriteFile(filepath.Join(dir, stagePath, "video.part"), data, 0640); e != nil {
			t.Fatal(e)
		}
		c := model.Commit{LeaseRef: lease.Ref(), StageID: stage, Size: uint64(len(data)), Checksum: sum}
		r := mutate(t, x.s, worker, "commit", c, func(tx pgx.Tx) (any, int, error) { return x.s.PreparePublication(t.Context(), tx, worker, download, c) })
		var reply struct {
			ID string `json:"publication_id"`
		}
		json.Unmarshal(r.Body, &reply)
		pub, e := x.s.Publication(t.Context(), reply.ID)
		if e != nil {
			t.Fatal(e)
		}
		pubs = append(pubs, pub)
	}
	svc := &publication.Service{Store: x.s, Volume: &storage.Volume{Config: config.Storage{Root: dir, Mode: "test", VolumeID: "dedup-volume"}}, Log: slog.New(slog.NewJSONHandler(io.Discard, nil))}
	for _, p := range pubs {
		if e = svc.Publish(t.Context(), p.ID); e != nil {
			t.Fatal(e)
		}
	}
	if _, e = root.Stat("published/" + pubs[1].Path); !os.IsNotExist(e) {
		t.Fatal("duplicate physical publication left behind", e)
	}
	// Recreate the post-commit/pre-cleanup crash state and run recovery.
	copyDir := filepath.Join(dir, "published", filepath.Dir(pubs[1].Path))
	os.MkdirAll(copyDir, 0750)
	os.WriteFile(filepath.Join(copyDir, "video"), data, 0640)
	x.s.Pool.Exec(t.Context(), "UPDATE publications SET reconciled_at=NULL WHERE id=$1", pubs[1].ID)
	if e = svc.Scrub(t.Context(), 100); e != nil {
		t.Fatal(e)
	}
	if _, e = root.Stat("published/" + pubs[1].Path); !os.IsNotExist(e) {
		t.Fatal("interrupted duplicate cleanup not recovered", e)
	}
	_, got, e := storage.Hash(root, "published/"+pubs[0].Path)
	if e != nil || got != sum {
		t.Fatal("accepted file was modified by duplicate cleanup", got, e)
	}
}
