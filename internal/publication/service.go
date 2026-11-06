package publication

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/jackc/pgx/v5"
	"github.com/rychkin/Begom/internal/db"
	"github.com/rychkin/Begom/internal/storage"
	"io"
	"log/slog"
	"os"
	"path"
	"time"
)

type Service struct {
	Store  *db.Store
	Volume *storage.Volume
	Log    *slog.Logger
}

func (s *Service) Publish(ctx context.Context, id string) error {
	p, e := s.Store.Publication(ctx, id)
	if e != nil {
		return e
	}
	if p.State != "verifying" {
		return nil
	}
	if _, e = s.Store.CheckLease(ctx, p.WorkerID, p.Ref()); e != nil {
		return s.Store.MarkPublication(ctx, id, "orphan", "stale_attempt")
	}
	root, e := s.Volume.OpenRoot()
	if e != nil {
		return e
	}
	defer root.Close()
	stage := storage.Stage(p.WorkerID, p.AttemptID, p.StageID)
	dest := path.Join("published", path.Dir(p.Path))
	source := path.Join(stage, "video.part")
	// A rename may have succeeded before an interrupted SQL commit.
	_, statErr := root.Stat(path.Join("published", p.Path))
	alreadyMoved := statErr == nil
	if alreadyMoved {
		source = path.Join("published", p.Path)
	} else if !errors.Is(statErr, os.ErrNotExist) {
		return statErr
	}
	if !alreadyMoved {
		if _, se := root.Lstat(source); errors.Is(se, os.ErrNotExist) {
			if _, ve := storage.StatRegular(root, path.Join(stage, "video")); ve == nil {
				source = path.Join(stage, "video")
			} else {
				return ve
			}
		} else if se != nil {
			return se
		}
	}
	size, checksum, e := storage.Hash(root, source)
	if e != nil {
		return e
	}
	if size != p.Size || checksum != p.Checksum {
		return s.Store.MarkPublication(ctx, id, "failed", "checksum_mismatch")
	}
	manifest := path.Join(stage, "metadata.json")
	if alreadyMoved {
		manifest = path.Join(dest, "metadata.json")
	}
	f, e := root.Open(manifest)
	if e != nil {
		return s.Store.MarkPublication(ctx, id, "failed", "missing_metadata")
	}
	var m struct {
		CanonicalID string `json:"canonical_id"`
	}
	e = json.NewDecoder(io.LimitReader(f, 2<<20)).Decode(&m)
	f.Close()
	if e != nil || m.CanonicalID != p.Metadata.CanonicalID {
		return s.Store.MarkPublication(ctx, id, "failed", "invalid_metadata")
	}
	if _, e = s.Store.CheckLease(ctx, p.WorkerID, p.Ref()); e != nil {
		return s.Store.MarkPublication(ctx, id, "orphan", "stale_attempt")
	}
	if !alreadyMoved {
		// Never publish an inode still owned or held open by an agent. Copy to
		// the publisher's private namespace, verify, then atomically rename.
		private := path.Join("publishing", id)
		if e = storage.MkdirAll(root, private, 0750); e != nil {
			return e
		}
		copyOne := func(src, dst string) error {
			if _, err := storage.StatRegular(root, src); err != nil {
				return err
			}
			in, err := root.Open(src)
			if err != nil {
				return err
			}
			defer in.Close()
			// A partial copy belongs exclusively to this publication.
			root.Remove(dst)
			out, err := root.OpenFile(dst, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0640)
			if err != nil {
				return err
			}
			_, err = io.Copy(out, io.LimitReader(in, int64(p.Size)+1))
			if err == nil {
				err = out.Sync()
			}
			ce := out.Close()
			if err == nil {
				err = ce
			}
			return err
		}
		if e = copyOne(source, path.Join(private, "video")); e != nil {
			return e
		}
		if e = storage.WriteJSON(root, path.Join(private, "metadata.json"), p.Metadata); e != nil {
			return e
		}
		copied, hash, err := storage.Hash(root, path.Join(private, "video"))
		if err != nil {
			return err
		}
		if copied != p.Size || hash != p.Checksum {
			return s.Store.MarkPublication(ctx, id, "failed", "checksum_mismatch")
		}
		if e = storage.WriteJSON(root, path.Join(private, "publication.json"), map[string]any{"publication_id": id, "checksum": checksum, "size_bytes": size}); e != nil {
			return e
		}
		if e = storage.SyncDir(root, private); e != nil {
			return e
		}
		if e = storage.MkdirAll(root, path.Dir(dest), 0750); e != nil {
			return e
		}
		if _, e = s.Volume.Check(); e != nil {
			return e
		}
		if e = root.Rename(private, dest); e != nil {
			return e
		}
		if e = storage.SyncDir(root, path.Dir(dest)); e != nil {
			return e
		}
		if e = storage.SyncDir(root, "publishing"); e != nil {
			return e
		}
	}
	_, e = s.Store.FinishPublication(ctx, p)
	if e != nil {
		var de *db.Error
		if errors.As(e, &de) && de.Status == 409 {
			return s.Store.MarkPublication(ctx, id, "orphan", "stale_attempt")
		}
		return e
	}
	s.Log.Info("publication complete", "operation", "publication.ready", "result", "ok", "job_id", p.JobID, "publication_id", id)
	if e = root.RemoveAll(stage); e != nil {
		s.Log.Warn("temporary file cleanup deferred", "operation", "stage.cleanup", "result", "retry", "error_code", "storage_cleanup_failed")
	}
	return s.cleanupDuplicates(ctx)
}

// Concurrent discoveries can finish downloading the same canonical material
// before either file is ready. SQL chooses one file; remove only the losing
// publication's private directories. The SQL selection makes cleanup recoverable
// after a crash immediately following FinishPublication.
func (s *Service) cleanupDuplicates(ctx context.Context) error {
	rows, e := s.Store.Pool.Query(ctx, "SELECT p.id,p.relative_path FROM publications p WHERE p.state='ready' AND p.reconciled_at IS NULL AND NOT EXISTS(SELECT 1 FROM files f WHERE f.publication_id=p.id) ORDER BY p.created_at LIMIT 100")
	if e != nil {
		return e
	}
	type duplicate struct{ id, path string }
	var copies []duplicate
	for rows.Next() {
		var d duplicate
		if e = rows.Scan(&d.id, &d.path); e != nil {
			rows.Close()
			return e
		}
		copies = append(copies, d)
	}
	rows.Close()
	if e = rows.Err(); e != nil || len(copies) == 0 {
		return e
	}
	root, e := s.Volume.OpenRoot()
	if e != nil {
		return e
	}
	defer root.Close()
	for _, d := range copies {
		if !storage.ValidPath(d.path) || path.Base(path.Dir(d.path)) != d.id || path.Base(d.path) != "video" {
			return storage.ErrUnsafePath
		}
		for _, dir := range []string{path.Join("published", path.Dir(d.path)), path.Join("publishing", d.id)} {
			if _, e = s.Volume.Check(); e != nil {
				return e
			}
			if e = root.RemoveAll(dir); e != nil {
				return e
			}
			if e = storage.SyncDir(root, path.Dir(dir)); e != nil && !errors.Is(e, os.ErrNotExist) {
				return e
			}
		}
		if _, e = s.Store.Pool.Exec(ctx, "UPDATE publications SET reconciled_at=clock_timestamp() WHERE id=$1", d.id); e != nil {
			return e
		}
	}
	return nil
}

func (s *Service) Reconcile(ctx context.Context) error {
	if _, e := s.Volume.Check(); e != nil {
		return e
	}
	rows, e := s.Store.Pool.Query(ctx, "SELECT id FROM publications WHERE state='verifying' ORDER BY created_at LIMIT 100")
	if e != nil {
		return e
	}
	var ids []string
	for rows.Next() {
		var id string
		if e = rows.Scan(&id); e != nil {
			rows.Close()
			return e
		}
		ids = append(ids, id)
	}
	rows.Close()
	if e = rows.Err(); e != nil {
		return e
	}
	for _, id := range ids {
		if e = s.Publish(ctx, id); e != nil {
			s.Log.Warn("publication awaits recovery", "operation", "publication.reconcile", "result", "retry", "error_code", "storage_or_database_unavailable", "publication_id", id)
		}
	}
	root, e := s.Volume.OpenRoot()
	if e != nil {
		return e
	}
	defer root.Close()
	rows, e = s.Store.Pool.Query(ctx, "SELECT id,storage_path,size_bytes::text FROM files WHERE ready ORDER BY created_at")
	if e != nil {
		return e
	}
	type file struct{ id, p, size string }
	var files []file
	for rows.Next() {
		var f file
		if e = rows.Scan(&f.id, &f.p, &f.size); e != nil {
			rows.Close()
			return e
		}
		files = append(files, f)
	}
	rows.Close()
	for _, f := range files {
		st, e := storage.StatRegular(root, path.Join("published", f.p))
		if e != nil && !errors.Is(e, os.ErrNotExist) && !errors.Is(e, storage.ErrUnsafePath) {
			return e
		}
		if errors.Is(e, os.ErrNotExist) || errors.Is(e, storage.ErrUnsafePath) || st != nil && uint64(st.Size()) != uint64(db.Number(f.size)) {
			if e = s.invalidate(ctx, f.id, "missing_or_changed"); e != nil {
				return e
			}
			s.Log.Error("catalog file unavailable", "operation", "file.reconcile", "result", "invalid", "error_code", "missing_or_changed", "file_id", f.id)
		}
	}
	return rows.Err()
}

// Scrub independently rehashes published inodes in bounded batches. Invalid
// records are withdrawn transactionally before physical quarantine.
func (s *Service) Scrub(ctx context.Context, limit int) error {
	if limit < 1 || limit > 1000 {
		return errors.New("invalid scrub batch")
	}
	root, e := s.Volume.OpenRoot()
	if e != nil {
		return e
	}
	defer root.Close()
	rows, e := s.Store.Pool.Query(ctx, "SELECT id,storage_path,size_bytes::text,checksum FROM files WHERE ready ORDER BY verified_at NULLS FIRST,id LIMIT $1", limit)
	if e != nil {
		return e
	}
	type record struct{ id, p, size, hash string }
	records := []record{}
	for rows.Next() {
		var f record
		if e = rows.Scan(&f.id, &f.p, &f.size, &f.hash); e != nil {
			rows.Close()
			return e
		}
		records = append(records, f)
	}
	rows.Close()
	if e = rows.Err(); e != nil {
		return e
	}
	for _, f := range records {
		if _, e = s.Volume.Check(); e != nil {
			return e
		}
		size, hash, e := storage.Hash(root, path.Join("published", f.p))
		if e != nil && !errors.Is(e, os.ErrNotExist) && !errors.Is(e, storage.ErrUnsafePath) {
			return e
		}
		if e != nil || size != uint64(db.Number(f.size)) || hash != f.hash {
			if e = s.invalidate(ctx, f.id, "catalog_checksum_mismatch"); e != nil {
				return e
			}
			continue
		}
		if _, e = s.Store.Pool.Exec(ctx, "UPDATE files SET verified_at=clock_timestamp() WHERE id=$1 AND ready", f.id); e != nil {
			return e
		}
	}
	return s.quarantine(ctx)
}
func (s *Service) invalidate(ctx context.Context, id, code string) error {
	tx, e := s.Store.Pool.Begin(ctx)
	if e != nil {
		return e
	}
	defer tx.Rollback(ctx)
	var media, pub string
	e = tx.QueryRow(ctx, "UPDATE files SET ready=false WHERE id=$1 AND ready RETURNING media_id,publication_id", id).Scan(&media, &pub)
	if errors.Is(e, pgx.ErrNoRows) {
		return nil
	}
	if e != nil {
		return e
	}
	if _, e = tx.Exec(ctx, "UPDATE media_metadata SET accepted_file_id=NULL WHERE id=$1 AND accepted_file_id=$2", media, id); e != nil {
		return e
	}
	if _, e = tx.Exec(ctx, "UPDATE items SET status='failed',error_code=$2,updated_at=clock_timestamp() WHERE media_id=$1 AND status='completed'", media, code); e != nil {
		return e
	}
	if _, e = tx.Exec(ctx, "UPDATE publications SET state='failed',error_code=$2,reconciled_at=NULL WHERE id=$1", pub, code); e != nil {
		return e
	}
	if e = db.Audit(ctx, tx, "system", "catalog.withdraw", id, "", map[string]string{"error_code": code}); e != nil {
		return e
	}
	return tx.Commit(ctx)
}
func (s *Service) quarantine(ctx context.Context) error {
	if e := s.cleanupDuplicates(ctx); e != nil {
		return e
	}
	root, e := s.Volume.OpenRoot()
	if e != nil {
		return e
	}
	defer root.Close()
	rows, e := s.Store.Pool.Query(ctx, "SELECT id,relative_path FROM publications WHERE state IN ('failed','orphan') AND reconciled_at IS NULL ORDER BY created_at LIMIT 100")
	if e != nil {
		return e
	}
	type record struct{ id, p string }
	records := []record{}
	for rows.Next() {
		var p record
		if e = rows.Scan(&p.id, &p.p); e != nil {
			rows.Close()
			return e
		}
		records = append(records, p)
	}
	rows.Close()
	if e = rows.Err(); e != nil {
		return e
	}
	if e = storage.MkdirAll(root, "quarantine", 0700); e != nil {
		return e
	}
	for _, p := range records {
		if _, e = s.Volume.Check(); e != nil {
			return e
		}
		for _, src := range []string{path.Join("published", path.Dir(p.p)), path.Join("publishing", p.id)} {
			if _, e = root.Lstat(src); errors.Is(e, os.ErrNotExist) {
				continue
			} else if e != nil {
				return e
			}
			if e = root.Rename(src, path.Join("quarantine", p.id+"-"+path.Base(path.Dir(src)))); e != nil {
				return e
			}
		}
		if _, e = s.Store.Pool.Exec(ctx, "UPDATE publications SET reconciled_at=clock_timestamp() WHERE id=$1", p.id); e != nil {
			return e
		}
	}
	return storage.SyncDir(root, "quarantine")
}
func (s *Service) Run(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if e := s.Scrub(ctx, 100); e != nil {
					s.Log.Warn("catalog scrub deferred", "operation", "catalog.scrub", "result", "retry", "error_code", "dependency_unavailable")
				}
			}
		}
	}()
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if e := s.Reconcile(ctx); e != nil {
				s.Log.Warn("recovery dependency unavailable", "operation", "recovery", "result", "retry", "error_code", "dependency_unavailable")
			}
			if e := s.quarantine(ctx); e != nil {
				s.Log.Warn("quarantine deferred", "operation", "catalog.quarantine", "result", "retry", "error_code", "dependency_unavailable")
			}
		}
	}
}
