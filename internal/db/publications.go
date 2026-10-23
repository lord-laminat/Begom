package db

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"github.com/jackc/pgx/v5"
	"github.com/rychkin/Begom/internal/model"
	"path"
)

type Publication struct {
	ID         string         `json:"publication_id"`
	DownloadID string         `json:"download_id"`
	JobID      string         `json:"job_id"`
	ChunkID    string         `json:"chunk_id"`
	RunID      string         `json:"run_id"`
	AttemptID  string         `json:"attempt_id"`
	WorkerID   string         `json:"-"`
	StageID    string         `json:"-"`
	MediaID    string         `json:"-"`
	ExternalID model.Number   `json:"external_id"`
	State      string         `json:"state"`
	Path       string         `json:"-"`
	Size       uint64         `json:"size_bytes"`
	Checksum   string         `json:"checksum"`
	MIME       string         `json:"mime_type"`
	ErrorCode  string         `json:"error_code,omitempty"`
	Metadata   model.Metadata `json:"metadata"`
}

func (p Publication) Ref() model.LeaseRef {
	return model.LeaseRef{JobID: p.JobID, ChunkID: p.ChunkID, RunID: p.RunID, AttemptID: p.AttemptID}
}
func (s *Store) PreparePublication(ctx context.Context, tx pgx.Tx, worker, download string, c model.Commit) (any, int, error) {
	if _, _, e := s.ValidateLeaseTx(ctx, tx, worker, c.LeaseRef); e != nil {
		return nil, 0, e
	}
	digest, e := hex.DecodeString(c.Checksum)
	if e != nil || len(digest) != 32 || !model.ValidID(c.StageID) || c.Size == 0 {
		return nil, 0, Bad("invalid_commit", "Invalid size, checksum or stage")
	}
	var mid, external, reserved string
	e = tx.QueryRow(ctx, "SELECT d.media_id,d.external_id::text,COALESCE(r.bytes,0)::text FROM downloads d LEFT JOIN storage_reservations r ON r.download_id=d.id AND r.expires_at>clock_timestamp() WHERE d.id=$1 AND d.worker_id=$2 AND d.attempt_id=$3 AND d.stage_id=$4 AND d.job_id=$5 FOR UPDATE OF d", download, worker, c.AttemptID, c.StageID, c.JobID).Scan(&mid, &external, &reserved)
	if errors.Is(e, pgx.ErrNoRows) {
		return nil, 0, Conflict("invalid_download")
	}
	if e != nil {
		return nil, 0, e
	}
	if c.Size > uint64(Number(reserved))/2 {
		return nil, 0, Conflict("insufficient_reservation")
	}
	id := model.NewID()
	rel := path.Join(s.SourceID, external, id, "video")
	var state string
	e = tx.QueryRow(ctx, "INSERT INTO publications(id,download_id,job_id,chunk_id,run_id,attempt_id,state,expected_size,expected_checksum,relative_path) VALUES($1,$2,$3,$4,$5,$6,'verifying',$7,$8,$9) ON CONFLICT(download_id) DO UPDATE SET download_id=EXCLUDED.download_id RETURNING id,state", id, download, c.JobID, c.ChunkID, c.RunID, c.AttemptID, c.Size, c.Checksum, rel).Scan(&id, &state)
	if e != nil {
		return nil, 0, e
	}
	e = Audit(ctx, tx, worker, "publication.prepare", id, "", map[string]string{"download_id": download})
	return map[string]string{"publication_id": id, "state": state}, 202, e
}
func (s *Store) Publication(ctx context.Context, id string) (Publication, error) {
	var p Publication
	var n, size string
	var b []byte
	e := s.Pool.QueryRow(ctx, "SELECT p.id,p.download_id,p.job_id,p.chunk_id,p.run_id,p.attempt_id,p.state,p.relative_path,p.expected_size::text,p.expected_checksum,p.error_code,d.worker_id,d.stage_id,d.media_id,d.external_id::text,m.data FROM publications p JOIN downloads d ON d.id=p.download_id JOIN media_metadata m ON m.id=d.media_id WHERE p.id=$1", id).Scan(&p.ID, &p.DownloadID, &p.JobID, &p.ChunkID, &p.RunID, &p.AttemptID, &p.State, &p.Path, &size, &p.Checksum, &p.ErrorCode, &p.WorkerID, &p.StageID, &p.MediaID, &n, &b)
	if e != nil {
		return p, e
	}
	p.ExternalID = Number(n)
	p.Size = uint64(Number(size))
	e = json.Unmarshal(b, &p.Metadata)
	p.Metadata.DownloadURL = ""
	p.MIME = p.Metadata.MIME
	if p.MIME == "" {
		p.MIME = "video/mp4"
	}
	return p, e
}
func (s *Store) FinishPublication(ctx context.Context, p Publication) (string, error) {
	tx, e := s.Pool.Begin(ctx)
	if e != nil {
		return "", e
	}
	defer tx.Rollback(ctx)
	if _, _, e = s.ValidateLeaseTx(ctx, tx, p.WorkerID, p.Ref()); e != nil {
		return "", e
	}
	var accepted *string
	if e = tx.QueryRow(ctx, "SELECT accepted_file_id FROM media_metadata WHERE id=$1 FOR UPDATE", p.MediaID).Scan(&accepted); e != nil {
		return "", e
	}
	fileID := model.NewID()
	if accepted != nil {
		var ready bool
		if e = tx.QueryRow(ctx, "SELECT ready FROM files WHERE id=$1", *accepted).Scan(&ready); e != nil {
			return "", e
		}
		if ready {
			fileID = *accepted
		} else {
			accepted = nil
		}
	}
	if accepted == nil {
		if _, e = tx.Exec(ctx, "INSERT INTO files(id,download_id,media_id,publication_id,storage_path,size_bytes,checksum,mime_type,ready) VALUES($1,$2,$3,$4,$5,$6,$7,$8,true)", fileID, p.DownloadID, p.MediaID, p.ID, p.Path, p.Size, p.Checksum, p.MIME); e != nil {
			return "", e
		}
		if _, e = tx.Exec(ctx, "UPDATE media_metadata SET accepted_file_id=$2 WHERE id=$1", p.MediaID, fileID); e != nil {
			return "", e
		}
	}
	if _, e = tx.Exec(ctx, "UPDATE publications SET state='ready',updated_at=clock_timestamp(),reconciled_at=CASE WHEN $2 THEN clock_timestamp() ELSE NULL END WHERE id=$1", p.ID, accepted == nil); e != nil {
		return "", e
	}
	if _, e = tx.Exec(ctx, "UPDATE downloads SET state='completed',size_bytes=$2,offset_bytes=$2,checksum=$3 WHERE id=$1", p.DownloadID, p.Size, p.Checksum); e != nil {
		return "", e
	}
	if _, e = tx.Exec(ctx, "DELETE FROM storage_reservations WHERE download_id=$1", p.DownloadID); e != nil {
		return "", e
	}
	if _, e = tx.Exec(ctx, "UPDATE items SET status='completed',final=true,updated_at=clock_timestamp() WHERE job_id=$1 AND external_id=$2", p.JobID, p.ExternalID.String()); e != nil {
		return "", e
	}
	if e = s.Advance(ctx, tx, p.JobID, p.ChunkID); e != nil {
		return "", e
	}
	if e = Audit(ctx, tx, p.WorkerID, "publication.ready", p.ID, "", map[string]string{"file_id": fileID}); e != nil {
		return "", e
	}
	if e = Event(ctx, tx, p.JobID, p.WorkerID, p.ChunkID, p.AttemptID, "publication.ready", "completed", "", &p.ExternalID); e != nil {
		return "", e
	}
	return fileID, tx.Commit(ctx)
}
func (s *Store) MarkPublication(ctx context.Context, id, state, code string) error {
	_, e := s.Pool.Exec(ctx, "UPDATE publications SET state=$2,error_code=$3,updated_at=clock_timestamp() WHERE id=$1 AND state<>'ready'", id, state, code)
	return e
}
