package db

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/jackc/pgx/v5"
	"github.com/rychkin/Begom/internal/model"
	"net/url"
	"strings"
	"time"
)

// ProgressTx never accepts an agent-supplied checkpoint or a completed file.
func (s *Store) ProgressTx(ctx context.Context, tx pgx.Tx, worker string, p model.Progress, free, minFree uint64) (any, int, error) {
	start, end, e := s.ValidateLeaseTx(ctx, tx, worker, p.LeaseRef)
	if e != nil {
		return nil, 0, e
	}
	if len(p.Results) == 0 || len(p.Results) > 500 || !model.ValidID(p.OperationID) {
		return nil, 0, Bad("invalid_progress", "Invalid result batch")
	}
	var cfgJSON []byte
	var cfg model.JobConfig
	if e = tx.QueryRow(ctx, "SELECT config FROM jobs WHERE id=$1", p.JobID).Scan(&cfgJSON); e != nil {
		return nil, 0, e
	}
	if e = json.Unmarshal(cfgJSON, &cfg); e != nil {
		return nil, 0, e
	}
	accepted := []map[string]any{}
	for _, r := range p.Results {
		if r.ReserveBytes > 2*cfg.MaxFileBytes || r.Offset > cfg.MaxFileBytes {
			return nil, 0, Bad("file_size_limit", "Download exceeds the configured bound")
		}
		if r.ExternalID < start || r.ExternalID > end {
			return nil, 0, Bad("outside_chunk", "Result is outside leased block")
		}
		if r.ErrorCode != "" && !model.ValidID(r.ErrorCode) {
			return nil, 0, Bad("invalid_error_code", "Invalid error code")
		}
		if r.Status == "completed" || r.Status == "cancelled" {
			return nil, 0, Bad("invalid_status", "Completion is confirmed only by publication")
		}
		switch r.Status {
		case "pending", "checking", "not_found", "found", "download_queued", "downloading", "skipped", "forbidden", "rate_limited", "retry_wait", "failed":
		default:
			return nil, 0, Bad("invalid_status", "Unknown processing status")
		}
		var old, oldAttempt, oldSequence string
		var final bool
		e = tx.QueryRow(ctx, "SELECT status,final,attempt_id,sequence::text FROM items WHERE job_id=$1 AND external_id=$2 FOR UPDATE", p.JobID, r.ExternalID.String()).Scan(&old, &final, &oldAttempt, &oldSequence)
		if e != nil && !errors.Is(e, pgx.ErrNoRows) {
			return nil, 0, e
		}
		if final || oldAttempt == p.AttemptID && p.Sequence > 0 && uint64(Number(oldSequence)) >= p.Sequence {
			accepted = append(accepted, map[string]any{"external_id": r.ExternalID, "status": old})
			continue
		}
		var media any
		metadata := r.Metadata
		if metadata != nil {
			if len(metadata.Title) > 4096 || len(metadata.CanonicalID) > 100 || metadata.CanonicalID == "" || len(metadata.PageURL) > 8192 {
				return nil, 0, Bad("invalid_metadata", "Invalid metadata")
			}
			metadata.FoundAt = time.Now().UTC()
			protected, e := s.SealURL(metadata.DownloadURL)
			if e != nil {
				return nil, 0, e
			}
			safe := *metadata
			safe.DownloadURL = ""
			safe.PageURL = SafeURL(metadata.PageURL)
			var mid string
			e = tx.QueryRow(ctx, "INSERT INTO media_metadata(id,source_id,external_id,canonical_id,title,page_url,data,protected_url) VALUES($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT(source_id,canonical_id) DO UPDATE SET protected_url=COALESCE(EXCLUDED.protected_url,media_metadata.protected_url) RETURNING id", model.NewID(), s.SourceID, r.ExternalID.String(), metadata.CanonicalID, metadata.Title, SafeURL(metadata.PageURL), JSON(safe), protected).Scan(&mid)
			if e != nil {
				return nil, 0, e
			}
			media = mid
		} else {
			var mid *string
			e = tx.QueryRow(ctx, "SELECT media_id FROM items WHERE job_id=$1 AND external_id=$2", p.JobID, r.ExternalID.String()).Scan(&mid)
			if e != nil && !errors.Is(e, pgx.ErrNoRows) {
				return nil, 0, e
			}
			if mid != nil {
				media = *mid
			}
		}
		if media != nil && (r.Status == "found" || r.Status == "download_queued" || r.Status == "downloading") {
			var ready bool
			if e = tx.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM files WHERE media_id=$1 AND ready)", media).Scan(&ready); e != nil {
				return nil, 0, e
			}
			if ready {
				r.Status = "completed"
				r.DownloadID = ""
				r.StageID = ""
			}
		}
		stored := r
		stored.Metadata = nil
		_, e = tx.Exec(ctx, "INSERT INTO items(job_id,external_id,run_id,chunk_id,attempt_id,status,final,error_code,media_id,data) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) ON CONFLICT(job_id,external_id) DO UPDATE SET run_id=EXCLUDED.run_id,chunk_id=EXCLUDED.chunk_id,attempt_id=EXCLUDED.attempt_id,status=EXCLUDED.status,final=EXCLUDED.final,error_code=EXCLUDED.error_code,media_id=COALESCE(EXCLUDED.media_id,items.media_id),data=EXCLUDED.data,updated_at=clock_timestamp()", p.JobID, r.ExternalID.String(), p.RunID, p.ChunkID, p.AttemptID, r.Status, model.Final(r.Status), r.ErrorCode, media, JSON(stored))
		if e != nil {
			return nil, 0, e
		}
		if _, e = tx.Exec(ctx, "UPDATE items SET sequence=$3 WHERE job_id=$1 AND external_id=$2", p.JobID, r.ExternalID.String(), p.Sequence); e != nil {
			return nil, 0, e
		}
		if r.DownloadID != "" {
			if !model.ValidID(r.DownloadID) || !model.ValidID(r.StageID) || media == nil {
				return nil, 0, Bad("invalid_download", "Invalid download reference")
			}
			var existingOwner, existingAttempt, existingStage string
			e = tx.QueryRow(ctx, "SELECT worker_id,attempt_id,stage_id FROM downloads WHERE id=$1", r.DownloadID).Scan(&existingOwner, &existingAttempt, &existingStage)
			if e == nil && (existingOwner != worker || existingAttempt != p.AttemptID || existingStage != r.StageID) {
				return nil, 0, Conflict("download_owner_mismatch")
			}
			if e != nil && !errors.Is(e, pgx.ErrNoRows) {
				return nil, 0, e
			}
			_, e = tx.Exec(ctx, "INSERT INTO downloads(id,job_id,external_id,worker_id,attempt_id,stage_id,media_id,state,offset_bytes,etag,checksum) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) ON CONFLICT(id) DO UPDATE SET state=EXCLUDED.state,offset_bytes=EXCLUDED.offset_bytes,etag=EXCLUDED.etag,checksum=EXCLUDED.checksum", r.DownloadID, p.JobID, r.ExternalID.String(), worker, p.AttemptID, r.StageID, media, r.Status, r.Offset, r.ETag, r.Checksum)
			if e != nil {
				return nil, 0, e
			}
			if r.ReserveBytes > 0 {
				if _, e = tx.Exec(ctx, "SELECT pg_advisory_xact_lock(82939104)"); e != nil {
					return nil, 0, e
				}
				var used string
				if e = tx.QueryRow(ctx, "SELECT COALESCE(sum(GREATEST(r.bytes-d.offset_bytes,0)),0)::text FROM storage_reservations r JOIN downloads d ON d.id=r.download_id WHERE r.expires_at>clock_timestamp() AND r.download_id<>$1", r.DownloadID).Scan(&used); e != nil {
					return nil, 0, e
				}
				other := uint64(Number(used))
				var sharedMin string
				if e = tx.QueryRow(ctx, "SELECT COALESCE(max((config->>'min_free_bytes')::numeric),0)::text FROM jobs WHERE state IN ('running','pausing')").Scan(&sharedMin); e != nil {
					return nil, 0, e
				}
				minFree = max(minFree, uint64(Number(sharedMin)))
				remaining := r.ReserveBytes - min(r.ReserveBytes, r.Offset)
				if free < minFree || other > free-minFree || remaining > free-minFree-other {
					return nil, 0, &Error{503, "insufficient_storage", "Storage reserve is exhausted"}
				}
				if _, e = tx.Exec(ctx, "INSERT INTO storage_reservations(download_id,attempt_id,bytes,expires_at) SELECT $1,$2,$3,lease_expires_at FROM range_chunks WHERE id=$4 ON CONFLICT(download_id) DO UPDATE SET bytes=EXCLUDED.bytes,expires_at=EXCLUDED.expires_at", r.DownloadID, p.AttemptID, r.ReserveBytes, p.ChunkID); e != nil {
					return nil, 0, e
				}
			}
		}
		if e = Event(ctx, tx, p.JobID, worker, p.ChunkID, p.AttemptID, "item.result", r.Status, r.ErrorCode, &r.ExternalID); e != nil {
			return nil, 0, e
		}
		accepted = append(accepted, map[string]any{"external_id": r.ExternalID, "status": r.Status})
	}
	if e = s.Advance(ctx, tx, p.JobID, p.ChunkID); e != nil {
		return nil, 0, e
	}
	var next *string
	if e = tx.QueryRow(ctx, "SELECT next_id::text FROM range_chunks WHERE id=$1", p.ChunkID).Scan(&next); e != nil {
		return nil, 0, e
	}
	return map[string]any{"accepted": accepted, "next_id": next}, 200, nil
}
func SafeURL(raw string) string {
	u, e := url.Parse(raw)
	if e != nil {
		return ""
	}
	u.RawQuery = ""
	u.Fragment = ""
	u.User = nil
	return u.String()
}
func (s *Store) Items(ctx context.Context, job, status, after string, limit int) ([]map[string]any, error) {
	if after == "" {
		after = "-1"
	}
	if strings.Trim(after, "0123456789-") != "" {
		return nil, Bad("invalid_cursor", "Invalid item cursor")
	}
	rows, e := s.Pool.Query(ctx, "SELECT i.external_id::text,i.status,i.error_code,i.updated_at,m.data,f.id,f.size_bytes::text,f.checksum,f.storage_path,f.created_at,d.worker_id FROM items i LEFT JOIN media_metadata m ON m.id=i.media_id LEFT JOIN files f ON f.id=m.accepted_file_id AND f.ready AND i.status='completed' LEFT JOIN downloads d ON d.id=f.download_id WHERE i.job_id=$1 AND ($2='' OR i.status=$2) AND i.external_id>$3::numeric ORDER BY i.external_id LIMIT $4", job, status, after, limit)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := []map[string]any{}
	for rows.Next() {
		var id, st, code string
		var at time.Time
		var metadata []byte
		var file, size, checksum, path *string
		var downloaded *time.Time
		var worker *string
		if e = rows.Scan(&id, &st, &code, &at, &metadata, &file, &size, &checksum, &path, &downloaded, &worker); e != nil {
			return nil, e
		}
		v := map[string]any{"external_id": id, "status": st, "error_code": code, "updated_at": at}
		if metadata != nil {
			var m model.Metadata
			if e = json.Unmarshal(metadata, &m); e != nil {
				return nil, e
			}
			m.DownloadURL = ""
			v["metadata"] = m
		}
		if file != nil {
			v["file_id"] = *file
			v["size_bytes"] = size
			v["checksum"] = checksum
			v["storage_path"] = path
			v["downloaded_at"] = downloaded
			v["worker_id"] = worker
		}
		out = append(out, v)
	}
	return out, rows.Err()
}
