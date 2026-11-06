package publication

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"image/jpeg"
	"io"
	"math"
	"os"
	"os/exec"
	"path"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/rychkin/Begom/internal/storage"
)

type previewBuffer struct{ bytes.Buffer }

func (b *previewBuffer) Write(p []byte) (int, error) {
	if len(p) > 2<<20-b.Len() {
		return 0, errors.New("preview_too_large")
	}
	return b.Buffer.Write(p)
}

// Preview processes one committed file. The advisory lock also fences recovery
// from an earlier coordinator; preview failure never withdraws the video.
func (s *Service) Preview(ctx context.Context) (bool, error) {
	conn, e := s.Store.Pool.Acquire(ctx)
	if e != nil {
		return false, e
	}
	defer conn.Release()
	var locked bool
	if e = conn.QueryRow(ctx, "SELECT pg_try_advisory_lock(82939109)").Scan(&locked); e != nil || !locked {
		return false, e
	}
	defer conn.Exec(context.Background(), "SELECT pg_advisory_unlock(82939109)")
	if _, e = conn.Exec(ctx, "INSERT INTO file_previews(file_id) SELECT id FROM files WHERE ready ON CONFLICT DO NOTHING"); e != nil {
		return false, e
	}
	var id, rel string
	var attempt int
	e = conn.QueryRow(ctx, `UPDATE file_previews p SET state='processing',attempts=attempts+1,claimed_at=clock_timestamp(),updated_at=clock_timestamp() WHERE file_id=(SELECT p.file_id FROM file_previews p JOIN files f ON f.id=p.file_id WHERE f.ready AND (p.state='pending' OR p.state='processing' OR p.state='failed' AND p.attempts<3 AND p.updated_at<clock_timestamp()-interval '30 seconds') ORDER BY p.updated_at,p.file_id LIMIT 1) RETURNING file_id,attempts,(SELECT storage_path FROM files WHERE id=p.file_id)`).Scan(&id, &attempt, &rel)
	if errors.Is(e, pgx.ErrNoRows) {
		return false, nil
	}
	if e != nil {
		return false, e
	}
	qctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	thumb, size, hash, duration, err := s.generatePreview(qctx, id, rel)
	if err != nil {
		_, e = conn.Exec(ctx, "UPDATE file_previews SET state='failed',error_code='preview_generation_failed',updated_at=clock_timestamp() WHERE file_id=$1 AND attempts=$2", id, attempt)
		return true, e
	}
	_, e = conn.Exec(ctx, "UPDATE file_previews SET state='ready',relative_path=$3,size_bytes=$4,checksum=$5,duration_ms=$6,error_code='',updated_at=clock_timestamp() WHERE file_id=$1 AND attempts=$2 AND EXISTS(SELECT 1 FROM files f WHERE f.id=$1 AND f.ready)", id, attempt, thumb, size, hash, duration)
	return true, e
}

func (s *Service) generatePreview(ctx context.Context, id, rel string) (string, uint64, string, *int64, error) {
	if !storage.ValidPath(rel) {
		return "", 0, "", nil, storage.ErrUnsafePath
	}
	root, e := s.Volume.OpenRoot()
	if e != nil {
		return "", 0, "", nil, e
	}
	defer root.Close()
	if _, e = storage.StatRegular(root, path.Join("published", rel)); e != nil {
		return "", 0, "", nil, e
	}
	input, e := root.Open(path.Join("published", rel))
	if e != nil {
		return "", 0, "", nil, e
	}
	defer input.Close()
	probe := exec.CommandContext(ctx, "ffprobe", "-v", "error", "-protocol_whitelist", "file,pipe", "-format_whitelist", "mov,matroska,webm,avi", "-show_entries", "format=duration", "-of", "json", "/proc/self/fd/3")
	probe.ExtraFiles = []*os.File{input}
	var probeOut previewBuffer
	probe.Stdout = &probeOut
	if e = probe.Run(); e != nil {
		return "", 0, "", nil, e
	}
	var info struct {
		Format struct {
			Duration string `json:"duration"`
		} `json:"format"`
	}
	if e = json.Unmarshal(probeOut.Bytes(), &info); e != nil {
		return "", 0, "", nil, e
	}
	var duration *int64
	seek := 0.0
	if seconds, err := strconv.ParseFloat(info.Format.Duration, 64); err == nil && seconds > 0 && !math.IsInf(seconds, 0) && seconds < 1e12 {
		ms := int64(seconds * 1000)
		duration = &ms
		seek = math.Min(seconds/10, 10)
	}
	if _, e = input.Seek(0, io.SeekStart); e != nil {
		return "", 0, "", nil, e
	}
	cmd := exec.CommandContext(ctx, "ffmpeg", "-nostdin", "-v", "error", "-threads", "1", "-filter_threads", "1", "-protocol_whitelist", "file,pipe", "-format_whitelist", "mov,matroska,webm,avi", "-ss", strconv.FormatFloat(seek, 'f', 3, 64), "-i", "/proc/self/fd/3", "-an", "-vf", "scale=320:180:force_original_aspect_ratio=decrease,pad=320:180:(ow-iw)/2:(oh-ih)/2", "-frames:v", "1", "-threads", "1", "-f", "image2pipe", "-vcodec", "mjpeg", "pipe:1")
	cmd.ExtraFiles = []*os.File{input}
	var image previewBuffer
	cmd.Stdout = &image
	if e = cmd.Run(); e != nil {
		return "", 0, "", nil, e
	}
	shape, e := jpeg.DecodeConfig(bytes.NewReader(image.Bytes()))
	if e != nil || shape.Width != 320 || shape.Height != 180 {
		return "", 0, "", nil, errors.New("invalid_preview")
	}
	if e = storage.MkdirAll(root, "publishing", 0750); e != nil {
		return "", 0, "", nil, e
	}
	tmp := path.Join("publishing", "preview-"+id+".part")
	defer root.Remove(tmp)
	root.Remove(tmp)
	f, e := root.OpenFile(tmp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0640)
	if e != nil {
		return "", 0, "", nil, e
	}
	_, e = f.Write(image.Bytes())
	if e == nil {
		e = f.Sync()
	}
	ce := f.Close()
	if e == nil {
		e = ce
	}
	if e != nil {
		return "", 0, "", nil, e
	}
	size, hash, e := storage.Hash(root, tmp)
	if e != nil {
		return "", 0, "", nil, e
	}
	thumb := path.Join(path.Dir(rel), "thumbnail.jpg")
	if _, e = s.Volume.Check(); e != nil {
		return "", 0, "", nil, e
	}
	if e = root.Rename(tmp, path.Join("published", thumb)); e != nil {
		return "", 0, "", nil, e
	}
	if e = storage.SyncDir(root, path.Join("published", path.Dir(rel))); e != nil {
		return "", 0, "", nil, e
	}
	return thumb, size, hash, duration, nil
}

func (s *Service) ReconcilePreviews(ctx context.Context) error {
	root, e := s.Volume.OpenRoot()
	if e != nil {
		return e
	}
	defer root.Close()
	rows, e := s.Store.Pool.Query(ctx, "SELECT p.file_id,p.relative_path,p.size_bytes,p.checksum FROM file_previews p JOIN files f ON f.id=p.file_id WHERE p.state='ready' AND f.ready")
	if e != nil {
		return e
	}
	type record struct {
		id, rel, hash string
		size          uint64
	}
	var records []record
	for rows.Next() {
		var r record
		if e = rows.Scan(&r.id, &r.rel, &r.size, &r.hash); e != nil {
			rows.Close()
			return e
		}
		records = append(records, r)
	}
	rows.Close()
	if e = rows.Err(); e != nil {
		return e
	}
	for _, r := range records {
		size, hash, err := storage.Hash(root, path.Join("published", r.rel))
		if err != nil && !errors.Is(err, os.ErrNotExist) && !errors.Is(err, storage.ErrUnsafePath) {
			return err
		}
		if err != nil || size != r.size || hash != r.hash {
			if _, e = s.Store.Pool.Exec(ctx, "UPDATE file_previews SET state='pending',attempts=0,error_code='preview_missing_or_changed',updated_at=clock_timestamp() WHERE file_id=$1", r.id); e != nil {
				return e
			}
		}
	}
	return nil
}
