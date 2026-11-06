package db

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/rychkin/Begom/internal/model"
)

type CatalogItem struct {
	ID         string         `json:"media_id"`
	ExternalID model.Number   `json:"external_id"`
	Metadata   model.Metadata `json:"metadata"`
	CheckedAt  time.Time      `json:"checked_at"`
}

type FileRecord struct {
	ID            string         `json:"file_id"`
	MediaID       string         `json:"media_id"`
	ExternalID    model.Number   `json:"external_id"`
	Metadata      model.Metadata `json:"metadata"`
	Size          model.Number   `json:"size_bytes"`
	Checksum      string         `json:"checksum"`
	MIME          string         `json:"mime_type"`
	CreatedAt     time.Time      `json:"downloaded_at"`
	PreviewState  string         `json:"preview_state"`
	DurationMS    *int64         `json:"duration_ms"`
	StoragePath   string         `json:"-"`
	ThumbnailPath string         `json:"-"`
	ThumbnailSize int64          `json:"-"`
}

type ListQuery struct {
	Q, Job, Quality, Sort, After string
	Limit                        int
}
type listCursor struct {
	ID    string    `json:"id"`
	At    time.Time `json:"at"`
	Title string    `json:"title"`
}

func (q ListQuery) validate() (listCursor, error) {
	var c listCursor
	if q.Limit < 1 || q.Limit > 200 || len(q.Q) > 200 || len(q.Quality) > 50 || q.Job != "" && !model.ValidID(q.Job) {
		return c, Bad("invalid_query", "Invalid search or pagination parameters")
	}
	if q.Sort != "" && q.Sort != "newest" && q.Sort != "oldest" && q.Sort != "title" {
		return c, Bad("invalid_sort", "Choose newest, oldest or title")
	}
	if q.After != "" {
		b, e := base64.RawURLEncoding.DecodeString(q.After)
		if e != nil || len(b) > 8192 || json.Unmarshal(b, &c) != nil || !model.ValidID(c.ID) {
			return c, Bad("invalid_cursor", "Invalid page cursor")
		}
	}
	return c, nil
}
func cursor(id string, at time.Time, title string) string {
	return base64.RawURLEncoding.EncodeToString(JSON(listCursor{id, at, strings.ToLower(title)}))
}
func searchPattern(q string) string {
	return "%" + strings.NewReplacer("\\", "\\\\", "%", "\\%", "_", "\\_").Replace(q) + "%"
}

func (s *Store) Catalog(ctx context.Context, user string, admin bool, q ListQuery) ([]CatalogItem, string, error) {
	c, e := q.validate()
	if e != nil {
		return nil, "", e
	}
	rows, e := s.Pool.Query(ctx, `SELECT m.id,(SELECT min(i.external_id)::text FROM items i JOIN jobs j ON j.id=i.job_id JOIN media_aliases ma ON ma.media_id=m.id AND ma.source_id=m.source_id AND ma.external_id=i.external_id WHERE i.media_id=m.id AND i.status='available' AND EXISTS(SELECT 1 FROM media_aliases current WHERE current.media_id=m.id AND current.source_id=m.source_id AND current.external_id=i.external_id) AND j.kind='discovery' AND ($4='' OR j.id=$4) AND ($3 OR EXISTS(SELECT 1 FROM job_access a WHERE a.job_id=j.id AND a.user_id=$2))),m.data,m.checked_at,m.found_at FROM media_metadata m WHERE m.source_id=$1 AND m.downloadable
 AND EXISTS(SELECT 1 FROM items i JOIN jobs j ON j.id=i.job_id WHERE i.media_id=m.id AND i.status='available' AND EXISTS(SELECT 1 FROM media_aliases current WHERE current.media_id=m.id AND current.source_id=m.source_id AND current.external_id=i.external_id) AND j.kind='discovery' AND ($4='' OR j.id=$4) AND ($3 OR EXISTS(SELECT 1 FROM job_access a WHERE a.job_id=j.id AND a.user_id=$2)))
 AND ($5='' OR m.title ILIKE $6 OR EXISTS(SELECT 1 FROM media_aliases current WHERE current.media_id=m.id AND current.external_id::text=$5) OR m.canonical_id=$5)
 AND ($7='' OR (m.found_at,m.id)<($8,$7)) ORDER BY m.found_at DESC,m.id DESC LIMIT $9`, s.SourceID, user, admin, q.Job, q.Q, searchPattern(q.Q), c.ID, c.At, q.Limit)
	if e != nil {
		return nil, "", e
	}
	defer rows.Close()
	out := []CatalogItem{}
	next := ""
	for rows.Next() {
		var v CatalogItem
		var id string
		var data []byte
		var at time.Time
		if e = rows.Scan(&v.ID, &id, &data, &v.CheckedAt, &at); e != nil {
			return nil, "", e
		}
		v.ExternalID = Number(id)
		if e = json.Unmarshal(data, &v.Metadata); e != nil {
			return nil, "", e
		}
		v.Metadata.DownloadURL = ""
		out = append(out, v)
		next = cursor(v.ID, at, v.Metadata.Title)
	}
	if len(out) < q.Limit {
		next = ""
	}
	return out, next, rows.Err()
}

const fileSelect = `SELECT f.id,f.media_id,d.external_id::text,f.metadata,f.size_bytes::text,f.checksum,f.mime_type,f.created_at,COALESCE(p.state,'pending'),p.duration_ms,f.storage_path,COALESCE(p.relative_path,''),COALESCE(p.size_bytes,0) FROM files f JOIN downloads d ON d.id=f.download_id LEFT JOIN file_previews p ON p.file_id=f.id `
const fileAccess = ` f.ready AND ($2 OR EXISTS(SELECT 1 FROM items i JOIN job_access a ON a.job_id=i.job_id WHERE i.media_id=f.media_id AND i.status='completed' AND a.user_id=$1)) `

func readFile(row pgx.Row) (FileRecord, error) {
	var v FileRecord
	var id, size string
	var data []byte
	e := row.Scan(&v.ID, &v.MediaID, &id, &data, &size, &v.Checksum, &v.MIME, &v.CreatedAt, &v.PreviewState, &v.DurationMS, &v.StoragePath, &v.ThumbnailPath, &v.ThumbnailSize)
	if e != nil {
		return v, e
	}
	v.ExternalID = Number(id)
	v.Size = Number(size)
	e = json.Unmarshal(data, &v.Metadata)
	v.Metadata.DownloadURL = ""
	return v, e
}
func (s *Store) File(ctx context.Context, user string, admin bool, id string) (FileRecord, error) {
	return readFile(s.Pool.QueryRow(ctx, fileSelect+" WHERE "+fileAccess+" AND f.id=$3", user, admin, id))
}
func (s *Store) Files(ctx context.Context, user string, admin bool, q ListQuery) ([]FileRecord, string, error) {
	c, e := q.validate()
	if e != nil {
		return nil, "", e
	}
	order := "f.created_at DESC,f.id DESC"
	bound := "(f.created_at,f.id)<($9,$8)"
	if q.Sort == "oldest" {
		order = "f.created_at,f.id"
		bound = "(f.created_at,f.id)>($9,$8)"
	}
	if q.Sort == "title" {
		order = "lower(f.metadata->>'title'),f.id"
		bound = "(lower(f.metadata->>'title'),f.id)>($10,$8)"
	}
	rows, e := s.Pool.Query(ctx, fileSelect+" WHERE "+fileAccess+` AND ($3='' OR EXISTS(SELECT 1 FROM items i JOIN job_access a ON a.job_id=i.job_id WHERE i.media_id=f.media_id AND i.status='completed' AND i.job_id=$3 AND ($2 OR a.user_id=$1)))
 AND ($4='' OR f.metadata->>'title' ILIKE $5 OR d.external_id::text=$4 OR f.metadata->>'canonical_id'=$4) AND ($6='' OR f.metadata->>'quality'=$6)
 AND ($8='' OR `+bound+`) AND $9::timestamptz IS NOT NULL AND $10::text IS NOT NULL ORDER BY `+order+" LIMIT $7", user, admin, q.Job, q.Q, searchPattern(q.Q), q.Quality, q.Limit, c.ID, c.At, c.Title)
	if e != nil {
		return nil, "", e
	}
	defer rows.Close()
	out := []FileRecord{}
	next := ""
	for rows.Next() {
		v, err := readFile(rows)
		if err != nil {
			return nil, "", err
		}
		out = append(out, v)
		next = cursor(v.ID, v.CreatedAt, v.Metadata.Title)
	}
	if len(out) < q.Limit {
		next = ""
	}
	return out, next, rows.Err()
}
