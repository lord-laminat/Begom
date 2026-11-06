package coordinator

import (
	"context"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/rychkin/Begom/internal/auth"
	"github.com/rychkin/Begom/internal/db"
	"github.com/rychkin/Begom/internal/storage"
)

func listQuery(r *http.Request) db.ListQuery {
	q := r.URL.Query()
	return db.ListQuery{Q: q.Get("q"), Job: q.Get("job_id"), Quality: q.Get("quality"), Sort: q.Get("sort"), After: q.Get("after"), Limit: auth.ParseLimit(q.Get("limit"), 100)}
}
func (s *Server) catalog(w http.ResponseWriter, r *http.Request, u auth.User) {
	q := listQuery(r)
	q.Job = r.URL.Query().Get("discovery_job_id")
	items, next, e := s.Store.Catalog(r.Context(), u.ID, u.Admin(), q)
	if e != nil {
		s.fail(w, r, e)
		return
	}
	s.json(w, 200, map[string]any{"items": items, "next_after": next})
}
func (s *Server) files(w http.ResponseWriter, r *http.Request, u auth.User) {
	items, next, e := s.Store.Files(r.Context(), u.ID, u.Admin(), listQuery(r))
	if e != nil {
		s.fail(w, r, e)
		return
	}
	s.json(w, 200, map[string]any{"files": items, "next_after": next})
}
func (s *Server) file(w http.ResponseWriter, r *http.Request, u auth.User) {
	v, e := s.Store.File(r.Context(), u.ID, u.Admin(), r.PathValue("fileId"))
	if e != nil {
		s.fail(w, r, e)
		return
	}
	s.json(w, 200, v)
}
func (s *Server) thumbnail(w http.ResponseWriter, r *http.Request, u auth.User) {
	v, e := s.Store.File(r.Context(), u.ID, u.Admin(), r.PathValue("fileId"))
	if e != nil {
		s.fail(w, r, e)
		return
	}
	if v.PreviewState != "ready" || v.ThumbnailPath == "" {
		s.fail(w, r, db.Forbidden())
		return
	}
	s.sendPublished(w, r, v.ThumbnailPath, "image/jpeg", uint64(v.ThumbnailSize))
}
func (s *Server) sendPublished(w http.ResponseWriter, r *http.Request, rel, mime string, size uint64) {
	if !storage.ValidPath(rel) {
		s.fail(w, r, db.Conflict("invalid_catalog_path"))
		return
	}
	root, e := s.Volume.OpenRoot()
	if e != nil {
		s.fail(w, r, e)
		return
	}
	defer root.Close()
	st, e := storage.StatRegular(root, path.Join("published", rel))
	if e != nil || !st.Mode().IsRegular() || uint64(st.Size()) != size {
		s.fail(w, r, db.Conflict("file_unavailable"))
		return
	}
	encoded := []string{}
	for _, part := range strings.Split(rel, "/") {
		encoded = append(encoded, url.PathEscape(part))
	}
	w.Header().Set("X-Accel-Redirect", "/_published/"+strings.Join(encoded, "/"))
	w.Header().Set("Content-Type", mime)
	if r.URL.Query().Get("download") == "1" {
		ext := map[string]string{"video/mp4": ".mp4", "video/webm": ".webm", "video/quicktime": ".mov", "video/x-matroska": ".mkv"}[mime]
		w.Header().Set("Content-Disposition", "attachment; filename="+strconv.Quote("begom-"+r.PathValue("fileId")+ext))
	}
	w.WriteHeader(200)
}

func (s *Server) runPreviews(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	nextCheck := time.Time{}
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !s.Dispatch.Epoch.Owns() {
				continue
			}
			if time.Now().After(nextCheck) {
				nextCheck = time.Now().Add(time.Minute)
				if e := s.Publisher.ReconcilePreviews(ctx); e != nil {
					s.Log.Warn("preview reconciliation deferred", "operation", "preview.reconcile", "error_code", "preview_unavailable")
				}
			}
			pctx, cancel := context.WithCancel(ctx)
			done := make(chan struct{})
			go func() {
				t := time.NewTicker(250 * time.Millisecond)
				defer t.Stop()
				for {
					select {
					case <-done:
						return
					case <-pctx.Done():
						return
					case <-t.C:
						if !s.Dispatch.Epoch.Owns() {
							cancel()
							return
						}
					}
				}
			}()
			_, e := s.Publisher.Preview(pctx)
			cancel()
			close(done)
			if e != nil && ctx.Err() == nil {
				s.Log.Warn("preview generation deferred", "operation", "preview.generate", "error_code", "preview_unavailable")
			}
		}
	}
}
