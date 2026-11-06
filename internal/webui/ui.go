package webui

import (
	"embed"
	"github.com/rychkin/Begom/internal/model"
	"html/template"
	"net/http"
	"strings"
)

//go:embed assets/*
var assets embed.FS

func Handler() http.Handler {
	t := template.Must(template.ParseFS(assets, "assets/index.html"))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/assets/") {
			b, e := assets.ReadFile(strings.TrimPrefix(r.URL.Path, "/"))
			if e != nil {
				http.NotFound(w, r)
				return
			}
			if strings.HasSuffix(r.URL.Path, ".js") {
				w.Header().Set("Content-Type", "text/javascript")
			} else if strings.HasSuffix(r.URL.Path, ".css") {
				w.Header().Set("Content-Type", "text/css")
			}
			w.Write(b)
			return
		}
		terminal := strings.HasPrefix(r.URL.Path, "/workers/") && strings.HasSuffix(r.URL.Path, "/terminal") && model.ValidID(strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/workers/"), "/terminal"))
		watch := strings.HasPrefix(r.URL.Path, "/watch/") && model.ValidID(strings.TrimPrefix(r.URL.Path, "/watch/"))
		if r.URL.Path != "/" && r.URL.Path != "/catalog" && r.URL.Path != "/gallery" && !watch && !terminal {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		if terminal {
			w.Header().Set("Content-Security-Policy", "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; connect-src 'self'; media-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'self'")
			page := template.Must(template.ParseFS(assets, "assets/terminal.html"))
			page.Execute(w, map[string]string{"Version": model.Version})
			return
		}
		t.Execute(w, map[string]string{"Version": model.Version})
	})
}
