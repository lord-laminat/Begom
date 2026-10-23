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
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		t.Execute(w, map[string]string{"Version": model.Version})
	})
}
