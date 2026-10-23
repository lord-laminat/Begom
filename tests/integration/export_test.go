//go:build integration

package integration

import (
	"encoding/json"
	"github.com/rychkin/Begom/internal/auth"
	"github.com/rychkin/Begom/internal/config"
	"github.com/rychkin/Begom/internal/coordinator"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestExportIncludesMoreThanTenThousandRows(t *testing.T) {
	x := setup(t)
	job := x.job(t, "[0,12000]", 1000)
	if _, e := x.s.Pool.Exec(t.Context(), "INSERT INTO range_chunks(id,job_id,range_start,range_end,status) VALUES('export-chunk',$1,0,12000,'pending')", job); e != nil {
		t.Fatal(e)
	}
	if _, e := x.s.Pool.Exec(t.Context(), "INSERT INTO items(job_id,external_id,run_id,chunk_id,attempt_id,status,final) SELECT $1,g,'export-run','export-chunk','export-attempt','not_found',true FROM generate_series(0,12000)g", job); e != nil {
		t.Fatal(e)
	}
	a := &auth.Service{Store: x.s}
	_, token, _, e := a.Login(t.Context(), "test-admin", "integration-only-password")
	if e != nil {
		t.Fatal(e)
	}
	s := &coordinator.Server{Store: x.s, Auth: a, Config: config.Coordinator{}, Log: slog.New(slog.NewJSONHandler(io.Discard, nil))}
	req := httptest.NewRequest(http.MethodGet, "/api/jobs/"+job+"/export?format=json", nil)
	req.AddCookie(&http.Cookie{Name: auth.SessionCookie, Value: token})
	rec := httptest.NewRecorder()
	s.Human().ServeHTTP(rec, req)
	if rec.Code != 200 {
		t.Fatal(rec.Code, rec.Body.String())
	}
	var result struct {
		Items    []json.RawMessage `json:"items"`
		Complete bool              `json:"complete"`
		Count    int               `json:"count"`
	}
	if e = json.Unmarshal(rec.Body.Bytes(), &result); e != nil {
		t.Fatal(e)
	}
	if !result.Complete || result.Count != 12001 || len(result.Items) != 12001 {
		t.Fatal("export truncated", result.Count, len(result.Items))
	}
}
