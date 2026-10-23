// Package acceptance measures the deployed mock stand. It never targets a real
// donor and records unexecuted scenarios separately from successful checks.
package acceptance

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/rychkin/Begom/internal/config"
	"github.com/rychkin/Begom/internal/mockdonor"
	"github.com/rychkin/Begom/internal/model"
	"github.com/rychkin/Begom/internal/wire"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

type Check struct {
	Name     string `json:"name"`
	Status   string `json:"status"`
	Expected string `json:"expected"`
	Observed any    `json:"observed,omitempty"`
}
type Report struct {
	Version  string    `json:"version"`
	Started  time.Time `json:"started_at"`
	Finished time.Time `json:"finished_at"`
	Profile  string    `json:"profile"`
	Checks   []Check   `json:"checks"`
	Jobs     []string  `json:"job_ids"`
}
type Options struct {
	Client    config.Client
	Output    string
	Driver    string
	Inventory string
}
type runner struct {
	c *wire.Client
	o Options
	r Report
}

var mockHTTP = &http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{Proxy: nil}, CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }}

func Run(ctx context.Context, c *wire.Client, o Options) (err error) {
	a := &runner{c: c, o: o, r: Report{Version: model.Version, Started: time.Now().UTC(), Profile: o.Driver, Checks: []Check{}, Jobs: []string{}}}
	defer func() {
		a.r.Finished = time.Now().UTC()
		if e := os.MkdirAll(o.Output, 0755); e != nil {
			if err == nil {
				err = e
			}
			return
		}
		b, _ := json.MarshalIndent(a.r, "", "  ")
		if e := os.WriteFile(filepath.Join(o.Output, "report.json"), b, 0644); err == nil && e != nil {
			err = e
		}
		var md strings.Builder
		fmt.Fprintf(&md, "# Приёмка Begom %s\n\nПрофиль: `%s`. Начало: %s.\n\n| Проверка | Результат | Критерий | Измерение |\n|---|---|---|---|\n", a.r.Version, a.r.Profile, a.r.Started.Format(time.RFC3339))
		for _, check := range a.r.Checks {
			value, _ := json.Marshal(check.Observed)
			fmt.Fprintf(&md, "| %s | %s | %s | %s |\n", check.Name, check.Status, check.Expected, string(value))
		}
		os.WriteFile(filepath.Join(o.Output, "report.md"), []byte(md.String()), 0644)
	}()
	return a.run(ctx)
}
func (a *runner) check(name, expected string, ok bool, observed any) error {
	status := "passed"
	if !ok {
		status = "failed"
	}
	a.r.Checks = append(a.r.Checks, Check{name, status, expected, observed})
	if !ok {
		return fmt.Errorf("acceptance failed: %s", name)
	}
	return nil
}
func (a *runner) wait(ctx context.Context, timeout time.Duration, fn func() bool) error {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	for {
		if fn() {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			return errors.New("acceptance condition timed out")
		case <-tick.C:
		}
	}
}
func (a *runner) stats(ctx context.Context) (mockdonor.Stats, error) {
	var out mockdonor.Stats
	req, e := http.NewRequestWithContext(ctx, "GET", a.o.Client.MockURL+"/stats", nil)
	if e != nil {
		return out, e
	}
	response, e := mockHTTP.Do(req)
	if e != nil {
		return out, e
	}
	defer response.Body.Close()
	e = json.NewDecoder(io.LimitReader(response.Body, 8<<20)).Decode(&out)
	return out, e
}
func (a *runner) configureMock(ctx context.Context) error {
	token, e := config.Secret(a.o.Client.MockTokenFile)
	if e != nil {
		return e
	}
	body := `{"delay_ms":4000,"demo":false,"reset":true,"cases":{}}`
	r, e := http.NewRequestWithContext(ctx, "PUT", a.o.Client.MockURL+"/control", strings.NewReader(body))
	if e != nil {
		return e
	}
	r.Header.Set("Authorization", "Bearer "+token)
	r.Header.Set("Content-Type", "application/json")
	response, e := mockHTTP.Do(r)
	if e != nil {
		return e
	}
	defer response.Body.Close()
	if response.StatusCode != 200 {
		return errors.New("mock stand cannot be configured while other work is active")
	}
	return nil
}
func (a *runner) create(ctx context.Context, rangeText string, workers []string, block uint64) (string, model.JobConfig, error) {
	cfg := model.JobConfig{RangeText: &rangeText, WorkerIDs: workers, Threads: 40, BlockSize: block, Retry: model.RetryPolicy{MaxRetries: 4}, Limits: model.Limits{RPS: 80, WorkerRPS: 40, CheckRPS: 80, DownloadRPS: 80, Connections: 120, WorkerConnections: 40}, HTTPTimeoutMS: 20000, MaxFileBytes: 32 << 20}
	if _, e := cfg.Normalize(); e != nil {
		return "", cfg, e
	}
	var reply struct {
		ID string `json:"job_id"`
	}
	if e := a.c.Do(ctx, "POST", "/api/jobs", cfg, &reply, model.NewID()); e != nil {
		return "", cfg, e
	}
	a.r.Jobs = append(a.r.Jobs, reply.ID)
	return reply.ID, cfg, a.c.Do(ctx, "POST", "/api/jobs/"+reply.ID+"/start", struct{}{}, nil, model.NewID())
}
func (a *runner) job(ctx context.Context, id string) (model.Job, error) {
	var out model.Job
	e := a.c.Do(ctx, "GET", "/api/jobs/"+id, nil, &out, "")
	return out, e
}
func (a *runner) done(ctx context.Context, id string) error {
	return a.wait(ctx, 3*time.Minute, func() bool { j, e := a.job(ctx, id); return e == nil && j.State == "completed" })
}

type item struct {
	ID       string `json:"external_id"`
	Status   string `json:"status"`
	FileID   string `json:"file_id"`
	Checksum string `json:"checksum"`
}

func (a *runner) items(ctx context.Context, id string) ([]item, error) {
	var out struct {
		Items []item `json:"items"`
	}
	e := a.c.Do(ctx, "GET", "/api/jobs/"+id+"/items?limit=500", nil, &out, "")
	return out.Items, e
}
func (a *runner) op(ctx context.Context, action, target string) error {
	var cmd *exec.Cmd
	switch a.o.Driver {
	case "compose":
		project := os.Getenv("COMPOSE_PROJECT_NAME")
		if project == "" {
			project = "begom"
		}
		switch action {
		case "kill":
			cmd = exec.CommandContext(ctx, "docker", "compose", "-p", project, "stop", "--timeout", "0", target)
		case "start":
			cmd = exec.CommandContext(ctx, "docker", "compose", "-p", project, "up", "-d", "--no-deps", target)
		}
	case "ansible":
		if a.o.Inventory == "" {
			return errors.New("inventory required")
		}
		cmd = exec.CommandContext(ctx, "ansible-playbook", "-i", a.o.Inventory, "deploy/ansible/operations.yml", "-e", "begom_operation="+action, "-e", "begom_target="+target)
	default:
		return errors.New("failure driver unavailable")
	}
	if cmd == nil {
		return errors.New("unknown stand action")
	}
	if e := cmd.Run(); e != nil {
		return errors.New("stand operation failed: " + action + " " + target)
	}
	return nil
}
func (a *runner) run(ctx context.Context) error {
	var snapshot struct {
		Jobs    []model.Job    `json:"jobs"`
		Workers []model.Worker `json:"workers"`
		Profile struct {
			Mock bool `json:"mock"`
		} `json:"source_profile"`
	}
	if e := a.c.Do(ctx, "GET", "/api/snapshot", nil, &snapshot, ""); e != nil {
		return e
	}
	if !snapshot.Profile.Mock || a.o.Client.MockURL == "" {
		return errors.New("acceptance refuses to run against a non-mock source")
	}
	for _, job := range snapshot.Jobs {
		if job.State == "running" || job.State == "pausing" || job.State == "stopping" || job.State == "validating" {
			return errors.New("acceptance requires an idle stand; stop other jobs first")
		}
	}
	if e := a.wait(ctx, 30*time.Second, func() bool {
		if a.c.Do(ctx, "GET", "/api/snapshot", nil, &snapshot, "") != nil {
			return false
		}
		n := 0
		for _, w := range snapshot.Workers {
			if w.Online && w.Metrics.StorageHealthy {
				n++
			}
		}
		return n >= 3
	}); e != nil {
		return e
	}
	workers := []string{}
	for _, w := range snapshot.Workers {
		if w.Online && w.Metrics.StorageHealthy {
			workers = append(workers, w.ID)
		}
	}
	workers = workers[:3]
	a.check("workers", "at least three healthy workers", true, len(workers))
	if e := a.configureMock(ctx); e != nil {
		return e
	}
	load, _, e := a.create(ctx, "[0,239]", workers, 80)
	if e != nil {
		return e
	}
	if e = a.wait(ctx, 90*time.Second, func() bool { st, e := a.stats(ctx); return e == nil && st.Peak >= 100 }); e != nil {
		st, _ := a.stats(ctx)
		return a.check("parallel_operations", "peak >= 100 actual mock requests", false, st.Peak)
	}
	st, _ := a.stats(ctx)
	a.check("parallel_operations", "peak >= 100 actual mock requests", true, st.Peak)
	if e = a.done(ctx, load); e != nil {
		return e
	}
	st, e = a.stats(ctx)
	if e != nil {
		return e
	}
	maxWindow := MaxWindow(st.Records, time.Second)
	if e = a.check("global_rate_window", "at most 80 arrivals per sliding second", maxWindow <= 80, maxWindow); e != nil {
		return e
	}
	items, e := a.items(ctx, load)
	if e != nil {
		return e
	}
	unique := map[string]bool{}
	for _, i := range items {
		if i.Status != "completed" || i.FileID == "" || unique[i.FileID] {
			return a.check("confirmed_files", "240 completed IDs with distinct canonical files", false, i)
		}
		unique[i.FileID] = true
	}
	if e = a.check("confirmed_files", "240 completed IDs", len(items) == 240, len(items)); e != nil {
		return e
	}
	for _, i := range items[:min(20, len(items))] {
		request, e := http.NewRequestWithContext(ctx, "GET", a.c.Base+"/api/files/"+i.FileID+"/content", nil)
		if e != nil {
			return e
		}
		response, e := a.c.HTTP.Do(request)
		if e != nil {
			return e
		}
		h := sha256.New()
		_, e = io.Copy(h, io.LimitReader(response.Body, 32<<20))
		response.Body.Close()
		if e != nil || response.StatusCode != 200 || hex.EncodeToString(h.Sum(nil)) != i.Checksum {
			return a.check("published_integrity", "SHA-256 of delivered bytes equals catalog", false, i.ID)
		}
	}
	a.check("published_integrity", "SHA-256 of twenty delivered videos equals catalog", true, 20)
	first := items[0]
	request, _ := http.NewRequestWithContext(ctx, "GET", a.c.Base+"/api/files/"+first.FileID+"/content", nil)
	request.Header.Set("Range", "bytes=0-15")
	response, e := a.c.HTTP.Do(request)
	if e != nil {
		return e
	}
	data, _ := io.ReadAll(io.LimitReader(response.Body, 128))
	response.Body.Close()
	if e = a.check("client_range", "206 and sixteen bytes", response.StatusCode == 206 && len(data) == 16, map[string]any{"status": response.StatusCode, "bytes": len(data)}); e != nil {
		return e
	}
	duplicate, _, e := a.create(ctx, "[0,19]", workers, 10)
	if e != nil {
		return e
	}
	if e = a.done(ctx, duplicate); e != nil {
		return e
	}
	reused, e := a.items(ctx, duplicate)
	if e != nil {
		return e
	}
	allReused := len(reused) == 20
	for _, i := range reused {
		allReused = allReused && unique[i.FileID]
	}
	if e = a.check("canonical_deduplication", "subsequent jobs reuse ready file IDs", allReused, len(reused)); e != nil {
		return e
	}
	if a.o.Driver == "none" {
		a.r.Checks = append(a.r.Checks, Check{Name: "failure_and_restart", Status: "not_run", Expected: "requires compose or ansible driver"})
		return nil
	}
	failJob, cfg, e := a.create(ctx, "[1000,1009]", workers[:1], 10)
	if e != nil {
		return e
	}
	var old model.Lease
	if e = a.wait(ctx, 10*time.Second, func() bool {
		var w model.Worker
		if a.c.Do(ctx, "GET", "/api/workers/"+workers[0], nil, &w, "") != nil {
			return false
		}
		for _, l := range w.Leases {
			if l.JobID == failJob {
				old = l
				return true
			}
		}
		return false
	}); e != nil {
		return e
	}
	cfg.WorkerIDs = workers
	if e = a.c.Do(ctx, "PATCH", "/api/jobs/"+failJob+"/config", cfg, nil, model.NewID()); e != nil {
		return e
	}
	failedAt := time.Now()
	if e = a.op(ctx, "kill", workers[0]); e != nil {
		return e
	}
	defer a.op(context.Background(), "start", workers[0])
	var reassigned time.Time
	e = a.wait(ctx, 14*time.Second, func() bool {
		var out struct {
			Events []struct {
				Chunk     string    `json:"chunk_id"`
				Attempt   string    `json:"attempt_id"`
				Worker    string    `json:"worker_id"`
				Operation string    `json:"operation"`
				At        time.Time `json:"timestamp"`
			} `json:"events"`
		}
		if a.c.Do(ctx, "GET", "/api/jobs/"+failJob+"/events", nil, &out, "") != nil {
			return false
		}
		for _, event := range out.Events {
			if event.Chunk == old.ChunkID && event.Attempt != old.AttemptID && event.Worker != workers[0] && event.Operation == "lease.granted" {
				reassigned = event.At
				return true
			}
		}
		return false
	})
	seconds := reassigned.Sub(failedAt).Seconds()
	if e = a.check("agent_failure_reassignment", "new assignment within 2*2 + 10 = 14 seconds", e == nil && seconds >= 0 && seconds <= 14, seconds); e != nil {
		return e
	}
	if e = a.done(ctx, failJob); e != nil {
		return e
	}
	if e = a.op(ctx, "start", workers[0]); e != nil {
		return e
	}
	restart, _, e := a.create(ctx, "[2000,2119]", workers, 40)
	if e != nil {
		return e
	}
	var before []item
	if e = a.wait(ctx, 90*time.Second, func() bool {
		j, e := a.job(ctx, restart)
		if e != nil || j.State != "running" {
			return false
		}
		before, e = a.items(ctx, restart)
		n := 0
		for _, i := range before {
			if i.Status == "completed" {
				n++
			}
		}
		return e == nil && n > 0 && n < 120
	}); e != nil {
		return e
	}
	confirmed := map[string]string{}
	for _, i := range before {
		if i.Status == "completed" {
			confirmed[i.ID] = i.FileID
		}
	}
	if e = a.op(ctx, "kill", "coordinator"); e != nil {
		return e
	}
	if e = a.op(ctx, "start", "coordinator"); e != nil {
		return e
	}
	if e = a.done(ctx, restart); e != nil {
		return e
	}
	after, e := a.items(ctx, restart)
	if e != nil {
		return e
	}
	preserved := true
	for _, i := range after {
		if f, ok := confirmed[i.ID]; ok && f != i.FileID {
			preserved = false
		}
	}
	if e = a.check("coordinator_restart", "all confirmed file IDs survive restart", preserved && len(after) == 120, map[string]any{"confirmed_before": len(confirmed), "items_after": len(after)}); e != nil {
		return e
	}
	if e = a.pauseAndStop(ctx, workers); e != nil {
		return e
	}
	a.r.Checks = append(a.r.Checks, Check{Name: "browser_metric_latency", Status: "separate_suite", Expected: "tests/browser measures collect-to-render <= 5s"}, Check{Name: "NFS_Kerberos_isolation", Status: "separate_stand", Expected: "deploy/ansible acceptance on independent Linux nodes"})
	return nil
}
func (a *runner) pauseAndStop(ctx context.Context, workers []string) error {
	id, _, e := a.create(ctx, "[3000,3359]", workers, 120)
	if e != nil {
		return e
	}
	if e = a.wait(ctx, 30*time.Second, func() bool { st, e := a.stats(ctx); return e == nil && st.Active >= 30 }); e != nil {
		return e
	}
	before, e := a.job(ctx, id)
	if e != nil {
		return e
	}
	if e = a.c.Do(ctx, "POST", "/api/jobs/"+id+"/pause", struct{}{}, nil, model.NewID()); e != nil {
		return e
	}
	var paused model.Job
	e = a.wait(ctx, time.Minute, func() bool { j, err := a.job(ctx, id); paused = j; return err == nil && j.State == "paused" })
	final := uint64(0)
	for status, count := range paused.Counts {
		if model.Final(status) {
			final += count
		}
	}
	if e = a.check("soft_pause", "finish in-flight IDs and release the rest of each block", e == nil && final > 0 && final < 360, final); e != nil {
		return e
	}
	if e = a.c.Do(ctx, "POST", "/api/jobs/"+id+"/resume", struct{}{}, nil, model.NewID()); e != nil {
		return e
	}
	resumed, e := a.job(ctx, id)
	if e != nil {
		return e
	}
	if e = a.check("resume_generation", "resume changes run_id", resumed.RunID != before.RunID, nil); e != nil {
		return e
	}
	if e = a.done(ctx, id); e != nil {
		return e
	}
	stop, _, e := a.create(ctx, "[9000,]", workers, 80)
	if e != nil {
		return e
	}
	if e = a.wait(ctx, 30*time.Second, func() bool { st, e := a.stats(ctx); return e == nil && st.Active >= 20 }); e != nil {
		return e
	}
	at := time.Now()
	if e = a.c.Do(ctx, "POST", "/api/jobs/"+stop+"/stop", struct{}{}, nil, model.NewID()); e != nil {
		return e
	}
	e = a.wait(ctx, 5*time.Second, func() bool { st, err := a.stats(ctx); return err == nil && st.Active == 0 })
	return a.check("immediate_stop", "cancel active HTTP and preserve checkpoints within five seconds", e == nil, time.Since(at).Seconds())
}
func MaxWindow(records []mockdonor.Record, window time.Duration) int {
	if len(records) == 0 {
		return 0
	}
	sort.Slice(records, func(i, j int) bool { return records[i].At.Before(records[j].At) })
	maxCount, start := 0, 0
	for end := range records {
		for start < end && !records[start].At.After(records[end].At.Add(-window)) {
			start++
		}
		maxCount = max(maxCount, end-start+1)
	}
	return maxCount
}
