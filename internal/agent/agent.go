package agent

import (
	"context"
	"errors"
	"fmt"
	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/rychkin/Begom/internal/config"
	"github.com/rychkin/Begom/internal/donor"
	"github.com/rychkin/Begom/internal/hostmetrics"
	"github.com/rychkin/Begom/internal/model"
	"github.com/rychkin/Begom/internal/storage"
	"github.com/rychkin/Begom/internal/target"
	"github.com/rychkin/Begom/internal/wire"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Runner struct {
	Config        config.Agent
	Client        *wire.Client
	HTTP          *DonorHTTP
	Volume        *storage.Volume
	Log           *slog.Logger
	sampler       *hostmetrics.Sampler
	session       string
	mu            sync.Mutex
	current       *execution
	epoch         string
	metrics       model.Metrics
	downloads     map[string]model.ActiveDownload
	cap           atomic.Int64
	checks        atomic.Int64
	downloadCount atomic.Int64
	downloadBytes atomic.Uint64
	errorCount    atomic.Uint64
	controlled    atomic.Bool
	sequence      atomic.Uint64
	started       time.Time
}
type execution struct {
	lease    model.Lease
	ctx      context.Context
	cancel   context.CancelFunc
	mu       sync.Mutex
	config   model.JobConfig
	deadline time.Time
	active   atomic.Int64
	pausing  atomic.Bool
}

func New(c config.Agent, log *slog.Logger) (*Runner, error) {
	client, e := wire.New(c.CoordinatorURL, c.TLS, true)
	if e != nil {
		return nil, e
	}
	v := &storage.Volume{Config: c.Storage}
	a := &Runner{Config: c, Client: client, Volume: v, Log: log, sampler: hostmetrics.New(v), session: model.NewID(), downloads: map[string]model.ActiveDownload{}, started: time.Now()}
	client.AgentSession = a.session
	cap := c.Threads
	if cap < 1 {
		cap = 8
	}
	a.cap.Store(int64(cap))
	a.HTTP = NewHTTP(client, c.WorkerID, target.Policy{Source: c.Source})
	return a, nil
}
func (a *Runner) Run(ctx context.Context) error {
	go a.sample(ctx)
	for ctx.Err() == nil {
		var response struct {
			HeartbeatMS int `json:"heartbeat_interval_ms"`
		}
		in := model.Register{WorkerID: a.Config.WorkerID, Version: model.Version, APIVersion: model.APIVersion, SessionID: a.session, StorageRoot: a.Config.Storage.Root, SourceID: a.Config.Source.ID}
		e := a.Client.Retry(ctx, "POST", "/api/workers/register", in, &response, model.NewID())
		if e == nil {
			a.Log.Info("agent registered", "operation", "agent.register", "result", "ok", "worker_id", a.Config.WorkerID)
			break
		}
		a.Log.Warn("registration unavailable", "operation", "agent.register", "result", "retry", "error_code", "control_unavailable")
		if !sleep(ctx, time.Second) {
			return ctx.Err()
		}
	}
	go a.heartbeats(ctx)
	go a.control(ctx)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			a.cancelCurrent()
			return nil
		case <-ticker.C:
			a.mu.Lock()
			busy := a.current != nil
			healthy := a.metrics.StorageHealthy
			a.mu.Unlock()
			if busy || !healthy || !a.controlled.Load() {
				continue
			}
			var reply struct {
				Lease *model.Lease `json:"lease"`
			}
			sent := time.Now()
			e := a.Client.Do(ctx, "POST", "/api/workers/"+a.Config.WorkerID+"/lease", struct{}{}, &reply, model.NewID())
			if e != nil {
				continue
			}
			if reply.Lease != nil {
				ectx, cancel := context.WithCancel(ctx)
				x := &execution{lease: *reply.Lease, ctx: ectx, cancel: cancel, config: reply.Lease.Config, deadline: sent.Add(reply.Lease.ExpiresAt.Sub(reply.Lease.ServerTime) - 100*time.Millisecond)}
				a.mu.Lock()
				a.current = x
				a.mu.Unlock()
				go a.execute(x)
			}
		}
	}
}
func (a *Runner) cancelCurrent() {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.current != nil {
		a.current.cancel()
	}
}
func (a *Runner) sample(ctx context.Context) {
	var previousErrors uint64
	var previousAt time.Time
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		m := a.sampler.Sample()
		m.ActiveChecks = int(a.checks.Load())
		m.ActiveDownloads = int(a.downloadCount.Load())
		m.Bytes = a.downloadBytes.Load()
		m.Errors = a.errorCount.Load()
		if !previousAt.IsZero() {
			m.RecentErrorsPerSecond = float64(m.Errors-previousErrors) / m.MeasuredAt.Sub(previousAt).Seconds()
		}
		previousErrors = m.Errors
		previousAt = m.MeasuredAt
		a.mu.Lock()
		a.metrics = m
		a.mu.Unlock()
		if !m.StorageHealthy {
			a.cancelCurrent()
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
func (a *Runner) heartbeats(ctx context.Context) {
	ticker := time.NewTicker(2 * time.Second)
	watchdog := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	defer watchdog.Stop()
	send := func() {
		a.mu.Lock()
		h := model.Heartbeat{SessionID: a.session, Version: model.Version, Metrics: a.metrics, Leases: []model.LeaseRef{}, Downloads: []model.ActiveDownload{}}
		x := a.current
		if x != nil {
			h.Leases = append(h.Leases, x.lease.Ref())
		}
		for _, d := range a.downloads {
			h.Downloads = append(h.Downloads, d)
		}
		a.mu.Unlock()
		if h.Metrics.MeasuredAt.IsZero() {
			return
		}
		sent := time.Now()
		var reply model.HeartbeatReply
		qctx, cancel := context.WithTimeout(ctx, 1500*time.Millisecond)
		e := a.Client.Retry(qctx, "POST", "/api/workers/"+a.Config.WorkerID+"/heartbeat", h, &reply, model.NewID())
		cancel()
		if e != nil {
			a.cancelCurrent()
			return
		}
		a.cap.Store(int64(reply.Worker.Threads))
		a.mu.Lock()
		if a.epoch != "" && a.epoch != reply.Epoch && a.current != nil {
			a.current.cancel()
		}
		a.epoch = reply.Epoch
		a.mu.Unlock()
		for _, renew := range reply.Leases {
			if x != nil && renew.ChunkID == x.lease.ChunkID {
				if renew.State == "pausing" {
					x.pausing.Store(true)
				}
				x.mu.Lock()
				x.config = renew.Config
				x.deadline = sent.Add(renew.ExpiresAt.Sub(reply.ServerTime) - 100*time.Millisecond)
				x.mu.Unlock()
				if !renew.Valid {
					x.cancel()
				}
			}
		}
	}
	send()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			send()
		case <-watchdog.C:
			a.mu.Lock()
			x := a.current
			a.mu.Unlock()
			if x != nil {
				x.mu.Lock()
				expired := time.Now().After(x.deadline)
				x.mu.Unlock()
				if expired {
					x.cancel()
				}
			}
		}
	}
}
func (a *Runner) control(ctx context.Context) {
	for ctx.Err() == nil {
		u := strings.Replace(a.Client.Base, "https://", "wss://", 1) + "/api/workers/" + a.Config.WorkerID + "/control"
		c, _, e := websocket.Dial(ctx, u, &websocket.DialOptions{HTTPClient: a.Client.HTTP, HTTPHeader: http.Header{"Agent-Session": []string{a.session}, "API-Version": []string{model.APIVersion}}})
		if e != nil {
			a.controlled.Store(false)
			a.cancelCurrent()
			sleep(ctx, time.Second)
			continue
		}
		a.controlled.Store(true)
		for {
			var v struct {
				ServerTime time.Time `json:"server_time"`
				Epoch      string    `json:"epoch"`
				Ready      bool      `json:"ready"`
				Leases     map[string]struct {
					Attempt string `json:"attempt_id"`
					State   string `json:"state"`
				} `json:"leases"`
			}
			qctx, cancel := context.WithTimeout(ctx, 3*time.Second)
			e = wsjson.Read(qctx, c, &v)
			cancel()
			if e != nil {
				break
			}
			a.mu.Lock()
			x := a.current
			if x != nil && !v.ServerTime.Before(x.lease.ServerTime) {
				l, ok := v.Leases[x.lease.ChunkID]
				if ok && l.State == "pausing" {
					x.pausing.Store(true)
				}
				if !v.Ready || !ok || l.Attempt != x.lease.AttemptID || (l.State != "running" && l.State != "pausing") {
					x.cancel()
				}
			}
			a.mu.Unlock()
		}
		c.CloseNow()
		a.controlled.Store(false)
		a.cancelCurrent()
		sleep(ctx, time.Second)
	}
}
func (a *Runner) execute(x *execution) {
	defer func() {
		x.cancel()
		a.mu.Lock()
		if a.current == x {
			a.current = nil
		}
		a.mu.Unlock()
	}()
	a.Log.Info("leased block started", "operation", "chunk.execute", "result", "started", "job_id", x.lease.JobID, "chunk_id", x.lease.ChunkID, "attempt_id", x.lease.AttemptID)
	confirmed := map[model.Number]bool{}
	for _, id := range x.lease.Confirmed {
		confirmed[id] = true
	}
	var wg sync.WaitGroup
	failed := atomic.Bool{}
	if x.lease.NextID != nil {
		for id := *x.lease.NextID; ; {
			if x.ctx.Err() != nil || x.pausing.Load() {
				break
			}
			if !confirmed[id] {
				for {
					x.mu.Lock()
					cap := x.config.ThreadsFor(a.Config.WorkerID, int(a.cap.Load()))
					x.mu.Unlock()
					if int(x.active.Load()) < cap {
						break
					}
					if x.pausing.Load() || !sleep(x.ctx, 10*time.Millisecond) {
						break
					}
				}
				if x.ctx.Err() != nil || x.pausing.Load() {
					break
				}
				x.active.Add(1)
				wg.Add(1)
				go func(id model.Number) {
					defer wg.Done()
					defer x.active.Add(-1)
					if e := a.process(x, id); e != nil {
						failed.Store(true)
						a.errorCount.Add(1)
						x.cancel()
						a.Log.Warn("block execution interrupted", "operation", "item.process", "result", "retry", "error_code", "execution_interrupted", "job_id", x.lease.JobID, "external_id", id.String())
					}
				}(id)
			}
			if id == x.lease.To {
				break
			}
			id++
		}
	}
	wg.Wait()
	endpoint := "complete"
	if failed.Load() || x.ctx.Err() != nil || x.pausing.Load() {
		endpoint = "fail"
	}
	qctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	a.Client.Retry(qctx, "POST", "/api/workers/"+a.Config.WorkerID+"/"+endpoint, x.lease.Ref(), nil, model.NewID())
}
func (a *Runner) report(x *execution, r model.ItemResult) (string, error) {
	p := model.Progress{LeaseRef: x.lease.Ref(), OperationID: model.NewID(), Sequence: a.sequence.Add(1), Results: []model.ItemResult{r}}
	var reply struct {
		Accepted []struct {
			Status string `json:"status"`
		} `json:"accepted"`
	}
	e := a.Client.Retry(x.ctx, "POST", "/api/workers/"+a.Config.WorkerID+"/progress", p, &reply, p.OperationID)
	if e != nil {
		return "", e
	}
	if len(reply.Accepted) != 1 {
		return "", errors.New("invalid_progress_confirmation")
	}
	if reply.Accepted[0].Status == "failed" {
		a.errorCount.Add(1)
	}
	return reply.Accepted[0].Status, nil
}
func (a *Runner) process(x *execution, id model.Number) error {
	if _, e := a.report(x, model.ItemResult{ExternalID: id, Status: "checking"}); e != nil {
		return e
	}
	x.mu.Lock()
	cfg := x.config
	x.mu.Unlock()
	a.checks.Add(1)
	defer a.checks.Add(-1)
	pageURL, e := donor.PageURL(a.Config.Source.PageTemplate, id)
	if e != nil {
		return e
	}
	var page donor.Page
	var assets []donor.Asset
	lastCode := "donor_request_failed"
	for attempt := 0; attempt <= cfg.Retry.MaxRetries; attempt++ {
		body, finalURL, status, retryAfter, err := a.getBody(x, "check", pageURL, cfg)
		if err == nil {
			state, retry := donor.Classify(status)
			if state == "not_found" || state == "forbidden" {
				_, e = a.report(x, model.ItemResult{ExternalID: id, Status: state})
				return e
			}
			if !retry && state != "found" {
				_, e = a.report(x, model.ItemResult{ExternalID: id, Status: "failed", ErrorCode: "unexpected_http_status"})
				return e
			}
			if state == "found" {
				page, err = donor.ParsePage(body, finalURL)
				if err != nil {
					_, e = a.report(x, model.ItemResult{ExternalID: id, Status: "failed", ErrorCode: "unsupported_page_schema"})
					return e
				}
				if page.DownloadLink == "" {
					_, e = a.report(x, model.ItemResult{ExternalID: id, Status: "skipped", ErrorCode: "download_not_allowed", Metadata: &page.Metadata})
					return e
				}
				base, _ := url.Parse(finalURL)
				downloadLink, errURL := url.Parse(page.DownloadLink)
				if errURL != nil {
					return a.finishError(x, id, "invalid_download_link")
				}
				link := base.ResolveReference(downloadLink).String()
				body, _, status, retryAfter, err = a.getBody(x, "check", link, cfg)
				if err == nil {
					state, retry = donor.Classify(status)
					if state == "forbidden" {
						_, e = a.report(x, model.ItemResult{ExternalID: id, Status: "forbidden", Metadata: &page.Metadata})
						return e
					}
					if state == "found" {
						for kind, count := range donor.Resources(body) {
							if page.Metadata.Resources == nil {
								page.Metadata.Resources = map[string]int{}
							}
							page.Metadata.Resources[kind] += count
						}
						assets, err = donor.ParseAssets(body, link)
						if err == nil {
							break
						}
						return a.finishError(x, id, "unsupported_download_schema")
					}
					if !retry {
						return a.finishError(x, id, "download_catalog_unavailable")
					}
				}
			}
		}
		if attempt == cfg.Retry.MaxRetries {
			break
		}
		statusName := "retry_wait"
		if status == 429 {
			statusName = "rate_limited"
			lastCode = "rate_limited"
		}
		if _, e = a.report(x, model.ItemResult{ExternalID: id, Status: statusName, ErrorCode: lastCode}); e != nil {
			return e
		}
		delay := max(donor.RetryDelay(cfg.Retry, attempt, time.Duration(rand.IntN(250))*time.Millisecond), retryAfter)
		if !sleep(x.ctx, delay) {
			return x.ctx.Err()
		}
	}
	if assets == nil {
		return a.finishError(x, id, lastCode)
	}
	asset, ok := donor.Best(assets, cfg.MaxFileBytes)
	if !ok {
		_, e = a.report(x, model.ItemResult{ExternalID: id, Status: "skipped", ErrorCode: "no_permitted_video_asset", Metadata: &page.Metadata})
		return e
	}
	if _, e = a.HTTP.Policy.URL(asset.URL); e != nil {
		return a.finishError(x, id, "download_host_not_allowed")
	}
	page.Metadata.DownloadURL = asset.URL
	page.Metadata.Size = uint64(asset.Size)
	page.Metadata.MIME = donor.MIME(asset)
	page.Metadata.Quality = asset.Quality
	downloadID, stageID := model.NewID(), model.NewID()
	reserve := uint64(asset.Size)
	if reserve == 0 {
		reserve = min(8<<20, cfg.MaxFileBytes)
	}
	result := model.ItemResult{ExternalID: id, Status: "download_queued", Metadata: &page.Metadata, DownloadID: downloadID, StageID: stageID, ReserveBytes: reserve * 2}
	accepted, e := a.report(x, result)
	if e != nil {
		return e
	}
	if accepted == "completed" {
		return nil
	}
	a.checks.Add(-1)
	defer a.checks.Add(1)
	a.downloadCount.Add(1)
	defer a.downloadCount.Add(-1)
	a.mu.Lock()
	a.downloads[downloadID] = model.ActiveDownload{JobID: x.lease.JobID, ExternalID: id, Title: page.Metadata.Title, Size: uint64(asset.Size)}
	a.mu.Unlock()
	defer func() { a.mu.Lock(); delete(a.downloads, downloadID); a.mu.Unlock() }()
	size, checksum, err := a.download(x, result, asset.URL, cfg)
	if err != nil {
		if x.ctx.Err() != nil {
			return x.ctx.Err()
		}
		if err.Error() == "download_forbidden" {
			_, e := a.report(x, model.ItemResult{ExternalID: id, Status: "forbidden", ErrorCode: "download_forbidden"})
			return e
		}
		return a.finishError(x, id, "download_failed")
	}
	commit := model.Commit{LeaseRef: x.lease.Ref(), StageID: stageID, Size: size, Checksum: checksum}
	var publication struct {
		ID string `json:"publication_id"`
	}
	if e = a.Client.Retry(x.ctx, "POST", "/api/workers/"+a.Config.WorkerID+"/downloads/"+downloadID+"/commit", commit, &publication, model.NewID()); e != nil {
		return e
	}
	for {
		var p struct {
			State string `json:"state"`
		}
		if e = a.Client.Do(x.ctx, "GET", "/api/workers/"+a.Config.WorkerID+"/publications/"+publication.ID, nil, &p, ""); e != nil {
			return e
		}
		if p.State == "ready" {
			return nil
		}
		if p.State != "verifying" {
			return a.finishError(x, id, "publication_failed")
		}
		if !sleep(x.ctx, 100*time.Millisecond) {
			return x.ctx.Err()
		}
	}
}
func (a *Runner) finishError(x *execution, id model.Number, code string) error {
	_, e := a.report(x, model.ItemResult{ExternalID: id, Status: "failed", ErrorCode: code})
	return e
}
func (a *Runner) getBody(x *execution, class, raw string, c model.JobConfig) ([]byte, string, int, time.Duration, error) {
	response, e := a.HTTP.Get(x.ctx, x.lease.Ref(), class, raw, nil, time.Duration(c.HTTPTimeoutMS)*time.Millisecond)
	if e != nil {
		return nil, raw, 0, 0, e
	}
	defer response.Body.Close()
	b, e := io.ReadAll(io.LimitReader(response.Body, (2<<20)+1))
	if len(b) > 2<<20 {
		return nil, raw, response.StatusCode, 0, errors.New("metadata_response_too_large")
	}
	return b, response.Request.URL.String(), response.StatusCode, donor.RetryAfter(response.Header.Get("Retry-After"), time.Now()), e
}
func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
func (a *Runner) Metrics(w http.ResponseWriter, r *http.Request) {
	if !wire.MetricsIdentity(r) {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	if r.TLS == nil || len(r.TLS.VerifiedChains) == 0 {
		http.Error(w, "authentication required", 401)
		return
	}
	a.mu.Lock()
	m := a.metrics
	a.mu.Unlock()
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	hostmetrics.Prometheus(w, "begom_host_", "", m)
	fmt.Fprintf(w, "begom_agent_up 1\nbegom_active_checks %d\nbegom_active_downloads %d\nbegom_download_bytes_total %d\nbegom_agent_errors_total %d\nbegom_agent_cpu_percent %g\nbegom_agent_ram_used_bytes %d\nbegom_agent_disk_free_bytes %d\nbegom_agent_measurement_timestamp_seconds %g\n", m.ActiveChecks, m.ActiveDownloads, m.Bytes, m.Errors, m.CPU, m.RAMUsed, m.DiskFree, float64(m.MeasuredAt.UnixMilli())/1000)
}
