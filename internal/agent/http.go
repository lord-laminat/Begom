package agent

import (
	"context"
	"errors"
	"github.com/rychkin/Begom/internal/dispatch"
	"github.com/rychkin/Begom/internal/donor"
	"github.com/rychkin/Begom/internal/model"
	"github.com/rychkin/Begom/internal/target"
	"github.com/rychkin/Begom/internal/wire"
	"io"
	"net/http"
	"sync"
	"time"
)

type DonorHTTP struct {
	Client    *wire.Client
	Worker    string
	Policy    target.Policy
	Transport *http.Transport
	mu        sync.Mutex
	used      map[string]time.Time
}

func NewHTTP(c *wire.Client, worker string, policy target.Policy) *DonorHTTP {
	t := &http.Transport{Proxy: nil, DialContext: policy.DialContext, DisableKeepAlives: true, ForceAttemptHTTP2: false, TLSHandshakeTimeout: 10 * time.Second, ResponseHeaderTimeout: 30 * time.Second}
	return &DonorHTTP{Client: c, Worker: worker, Policy: policy, Transport: t, used: map[string]time.Time{}}
}
func (h *DonorHTTP) permit(ctx context.Context, ref model.LeaseRef, class string) (model.Permit, error) {
	uid := model.NewID()
	p := model.PermitRequest{LeaseRef: ref, Class: class, RequestUID: uid}
	endpoint := "/api/workers/" + h.Worker + "/permits"
	var response struct {
		State  string        `json:"state"`
		Permit *model.Permit `json:"permit"`
	}
	if e := h.Client.Retry(ctx, "POST", endpoint, p, &response, uid); e != nil {
		return model.Permit{}, e
	}
	defer func() {
		if response.State == "pending" {
			qctx, c := context.WithTimeout(context.Background(), time.Second)
			defer c()
			h.Client.Do(qctx, "DELETE", endpoint+"/"+uid, nil, nil, model.NewID())
		}
	}()
	for {
		if response.State == "issued" && response.Permit != nil {
			return *response.Permit, nil
		}
		if response.State != "pending" {
			return model.Permit{}, errors.New("permit_expired")
		}
		timer := time.NewTimer(40 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return model.Permit{}, ctx.Err()
		case <-timer.C:
		}
		if e := h.Client.Do(ctx, "GET", endpoint+"/"+uid, nil, &response, ""); e != nil {
			return model.Permit{}, e
		}
	}
}
func (h *DonorHTTP) Get(ctx context.Context, ref model.LeaseRef, class, raw string, headers http.Header, timeout time.Duration) (*http.Response, error) {
	for redirects := 0; redirects <= 10; redirects++ {
		u, e := h.Policy.URL(raw)
		if e != nil {
			return nil, e
		}
		var permit model.Permit
		for {
			permit, e = h.permit(ctx, ref, class)
			if e != nil {
				return nil, e
			}
			if wait := time.Until(permit.NotBefore) + dispatch.ClockMargin; wait > 0 {
				if !sleep(ctx, wait) {
					return nil, ctx.Err()
				}
			}
			remaining := time.Until(permit.ExpiresAt) - dispatch.ClockMargin
			if remaining <= 0 {
				continue
			}
			h.mu.Lock()
			for id, until := range h.used {
				if time.Now().After(until) {
					delete(h.used, id)
				}
			}
			_, duplicate := h.used[permit.ID]
			if !duplicate {
				h.used[permit.ID] = time.Now().Add(2 * dispatch.ConnectionLease)
			}
			h.mu.Unlock()
			if duplicate {
				return nil, errors.New("permit_already_consumed")
			}
			break
		}
		requestCtx, cancel := context.WithTimeout(ctx, timeout)
		req, e := http.NewRequestWithContext(requestCtx, "GET", u.String(), nil)
		if e != nil {
			cancel()
			return nil, e
		}
		req.Header = headers.Clone()
		if req.Header == nil {
			req.Header = http.Header{}
		}
		req.Header.Set("User-Agent", "Begom/"+model.Version)
		req.Header.Set("X-Begom-Worker", h.Worker)
		body := &trackedBody{http: h, permit: permit, cancel: cancel, ctx: requestCtx, started: time.Now(), done: make(chan struct{}), deadline: time.Now().Add(permit.ConnectionExpiresAt.Sub(permit.ServerTime) - dispatch.PermitTTL - dispatch.ClockMargin)}
		go body.renew()
		client := &http.Client{Transport: h.Transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
		response, e := client.Do(req)
		if e != nil {
			body.Close()
			return nil, errors.New("donor_request_failed")
		}
		body.body = response.Body
		body.feedback.Status = response.StatusCode
		body.feedback.RetryAfterMS = donor.RetryAfter(response.Header.Get("Retry-After"), time.Now()).Milliseconds()
		response.Body = body
		if response.StatusCode >= 300 && response.StatusCode <= 399 && response.Header.Get("Location") != "" {
			location, e := response.Location()
			response.Body.Close()
			if e != nil {
				return nil, errors.New("invalid_redirect")
			}
			raw = location.String()
			continue
		}
		return response, nil
	}
	return nil, errors.New("too_many_redirects")
}

type trackedBody struct {
	http     *DonorHTTP
	permit   model.Permit
	body     io.ReadCloser
	cancel   context.CancelFunc
	ctx      context.Context
	started  time.Time
	once     sync.Once
	mu       sync.Mutex
	feedback model.Feedback
	done     chan struct{}
	deadline time.Time
}

func (b *trackedBody) Read(p []byte) (int, error) {
	n, e := b.body.Read(p)
	b.mu.Lock()
	b.feedback.Bytes += uint64(n)
	b.mu.Unlock()
	return n, e
}
func (b *trackedBody) Close() error {
	var err error
	b.once.Do(func() {
		close(b.done)
		b.cancel()
		if b.body != nil {
			err = b.body.Close()
		}
		b.mu.Lock()
		feedback := b.feedback
		feedback.DurationMS = time.Since(b.started).Milliseconds()
		b.mu.Unlock()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		b.http.Client.Retry(ctx, "POST", "/api/workers/"+b.http.Worker+"/connections/"+b.permit.ConnectionID+"/release", feedback, nil, model.NewID())
	})
	return err
}
func (b *trackedBody) renew() {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-b.done:
			return
		case <-b.ctx.Done():
			return
		case <-t.C:
			if time.Now().After(b.deadline) {
				b.cancel()
				return
			}
			sent := time.Now()
			var r struct {
				Expires time.Time `json:"connection_expires_at"`
				Server  time.Time `json:"server_time"`
				Epoch   string    `json:"epoch"`
			}
			ctx, cancel := context.WithTimeout(b.ctx, 1500*time.Millisecond)
			e := b.http.Client.Do(ctx, "POST", "/api/workers/"+b.http.Worker+"/connections/"+b.permit.ConnectionID+"/renew", struct{}{}, &r, model.NewID())
			cancel()
			if e != nil || r.Epoch != b.permit.Epoch {
				b.cancel()
				return
			}
			b.deadline = sent.Add(r.Expires.Sub(r.Server) - dispatch.ClockMargin)
		}
	}
}
