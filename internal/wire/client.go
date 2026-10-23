package wire

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"github.com/rychkin/Begom/internal/config"
	"github.com/rychkin/Begom/internal/model"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"os"
	"strings"
	"time"
)

type Error struct {
	Status int
	API    model.APIError
}

func (e *Error) Error() string { return e.API.Code }
func TLS(c config.TLS, client bool) (*tls.Config, error) {
	b, e := os.ReadFile(c.CA)
	if e != nil {
		return nil, errors.New("CA unavailable")
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(b) {
		return nil, errors.New("invalid CA")
	}
	cfg := &tls.Config{MinVersion: tls.VersionTLS13, RootCAs: pool, ClientCAs: pool}
	if client {
		pair, e := tls.LoadX509KeyPair(c.Cert, c.Key)
		if e != nil {
			return nil, errors.New("client identity unavailable")
		}
		cfg.Certificates = []tls.Certificate{pair}
	}
	return cfg, nil
}

type Client struct {
	Base         string
	HTTP         *http.Client
	CSRF         string
	AgentSession string
}

func New(base string, c config.TLS, mutual bool) (*Client, error) {
	u, e := url.Parse(base)
	if e != nil || u.Scheme != "https" || u.User != nil {
		return nil, errors.New("control URL must use HTTPS")
	}
	cfg, e := TLS(c, mutual)
	if e != nil {
		return nil, e
	}
	jar, _ := cookiejar.New(nil)
	return &Client{Base: strings.TrimRight(base, "/"), HTTP: &http.Client{Transport: &http.Transport{TLSClientConfig: cfg, Proxy: nil, MaxIdleConnsPerHost: 128}, Jar: jar, Timeout: 8 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}, nil
}
func (c *Client) Do(ctx context.Context, method, p string, in, out any, key string) error {
	var body []byte
	var e error
	if in != nil {
		body, e = json.Marshal(in)
		if e != nil {
			return e
		}
	}
	req, e := http.NewRequestWithContext(ctx, method, c.Base+p, bytes.NewReader(body))
	if e != nil {
		return e
	}
	req.Header.Set("API-Version", model.APIVersion)
	if c.AgentSession != "" {
		req.Header.Set("Agent-Session", c.AgentSession)
	}
	req.Header.Set("Content-Type", "application/json")
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	if c.CSRF != "" {
		req.Header.Set("X-CSRF-Token", c.CSRF)
	}
	r, e := c.HTTP.Do(req)
	if e != nil {
		return errors.New("control connection failed")
	}
	defer r.Body.Close()
	b, e := io.ReadAll(io.LimitReader(r.Body, 4<<20))
	if e != nil {
		return errors.New("control response interrupted")
	}
	if r.StatusCode < 200 || r.StatusCode >= 300 {
		var api model.APIError
		if json.Unmarshal(b, &api) != nil {
			api.Code = "invalid_control_response"
		}
		return &Error{Status: r.StatusCode, API: api}
	}
	if out != nil && len(b) > 0 {
		if e = json.Unmarshal(b, out); e != nil {
			return errors.New("invalid control response")
		}
	}
	return nil
}

// Retry preserves the body and key across uncertain outcomes.
func (c *Client) Retry(ctx context.Context, method, p string, in, out any, key string) error {
	var last error
	for i := 0; i < 4; i++ {
		last = c.Do(ctx, method, p, in, out, key)
		if last == nil {
			return nil
		}
		var api *Error
		if errors.As(last, &api) && api.Status < 500 && api.Status != 429 {
			return last
		}
		timer := time.NewTimer(time.Duration(100*(1<<i)) * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	return last
}
