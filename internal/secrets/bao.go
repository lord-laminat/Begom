// Package secrets is a bounded-cache AppRole client for OpenBao KV v2.
package secrets

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"github.com/rychkin/Begom/internal/config"
	"github.com/rychkin/Begom/internal/wire"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

type cached struct {
	value map[string]string
	until time.Time
}
type Bao struct {
	Config  config.Bao
	HTTP    *http.Client
	mu      sync.Mutex
	token   string
	expires time.Time
	cache   map[string]cached
}

func New(c config.Bao) (*Bao, error) {
	if !strings.HasPrefix(c.URL, "https://") {
		return nil, errors.New("secret service must use HTTPS")
	}
	cfg, e := wire.TLS(config.TLS{CA: c.CA}, false)
	if e != nil {
		return nil, e
	}
	return &Bao{Config: c, HTTP: &http.Client{Transport: &http.Transport{TLSClientConfig: cfg, Proxy: nil}, Timeout: 4 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}, cache: map[string]cached{}}, nil
}
func (b *Bao) call(ctx context.Context, method, p string, data any, token string, out any) error {
	raw, e := json.Marshal(data)
	if e != nil {
		return e
	}
	req, e := http.NewRequestWithContext(ctx, method, strings.TrimRight(b.Config.URL, "/")+p, bytes.NewReader(raw))
	if e != nil {
		return e
	}
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("X-Vault-Token", token)
	}
	r, e := b.HTTP.Do(req)
	if e != nil {
		return errors.New("secret service unavailable")
	}
	defer r.Body.Close()
	if r.StatusCode < 200 || r.StatusCode >= 300 {
		return errors.New("secret service rejected operation")
	}
	if out == nil {
		return nil
	}
	return json.NewDecoder(io.LimitReader(r.Body, 2<<20)).Decode(out)
}
func (b *Bao) login(ctx context.Context) error {
	if time.Until(b.expires) > 15*time.Second {
		return nil
	}
	role, e := config.Secret(b.Config.RoleIDFile)
	if e != nil {
		return e
	}
	secret, e := config.Secret(b.Config.SecretIDFile)
	if e != nil {
		return e
	}
	var r struct {
		Auth struct {
			Token string `json:"client_token"`
			Lease int    `json:"lease_duration"`
		} `json:"auth"`
	}
	if e = b.call(ctx, "POST", "/v1/auth/approle/login", map[string]string{"role_id": role, "secret_id": secret}, "", &r); e != nil {
		return e
	}
	if r.Auth.Token == "" || r.Auth.Lease < 30 {
		return errors.New("invalid secret-service identity")
	}
	b.token = r.Auth.Token
	b.expires = time.Now().Add(time.Duration(r.Auth.Lease) * time.Second)
	return nil
}
func ValidRef(ref string) bool {
	if len(ref) > 200 || strings.HasPrefix(ref, "/") {
		return false
	}
	for _, p := range strings.Split(ref, "/") {
		if p == "" || p == "." || p == ".." {
			return false
		}
		for _, c := range p {
			if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-' || c == '_') {
				return false
			}
		}
	}
	return true
}
func (b *Bao) Get(ctx context.Context, ref string) (map[string]string, error) {
	if !b.allowed(ref) {
		return nil, errors.New("secret scope denied")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if c, ok := b.cache[ref]; ok && time.Now().Before(c.until) {
		return clone(c.value), nil
	}
	if e := b.login(ctx); e != nil {
		return nil, e
	}
	var r struct {
		Data struct {
			Data map[string]string `json:"data"`
		} `json:"data"`
	}
	if e := b.call(ctx, "GET", "/v1/secret/data/"+ref, nil, b.token, &r); e != nil {
		return nil, e
	}
	if r.Data.Data == nil {
		return nil, errors.New("secret not found")
	}
	b.cache[ref] = cached{r.Data.Data, time.Now().Add(time.Minute)}
	return clone(r.Data.Data), nil
}
func clone(m map[string]string) map[string]string {
	out := map[string]string{}
	for k, v := range m {
		out[k] = v
	}
	return out
}
func (b *Bao) Put(ctx context.Context, ref string, data map[string]string) error {
	if !b.allowed(ref) {
		return errors.New("secret scope denied")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if e := b.login(ctx); e != nil {
		return e
	}
	if e := b.call(ctx, "POST", "/v1/secret/data/"+ref, map[string]any{"data": data}, b.token, nil); e != nil {
		return e
	}
	delete(b.cache, ref)
	return nil
}
func (b *Bao) allowed(ref string) bool {
	if !ValidRef(ref) {
		return false
	}
	if strings.HasPrefix(ref, b.Config.Prefix+"/") {
		return true
	}
	if b.Config.Prefix == "begom/coordinator" && strings.HasPrefix(ref, "begom/agents/") && strings.HasSuffix(ref, "/tls") {
		return len(strings.Split(ref, "/")) == 4
	}
	return false
}
func (b *Bao) Certificate(ctx context.Context, ref string) (tls.Certificate, error) {
	s, e := b.Get(ctx, ref)
	if e != nil {
		return tls.Certificate{}, e
	}
	pair, e := tls.X509KeyPair([]byte(s["cert"]), []byte(s["key"]))
	if e != nil {
		return tls.Certificate{}, errors.New("invalid secret certificate")
	}
	return pair, nil
}
func WriteSecret(path string, b []byte) error {
	f, e := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if e != nil {
		return e
	}
	defer f.Close()
	_, e = f.Write(b)
	return e
}
