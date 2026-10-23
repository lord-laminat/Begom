package bootstrap

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/rychkin/Begom/internal/config"
	"github.com/rychkin/Begom/internal/model"
	"github.com/rychkin/Begom/internal/pki"
	"github.com/rychkin/Begom/internal/wire"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type BaoKeys struct {
	Keys      []string `json:"keys_base64"`
	RootToken string   `json:"root_token"`
}
type baoAPI struct {
	url   string
	http  *http.Client
	token string
}

func (b *baoAPI) call(ctx context.Context, method, path string, v, out any) error {
	body, e := json.Marshal(v)
	if e != nil {
		return e
	}
	r, e := http.NewRequestWithContext(ctx, method, b.url+path, bytes.NewReader(body))
	if e != nil {
		return e
	}
	r.Header.Set("Content-Type", "application/json")
	if b.token != "" {
		r.Header.Set("X-Vault-Token", b.token)
	}
	response, e := b.http.Do(r)
	if e != nil {
		return errors.New("OpenBao unavailable")
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return errors.New("OpenBao bootstrap operation rejected: " + path)
	}
	if out != nil {
		return json.NewDecoder(io.LimitReader(response.Body, 2<<20)).Decode(out)
	}
	return nil
}
func BaoInit(ctx context.Context, dir, endpoint string) error {
	tc, e := wire.TLS(config.TLS{CA: filepath.Join(dir, "secrets/ca.crt")}, false)
	if e != nil {
		return e
	}
	api := &baoAPI{url: strings.TrimRight(endpoint, "/"), http: &http.Client{Transport: &http.Transport{TLSClientConfig: tc, Proxy: nil}, Timeout: 5 * time.Second}}
	var status struct {
		Initialized bool `json:"initialized"`
		Sealed      bool `json:"sealed"`
	}
	for i := 0; i < 60; i++ {
		e = api.call(ctx, "GET", "/v1/sys/seal-status", nil, &status)
		if e == nil {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
	if e != nil {
		return e
	}
	var keys BaoKeys
	keyPath := filepath.Join(dir, "bootstrap/bao-init.json")
	if !status.Initialized {
		// Raft initialization may exceed the normal control-plane timeout. Never
		// retry PUT /sys/init: losing that one response loses recovery material.
		api.http.Timeout = 120 * time.Second
		shares, threshold := 1, 1
		var p Profile
		if config.Read(filepath.Join(dir, "bootstrap/profile.json"), &p) == nil && p.Shares > 0 {
			shares, threshold = p.Shares, p.Threshold
		}
		if e = api.call(ctx, "PUT", "/v1/sys/init", map[string]any{"secret_shares": shares, "secret_threshold": threshold}, &keys); e != nil {
			return e
		}
		b, _ := json.Marshal(keys)
		if e = write(keyPath, b, 0600); e != nil {
			return e
		}
		api.http.Timeout = 10 * time.Second
		var profile Profile
		if config.Read(filepath.Join(dir, "bootstrap/profile.json"), &profile) == nil && os.Geteuid() == 0 {
			os.Chown(keyPath, profile.OperatorUID, profile.OperatorGID)
		}
	} else {
		if e = config.Read(keyPath, &keys); e != nil {
			return errors.New("OpenBao is initialized but recovery material is unavailable")
		}
	}
	if len(keys.Keys) == 0 || keys.RootToken == "" {
		return errors.New("invalid bootstrap material")
	}
	if status.Sealed || !status.Initialized {
		for _, key := range keys.Keys {
			if e = api.call(ctx, "PUT", "/v1/sys/unseal", map[string]string{"key": key}, nil); e != nil {
				return e
			}
		}
	}
	api.token = keys.RootToken
	if _, e = os.Stat(filepath.Join(dir, "bootstrap/bao.seeded")); e == nil {
		return nil
	}
	var mounts map[string]json.RawMessage
	if e = api.call(ctx, "GET", "/v1/sys/mounts", nil, &mounts); e != nil {
		return e
	}
	if raw, ok := mounts["data"]; ok {
		if e = json.Unmarshal(raw, &mounts); e != nil {
			return e
		}
	}
	if _, ok := mounts["secret/"]; !ok {
		if e = api.call(ctx, "POST", "/v1/sys/mounts/secret", map[string]any{"type": "kv", "options": map[string]string{"version": "2"}}, nil); e != nil {
			return e
		}
	}
	var methods map[string]json.RawMessage
	if e = api.call(ctx, "GET", "/v1/sys/auth", nil, &methods); e != nil {
		return e
	}
	if raw, ok := methods["data"]; ok {
		if e = json.Unmarshal(raw, &methods); e != nil {
			return e
		}
	}
	if _, ok := methods["approle/"]; !ok {
		if e = api.call(ctx, "POST", "/v1/sys/auth/approle", map[string]string{"type": "approle"}, nil); e != nil {
			return e
		}
	}
	roles := map[string]string{"coordinator": "begom/coordinator", "backup": "begom/backup"}
	for i := 1; i <= 3; i++ {
		id := fmt.Sprintf("worker-%d", i)
		roles[id] = "begom/agents/" + id
	}
	for role, prefix := range roles {
		policy := fmt.Sprintf("path \"secret/data/%s/*\" { capabilities = [\"read\"] }", prefix)
		if role == "coordinator" {
			policy = fmt.Sprintf("path \"secret/data/%s/*\" { capabilities = [\"create\",\"update\",\"read\"] }\npath \"secret/data/begom/agents/+/tls\" { capabilities = [\"create\",\"update\",\"read\"] }", prefix)
		}
		if e = api.call(ctx, "PUT", "/v1/sys/policies/acl/begom-"+role, map[string]string{"policy": policy}, nil); e != nil {
			return e
		}
		if e = api.call(ctx, "POST", "/v1/auth/approle/role/begom-"+role, map[string]any{"token_policies": []string{"begom-" + role}, "token_ttl": "5m", "token_max_ttl": "10m", "secret_id_num_uses": 0}, nil); e != nil {
			return e
		}
		var roleID struct {
			Data struct {
				ID string `json:"role_id"`
			} `json:"data"`
		}
		var secretID struct {
			Data struct {
				ID string `json:"secret_id"`
			} `json:"data"`
		}
		if e = api.call(ctx, "GET", "/v1/auth/approle/role/begom-"+role+"/role-id", nil, &roleID); e != nil {
			return e
		}
		if e = api.call(ctx, "POST", "/v1/auth/approle/role/begom-"+role+"/secret-id", map[string]any{}, &secretID); e != nil {
			return e
		}
		for suffix, value := range map[string]string{"role-id": roleID.Data.ID, "secret-id": secretID.Data.ID} {
			p := filepath.Join(dir, "secrets", role+"-"+suffix)
			if e = os.WriteFile(p, []byte(value), 0440); e != nil {
				return e
			}
			if os.Geteuid() == 0 {
				if e = os.Chown(p, 10000, 10000); e != nil {
					return e
				}
			}
		}
		if role != "backup" {
			name := role
			cert, e := os.ReadFile(filepath.Join(dir, "secrets", name+".crt"))
			if e != nil {
				return e
			}
			key, e := os.ReadFile(filepath.Join(dir, "secrets", name+".key"))
			if e != nil {
				return e
			}
			if e = api.put(ctx, prefix+"/tls", map[string]string{"cert": string(cert), "key": string(key)}); e != nil {
				return e
			}
		}
		if strings.HasPrefix(role, "worker-") {
			private, e := os.ReadFile(filepath.Join(dir, "bootstrap", role+"-ssh-client.key"))
			if e != nil {
				return e
			}
			if e = api.put(ctx, "begom/coordinator/ssh/"+role, map[string]string{"private_key": string(private)}); e != nil {
				return e
			}
		}
	}
	values := map[string]string{}
	for key, file := range map[string]string{"database_url": "database-url", "metadata_key": "metadata-key", "admin_password": "admin-password"} {
		v, e := config.Secret(filepath.Join(dir, "secrets", file))
		if e != nil {
			return e
		}
		values[key] = v
	}
	if e = api.put(ctx, "begom/coordinator/runtime", values); e != nil {
		return e
	}
	ca, e := os.ReadFile(filepath.Join(dir, "secrets/ca.crt"))
	if e != nil {
		return e
	}
	cakey, e := os.ReadFile(filepath.Join(dir, "bootstrap/ca.key"))
	if e != nil {
		return e
	}
	if e = api.put(ctx, "begom/coordinator/pki", map[string]string{"cert": string(ca), "key": string(cakey)}); e != nil {
		return e
	}
	if e = write(filepath.Join(dir, "bootstrap/bao.seeded"), []byte("initialized\n"), 0600); e != nil {
		return e
	}
	var profile Profile
	if os.Geteuid() == 0 && config.Read(filepath.Join(dir, "bootstrap/profile.json"), &profile) == nil {
		os.Chown(filepath.Join(dir, "bootstrap/bao.seeded"), profile.OperatorUID, profile.OperatorGID)
	}
	return nil
}
func (b *baoAPI) put(ctx context.Context, ref string, v any) error {
	return b.call(ctx, "POST", "/v1/secret/data/"+ref, map[string]any{"data": v}, nil)
}

// RotateWorker replaces a leaf identity in KV. Agents refresh certificates on
// subsequent TLS connections; old serials can be revoked through the API.
func RotateWorker(ctx context.Context, dir, endpoint, id string) error {
	if !model.ValidID(id) {
		return errors.New("invalid worker")
	}
	var keys BaoKeys
	if e := config.Read(filepath.Join(dir, "bootstrap/bao-init.json"), &keys); e != nil {
		return e
	}
	tc, e := wire.TLS(config.TLS{CA: filepath.Join(dir, "secrets/ca.crt")}, false)
	if e != nil {
		return e
	}
	api := &baoAPI{url: strings.TrimRight(endpoint, "/"), http: &http.Client{Transport: &http.Transport{TLSClientConfig: tc}, Timeout: 5 * time.Second}, token: keys.RootToken}
	var data struct {
		Data struct {
			Data map[string]string `json:"data"`
		} `json:"data"`
	}
	if e = api.call(ctx, "GET", "/v1/secret/data/begom/coordinator/pki", nil, &data); e != nil {
		return e
	}
	authority, e := pki.Parse([]byte(data.Data.Data["cert"]), []byte(data.Data.Data["key"]))
	if e != nil {
		return e
	}
	cert, key, e := authority.Issue(id, "spiffe://begom/worker/"+id, []string{id, "localhost", "127.0.0.1"}, true)
	if e != nil {
		return e
	}
	return api.put(ctx, "begom/agents/"+id+"/tls", map[string]string{"cert": string(cert), "key": string(key)})
}
