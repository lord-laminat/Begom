package bootstrap

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/rychkin/Begom/internal/config"
	"github.com/rychkin/Begom/internal/pki"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// LinuxProfile describes DNS names on the private network, never credentials.
type LinuxProfile struct {
	PublicHost  string   `json:"public_host"`
	Coordinator string   `json:"coordinator_host"`
	Postgres    string   `json:"postgres_host"`
	OpenBao     string   `json:"openbao_host"`
	Workers     []string `json:"worker_hosts"`
	Source      string   `json:"source"`
	MockURL     string   `json:"mock_url"`
	MockCIDRs   []string `json:"mock_cidrs"`
}

func InitLinux(dir string, p LinuxProfile) error {
	if len(p.Workers) != 3 || p.PublicHost == "" || p.Coordinator == "" || p.Postgres == "" || p.OpenBao == "" {
		return errors.New("linux profile requires public, coordinator, postgres, openbao DNS names and three worker hosts")
	}
	if p.Source == "" {
		p.Source = "kinescope"
	}
	if e := InitSource(dir, p.Source); e != nil {
		return e
	}
	var profile Profile
	if e := config.Read(filepath.Join(dir, "bootstrap/profile.json"), &profile); e != nil {
		return e
	}
	profile.Shares = 5
	profile.Threshold = 3
	replace := func(name string, v any) error {
		b, e := json.MarshalIndent(v, "", "  ")
		if e != nil {
			return e
		}
		return os.WriteFile(filepath.Join(dir, name), b, 0600)
	}
	if e := replace("bootstrap/profile.json", profile); e != nil {
		return e
	}
	certPEM, e := os.ReadFile(filepath.Join(dir, "secrets/ca.crt"))
	if e != nil {
		return e
	}
	keyPEM, e := os.ReadFile(filepath.Join(dir, "bootstrap/ca.key"))
	if e != nil {
		return e
	}
	ca, e := pki.Parse(certPEM, keyPEM)
	if e != nil {
		return e
	}
	identities := map[string]struct {
		host, uri string
		client    bool
	}{"web": {p.PublicHost, "", false}, "coordinator": {p.Coordinator, "spiffe://begom/service/coordinator", true}, "postgres": {p.Postgres, "", false}, "openbao": {p.OpenBao, "", false}}
	for i, host := range p.Workers {
		id := fmt.Sprintf("worker-%d", i+1)
		identities[id] = struct {
			host, uri string
			client    bool
		}{host, "spiffe://begom/worker/" + id, true}
	}
	for name, id := range identities {
		lifetime := 48 * time.Hour
		if !id.client {
			lifetime = 90 * 24 * time.Hour
		}
		cert, key, err := ca.IssueFor(name, id.uri, []string{id.host, "localhost", "127.0.0.1"}, id.client, lifetime)
		if err != nil {
			return err
		}
		if err = os.WriteFile(filepath.Join(dir, "secrets", name+".crt"), cert, 0600); err != nil {
			return err
		}
		if err = os.WriteFile(filepath.Join(dir, "secrets", name+".key"), key, 0600); err != nil {
			return err
		}
	}
	var c config.Coordinator
	if e = config.Read(filepath.Join(dir, "config/coordinator.json"), &c); e != nil {
		return e
	}
	c.Listen = "127.0.0.1:8080"
	c.PublicOrigin = "https://" + p.PublicHost
	c.Storage.Root = "/srv/begom"
	c.Storage.MountPath = "/srv/begom"
	c.Storage.Mode = "nfs"
	c.Storage.MinFreeBytes = 1 << 30
	c.Bao.URL = "https://" + p.OpenBao + ":8200"
	c.Bao.CA = "/etc/begom/ca.crt"
	c.Bao.RoleIDFile = "/etc/begom/role-id"
	c.Bao.SecretIDFile = "/etc/begom/secret-id"
	c.AgentTLS.CA = "/etc/begom/ca.crt"
	c.NodePackageDir = "/var/lib/begom/packages"
	if p.Source == "mock" {
		if p.MockURL == "" || len(p.MockCIDRs) == 0 {
			return errors.New("mock linux stand requires explicit URL and allowed CIDRs")
		}
		c.Source.PageTemplate = p.MockURL + "/{id}"
		c.Source.AllowedHosts = []string{mustHost(p.MockURL)}
		c.Source.MockCIDRs = p.MockCIDRs
	}
	for i := range c.Workers {
		c.Workers[i].Host = p.Workers[i]
		c.Workers[i].SSHPort = 2222
		c.Workers[i].MinFreeBytes = 1 << 30
		c.Workers[i].StoragePath = c.Storage.Root
	}
	if e = replace("config/coordinator.json", c); e != nil {
		return e
	}
	for i, host := range p.Workers {
		_ = host
		id := fmt.Sprintf("worker-%d", i+1)
		var a config.Agent
		if e = config.Read(filepath.Join(dir, "config", id+".json"), &a); e != nil {
			return e
		}
		a.CoordinatorURL = "https://" + p.Coordinator + ":8444"
		a.Storage = c.Storage
		a.Bao = c.Bao
		a.Bao.Prefix = "begom/agents/" + id
		a.Source = c.Source
		a.TLS = config.TLS{Cert: "/etc/begom/agent.crt", Key: "/etc/begom/agent.key", CA: "/etc/begom/ca.crt"}
		if e = replace("config/"+id+".json", a); e != nil {
			return e
		}
	}
	// Paths in KV refer to the service's host, not the provisioning controller.
	for _, file := range []string{"database-url", "backup-db-url", "metrics-db-url", "restore-db-url"} {
		b, err := os.ReadFile(filepath.Join(dir, "secrets", file))
		if err != nil {
			return err
		}
		value := strings.ReplaceAll(string(b), "@postgres:", "@"+p.Postgres+":")
		value = strings.ReplaceAll(value, "/run/secrets/ca.crt", "/etc/begom/ca.crt")
		if err = os.WriteFile(filepath.Join(dir, "secrets", file), []byte(value), 0600); err != nil {
			return err
		}
	}
	return replace("client.json", config.Client{URL: c.PublicOrigin, CA: filepath.Join(dir, "secrets/ca.crt"), Username: "admin", PasswordFile: filepath.Join(dir, "client-password"), MockURL: p.MockURL, MockTokenFile: filepath.Join(dir, "mock-token")})
}
func mustHost(raw string) string {
	u, e := url.Parse(raw)
	if e != nil {
		return ""
	}
	return u.Hostname()
}
