// Package bootstrap generates a private, gitignored local deployment profile.
package bootstrap

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/rychkin/Begom/internal/config"
	"github.com/rychkin/Begom/internal/model"
	"github.com/rychkin/Begom/internal/pki"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type Profile struct {
	OperatorUID int    `json:"operator_uid"`
	OperatorGID int    `json:"operator_gid"`
	VolumeID    string `json:"volume_id"`
	Shares      int    `json:"shares,omitempty"`
	Threshold   int    `json:"threshold,omitempty"`
}

func write(path string, b []byte, mode os.FileMode) error {
	if e := os.MkdirAll(filepath.Dir(path), 0750); e != nil {
		return e
	}
	f, e := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, mode)
	if e != nil {
		return e
	}
	defer f.Close()
	_, e = f.Write(b)
	if e == nil {
		e = f.Sync()
	}
	return e
}
func jsonFile(path string, v any) error {
	b, e := json.MarshalIndent(v, "", "  ")
	if e != nil {
		return e
	}
	return write(path, b, 0644)
}
func Init(dir string) error {
	return InitSource(dir, "mock")
}
func InitSource(dir, donor string) error {
	if donor != "mock" && donor != "kinescope" {
		return errors.New("source must be mock or kinescope")
	}
	abs, e := filepath.Abs(dir)
	if e != nil {
		return e
	}
	if _, e = os.Stat(filepath.Join(abs, "client.json")); e == nil {
		return errors.New("profile already exists; init never overwrites identities")
	}
	ca, e := pki.New()
	if e != nil {
		return e
	}
	for _, p := range []string{"secrets", "config", "bootstrap", "storage/staging", "storage/published", "storage/quarantine", "packages", "fluent-buffer"} {
		if e = os.MkdirAll(filepath.Join(abs, p), 0750); e != nil {
			return e
		}
	}
	profile := Profile{OperatorUID: os.Getuid(), OperatorGID: os.Getgid(), VolumeID: model.NewID()}
	if e = jsonFile(filepath.Join(abs, "bootstrap/profile.json"), profile); e != nil {
		return e
	}
	for name, b := range map[string][]byte{"ca.crt": ca.CertPEM, "ca.key": ca.KeyPEM} {
		target := filepath.Join(abs, "secrets", name)
		if name == "ca.key" {
			target = filepath.Join(abs, "bootstrap", name)
		}
		if e = write(target, b, 0600); e != nil {
			return e
		}
	}
	identities := []struct {
		name, identity string
		hosts          []string
		client         bool
	}{{"web", "", []string{"localhost", "127.0.0.1", "::1"}, false}, {"coordinator", "spiffe://begom/service/coordinator", []string{"coordinator", "localhost", "127.0.0.1"}, true}, {"postgres", "", []string{"postgres", "localhost", "127.0.0.1"}, false}, {"openbao", "", []string{"openbao", "localhost", "127.0.0.1"}, false}, {"metrics", "spiffe://begom/service/prometheus", []string{"prometheus"}, true}}
	for i := 1; i <= 3; i++ {
		id := "worker-" + strconv.Itoa(i)
		identities = append(identities, struct {
			name, identity string
			hosts          []string
			client         bool
		}{id, "spiffe://begom/worker/" + id, []string{id, "localhost", "127.0.0.1"}, true})
	}
	for _, id := range identities {
		lifetime := 48 * time.Hour
		if !id.client || id.name == "metrics" {
			lifetime = 90 * 24 * time.Hour
		}
		cert, key, e := ca.IssueFor(id.name, id.identity, id.hosts, id.client, lifetime)
		if e != nil {
			return e
		}
		if e = write(filepath.Join(abs, "secrets", id.name+".crt"), cert, 0600); e != nil {
			return e
		}
		if e = write(filepath.Join(abs, "secrets", id.name+".key"), key, 0600); e != nil {
			return e
		}
	}
	password := model.NewID() + model.NewID()
	dbPassword := model.NewID() + model.NewID()
	appPassword, backupPassword, metricsPassword := model.NewID()+model.NewID(), model.NewID()+model.NewID(), model.NewID()+model.NewID()
	metadataKey := make([]byte, 32)
	if _, e = rand.Read(metadataKey); e != nil {
		return e
	}
	for name, value := range map[string]string{"admin-password": password, "postgres-password": dbPassword, "app-db-password": appPassword, "backup-db-password": backupPassword, "metrics-db-password": metricsPassword, "metadata-key": base64.StdEncoding.EncodeToString(metadataKey), "mock-token": model.NewID() + model.NewID(), "grafana-password": model.NewID() + model.NewID()} {
		if e = write(filepath.Join(abs, "secrets", name), []byte(value), 0600); e != nil {
			return e
		}
	}
	for _, role := range []string{"coordinator", "backup", "worker-1", "worker-2", "worker-3"} {
		for _, suffix := range []string{"role-id", "secret-id"} {
			if e = write(filepath.Join(abs, "secrets", role+"-"+suffix), nil, 0600); e != nil {
				return e
			}
		}
	}
	if e = write(filepath.Join(abs, "secrets/restic-password"), []byte(model.NewID()+model.NewID()), 0600); e != nil {
		return e
	}
	if e = write(filepath.Join(abs, "client-password"), []byte(password), 0600); e != nil {
		return e
	}
	mt, e := os.ReadFile(filepath.Join(abs, "secrets/mock-token"))
	if e != nil {
		return e
	}
	if e = write(filepath.Join(abs, "mock-token"), mt, 0600); e != nil {
		return e
	}
	if e = write(filepath.Join(abs, "storage/.begom-volume"), []byte(profile.VolumeID), 0444); e != nil {
		return e
	}
	pub, private, e := ed25519.GenerateKey(rand.Reader)
	if e != nil {
		return e
	}
	if e = write(filepath.Join(abs, "bootstrap/package.key"), []byte(base64.StdEncoding.EncodeToString(private)), 0600); e != nil {
		return e
	}
	if e = write(filepath.Join(abs, "secrets/package.pub"), []byte(base64.StdEncoding.EncodeToString(pub)), 0644); e != nil {
		return e
	}
	source := config.Source{ID: "mock", PageTemplate: "http://mockdonor:8090/{id}", AllowedHosts: []string{"mockdonor"}, Mock: true, MockCIDRs: []string{"172.30.69.0/24"}, Limits: model.Limits{RPS: 80, WorkerRPS: 40, CheckRPS: 80, DownloadRPS: 80, Connections: 120, WorkerConnections: 40}}
	if donor == "kinescope" {
		source = config.Source{ID: "kinescope", PageTemplate: "https://kinescope.io/{id}", AllowedHosts: []string{"kinescope.io", "*.kinescope.io", "*.kinescopecdn.net"}, Limits: model.Limits{RPS: 1, WorkerRPS: 1, CheckRPS: 1, DownloadRPS: 1, Connections: 4, WorkerConnections: 2}}
	}
	volume := config.Storage{Root: "/data", MountPath: "/data", VolumeID: profile.VolumeID, Mode: "local", MinFreeBytes: 32 << 20}
	bao := config.Bao{URL: "https://openbao:8200", CA: "/run/secrets/ca.crt", RoleIDFile: "/run/secrets/role-id", SecretIDFile: "/run/secrets/secret-id", Prefix: "begom/coordinator"}
	workers := []model.WorkerConfig{}
	for i := 1; i <= 3; i++ {
		id := "worker-" + strconv.Itoa(i)
		hostKey, hostPub, e := pki.SSH()
		if e != nil {
			return e
		}
		clientKey, clientPub, e := pki.SSH()
		if e != nil {
			return e
		}
		for file, data := range map[string][]byte{filepath.Join("secrets", id+"-ssh-host.key"): hostKey, filepath.Join("secrets", id+"-ssh-client.pub"): clientPub, filepath.Join("bootstrap", id+"-ssh-client.key"): clientKey} {
			if e = write(filepath.Join(abs, file), data, 0600); e != nil {
				return e
			}
		}
		workers = append(workers, model.WorkerConfig{ID: id, Name: "Рабочий сервер " + strconv.Itoa(i), Host: id, SSHPort: 2222, SSHUser: "begom-admin", SSHHostKey: string(hostPub), SSHSecret: "begom/coordinator/ssh/" + id, Threads: 40, Enabled: true, MinFreeBytes: 32 << 20, Connections: 40, RPS: 40, StoragePath: volume.Root})
		if e = os.MkdirAll(filepath.Join(abs, "updates", id), 0750); e != nil {
			return e
		}
		if e = os.MkdirAll(filepath.Join(abs, "storage/staging", id), 0770); e != nil {
			return e
		}
		av := volume
		av.MountPath = "/data/staging/" + id
		ab := bao
		ab.Prefix = "begom/agents/" + id
		c := config.Agent{WorkerID: id, CoordinatorURL: "https://coordinator:8444", TLS: config.TLS{Cert: "/run/secrets/agent.crt", Key: "/run/secrets/agent.key", CA: "/run/secrets/ca.crt"}, Storage: av, Source: source, Threads: 40, MetricsListen: ":9101", Bao: ab}
		if e = jsonFile(filepath.Join(abs, "config", id+".json"), c); e != nil {
			return e
		}
	}
	c := config.Coordinator{Listen: ":8080", AgentListen: ":8444", PublicOrigin: "https://localhost:8443", AgentTLS: config.TLS{CA: "/run/secrets/ca.crt"}, Storage: volume, Source: source, Bao: bao, HeartbeatMS: 2000, LeaseMS: 10000, SweepMS: 1000, Workers: workers, BootstrapUser: "admin", NodePackageDir: "/packages"}
	if e = jsonFile(filepath.Join(abs, "config/coordinator.json"), c); e != nil {
		return e
	}
	if e = jsonFile(filepath.Join(abs, "client.json"), config.Client{URL: "https://localhost:8443", CA: filepath.Join(abs, "secrets/ca.crt"), Username: "admin", PasswordFile: filepath.Join(abs, "client-password"), MockURL: "http://127.0.0.1:8090", MockTokenFile: filepath.Join(abs, "mock-token")}); e != nil {
		return e
	}
	dsn := fmt.Sprintf("postgres://begom:%s@postgres:5432/begom?sslmode=verify-full&sslrootcert=/run/secrets/ca.crt", appPassword)
	if e = write(filepath.Join(abs, "secrets/database-url"), []byte(dsn), 0600); e != nil {
		return e
	}
	for file, pair := range map[string][]string{"backup-db-url": {"begom_backup", backupPassword}, "metrics-db-url": {"begom_metrics", metricsPassword}, "restore-db-url": {"begom_admin", dbPassword}} {
		dsn := fmt.Sprintf("postgres://%s:%s@postgres:5432/begom?sslmode=verify-full&sslrootcert=/run/secrets/ca.crt", pair[0], pair[1])
		if e = write(filepath.Join(abs, "secrets", file), []byte(dsn), 0600); e != nil {
			return e
		}
	}
	backupConfig := map[string]any{"database_url_file": "/run/secrets/database-url", "storage": volume, "repository": "/repository", "password_file": "/run/secrets/restic-password", "work_dir": "/tmp"}
	if e = jsonFile(filepath.Join(abs, "config/backup.json"), backupConfig); e != nil {
		return e
	}
	return nil
}

// Permissions is a one-shot local container operation. It only touches generated
// paths, never traverses symlinks and never changes AGENTS.md or source files.
func Permissions(dir string) error {
	if os.Geteuid() != 0 {
		return errors.New("permissions helper requires root inside the init container")
	}
	var p Profile
	if e := config.Read(filepath.Join(dir, "bootstrap/profile.json"), &p); e != nil {
		return e
	}
	entries, e := os.ReadDir(filepath.Join(dir, "secrets"))
	if e != nil {
		return e
	}
	for _, f := range entries {
		if f.IsDir() || f.Type()&os.ModeSymlink != 0 {
			return errors.New("unexpected secret entry")
		}
		name := f.Name()
		uid, gid, mode := 10000, 10000, os.FileMode(0440)
		if name == "postgres.key" {
			uid, gid, mode = 999, 999, 0400
		}
		if name == "postgres-password" || strings.HasSuffix(name, "-db-password") {
			uid, gid, mode = 999, 999, 0400
		}
		if name == "web.key" {
			uid, gid, mode = 0, 0, 0400
		}
		if name == "metrics.key" {
			uid, gid, mode = 65534, 65534, 0400
		}
		if name == "grafana-password" {
			uid, gid, mode = 472, 472, 0400
		}
		if filepath.Ext(name) == ".crt" || filepath.Ext(name) == ".pub" {
			mode = 0444
		}
		path := filepath.Join(dir, "secrets", name)
		if e = os.Chown(path, uid, gid); e != nil {
			return e
		}
		if e = os.Chmod(path, mode); e != nil {
			return e
		}
	}
	for p, uid := range map[string]int{"storage": 10000, "storage/staging": 10000, "storage/published": 10000, "storage/quarantine": 10000, "storage/staging/worker-1": 10001, "storage/staging/worker-2": 10002, "storage/staging/worker-3": 10003} {
		target := filepath.Join(dir, p)
		st, e := os.Lstat(target)
		if e != nil {
			return e
		}
		if !st.IsDir() || st.Mode()&os.ModeSymlink != 0 {
			return errors.New("unexpected storage directory")
		}
		if e = os.Chown(target, uid, 10000); e != nil {
			return e
		}
		if e = os.Chmod(target, 0770); e != nil {
			return e
		}
	}
	for p, uid := range map[string]int{"/bao-data": 10000, "/grafana-data": 472, "/prometheus-data": 65534, "/loki-data": 10001, "/backup-data": 10000} {
		if _, e = os.Stat(p); e == nil {
			if e = os.Chown(p, uid, uid); e != nil {
				return e
			}
		}
	}
	for i := 1; i <= 3; i++ {
		p := filepath.Join(dir, "updates/worker-"+strconv.Itoa(i))
		if e = os.Chown(p, 10000+i, 10000); e != nil {
			return e
		}
		if e = os.Chmod(p, 0770); e != nil {
			return e
		}
	}
	if e = os.Chown(filepath.Join(dir, "packages"), p.OperatorUID, 10000); e != nil {
		return e
	}
	if e = os.Chmod(filepath.Join(dir, "packages"), 0750); e != nil {
		return e
	}
	return os.Chmod(filepath.Join(dir, "secrets"), 0755)
}
