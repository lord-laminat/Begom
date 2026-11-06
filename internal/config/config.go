package config

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/rychkin/Begom/internal/model"
	"gopkg.in/yaml.v3"
	"io"
	"os"
	"strings"
	"time"
)

type TLS struct {
	Cert string `json:"cert" yaml:"cert"`
	Key  string `json:"key" yaml:"key"`
	CA   string `json:"ca" yaml:"ca"`
}
type Bao struct {
	URL          string `json:"url" yaml:"url"`
	CA           string `json:"ca" yaml:"ca"`
	RoleIDFile   string `json:"role_id_file" yaml:"role_id_file"`
	SecretIDFile string `json:"secret_id_file" yaml:"secret_id_file"`
	Prefix       string `json:"prefix" yaml:"prefix"`
}
type Storage struct {
	Root         string `json:"root" yaml:"root"`
	VolumeID     string `json:"volume_id" yaml:"volume_id"`
	Mode         string `json:"mode" yaml:"mode"`
	MountPath    string `json:"mount_path" yaml:"mount_path"`
	MinFreeBytes uint64 `json:"min_free_bytes" yaml:"min_free_bytes"`
}
type Source struct {
	ID           string       `json:"source_id" yaml:"source_id"`
	PageTemplate string       `json:"page_template" yaml:"page_template"`
	AllowedHosts []string     `json:"allowed_hosts" yaml:"allowed_hosts"`
	MockCIDRs    []string     `json:"mock_cidrs" yaml:"mock_cidrs"`
	Mock         bool         `json:"mock" yaml:"mock"`
	Limits       model.Limits `json:"limits" yaml:"limits"`
}
type Coordinator struct {
	Listen                string               `json:"listen" yaml:"listen"`
	AgentListen           string               `json:"agent_listen" yaml:"agent_listen"`
	PublicOrigin          string               `json:"public_origin" yaml:"public_origin"`
	DatabaseURLFile       string               `json:"database_url_file" yaml:"database_url_file"`
	AgentTLS              TLS                  `json:"agent_tls" yaml:"agent_tls"`
	Storage               Storage              `json:"storage" yaml:"storage"`
	Source                Source               `json:"source" yaml:"source"`
	Bao                   Bao                  `json:"bao" yaml:"bao"`
	HeartbeatMS           int                  `json:"heartbeat_ms" yaml:"heartbeat_ms"`
	LeaseMS               int                  `json:"lease_ms" yaml:"lease_ms"`
	SweepMS               int                  `json:"sweep_ms" yaml:"sweep_ms"`
	Workers               []model.WorkerConfig `json:"workers" yaml:"workers"`
	BootstrapUser         string               `json:"bootstrap_user" yaml:"bootstrap_user"`
	BootstrapPasswordFile string               `json:"bootstrap_password_file" yaml:"bootstrap_password_file"`
	NodePackageDir        string               `json:"node_package_dir" yaml:"node_package_dir"`
	MetadataKeyFile       string               `json:"metadata_key_file" yaml:"metadata_key_file"`
}
type Agent struct {
	WorkerID       string  `json:"worker_id" yaml:"worker_id"`
	CoordinatorURL string  `json:"coordinator_url" yaml:"coordinator_url"`
	TLS            TLS     `json:"tls" yaml:"tls"`
	Storage        Storage `json:"storage" yaml:"storage"`
	Source         Source  `json:"source" yaml:"source"`
	MetricsListen  string  `json:"metrics_listen" yaml:"metrics_listen"`
	Threads        int     `json:"threads" yaml:"threads"`
	Bao            Bao     `json:"bao" yaml:"bao"`
}
type Client struct {
	URL           string `json:"url" yaml:"url"`
	CA            string `json:"ca" yaml:"ca"`
	Username      string `json:"username" yaml:"username"`
	PasswordFile  string `json:"password_file" yaml:"password_file"`
	MockURL       string `json:"mock_url" yaml:"mock_url"`
	MockTokenFile string `json:"mock_token_file" yaml:"mock_token_file"`
}

func Read(path string, out any) error {
	b, e := os.ReadFile(path)
	if e != nil {
		return e
	}
	if json.Valid(b) {
		d := json.NewDecoder(bytes.NewReader(b))
		d.DisallowUnknownFields()
		if e = d.Decode(out); e != nil {
			return e
		}
		if d.Decode(new(any)) != io.EOF {
			return fmt.Errorf("trailing configuration data")
		}
		return nil
	}
	d := yaml.NewDecoder(bytes.NewReader(b))
	d.KnownFields(true)
	return d.Decode(out)
}
func Secret(path string) (string, error) {
	b, e := os.ReadFile(path)
	if e != nil {
		return "", fmt.Errorf("cannot read configured secret file")
	}
	return strings.TrimSpace(string(b)), nil
}
func (c *Coordinator) Defaults() {
	if c.Listen == "" {
		c.Listen = ":8080"
	}
	if c.AgentListen == "" {
		c.AgentListen = ":8444"
	}
	if c.HeartbeatMS == 0 {
		c.HeartbeatMS = 2000
	}
	if c.LeaseMS == 0 {
		c.LeaseMS = 10000
	}
	if c.SweepMS == 0 {
		c.SweepMS = 1000
	}
	if c.Source.Limits.RPS == 0 {
		c.Source.Limits = model.Limits{RPS: 1, WorkerRPS: 1, CheckRPS: 1, DownloadRPS: 1, Connections: 4, WorkerConnections: 2}
	}
}
func (c Coordinator) LeaseDuration() time.Duration {
	return time.Duration(c.LeaseMS) * time.Millisecond
}
