// Package model defines version 1 of the coordinator/agent wire contract.
package model

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const Version = "1.0.0"
const APIVersion = "1"

// Number is transmitted as a decimal string so browsers retain all 64 bits.
type Number uint64

func (n Number) String() string               { return strconv.FormatUint(uint64(n), 10) }
func (n Number) MarshalJSON() ([]byte, error) { return json.Marshal(n.String()) }
func (n *Number) UnmarshalJSON(b []byte) error {
	s := string(b)
	if len(s) > 0 && s[0] == '"' {
		if err := json.Unmarshal(b, &s); err != nil {
			return err
		}
	}
	if s == "" || strings.Trim(s, "0123456789") != "" {
		return errors.New("expected an unsigned decimal integer")
	}
	v, err := strconv.ParseUint(s, 10, 64)
	*n = Number(v)
	return err
}
func NewID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b[:])
}

type Range struct {
	From Number  `json:"from" yaml:"from"`
	To   *Number `json:"to" yaml:"to"`
}

func ParseRange(s string) (Range, error) {
	s = strings.TrimSpace(s)
	r := Range{}
	parse := func(s string) (Number, error) {
		var n Number
		err := n.UnmarshalJSON([]byte(strings.TrimSpace(s)))
		return n, err
	}
	if s == "" {
		return r, nil
	}
	if strings.HasPrefix(s, "[") && strings.HasSuffix(s, "]") {
		p := strings.Split(s[1:len(s)-1], ",")
		if len(p) != 2 {
			return r, errors.New("range must be [A,B] or [A,]")
		}
		var err error
		r.From, err = parse(p[0])
		if err != nil {
			return r, err
		}
		if strings.TrimSpace(p[1]) != "" {
			n, e := parse(p[1])
			if e != nil {
				return r, e
			}
			r.To = &n
		}
	} else {
		n, err := parse(s)
		if err != nil {
			return r, err
		}
		r.To = &n
	}
	if r.To != nil && r.From > *r.To {
		return r, errors.New("range start exceeds end")
	}
	return r, nil
}
func BlockEnd(start Number, size uint64, to *Number) Number {
	end := Number(^uint64(0))
	if size > 0 && uint64(start) <= ^uint64(0)-(size-1) {
		end = start + Number(size-1)
	}
	if to != nil && end > *to {
		end = *to
	}
	return end
}

type Limits struct {
	RPS               float64 `json:"rps" yaml:"rps"`
	WorkerRPS         float64 `json:"worker_rps" yaml:"worker_rps"`
	CheckRPS          float64 `json:"check_rps" yaml:"check_rps"`
	DownloadRPS       float64 `json:"download_rps" yaml:"download_rps"`
	Connections       int     `json:"connections" yaml:"connections"`
	WorkerConnections int     `json:"worker_connections" yaml:"worker_connections"`
}
type RetryPolicy struct {
	MaxRetries int `json:"max_retries" yaml:"max_retries"`
	BaseMS     int `json:"base_ms" yaml:"base_ms"`
	MaxMS      int `json:"max_ms" yaml:"max_ms"`
}
type StopConditions struct {
	At                 *time.Time `json:"at,omitempty" yaml:"at"`
	MaxItems           *Number    `json:"max_items,omitempty" yaml:"max_items"`
	ConsecutiveMissing uint64     `json:"consecutive_missing" yaml:"consecutive_missing"`
	NoWorkersSeconds   int        `json:"no_workers_seconds" yaml:"no_workers_seconds"`
}
type JobConfig struct {
	RangeText       *string        `json:"range_text,omitempty" yaml:"range_text"`
	From            *Number        `json:"from,omitempty" yaml:"from"`
	To              *Number        `json:"to,omitempty" yaml:"to"`
	WorkerIDs       []string       `json:"worker_ids" yaml:"worker_ids"`
	Threads         int            `json:"threads_default" yaml:"threads_default"`
	ThreadsByWorker map[string]int `json:"threads_by_worker" yaml:"threads_by_worker"`
	BlockSize       uint64         `json:"block_size" yaml:"block_size"`
	Limits          Limits         `json:"limits" yaml:"limits"`
	MinFreeBytes    uint64         `json:"min_free_bytes" yaml:"min_free_bytes"`
	MaxFileBytes    uint64         `json:"max_file_bytes" yaml:"max_file_bytes"`
	Retry           RetryPolicy    `json:"retry_policy" yaml:"retry_policy"`
	HTTPTimeoutMS   int            `json:"http_timeout_ms" yaml:"http_timeout_ms"`
	Stop            StopConditions `json:"stop_conditions" yaml:"stop_conditions"`
}

func (c *JobConfig) Normalize() (Range, error) {
	var r Range
	var err error
	if c.RangeText != nil {
		if c.From != nil || c.To != nil {
			return r, errors.New("range_text and from/to are mutually exclusive")
		}
		r, err = ParseRange(*c.RangeText)
	} else {
		if c.From != nil {
			r.From = *c.From
		}
		r.To = c.To
		if r.To != nil && r.From > *r.To {
			err = errors.New("range start exceeds end")
		}
	}
	if err != nil {
		return r, err
	}
	if len(c.WorkerIDs) == 0 {
		return r, errors.New("worker_ids must not be empty")
	}
	seen := map[string]bool{}
	for _, id := range c.WorkerIDs {
		if !ValidID(id) || seen[id] {
			return r, errors.New("invalid or duplicate worker")
		}
		seen[id] = true
	}
	if c.Threads == 0 {
		c.Threads = 8
	}
	if c.BlockSize == 0 {
		c.BlockSize = 1000
	}
	if c.HTTPTimeoutMS == 0 {
		c.HTTPTimeoutMS = 20000
	}
	if c.MaxFileBytes == 0 {
		c.MaxFileBytes = 10 << 30
	}
	if c.Retry.BaseMS == 0 {
		c.Retry.BaseMS = 500
	}
	if c.Retry.MaxMS == 0 {
		c.Retry.MaxMS = 30000
	}
	if c.Stop.NoWorkersSeconds == 0 {
		c.Stop.NoWorkersSeconds = 30
	}
	if c.Threads < 1 || c.Threads > 1024 || c.BlockSize > 10000 || c.BlockSize < 1 || c.HTTPTimeoutMS < 100 || c.HTTPTimeoutMS > 3600000 || c.Retry.MaxRetries < 0 || c.Retry.MaxRetries > 20 || c.Retry.BaseMS < 1 || c.Retry.MaxMS < c.Retry.BaseMS || c.Retry.MaxMS > 3600000 || c.MaxFileBytes > 1<<60 || c.Stop.NoWorkersSeconds < 1 {
		return r, errors.New("parameters outside supported bounds")
	}
	for id, n := range c.ThreadsByWorker {
		if !seen[id] || n < 1 || n > 1024 {
			return r, errors.New("invalid worker thread override")
		}
	}
	for _, v := range []float64{c.Limits.RPS, c.Limits.WorkerRPS, c.Limits.CheckRPS, c.Limits.DownloadRPS} {
		if v < 0 || v > 100000 || v > 0 && v < .001 {
			return r, errors.New("invalid rate limit")
		}
	}
	if c.Limits.Connections < 0 || c.Limits.WorkerConnections < 0 {
		return r, errors.New("invalid connection limit")
	}
	if c.Stop.MaxItems != nil {
		if *c.Stop.MaxItems == 0 {
			return r, errors.New("max_items must be positive")
		}
		end := BlockEnd(r.From, uint64(*c.Stop.MaxItems), r.To)
		r.To = &end
	}
	return r, nil
}
func (c JobConfig) ThreadsFor(worker string, cap int) int {
	n := c.Threads
	if v := c.ThreadsByWorker[worker]; v > 0 {
		n = v
	}
	if cap > 0 && n > cap {
		n = cap
	}
	return n
}
func ValidID(s string) bool {
	if len(s) < 1 || len(s) > 80 {
		return false
	}
	for _, c := range s {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-' || c == '_') {
			return false
		}
	}
	return true
}

var versionPattern = regexp.MustCompile(`^[0-9]+\.[0-9]+\.[0-9]+(?:-[a-z0-9-]+)?$`)

func ValidVersion(s string) bool { return len(s) <= 64 && versionPattern.MatchString(s) }
func Final(s string) bool {
	switch s {
	case "completed", "not_found", "skipped", "forbidden", "failed":
		return true
	}
	return false
}
func Transition(from, action string) (string, error) {
	to := ""
	switch action {
	case "start":
		if from == "created" {
			to = "validating"
		}
	case "pause":
		if from == "running" {
			to = "pausing"
		}
	case "stop":
		if from == "running" || from == "pausing" || from == "paused" {
			to = "stopping"
		}
	case "resume":
		if from == "paused" || from == "stopped" {
			to = "running"
		}
		if from == "failed" {
			to = "validating"
		}
	}
	if to == "" {
		return "", fmt.Errorf("cannot %s a %s job", action, from)
	}
	return to, nil
}

type Job struct {
	ID                  string            `json:"job_id"`
	RunID               string            `json:"run_id"`
	State               string            `json:"state"`
	Range               Range             `json:"range"`
	Config              JobConfig         `json:"config"`
	ConfigVersion       int64             `json:"config_version"`
	Reason              string            `json:"reason,omitempty"`
	CreatedAt           time.Time         `json:"created_at"`
	UpdatedAt           time.Time         `json:"updated_at"`
	Counts              map[string]uint64 `json:"counts"`
	Bytes               uint64            `json:"bytes"`
	NextID              *Number           `json:"next_id"`
	Rate                float64           `json:"rate"`
	CurrentRate         float64           `json:"current_rate"`
	AverageDownloadRate float64           `json:"average_download_rate"`
	FoundCount          uint64            `json:"found_count"`
	ETASeconds          *float64          `json:"eta_seconds"`
}
type Metrics struct {
	MeasuredAt            time.Time `json:"measured_at"`
	CPU                   float64   `json:"cpu_percent"`
	Load                  float64   `json:"load"`
	RAMTotal              uint64    `json:"ram_total"`
	RAMUsed               uint64    `json:"ram_used"`
	SwapTotal             uint64    `json:"swap_total"`
	SwapUsed              uint64    `json:"swap_used"`
	DiskTotal             uint64    `json:"disk_total"`
	DiskFree              uint64    `json:"disk_free"`
	NetworkRX             uint64    `json:"network_rx_bytes"`
	NetworkTX             uint64    `json:"network_tx_bytes"`
	DiskRead              uint64    `json:"disk_read_bytes"`
	DiskWrite             uint64    `json:"disk_write_bytes"`
	OpenFiles             uint64    `json:"open_files"`
	Connections           uint64    `json:"connections"`
	Uptime                float64   `json:"uptime_seconds"`
	ActiveChecks          int       `json:"active_checks"`
	ActiveDownloads       int       `json:"active_downloads"`
	Bytes                 uint64    `json:"downloaded_bytes"`
	StorageHealthy        bool      `json:"storage_healthy"`
	Errors                uint64    `json:"errors"`
	RecentErrorsPerSecond float64   `json:"recent_errors_per_second"`
	NetworkBPS            float64   `json:"network_bytes_per_second"`
	DiskBPS               float64   `json:"disk_bytes_per_second"`
}
type WorkerConfig struct {
	ID            string  `json:"worker_id" yaml:"worker_id"`
	Name          string  `json:"name" yaml:"name"`
	Host          string  `json:"host" yaml:"host"`
	SSHPort       int     `json:"ssh_port" yaml:"ssh_port"`
	SSHUser       string  `json:"ssh_user" yaml:"ssh_user"`
	SSHSecret     string  `json:"ssh_secret_ref,omitempty" yaml:"ssh_secret_ref"`
	SSHHostKey    string  `json:"ssh_host_key" yaml:"ssh_host_key"`
	Threads       int     `json:"threads" yaml:"threads"`
	Enabled       bool    `json:"enabled" yaml:"enabled"`
	MinFreeBytes  uint64  `json:"min_free_bytes" yaml:"min_free_bytes"`
	StoragePath   string  `json:"storage_path" yaml:"storage_path"`
	RPS           float64 `json:"rps" yaml:"rps"`
	Connections   int     `json:"connections" yaml:"connections"`
	MaxNetworkBPS uint64  `json:"max_network_bps" yaml:"max_network_bps"`
	MaxDiskBPS    uint64  `json:"max_disk_bps" yaml:"max_disk_bps"`
}

func (w WorkerConfig) Validate() error {
	if !ValidID(w.ID) || w.Threads < 1 || w.Threads > 1024 || w.Connections < 0 || w.Connections > 10000 || w.SSHPort < 0 || w.SSHPort > 65535 || math.IsNaN(w.RPS) || math.IsInf(w.RPS, 0) || w.RPS < 0 || w.RPS > 100000 || w.RPS > 0 && w.RPS < .001 {
		return errors.New("invalid worker configuration")
	}
	return nil
}

type Worker struct {
	WorkerConfig
	Version       string           `json:"version"`
	LastHeartbeat *time.Time       `json:"last_heartbeat"`
	Metrics       Metrics          `json:"metrics"`
	Leases        []Lease          `json:"leases"`
	Downloads     []ActiveDownload `json:"downloads"`
	Online        bool             `json:"online"`
}
type Register struct {
	StorageRoot string `json:"storage_root"`
	SourceID    string `json:"source_id"`
	WorkerID    string `json:"worker_id"`
	Version     string `json:"version"`
	APIVersion  string `json:"api_version"`
	SessionID   string `json:"session_id"`
}
type Lease struct {
	JobID         string    `json:"job_id"`
	RunID         string    `json:"run_id"`
	ChunkID       string    `json:"chunk_id"`
	AttemptID     string    `json:"attempt_id"`
	From          Number    `json:"range_start"`
	To            Number    `json:"range_end"`
	NextID        *Number   `json:"next_id"`
	ExpiresAt     time.Time `json:"lease_expires_at"`
	ServerTime    time.Time `json:"server_time"`
	HeartbeatMS   int       `json:"heartbeat_interval_ms"`
	ConfigVersion int64     `json:"config_version"`
	Config        JobConfig `json:"config"`
	Confirmed     []Number  `json:"confirmed,omitempty"`
}
type LeaseRef struct {
	JobID     string `json:"job_id"`
	RunID     string `json:"run_id"`
	ChunkID   string `json:"chunk_id"`
	AttemptID string `json:"attempt_id"`
}

func (l Lease) Ref() LeaseRef { return LeaseRef{l.JobID, l.RunID, l.ChunkID, l.AttemptID} }

type Heartbeat struct {
	SessionID string           `json:"session_id"`
	Version   string           `json:"version"`
	Metrics   Metrics          `json:"metrics"`
	Leases    []LeaseRef       `json:"leases"`
	Downloads []ActiveDownload `json:"downloads"`
}
type ActiveDownload struct {
	JobID          string  `json:"job_id"`
	ExternalID     Number  `json:"external_id"`
	Title          string  `json:"title"`
	Offset         uint64  `json:"offset"`
	Size           uint64  `json:"size"`
	BytesPerSecond float64 `json:"bytes_per_second"`
}
type Renewal struct {
	ChunkID       string    `json:"chunk_id"`
	Valid         bool      `json:"valid"`
	ExpiresAt     time.Time `json:"lease_expires_at"`
	State         string    `json:"state"`
	ConfigVersion int64     `json:"config_version"`
	Config        JobConfig `json:"config"`
}
type HeartbeatReply struct {
	ServerTime time.Time    `json:"server_time"`
	Epoch      string       `json:"epoch"`
	Worker     WorkerConfig `json:"worker"`
	Leases     []Renewal    `json:"leases"`
}
type Metadata struct {
	CanonicalID string         `json:"canonical_id"`
	Title       string         `json:"title"`
	PageURL     string         `json:"page_url"`
	DownloadURL string         `json:"download_url,omitempty"`
	MIME        string         `json:"mime_type"`
	Size        uint64         `json:"size_bytes"`
	FoundAt     time.Time      `json:"found_at"`
	Quality     string         `json:"quality,omitempty"`
	Resources   map[string]int `json:"resources,omitempty"`
}
type ItemResult struct {
	ExternalID   Number    `json:"external_id"`
	Status       string    `json:"status"`
	ErrorCode    string    `json:"error_code,omitempty"`
	Metadata     *Metadata `json:"metadata,omitempty"`
	DownloadID   string    `json:"download_id,omitempty"`
	StageID      string    `json:"stage_id,omitempty"`
	Offset       uint64    `json:"offset,omitempty"`
	ETag         string    `json:"etag,omitempty"`
	Checksum     string    `json:"checksum,omitempty"`
	ReserveBytes uint64    `json:"reserve_bytes,omitempty"`
}
type Progress struct {
	LeaseRef
	OperationID string       `json:"operation_id"`
	Sequence    uint64       `json:"sequence"`
	Results     []ItemResult `json:"results"`
}
type Commit struct {
	LeaseRef
	StageID  string `json:"stage_id"`
	Size     uint64 `json:"size_bytes"`
	Checksum string `json:"checksum"`
}
type PermitRequest struct {
	LeaseRef
	RequestUID string `json:"request_uid"`
	Class      string `json:"request_class"`
}
type Permit struct {
	ID                  string    `json:"permit_id"`
	Epoch               string    `json:"epoch"`
	RequestUID          string    `json:"request_uid"`
	NotBefore           time.Time `json:"not_before"`
	ExpiresAt           time.Time `json:"expires_at"`
	ConnectionID        string    `json:"connection_id"`
	ConnectionExpiresAt time.Time `json:"connection_expires_at"`
	ServerTime          time.Time `json:"server_time"`
}
type Feedback struct {
	Status       int    `json:"status"`
	RetryAfterMS int64  `json:"retry_after_ms"`
	DurationMS   int64  `json:"duration_ms"`
	Bytes        uint64 `json:"bytes"`
}
type APIError struct {
	Code         string `json:"code"`
	Message      string `json:"message"`
	RequestID    string `json:"request_id"`
	RetryAfterMS int    `json:"retry_after_ms,omitempty"`
}
