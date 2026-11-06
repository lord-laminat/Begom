package config

import (
	"errors"
	"github.com/rychkin/Begom/internal/model"
	"math"
	"net/netip"
	"net/url"
	"path/filepath"
	"strings"
)

func (s Source) Validate() error {
	if !model.ValidID(s.ID) || strings.Count(s.PageTemplate, "{id}") != 1 || len(s.AllowedHosts) == 0 {
		return errors.New("invalid donor profile")
	}
	u, e := url.Parse(strings.Replace(s.PageTemplate, "{id}", "0", 1))
	if e != nil || u.User != nil || u.Hostname() == "" || u.Fragment != "" || u.Scheme != "https" && (!s.Mock || u.Scheme != "http") {
		return errors.New("invalid donor URL template")
	}
	if !s.Mock && len(s.MockCIDRs) > 0 {
		return errors.New("private donor networks require explicit mock profile")
	}
	for _, cidr := range s.MockCIDRs {
		prefix, e := netip.ParsePrefix(cidr)
		if e != nil || prefix.Bits() == 0 {
			return errors.New("invalid mock network")
		}
	}
	for _, v := range []float64{s.Limits.RPS, s.Limits.WorkerRPS, s.Limits.CheckRPS, s.Limits.DownloadRPS} {
		if math.IsNaN(v) || math.IsInf(v, 0) || v < 0 || v > 100000 || v > 0 && v < .001 {
			return errors.New("rate outside supported bounds")
		}
	}
	if s.Limits.RPS <= 0 || s.Limits.Connections < 1 || s.Limits.Connections > 10000 || s.Limits.WorkerConnections < 0 {
		return errors.New("global donor quota must be bounded")
	}
	return nil
}
func (s Storage) Validate() error {
	if !filepath.IsAbs(s.Root) || !filepath.IsAbs(s.MountPath) || !model.ValidID(s.VolumeID) || (s.Mode != "local" && s.Mode != "nfs" && s.Mode != "test") {
		return errors.New("invalid storage profile")
	}
	return nil
}
func (c Coordinator) Validate() error {
	if c.HeartbeatMS < 100 || c.HeartbeatMS > 60000 || c.LeaseMS < 2*c.HeartbeatMS+1000 || c.LeaseMS > 3600000 || c.SweepMS < 10 || c.SweepMS > c.HeartbeatMS {
		return errors.New("invalid lease timing")
	}
	u, e := url.Parse(c.PublicOrigin)
	if e != nil || u.Scheme != "https" || u.User != nil || u.Host == "" || u.RawQuery != "" || u.Path != "" {
		return errors.New("invalid public origin")
	}
	if e = c.Source.Validate(); e != nil {
		return e
	}
	return c.Storage.Validate()
}
func (c Agent) Validate() error {
	if !model.ValidID(c.WorkerID) || c.Threads < 1 || c.Threads > 1024 {
		return errors.New("invalid agent profile")
	}
	if e := c.Source.Validate(); e != nil {
		return e
	}
	return c.Storage.Validate()
}
