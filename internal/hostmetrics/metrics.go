// Package hostmetrics reads Linux counters without privileged subprocesses.
package hostmetrics

import (
	"bufio"
	"github.com/rychkin/Begom/internal/model"
	"github.com/rychkin/Begom/internal/storage"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Sampler struct {
	Volume      *storage.Volume
	mu          sync.Mutex
	idle, total uint64
	started     time.Time
	previous    model.Metrics
}

func New(v *storage.Volume) *Sampler { return &Sampler{Volume: v, started: time.Now()} }
func num(s string) uint64            { n, _ := strconv.ParseUint(s, 10, 64); return n }
func (s *Sampler) Sample() model.Metrics {
	s.mu.Lock()
	defer s.mu.Unlock()
	m := model.Metrics{MeasuredAt: time.Now().UTC(), Uptime: time.Since(s.started).Seconds()}
	if b, e := os.ReadFile("/proc/stat"); e == nil {
		line := strings.SplitN(string(b), "\n", 2)[0]
		p := strings.Fields(line)
		if len(p) >= 5 {
			var total uint64
			for _, v := range p[1:] {
				total += num(v)
			}
			idle := num(p[4])
			if len(p) > 5 {
				idle += num(p[5])
			}
			if total > s.total {
				m.CPU = 100 * (1 - float64(idle-s.idle)/float64(total-s.total))
			}
			s.total = total
			s.idle = idle
		}
	}
	if b, e := os.ReadFile("/proc/meminfo"); e == nil {
		values := map[string]uint64{}
		for _, line := range strings.Split(string(b), "\n") {
			p := strings.Fields(line)
			if len(p) >= 2 {
				values[strings.TrimSuffix(p[0], ":")] = num(p[1]) * 1024
			}
		}
		m.RAMTotal = values["MemTotal"]
		m.RAMUsed = m.RAMTotal - values["MemAvailable"]
		m.SwapTotal = values["SwapTotal"]
		m.SwapUsed = m.SwapTotal - values["SwapFree"]
	}
	if b, e := os.ReadFile("/proc/loadavg"); e == nil {
		p := strings.Fields(string(b))
		if len(p) > 0 {
			m.Load, _ = strconv.ParseFloat(p[0], 64)
		}
	}
	if b, e := os.ReadFile("/proc/net/dev"); e == nil {
		for _, line := range strings.Split(string(b), "\n") {
			p := strings.Fields(strings.ReplaceAll(line, ":", " "))
			if len(p) >= 17 && p[0] != "lo" {
				m.NetworkRX += num(p[1])
				m.NetworkTX += num(p[9])
			}
		}
	}
	if b, e := os.ReadFile("/proc/diskstats"); e == nil {
		for _, line := range strings.Split(string(b), "\n") {
			p := strings.Fields(line)
			if len(p) > 10 && !strings.HasPrefix(p[2], "loop") {
				m.DiskRead += num(p[5]) * 512
				m.DiskWrite += num(p[9]) * 512
			}
		}
	}
	if entries, e := os.ReadDir("/proc/self/fd"); e == nil {
		m.OpenFiles = uint64(len(entries))
	}
	for _, p := range []string{"/proc/net/tcp", "/proc/net/tcp6"} {
		f, e := os.Open(p)
		if e != nil {
			continue
		}
		scan := bufio.NewScanner(f)
		first := true
		for scan.Scan() {
			if first {
				first = false
				continue
			}
			m.Connections++
		}
		f.Close()
	}
	if s.Volume != nil {
		m.DiskFree, _ = s.Volume.Check()
		m.DiskTotal = DiskTotal(s.Volume)
		_, e := s.Volume.Check()
		m.StorageHealthy = e == nil
	}
	if !s.previous.MeasuredAt.IsZero() {
		seconds := m.MeasuredAt.Sub(s.previous.MeasuredAt).Seconds()
		if seconds > 0 {
			if m.NetworkRX >= s.previous.NetworkRX && m.NetworkTX >= s.previous.NetworkTX {
				m.NetworkBPS = float64(m.NetworkRX-s.previous.NetworkRX+m.NetworkTX-s.previous.NetworkTX) / seconds
			}
			if m.DiskRead >= s.previous.DiskRead && m.DiskWrite >= s.previous.DiskWrite {
				m.DiskBPS = float64(m.DiskRead-s.previous.DiskRead+m.DiskWrite-s.previous.DiskWrite) / seconds
			}
		}
	}
	s.previous = m
	return m
}
