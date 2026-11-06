package hostmetrics

import (
	"fmt"
	"github.com/rychkin/Begom/internal/model"
	"io"
)

func Prometheus(w io.Writer, prefix, labels string, m model.Metrics) {
	values := map[string]float64{
		"measurement_timestamp_seconds": float64(m.MeasuredAt.UnixMilli()) / 1000, "cpu_percent": m.CPU, "load": m.Load,
		"ram_total_bytes": float64(m.RAMTotal), "ram_used_bytes": float64(m.RAMUsed), "swap_total_bytes": float64(m.SwapTotal), "swap_used_bytes": float64(m.SwapUsed),
		"disk_total_bytes": float64(m.DiskTotal), "disk_free_bytes": float64(m.DiskFree), "network_receive_bytes_total": float64(m.NetworkRX), "network_transmit_bytes_total": float64(m.NetworkTX),
		"disk_read_bytes_total": float64(m.DiskRead), "disk_write_bytes_total": float64(m.DiskWrite), "open_files": float64(m.OpenFiles), "connections": float64(m.Connections), "uptime_seconds": m.Uptime,
		"active_checks": float64(m.ActiveChecks), "active_downloads": float64(m.ActiveDownloads), "download_bytes_total": float64(m.Bytes), "errors_total": float64(m.Errors),
	}
	values["storage_healthy"] = 0
	values["recent_errors_per_second"] = m.RecentErrorsPerSecond
	values["network_bytes_per_second"] = m.NetworkBPS
	values["disk_bytes_per_second"] = m.DiskBPS
	if m.StorageHealthy {
		values["storage_healthy"] = 1
	}
	for name, value := range values {
		fmt.Fprintf(w, "%s%s%s %g\n", prefix, name, labels, value)
	}
}
