package agent

import (
	"context"
	"github.com/rychkin/Begom/internal/model"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

// Run in a dedicated 4 MiB tmpfs, never fill the host's ordinary filesystem.
func TestDownloadENOSPC(t *testing.T) {
	dir := os.Getenv("BEGOM_FAULT_DIR")
	if dir == "" {
		t.Skip("requires the isolated tmpfs fault profile")
	}
	var fs syscall.Statfs_t
	if e := syscall.Statfs(dir, &fs); e != nil || uint64(fs.Blocks)*uint64(fs.Bsize) > 4<<20 || fs.Type != 0x01021994 {
		t.Fatal("fault profile must be an isolated tmpfs of at most 4 MiB", e)
	}
	root, e := os.MkdirTemp(dir, "begom-enospc-")
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { os.RemoveAll(root) })
	data := strings.Repeat("v", 8<<20)
	f := setupHTTP(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "video/mp4")
		w.Header().Set("Content-Length", "8388608")
		w.Write([]byte(data))
	})
	if e = os.WriteFile(filepath.Join(root, ".begom-volume"), []byte("test"), 0444); e != nil {
		t.Fatal(e)
	}
	f.runner.Volume.Config.Root = root
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	x := &execution{ctx: ctx, lease: model.Lease{AttemptID: model.NewID()}}
	result := model.ItemResult{StageID: model.NewID(), DownloadID: model.NewID(), ReserveBytes: 16 << 20, Metadata: &model.Metadata{Size: 8 << 20}}
	size, checksum, e := f.runner.download(x, result, f.server.URL, model.JobConfig{HTTPTimeoutMS: 5000, MaxFileBytes: 16 << 20})
	if e == nil || size != 0 || checksum != "" || f.permits.Load() != 1 {
		t.Fatal("out-of-space download must fail without a successful checksum or repeated request", size, checksum, e)
	}
	if e = syscall.Statfs(dir, &fs); e != nil || uint64(fs.Bavail)*uint64(fs.Bsize) > 64<<10 {
		t.Fatal("test did not reach actual ENOSPC", e)
	}
}
