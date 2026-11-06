package hostmetrics

import (
	"github.com/rychkin/Begom/internal/storage"
	"syscall"
)

func DiskTotal(v *storage.Volume) uint64 {
	var st syscall.Statfs_t
	p := v.Config.MountPath
	if p == "" {
		p = v.Config.Root
	}
	if syscall.Statfs(p, &st) != nil {
		return 0
	}
	return st.Blocks * uint64(st.Bsize)
}
