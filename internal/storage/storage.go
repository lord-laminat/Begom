// Package storage checks the mounted volume before opening paths via os.Root.
package storage

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/rychkin/Begom/internal/config"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
	"syscall"
)

type Volume struct{ Config config.Storage }

var ErrUnsafePath = errors.New("unsafe storage path")

func (v *Volume) Check() (uint64, error) {
	if v.Config.Root == "" || v.Config.VolumeID == "" {
		return 0, errors.New("storage configuration missing")
	}
	root, e := os.OpenRoot(v.Config.Root)
	if e != nil {
		return 0, errors.New("storage unavailable")
	}
	defer root.Close()
	f, e := root.Open(".begom-volume")
	if e != nil {
		return 0, errors.New("volume identity missing")
	}
	b, e := io.ReadAll(io.LimitReader(f, 256))
	f.Close()
	if e != nil || strings.TrimSpace(string(b)) != v.Config.VolumeID {
		return 0, errors.New("volume identity mismatch")
	}
	statPath := v.Config.MountPath
	if statPath == "" {
		statPath = v.Config.Root
	}
	var st syscall.Statfs_t
	if e = syscall.Statfs(statPath, &st); e != nil {
		return 0, errors.New("volume stat failed")
	}
	if v.Config.Mode == "nfs" && uint64(st.Type) != 0x6969 {
		return 0, errors.New("expected NFS filesystem")
	}
	if v.Config.Mode == "nfs" {
		var actual syscall.Statfs_t
		if e = syscall.Statfs(v.Config.Root, &actual); e != nil || actual.Type != st.Type || actual.Fsid != st.Fsid {
			return 0, errors.New("storage root is outside the required NFS volume")
		}
	}
	if v.Config.Mode != "nfs" && v.Config.Mode != "local" && v.Config.Mode != "test" {
		return 0, errors.New("unknown storage mode")
	}
	if v.Config.Mode != "test" {
		mount := v.Config.MountPath
		if mount == "" {
			mount = v.Config.Root
		}
		if !Mounted(mount) {
			return 0, errors.New("required mount missing")
		}
	}
	free := st.Bavail * uint64(st.Bsize)
	if free < v.Config.MinFreeBytes {
		return free, errors.New("storage reserve exhausted")
	}
	return free, nil
}
func Mounted(p string) bool {
	b, e := os.ReadFile("/proc/self/mountinfo")
	if e != nil {
		return false
	}
	p = filepath.Clean(p)
	for _, line := range strings.Split(string(b), "\n") {
		fields := strings.Fields(line)
		if len(fields) > 4 {
			m := strings.NewReplacer("\\040", " ", "\\011", "\t", "\\134", "\\").Replace(fields[4])
			if m == p {
				return true
			}
		}
	}
	return false
}
func ValidPath(p string) bool {
	if p == "" || !filepath.IsLocal(p) || strings.ContainsAny(p, "\\\x00") {
		return false
	}
	for _, s := range strings.Split(p, "/") {
		if s == "." || s == ".." || s == "" {
			return false
		}
	}
	return true
}
func (v *Volume) OpenRoot() (*os.Root, error) {
	if _, e := v.Check(); e != nil {
		return nil, e
	}
	return os.OpenRoot(v.Config.Root)
}
func Stage(worker, attempt, stage string) string { return path.Join("staging", worker, attempt, stage) }
func Hash(root *os.Root, p string) (uint64, string, error) {
	if !ValidPath(p) {
		return 0, "", errors.New("invalid storage path")
	}
	if _, e := StatRegular(root, p); e != nil {
		return 0, "", e
	}
	f, e := root.Open(p)
	if e != nil {
		return 0, "", e
	}
	defer f.Close()
	st, e := f.Stat()
	if e != nil || !st.Mode().IsRegular() {
		return 0, "", errors.New("expected regular file")
	}
	h := sha256.New()
	n, e := io.Copy(h, f)
	if e != nil {
		return 0, "", e
	}
	return uint64(n), hex.EncodeToString(h.Sum(nil)), nil
}
func WriteJSON(root *os.Root, p string, v any) error {
	if !ValidPath(p) {
		return errors.New("invalid storage path")
	}
	b, e := json.Marshal(v)
	if e != nil {
		return e
	}
	var nonce [12]byte
	if _, e = rand.Read(nonce[:]); e != nil {
		return e
	}
	temp := p + ".tmp-" + hex.EncodeToString(nonce[:])
	defer root.Remove(temp)
	f, e := root.OpenFile(temp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0640)
	if e != nil {
		return e
	}
	if _, e = f.Write(b); e == nil {
		e = f.Sync()
	}
	ce := f.Close()
	if e == nil {
		e = ce
	}
	if e != nil {
		return e
	}
	if e = root.Rename(temp, p); e != nil {
		return e
	}
	return SyncDir(root, path.Dir(p))
}
func SyncDir(root *os.Root, p string) error {
	f, e := root.Open(p)
	if e != nil {
		return e
	}
	defer f.Close()
	return f.Sync()
}
func MkdirAll(root *os.Root, p string, mode os.FileMode) error {
	if !ValidPath(p) {
		return fmt.Errorf("invalid storage path")
	}
	current := ""
	for _, part := range strings.Split(p, "/") {
		current = path.Join(current, part)
		if e := root.Mkdir(current, mode); e == nil {
			st, se := root.Lstat(current)
			if se != nil {
				return se
			}
			// Keep the inherited publisher group on every nested NFS directory.
			// Clearing setgid here makes grandchildren belong to the agent's
			// primary group and prevents the publisher from reading the stage.
			if e = root.Chmod(current, mode|(st.Mode()&os.ModeSetgid)); e != nil {
				return e
			}
		} else if errors.Is(e, os.ErrExist) {
			st, se := root.Lstat(current)
			if se != nil {
				return se
			}
			if !st.IsDir() || st.Mode()&os.ModeSymlink != 0 {
				return errors.New("storage directory is not a real directory")
			}
		} else {
			return e
		}
	}
	return nil
}
func StatRegular(root *os.Root, p string) (os.FileInfo, error) {
	if !ValidPath(p) {
		return nil, ErrUnsafePath
	}
	parts := strings.Split(p, "/")
	for i := range parts {
		st, e := root.Lstat(strings.Join(parts[:i+1], "/"))
		if e != nil {
			return nil, e
		}
		if st.Mode()&os.ModeSymlink != 0 {
			return nil, fmt.Errorf("%w: symlink rejected", ErrUnsafePath)
		}
		if i == len(parts)-1 {
			if !st.Mode().IsRegular() {
				return nil, fmt.Errorf("%w: expected regular file", ErrUnsafePath)
			}
			return st, nil
		}
		if !st.IsDir() {
			return nil, ErrUnsafePath
		}
	}
	return nil, errors.New("invalid storage path")
}
