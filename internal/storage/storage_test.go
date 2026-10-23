package storage

import (
	"crypto/sha256"
	"encoding/hex"
	"github.com/rychkin/Begom/internal/config"
	"os"
	"path/filepath"
	"testing"
)

func volume(t *testing.T) *Volume {
	t.Helper()
	dir := t.TempDir()
	if e := os.WriteFile(filepath.Join(dir, ".begom-volume"), []byte("test-volume"), 0444); e != nil {
		t.Fatal(e)
	}
	return &Volume{Config: config.Storage{Root: dir, VolumeID: "test-volume", Mode: "test"}}
}
func TestMissingMountNeverFallsBack(t *testing.T) {
	v := volume(t)
	v.Config.Mode = "nfs"
	if _, e := v.Check(); e == nil {
		t.Fatal("accepted ordinary directory as NFS")
	}
	v.Config.Mode = "local"
	if _, e := v.Check(); e == nil {
		t.Fatal("accepted unmounted local directory")
	}
	v.Config.Mode = "test"
	v.Config.VolumeID = "other"
	if _, e := v.Check(); e == nil {
		t.Fatal("accepted wrong volume")
	}
}
func TestPathsAndSymlinks(t *testing.T) {
	for _, p := range []string{"../x", "/x", "x/../y", "x//y", "x\\y", "", "x/./y"} {
		if ValidPath(p) {
			t.Error("accepted", p)
		}
	}
	v := volume(t)
	root, e := v.OpenRoot()
	if e != nil {
		t.Fatal(e)
	}
	defer root.Close()
	outside := t.TempDir()
	os.WriteFile(filepath.Join(outside, "secret"), []byte("outside"), 0600)
	if e = os.Symlink(outside, filepath.Join(v.Config.Root, "link")); e != nil {
		t.Fatal(e)
	}
	if _, _, e = Hash(root, "link/secret"); e == nil {
		t.Fatal("followed external symlink")
	}
	os.WriteFile(filepath.Join(v.Config.Root, "target"), []byte("inside"), 0600)
	os.Symlink("target", filepath.Join(v.Config.Root, "inside-link"))
	if _, _, e = Hash(root, "inside-link"); e == nil {
		t.Fatal("followed internal symlink")
	}
}
func TestDurableMetadataAndHash(t *testing.T) {
	v := volume(t)
	root, e := v.OpenRoot()
	if e != nil {
		t.Fatal(e)
	}
	defer root.Close()
	if e = MkdirAll(root, "staging/w/a/s", 0770); e != nil {
		t.Fatal(e)
	}
	st, _ := root.Stat("staging/w/a")
	if st.Mode().Perm() != 0770 {
		t.Fatal("umask removed publisher access", st.Mode())
	}
	if e = WriteJSON(root, "staging/w/a/s/metadata.json", map[string]string{"title": "test"}); e != nil {
		t.Fatal(e)
	}
	f, _ := root.Create("staging/w/a/s/video.part")
	f.Write([]byte("video"))
	f.Close()
	n, hash, e := Hash(root, "staging/w/a/s/video.part")
	want := sha256.Sum256([]byte("video"))
	if e != nil || n != 5 || hash != hex.EncodeToString(want[:]) {
		t.Fatal(n, hash, e)
	}
}

func TestNestedStagingPreservesPublisherGroupInheritance(t *testing.T) {
	v := volume(t)
	root, e := v.OpenRoot()
	if e != nil {
		t.Fatal(e)
	}
	defer root.Close()
	if e = MkdirAll(root, "staging/w", 0770); e != nil {
		t.Fatal(e)
	}
	if e = root.Chmod("staging/w", 0770|os.ModeSetgid); e != nil {
		t.Fatal(e)
	}
	if e = MkdirAll(root, "staging/w/attempt/stage", 0770); e != nil {
		t.Fatal(e)
	}
	for _, path := range []string{"staging/w/attempt", "staging/w/attempt/stage"} {
		st, e := root.Stat(path)
		if e != nil || st.Mode()&os.ModeSetgid == 0 || st.Mode().Perm() != 0770 {
			t.Fatal("publisher group inheritance lost", path, st, e)
		}
	}
}
