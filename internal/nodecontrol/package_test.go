package nodecontrol

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"os"
	"path/filepath"
	"strconv"
	"testing"
)

func TestSignedPackageAndRejectedPaths(t *testing.T) {
	pub, key, _ := ed25519.GenerateKey(rand.Reader)
	for _, name := range []string{"agent", "../agent", "dir/agent"} {
		t.Run(name, func(t *testing.T) {
			var archive bytes.Buffer
			gz := gzip.NewWriter(&archive)
			tw := tar.NewWriter(gz)
			payload := []byte("synthetic signed executable")
			tw.WriteHeader(&tar.Header{Name: name, Size: int64(len(payload)), Mode: 0755})
			tw.Write(payload)
			tw.Close()
			gz.Close()
			sum := sha256.Sum256(archive.Bytes())
			hash := hex.EncodeToString(sum[:])
			c := Command{Action: "install", Version: "1.0.0", SHA256: hash, Size: int64(archive.Len())}
			c.Signature = base64.StdEncoding.EncodeToString(ed25519.Sign(key, []byte(c.Version+":"+c.SHA256+":"+strconv.FormatInt(c.Size, 10))))
			dir := t.TempDir()
			os.MkdirAll(filepath.Join(dir, "releases"), 0755)
			pubFile := filepath.Join(dir, "public")
			os.WriteFile(pubFile, []byte(base64.StdEncoding.EncodeToString(pub)), 0444)
			e := InstallRelease(c, bytes.NewReader(archive.Bytes()), dir, pubFile)
			if name != "agent" {
				if e == nil {
					t.Fatal("unsafe archive accepted")
				}
				return
			}
			if e != nil {
				t.Fatal(e)
			}
			got, e := os.ReadFile(filepath.Join(dir, "current/agent"))
			if e != nil || !bytes.Equal(got, payload) {
				t.Fatal("published executable mismatch", e)
			}
			info, e := os.Stat(filepath.Join(dir, "current"))
			if e != nil || info.Mode().Perm() != 0755 {
				t.Fatal("release is not traversable by the unprivileged agent", e)
			}
			if e = InstallRelease(c, bytes.NewReader(archive.Bytes()), dir, pubFile); e != nil {
				t.Fatal("verified repeated installation", e)
			}
			if e = os.WriteFile(filepath.Join(dir, "current/agent"), []byte("corrupt executable"), 0755); e != nil {
				t.Fatal(e)
			}
			if InstallRelease(c, bytes.NewReader(archive.Bytes()), dir, pubFile) == nil {
				t.Fatal("corrupt existing release reused")
			}
			c.Signature = base64.StdEncoding.EncodeToString(make([]byte, ed25519.SignatureSize))
			if InstallRelease(c, bytes.NewReader(archive.Bytes()), dir, pubFile) == nil {
				t.Fatal("unsigned package accepted")
			}
		})
	}
}
