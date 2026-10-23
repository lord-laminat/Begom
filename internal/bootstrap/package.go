package bootstrap

import (
	"archive/tar"
	"compress/gzip"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"github.com/rychkin/Begom/internal/model"
	"github.com/rychkin/Begom/internal/nodecontrol"
	"io"
	"os"
	"path/filepath"
	"strconv"
)

func Package(agent, keyFile, output, version string) error {
	if !model.ValidVersion(version) {
		return errors.New("invalid version")
	}
	b, e := os.ReadFile(keyFile)
	if e != nil {
		return e
	}
	key, e := base64.StdEncoding.DecodeString(string(b))
	if e != nil || len(key) != ed25519.PrivateKeySize {
		return errors.New("invalid signing identity")
	}
	source, e := os.Open(agent)
	if e != nil {
		return e
	}
	defer source.Close()
	st, e := source.Stat()
	if e != nil || !st.Mode().IsRegular() {
		return errors.New("invalid agent binary")
	}
	if e = os.MkdirAll(output, 0750); e != nil {
		return e
	}
	archive, e := os.OpenFile(filepath.Join(output, version+".tgz"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	if e != nil {
		return e
	}
	gz := gzip.NewWriter(archive)
	tw := tar.NewWriter(gz)
	e = tw.WriteHeader(&tar.Header{Name: "agent", Mode: 0755, Size: st.Size(), Typeflag: tar.TypeReg})
	if e == nil {
		_, e = io.Copy(tw, source)
	}
	if ce := tw.Close(); e == nil {
		e = ce
	}
	if ce := gz.Close(); e == nil {
		e = ce
	}
	if ce := archive.Close(); e == nil {
		e = ce
	}
	if e != nil {
		return e
	}
	archive, e = os.Open(filepath.Join(output, version+".tgz"))
	if e != nil {
		return e
	}
	defer archive.Close()
	hash := sha256.New()
	size, e := io.Copy(hash, archive)
	if e != nil {
		return e
	}
	digest := hex.EncodeToString(hash.Sum(nil))
	signature := ed25519.Sign(key, []byte(version+":"+digest+":"+strconv.FormatInt(size, 10)))
	c := nodecontrol.Command{Action: "install", Version: version, SHA256: digest, Size: size, Signature: base64.StdEncoding.EncodeToString(signature)}
	manifest, _ := json.MarshalIndent(c, "", "  ")
	return write(filepath.Join(output, version+".json"), manifest, 0644)
}
