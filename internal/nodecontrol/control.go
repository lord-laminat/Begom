// Package nodecontrol contains the same closed command vocabulary at both ends
// of SSH. Commands are never passed to a shell.
package nodecontrol

import (
	"archive/tar"
	"bufio"
	"compress/gzip"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/rychkin/Begom/internal/hostmetrics"
	"github.com/rychkin/Begom/internal/logging"
	"github.com/rychkin/Begom/internal/model"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type Command struct {
	Action    string `json:"action"`
	Lines     int    `json:"lines,omitempty"`
	Version   string `json:"version,omitempty"`
	SHA256    string `json:"sha256,omitempty"`
	Signature string `json:"signature,omitempty"`
	Size      int64  `json:"size,omitempty"`
}

func Parse(line string) (Command, error) {
	f := strings.Fields(line)
	c := Command{}
	if len(f) == 0 || len(line) > 128 {
		return c, errors.New("invalid command")
	}
	c.Action = f[0]
	switch c.Action {
	case "help", "status", "resources", "start", "stop", "restart":
		if len(f) != 1 {
			return c, errors.New("unexpected argument")
		}
	case "logs":
		c.Lines = 100
		if len(f) == 2 {
			n, e := strconv.Atoi(f[1])
			if e != nil || n < 1 || n > 1000 {
				return c, errors.New("logs accepts 1..1000 lines")
			}
			c.Lines = n
		} else if len(f) > 2 {
			return c, errors.New("unexpected argument")
		}
	default:
		return c, errors.New("command is not permitted")
	}
	return c, nil
}
func Run(ctx context.Context, c Command, out io.Writer) error {
	var cmd *exec.Cmd
	switch c.Action {
	case "help":
		_, e := io.WriteString(out, "help | status | resources | logs [1..1000] | start | stop | restart\n")
		return e
	case "resources":
		return json.NewEncoder(out).Encode(hostmetrics.New(nil).Sample())
	case "status":
		cmd = exec.CommandContext(ctx, "systemctl", "show", "begom-agent.service", "--property=ActiveState,SubState,MainPID,ExecMainStatus")
	case "logs":
		if c.Lines < 1 || c.Lines > 1000 {
			return errors.New("invalid line limit")
		}
		cmd = exec.CommandContext(ctx, "journalctl", "--unit=begom-agent.service", "--no-pager", "--output=cat", "-n", strconv.Itoa(c.Lines))
	case "start", "stop", "restart":
		cmd = exec.CommandContext(ctx, "sudo", "-n", "/usr/local/libexec/begom-node-helper", "privileged", c.Action)
	default:
		return errors.New("command is not permitted")
	}
	b, e := cmd.CombinedOutput()
	if len(b) > 512<<10 {
		b = b[:512<<10]
	}
	_, we := io.WriteString(out, logging.Redact(string(b)))
	if we != nil {
		return we
	}
	if e != nil {
		return errors.New("node action failed")
	}
	return nil
}
func Console(ctx context.Context, in io.Reader, out io.Writer) error {
	s := bufio.NewScanner(in)
	s.Buffer(make([]byte, 1024), 1024)
	io.WriteString(out, "Begom: ограниченный терминал. Команда help показывает доступные действия.\n> ")
	for s.Scan() {
		c, e := Parse(s.Text())
		if e == nil {
			qctx, cancel := context.WithTimeout(ctx, 20*time.Second)
			e = Run(qctx, c, out)
			cancel()
		}
		if e != nil {
			io.WriteString(out, "error: "+e.Error()+"\n")
		}
		io.WriteString(out, "> ")
	}
	return s.Err()
}

// Install accepts only a signed archive containing the agent executable. All
// privileged target paths are fixed, owned by root and absent from the protocol.
func Install(ctx context.Context, c Command, in io.Reader) error {
	if os.Geteuid() != 0 {
		return errors.New("root helper required")
	}
	if e := InstallRelease(c, in, "/opt/begom", "/etc/begom/package.pub"); e != nil {
		return e
	}
	if exec.CommandContext(ctx, "systemctl", "restart", "begom-agent.service").Run() != nil {
		return errors.New("agent restart failed")
	}
	return nil
}

// InstallRelease is shared by the systemd helper and the isolated local supervisor.
func InstallRelease(c Command, in io.Reader, base, pubFile string) error {
	if !model.ValidVersion(c.Version) || c.Size < 1 || c.Size > 128<<20 {
		return errors.New("invalid installation request")
	}
	digest, de := hex.DecodeString(c.SHA256)
	if de != nil || len(digest) != 32 {
		return errors.New("invalid digest")
	}
	pub, e := os.ReadFile(pubFile)
	if e != nil {
		return e
	}
	key, e := base64.StdEncoding.DecodeString(strings.TrimSpace(string(pub)))
	if e != nil || len(key) != ed25519.PublicKeySize {
		return errors.New("invalid package key")
	}
	signature, e := base64.StdEncoding.DecodeString(c.Signature)
	if e != nil {
		return e
	}
	payload := []byte(c.Version + ":" + c.SHA256 + ":" + strconv.FormatInt(c.Size, 10))
	if !ed25519.Verify(key, payload, signature) {
		return errors.New("package signature rejected")
	}
	if e = os.MkdirAll(filepath.Join(base, "releases"), 0755); e != nil {
		return e
	}
	dir, e := os.MkdirTemp(filepath.Join(base, "releases"), ".install-")
	if e != nil {
		return e
	}
	defer os.RemoveAll(dir)
	archive, e := os.OpenFile(filepath.Join(dir, "package.tgz"), os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if e != nil {
		return e
	}
	defer archive.Close()
	h := sha256.New()
	n, e := io.Copy(io.MultiWriter(archive, h), io.LimitReader(in, c.Size+1))
	if e != nil || n != c.Size || hex.EncodeToString(h.Sum(nil)) != c.SHA256 {
		return errors.New("package checksum rejected")
	}
	if _, e = archive.Seek(0, io.SeekStart); e != nil {
		return e
	}
	gz, e := gzip.NewReader(archive)
	if e != nil {
		return e
	}
	defer gz.Close()
	tr := tar.NewReader(gz)
	count := 0
	for {
		hdr, e := tr.Next()
		if e == io.EOF {
			break
		}
		if e != nil {
			return e
		}
		if hdr.Name != "agent" || hdr.Typeflag != tar.TypeReg || hdr.Size < 1 || hdr.Size > 128<<20 || count != 0 {
			return errors.New("invalid package entry")
		}
		count++
		f, e := os.OpenFile(filepath.Join(dir, "agent"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0755)
		if e != nil {
			return e
		}
		_, e = io.Copy(f, io.LimitReader(tr, hdr.Size))
		if e == nil {
			e = f.Chmod(0755)
		}
		if e == nil {
			e = f.Sync()
		}
		f.Close()
		if e != nil {
			return e
		}
	}
	if count != 1 {
		return errors.New("agent missing from package")
	}
	if e = os.Chmod(dir, 0755); e != nil {
		return e
	}
	if e = syncDir(dir); e != nil {
		return e
	}
	release := filepath.Join(base, "releases", c.Version+"-"+c.SHA256[:12])
	if e = os.Rename(dir, release); e != nil {
		// An interrupted installation can leave a completed release. Reuse it
		// only when its executable still matches the verified archive.
		want, we := fileHash(filepath.Join(dir, "agent"))
		got, ge := fileHash(filepath.Join(release, "agent"))
		info, se := os.Lstat(release)
		if we != nil || ge != nil || se != nil || !info.IsDir() || info.Mode().Perm() != 0755 || want != got {
			return errors.New("existing release does not match verified package")
		}
	}
	if e = syncDir(filepath.Join(base, "releases")); e != nil {
		return e
	}
	link := filepath.Join(base, ".current-"+model.NewID())
	defer os.Remove(link)
	if e = os.Symlink(release, link); e != nil {
		return e
	}
	if e = os.Rename(link, filepath.Join(base, "current")); e != nil {
		return e
	}
	return syncDir(base)
}

func syncDir(path string) error {
	f, e := os.Open(path)
	if e != nil {
		return e
	}
	defer f.Close()
	return f.Sync()
}

func fileHash(path string) ([32]byte, error) {
	var sum [32]byte
	info, e := os.Lstat(path)
	if e != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0755 {
		return sum, errors.New("invalid release executable")
	}
	f, e := os.Open(path)
	if e != nil {
		return sum, e
	}
	defer f.Close()
	h := sha256.New()
	if _, e = io.Copy(h, io.LimitReader(f, (128<<20)+1)); e != nil {
		return sum, e
	}
	copy(sum[:], h.Sum(nil))
	return sum, nil
}
func Privileged(ctx context.Context, action string, in io.Reader) error {
	if os.Geteuid() != 0 {
		return errors.New("root helper required")
	}
	switch action {
	case "start", "stop", "restart":
		return exec.CommandContext(ctx, "systemctl", action, "begom-agent.service").Run()
	case "install":
		r := bufio.NewReaderSize(in, 4096)
		line, e := r.ReadSlice('\n')
		if e != nil || len(line) > 4096 {
			return errors.New("invalid package header")
		}
		var c Command
		if e = json.Unmarshal(line, &c); e != nil {
			return e
		}
		return Install(ctx, c, r)
	default:
		return fmt.Errorf("action denied")
	}
}
