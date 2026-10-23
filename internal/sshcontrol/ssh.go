package sshcontrol

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"github.com/rychkin/Begom/internal/model"
	"github.com/rychkin/Begom/internal/nodecontrol"
	"github.com/rychkin/Begom/internal/secrets"
	"golang.org/x/crypto/ssh"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Service struct {
	Bao        *secrets.Bao
	PackageDir string
}

func (s *Service) Connect(ctx context.Context, w model.WorkerConfig) (*ssh.Client, error) {
	if s.Bao == nil || w.SSHSecret == "" || w.SSHHostKey == "" || w.SSHUser == "" || strings.ContainsAny(w.Host, "\r\n\x00/") {
		return nil, errors.New("SSH profile is incomplete")
	}
	secret, e := s.Bao.Get(ctx, w.SSHSecret)
	if e != nil {
		return nil, e
	}
	auth := []ssh.AuthMethod{}
	if key := secret["private_key"]; key != "" {
		var signer ssh.Signer
		if secret["passphrase"] != "" {
			signer, e = ssh.ParsePrivateKeyWithPassphrase([]byte(key), []byte(secret["passphrase"]))
		} else {
			signer, e = ssh.ParsePrivateKey([]byte(key))
		}
		if e != nil {
			return nil, errors.New("SSH identity invalid")
		}
		auth = append(auth, ssh.PublicKeys(signer))
	} else if secret["password"] != "" {
		auth = append(auth, ssh.Password(secret["password"]))
	} else {
		return nil, errors.New("SSH identity missing")
	}
	pin, _, _, _, e := ssh.ParseAuthorizedKey([]byte(w.SSHHostKey))
	if e != nil {
		return nil, errors.New("SSH host key invalid")
	}
	algorithms := []string{pin.Type()}
	if pin.Type() == ssh.KeyAlgoRSA {
		algorithms = []string{ssh.KeyAlgoRSASHA512, ssh.KeyAlgoRSASHA256}
	}
	cfg := &ssh.ClientConfig{User: w.SSHUser, Auth: auth, HostKeyAlgorithms: algorithms, Timeout: 10 * time.Second, HostKeyCallback: func(_ string, _ net.Addr, key ssh.PublicKey) error {
		if subtle.ConstantTimeCompare(pin.Marshal(), key.Marshal()) != 1 {
			return errors.New("SSH host key mismatch")
		}
		return nil
	}}
	port := w.SSHPort
	if port == 0 {
		port = 22
	}
	address := net.JoinHostPort(w.Host, strconv.Itoa(port))
	var d net.Dialer
	c, e := d.DialContext(ctx, "tcp", address)
	if e != nil {
		return nil, errors.New("SSH connection failed")
	}
	c.SetDeadline(time.Now().Add(15 * time.Second))
	conn, ch, requests, e := ssh.NewClientConn(c, address, cfg)
	if e != nil {
		c.Close()
		return nil, errors.New("SSH negotiation failed")
	}
	c.SetDeadline(time.Time{})
	return ssh.NewClient(conn, ch, requests), nil
}
func (s *Service) Run(ctx context.Context, w model.WorkerConfig, action, version string) (string, error) {
	client, e := s.Connect(ctx, w)
	if e != nil {
		return "", e
	}
	defer client.Close()
	session, e := client.NewSession()
	if e != nil {
		return "", e
	}
	defer session.Close()
	c := nodecontrol.Command{Action: action}
	var input io.Reader
	if action == "install" || action == "update" {
		if !model.ValidVersion(version) {
			return "", errors.New("invalid version")
		}
		manifest, e := os.ReadFile(filepath.Join(s.PackageDir, version+".json"))
		if e != nil {
			return "", errors.New("package manifest unavailable")
		}
		if json.Unmarshal(manifest, &c) != nil {
			return "", errors.New("invalid manifest")
		}
		c.Action = "install"
		archive, e := os.Open(filepath.Join(s.PackageDir, version+".tgz"))
		if e != nil {
			return "", errors.New("package unavailable")
		}
		defer archive.Close()
		input = io.MultiReader(bytes.NewReader(append(dbJSON(c), '\n')), archive)
	} else {
		if action == "check" {
			c.Action = "status"
		}
		if _, e = nodecontrol.Parse(c.Action); e != nil {
			return "", e
		}
		input = bytes.NewReader(append(dbJSON(c), '\n'))
	}
	session.Stdin = input
	var out limitedBuffer
	session.Stdout = &out
	session.Stderr = &out
	done := make(chan error, 1)
	go func() { done <- session.Run("begom-node-helper rpc") }()
	select {
	case <-ctx.Done():
		client.Close()
		return "", ctx.Err()
	case e = <-done:
		if e != nil {
			return out.String(), errors.New("remote action failed")
		}
		return out.String(), nil
	}
}
func dbJSON(v any) []byte { b, _ := json.Marshal(v); return b }

type limitedBuffer struct {
	b  bytes.Buffer
	mu sync.Mutex
}

func (b *limitedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	n := len(p)
	remaining := (512 << 10) - b.b.Len()
	if remaining > 0 {
		if len(p) > remaining {
			p = p[:remaining]
		}
		b.b.Write(p)
	}
	return n, nil
}
func (b *limitedBuffer) String() string { b.mu.Lock(); defer b.mu.Unlock(); return b.b.String() }
