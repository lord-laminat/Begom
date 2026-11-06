package nodecontrol

import (
	"bufio"
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"github.com/rychkin/Begom/internal/hostmetrics"
	"github.com/rychkin/Begom/internal/logging"
	"golang.org/x/crypto/ssh"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"
)

type Supervisor struct {
	Context          context.Context
	AgentPath        string
	ConfigPath       string
	UpdateRoot       string
	PackageKey       string
	mu               sync.Mutex
	cmd              *exec.Cmd
	desired, running bool
	exited           chan struct{}
	logs             []string
}

func (s *Supervisor) Write(p []byte) (int, error) {
	safe := logging.Redact(string(p))
	s.mu.Lock()
	s.logs = append(s.logs, safe)
	if len(s.logs) > 1000 {
		s.logs = s.logs[len(s.logs)-1000:]
	}
	s.mu.Unlock()
	os.Stdout.Write([]byte(safe))
	return len(p), nil
}
func (s *Supervisor) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.desired = true
	return s.startLocked()
}
func (s *Supervisor) startLocked() error {
	if s.running {
		return nil
	}
	binary := s.AgentPath
	if _, e := os.Stat(filepath.Join(s.UpdateRoot, "current/agent")); e == nil {
		binary = filepath.Join(s.UpdateRoot, "current/agent")
	}
	cmd := exec.CommandContext(s.Context, binary, "--config", s.ConfigPath)
	cmd.Stdout = s
	cmd.Stderr = s
	if e := cmd.Start(); e != nil {
		return errors.New("agent start failed")
	}
	s.cmd = cmd
	s.running = true
	s.exited = make(chan struct{})
	exited := s.exited
	go func() {
		cmd.Wait()
		s.mu.Lock()
		s.running = false
		restart := s.desired
		s.mu.Unlock()
		close(exited)
		if restart {
			select {
			case <-s.Context.Done():
				return
			case <-time.After(time.Second):
				s.mu.Lock()
				if s.desired {
					s.startLocked()
				}
				s.mu.Unlock()
			}
		}
	}()
	return nil
}
func (s *Supervisor) Stop() error {
	s.mu.Lock()
	s.desired = false
	cmd, running, exited := s.cmd, s.running, s.exited
	s.mu.Unlock()
	if !running {
		return nil
	}
	cmd.Process.Signal(os.Interrupt)
	select {
	case <-exited:
		return nil
	case <-time.After(3 * time.Second):
		return cmd.Process.Kill()
	}
}
func (s *Supervisor) Run(c Command, out io.Writer, in io.Reader) error {
	switch c.Action {
	case "status":
		s.mu.Lock()
		running := s.running
		pid := 0
		if s.cmd != nil && s.cmd.Process != nil {
			pid = s.cmd.Process.Pid
		}
		s.mu.Unlock()
		return json.NewEncoder(out).Encode(map[string]any{"active": running, "pid": pid, "backend": "isolated-compose"})
	case "resources":
		return json.NewEncoder(out).Encode(hostmetrics.New(nil).Sample())
	case "help":
		_, e := io.WriteString(out, "help | status | resources | logs [1..1000] | start | stop | restart\n")
		return e
	case "logs":
		if c.Lines < 1 || c.Lines > 1000 {
			return errors.New("invalid log limit")
		}
		s.mu.Lock()
		lines := append([]string{}, s.logs[max(0, len(s.logs)-c.Lines):]...)
		s.mu.Unlock()
		for _, line := range lines {
			if _, e := io.WriteString(out, line); e != nil {
				return e
			}
		}
		return nil
	case "start":
		return s.Start()
	case "stop":
		return s.Stop()
	case "restart":
		if e := s.Stop(); e != nil {
			return e
		}
		return s.Start()
	case "install":
		if e := InstallRelease(c, in, s.UpdateRoot, s.PackageKey); e != nil {
			return e
		}
		if e := s.Stop(); e != nil {
			return e
		}
		return s.Start()
	default:
		return errors.New("command is not permitted")
	}
}
func (s *Supervisor) Serve(ctx context.Context, listen, hostKeyFile, authorizedKeyFile string) error {
	host, e := os.ReadFile(hostKeyFile)
	if e != nil {
		return e
	}
	signer, e := ssh.ParsePrivateKey(host)
	if e != nil {
		return e
	}
	authorized, e := os.ReadFile(authorizedKeyFile)
	if e != nil {
		return e
	}
	pub, _, _, _, e := ssh.ParseAuthorizedKey(authorized)
	if e != nil {
		return e
	}
	cfg := &ssh.ServerConfig{MaxAuthTries: 3, ServerVersion: "SSH-2.0-Begom", PublicKeyCallback: func(meta ssh.ConnMetadata, key ssh.PublicKey) (*ssh.Permissions, error) {
		if meta.User() != "begom-admin" || subtle.ConstantTimeCompare(pub.Marshal(), key.Marshal()) != 1 {
			return nil, errors.New("SSH authentication rejected")
		}
		return &ssh.Permissions{}, nil
	}}
	cfg.AddHostKey(signer)
	listener, e := net.Listen("tcp", listen)
	if e != nil {
		return e
	}
	defer listener.Close()
	go func() { <-ctx.Done(); listener.Close(); s.Stop() }()
	if e = s.Start(); e != nil {
		return e
	}
	sessions := make(chan struct{}, 16)
	for {
		conn, e := listener.Accept()
		if e != nil {
			if ctx.Err() != nil {
				return nil
			}
			return e
		}
		select {
		case sessions <- struct{}{}:
		default:
			conn.Close()
			continue
		}
		go func() {
			defer func() { <-sessions }()
			defer conn.Close()
			conn.SetDeadline(time.Now().Add(10 * time.Second))
			server, channels, requests, e := ssh.NewServerConn(conn, cfg)
			if e != nil {
				return
			}
			defer server.Close()
			conn.SetDeadline(time.Time{})
			cctx, cancel := context.WithTimeout(ctx, 15*time.Minute)
			defer cancel()
			go func() { <-cctx.Done(); conn.Close() }()
			go ssh.DiscardRequests(requests)
			for next := range channels {
				if next.ChannelType() != "session" {
					next.Reject(ssh.UnknownChannelType, "session required")
					continue
				}
				channel, requests, e := next.Accept()
				if e != nil {
					continue
				}
				go s.session(cctx, channel, requests)
			}
		}()
	}
}
func (s *Supervisor) session(ctx context.Context, ch ssh.Channel, requests <-chan *ssh.Request) {
	defer ch.Close()
	for request := range requests {
		if request.Type == "pty-req" || request.Type == "window-change" {
			request.Reply(true, nil)
			continue
		}
		if request.Type != "exec" {
			request.Reply(false, nil)
			continue
		}
		var command struct{ Command string }
		if ssh.Unmarshal(request.Payload, &command) != nil || (command.Command != "begom-node-helper console" && command.Command != "begom-node-helper rpc") {
			request.Reply(false, nil)
			return
		}
		request.Reply(true, nil)
		var e error
		if command.Command == "begom-node-helper console" {
			reader := bufio.NewScanner(ch)
			reader.Buffer(make([]byte, 1024), 1024)
			io.WriteString(ch, "Begom: help показывает доступные команды.\n> ")
			for reader.Scan() {
				c, err := Parse(reader.Text())
				if err == nil {
					err = s.Run(c, ch, nil)
				}
				if err != nil {
					io.WriteString(ch, "command rejected\n")
				}
				io.WriteString(ch, "> ")
				if ctx.Err() != nil {
					break
				}
			}
			e = reader.Err()
		} else {
			reader := bufio.NewReaderSize(ch, 4096)
			line, err := reader.ReadSlice('\n')
			if err != nil {
				e = err
			} else {
				var c Command
				e = json.Unmarshal(line, &c)
				if e == nil {
					if c.Action != "install" {
						_, e = Parse(c.Action)
					}
					if e == nil {
						e = s.Run(c, ch, reader)
					}
				}
			}
		}
		status := uint32(0)
		if e != nil {
			status = 1
			io.WriteString(ch.Stderr(), "node operation failed\n")
		}
		ch.SendRequest("exit-status", false, ssh.Marshal(struct{ Status uint32 }{status}))
		return
	}
}
