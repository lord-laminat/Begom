package coordinator

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/jackc/pgx/v5"
	"github.com/rychkin/Begom/internal/auth"
	"github.com/rychkin/Begom/internal/db"
	"github.com/rychkin/Begom/internal/model"
	"github.com/rychkin/Begom/internal/sshcontrol"
	"golang.org/x/crypto/ssh"
)

const terminalWindow = 256 << 10

type TerminalPolicy struct{ Session, Idle, Recheck time.Duration }

func (p TerminalPolicy) limits() TerminalPolicy {
	if p.Session <= 0 || p.Session > 15*time.Minute {
		p.Session = 15 * time.Minute
	}
	if p.Idle <= 0 || p.Idle > 2*time.Minute {
		p.Idle = 2 * time.Minute
	}
	if p.Recheck <= 0 || p.Recheck > time.Second {
		p.Recheck = time.Second
	}
	return p
}

type terminalOutput struct {
	ctx      context.Context
	conn     *websocket.Conn
	writeMu  sync.Mutex
	mu       sync.Mutex
	credit   int
	wake     chan struct{}
	activity atomic.Int64
	bytes    atomic.Uint64
}

func (t *terminalOutput) control(v any) error {
	t.writeMu.Lock()
	defer t.writeMu.Unlock()
	ctx, cancel := context.WithTimeout(t.ctx, 5*time.Second)
	defer cancel()
	return wsjson.Write(ctx, t.conn, v)
}
func (t *terminalOutput) ack(n int) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if n <= 0 || n > terminalWindow-t.credit {
		return false
	}
	t.credit += n
	select {
	case t.wake <- struct{}{}:
	default:
	}
	return true
}
func (t *terminalOutput) Write(p []byte) (int, error) {
	total := 0
	for len(p) > 0 {
		t.mu.Lock()
		n := min(len(p), 16<<10, t.credit)
		t.credit -= n
		t.mu.Unlock()
		if n == 0 {
			select {
			case <-t.ctx.Done():
				return total, t.ctx.Err()
			case <-t.wake:
				continue
			}
		}
		t.writeMu.Lock()
		ctx, cancel := context.WithTimeout(t.ctx, 5*time.Second)
		e := t.conn.Write(ctx, websocket.MessageBinary, p[:n])
		cancel()
		t.writeMu.Unlock()
		if e != nil {
			return total, e
		}
		t.bytes.Add(uint64(n))
		t.activity.Store(time.Now().UnixNano())
		p = p[n:]
		total += n
	}
	return total, nil
}

func (s *Server) sshStream(w http.ResponseWriter, r *http.Request, u auth.User) {
	if !u.Admin() {
		s.denied(w, r)
		return
	}
	worker, e := s.Store.GetWorker(r.Context(), r.PathValue("workerId"))
	if e != nil {
		s.fail(w, r, e)
		return
	}
	origin, _ := url.Parse(s.Config.PublicOrigin)
	c, e := websocket.Accept(w, r, &websocket.AcceptOptions{OriginPatterns: []string{origin.Host}})
	if e != nil {
		return
	}
	defer c.CloseNow()
	c.SetReadLimit(64 << 10)
	policy := s.TerminalPolicy.limits()
	ctx, cancel := context.WithTimeout(r.Context(), policy.Session)
	defer cancel()
	var first struct {
		Type   string `json:"type"`
		Ticket string `json:"ticket"`
		Cols   int    `json:"cols"`
		Rows   int    `json:"rows"`
	}
	qctx, stop := context.WithTimeout(ctx, 5*time.Second)
	e = wsjson.Read(qctx, c, &first)
	stop()
	if e != nil || first.Type != "auth" || !sshcontrol.ValidDimensions(first.Cols, first.Rows) {
		c.Close(websocket.StatusPolicyViolation, "invalid terminal handshake")
		return
	}
	var sessionID string
	e = s.Store.Pool.QueryRow(ctx, "UPDATE ssh_sessions SET used=true WHERE ticket_hash=$1 AND user_id=$2 AND worker_id=$3 AND NOT used AND expires_at>clock_timestamp() RETURNING id", auth.Digest(first.Ticket), u.ID, worker.ID).Scan(&sessionID)
	if e != nil {
		c.Close(websocket.StatusPolicyViolation, "ticket rejected")
		return
	}
	out := &terminalOutput{ctx: ctx, conn: c, credit: terminalWindow, wake: make(chan struct{}, 1)}
	out.activity.Store(time.Now().UnixNano())
	var inputBytes atomic.Uint64
	var reasonMu sync.Mutex
	reason := "disconnected"
	setReason := func(value string) { reasonMu.Lock(); reason = value; reasonMu.Unlock() }
	defer func() {
		reasonMu.Lock()
		closedReason := reason
		reasonMu.Unlock()
		qctx, done := context.WithTimeout(context.Background(), 2*time.Second)
		defer done()
		_, err := s.Store.Mutate(qctx, u.ID, "ssh.close", model.NewID(), []byte(sessionID), func(tx pgx.Tx) (any, int, error) {
			e := db.Audit(qctx, tx, u.ID, "ssh.session.close", worker.ID, "", map[string]any{"session_id": sessionID, "reason": closedReason, "bytes_in": inputBytes.Load(), "bytes_out": out.bytes.Load()})
			return map[string]bool{"ok": true}, 200, e
		})
		if err != nil {
			s.Log.Warn("terminal audit unavailable", "operation", "ssh.audit", "error_code", "database_unavailable")
		}
	}()
	client, e := s.SSH.ConnectTerminal(ctx, worker.WorkerConfig)
	if e != nil {
		setReason("ssh_unavailable")
		out.control(map[string]string{"type": "error", "message": "SSH недоступен: проверьте профиль терминала и host key"})
		return
	}
	defer client.Close()
	session, e := client.NewSession()
	if e != nil {
		return
	}
	defer session.Close()
	in, e := session.StdinPipe()
	if e != nil {
		return
	}
	session.Stdout = out
	session.Stderr = out
	if e = session.RequestPty("xterm-256color", first.Rows, first.Cols, ssh.TerminalModes{ssh.ECHO: 1, ssh.TTY_OP_ISPEED: 38400, ssh.TTY_OP_OSPEED: 38400}); e != nil {
		setReason("pty_rejected")
		return
	}
	if e = session.Shell(); e != nil {
		setReason("shell_rejected")
		return
	}
	if e = out.control(map[string]any{"type": "ready", "session_id": sessionID}); e != nil {
		return
	}
	go func() { <-ctx.Done(); client.Close() }()
	go func() {
		e := session.Wait()
		code := 0
		var exit *ssh.ExitError
		if errors.As(e, &exit) {
			code = exit.ExitStatus()
		} else if e != nil {
			code = -1
		}
		out.control(map[string]any{"type": "exit", "code": code})
		setReason("shell_exit")
		cancel()
	}()
	go func() {
		ticker := time.NewTicker(policy.Recheck)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if time.Since(time.Unix(0, out.activity.Load())) > policy.Idle {
					setReason("idle_timeout")
					cancel()
					return
				}
				qctx, done := context.WithTimeout(ctx, time.Second)
				current, err := s.Auth.Authenticate(r.WithContext(qctx), false)
				if err == nil && s.Secrets != nil {
					_, err = s.Secrets.Get(qctx, s.Config.Bao.Prefix+"/runtime")
				}
				done()
				if err != nil || !current.Admin() || s.Dispatch != nil && !s.Dispatch.Epoch.Owns() {
					setReason("authorization_lost")
					cancel()
					return
				}
			}
		}
	}()
	for {
		kind, data, err := c.Read(ctx)
		if err != nil {
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				setReason("session_timeout")
			}
			return
		}
		if kind == websocket.MessageBinary {
			if _, e = in.Write(data); e != nil {
				return
			}
			inputBytes.Add(uint64(len(data)))
			out.activity.Store(time.Now().UnixNano())
			continue
		}
		var control struct {
			Type  string `json:"type"`
			Cols  int    `json:"cols"`
			Rows  int    `json:"rows"`
			Bytes int    `json:"bytes"`
		}
		if json.Unmarshal(data, &control) != nil {
			setReason("invalid_frame")
			return
		}
		switch control.Type {
		case "resize":
			if !sshcontrol.ValidDimensions(control.Cols, control.Rows) {
				return
			}
			if e = session.WindowChange(control.Rows, control.Cols); e != nil {
				return
			}
		case "ack":
			if !out.ack(control.Bytes) {
				return
			}
		default:
			return
		}
	}
}
