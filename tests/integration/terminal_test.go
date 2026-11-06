//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/rychkin/Begom/internal/auth"
	"github.com/rychkin/Begom/internal/config"
	"github.com/rychkin/Begom/internal/coordinator"
	"github.com/rychkin/Begom/internal/db"
	"github.com/rychkin/Begom/internal/model"
	"github.com/rychkin/Begom/internal/pki"
	"github.com/rychkin/Begom/internal/secrets"
	"github.com/rychkin/Begom/internal/sshcontrol"
	"golang.org/x/crypto/ssh"
)

func TestTerminalTicketsHostKeyAndLifecycle(t *testing.T) {
	for _, test := range []string{"session_timeout", "idle_timeout", "authorization_lost", "host_key", "expired_ticket"} {
		t.Run(test, func(t *testing.T) {
			x := setup(t)
			private, public, e := pki.SSH()
			if e != nil {
				t.Fatal(e)
			}
			host, hostPublic, e := pki.SSH()
			if e != nil {
				t.Fatal(e)
			}
			hostSigner, _ := ssh.ParsePrivateKey(host)
			listener, e := net.Listen("tcp", "127.0.0.1:0")
			if e != nil {
				t.Fatal(e)
			}
			t.Cleanup(func() { listener.Close() })
			var ptys atomic.Int64
			go func() {
				for {
					raw, e := listener.Accept()
					if e != nil {
						return
					}
					go func() {
						defer raw.Close()
						sshConfig := &ssh.ServerConfig{PublicKeyCallback: func(meta ssh.ConnMetadata, key ssh.PublicKey) (*ssh.Permissions, error) {
							if meta.User() != "begom-shell" || string(ssh.MarshalAuthorizedKey(key)) != string(public) {
								return nil, io.EOF
							}
							return &ssh.Permissions{}, nil
						}, Config: ssh.Config{}, NoClientAuth: false, ServerVersion: "SSH-2.0-BegomTest"}
						sshConfig.AddHostKey(hostSigner)
						server, channels, requests, e := ssh.NewServerConn(raw, sshConfig)
						if e != nil {
							return
						}
						defer server.Close()
						go ssh.DiscardRequests(requests)
						for next := range channels {
							ch, requests, e := next.Accept()
							if e != nil {
								continue
							}
							go func() {
								defer ch.Close()
								for request := range requests {
									switch request.Type {
									case "pty-req":
										ptys.Add(1)
										request.Reply(true, nil)
									case "shell":
										request.Reply(true, nil)
										go io.Copy(ch, ch)
									case "window-change":
										request.Reply(true, nil)
									default:
										request.Reply(false, nil)
									}
								}
							}()
						}
					}()
				}
			}()
			dir := t.TempDir()
			role := filepath.Join(dir, "role")
			secret := filepath.Join(dir, "secret")
			os.WriteFile(role, []byte("test-role"), 0600)
			os.WriteFile(secret, []byte("test-secret"), 0600)
			baoServer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/v1/auth/approle/login" {
					json.NewEncoder(w).Encode(map[string]any{"auth": map[string]any{"client_token": "test-token", "lease_duration": 300}})
					return
				}
				json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"data": map[string]string{"private_key": string(private)}}})
			}))
			defer baoServer.Close()
			ca := filepath.Join(dir, "ca.crt")
			os.WriteFile(ca, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: baoServer.Certificate().Raw}), 0600)
			bao, e := secrets.New(config.Bao{URL: baoServer.URL, CA: ca, RoleIDFile: role, SecretIDFile: secret, Prefix: "begom/coordinator"})
			if e != nil {
				t.Fatal(e)
			}
			address := listener.Addr().(*net.TCPAddr)
			pin := string(hostPublic)
			if test == "host_key" {
				pin = string(public)
			}
			worker := model.WorkerConfig{ID: "w1", Host: "127.0.0.1", Threads: 1, Enabled: true, TerminalSSH: model.TerminalSSH{Port: address.Port, User: "begom-shell", HostKey: pin, Secret: "begom/coordinator/terminal/w1"}}
			if _, e = x.s.Pool.Exec(t.Context(), "UPDATE workers SET config=$2 WHERE id=$1", worker.ID, db.JSON(worker)); e != nil {
				t.Fatal(e)
			}
			policy := coordinator.TerminalPolicy{Session: 3 * time.Second, Idle: 2 * time.Second, Recheck: 25 * time.Millisecond}
			if test == "session_timeout" {
				policy.Session = 350 * time.Millisecond
			}
			if test == "idle_timeout" {
				policy.Idle = 150 * time.Millisecond
			}
			a := &auth.Service{Store: x.s}
			s := &coordinator.Server{Store: x.s, Auth: a, SSH: &sshcontrol.Service{Bao: bao}, Log: slog.New(slog.NewJSONHandler(io.Discard, nil)), TerminalPolicy: policy}
			web := httptest.NewTLSServer(s.Human())
			defer web.Close()
			s.Config.PublicOrigin = web.URL
			a.Origin = web.URL
			client := web.Client()
			client.Jar, _ = cookiejar.New(nil)
			csrf := ""
			post := func(path string, body any) map[string]any {
				t.Helper()
				req, _ := http.NewRequestWithContext(t.Context(), "POST", web.URL+path, strings.NewReader(string(db.JSON(body))))
				req.Header.Set("Content-Type", "application/json")
				req.Header.Set("Idempotency-Key", model.NewID())
				req.Header.Set("X-CSRF-Token", csrf)
				response, e := client.Do(req)
				if e != nil {
					t.Fatal(e)
				}
				defer response.Body.Close()
				var result map[string]any
				json.NewDecoder(response.Body).Decode(&result)
				if response.StatusCode >= 300 {
					t.Fatal("terminal HTTP", response.StatusCode, result["code"])
				}
				return result
			}
			csrf = post("/api/auth/login", map[string]string{"username": "test-admin", "password": "integration-only-password"})["csrf_token"].(string)
			ticket := post("/api/workers/w1/ssh/sessions", struct{}{})["ticket"].(string)
			if test == "expired_ticket" {
				x.s.Pool.Exec(t.Context(), "UPDATE ssh_sessions SET expires_at=clock_timestamp()-interval '1 second'")
			}
			dial := func() *websocket.Conn {
				t.Helper()
				c, _, e := websocket.Dial(t.Context(), strings.Replace(web.URL, "https:", "wss:", 1)+"/api/workers/w1/ssh/stream", &websocket.DialOptions{HTTPClient: client})
				if e != nil {
					t.Fatal(e)
				}
				if e = wsjson.Write(t.Context(), c, map[string]any{"type": "auth", "ticket": ticket, "cols": 100, "rows": 30}); e != nil {
					t.Fatal(e)
				}
				return c
			}
			c := dial()
			defer c.CloseNow()
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
			defer cancel()
			var message map[string]any
			e = wsjson.Read(ctx, c, &message)
			if test == "expired_ticket" {
				if websocket.CloseStatus(e) != websocket.StatusPolicyViolation {
					t.Fatal("expired ticket accepted", e)
				}
				return
			}
			if test == "host_key" {
				if e != nil || message["type"] != "error" || ptys.Load() != 0 {
					t.Fatal("host key was not enforced", message, e)
				}
				return
			}
			if e != nil || message["type"] != "ready" || ptys.Load() != 1 {
				t.Fatal("PTY shell not requested", message, e)
			}
			if test == "authorization_lost" {
				x.s.Pool.Exec(t.Context(), "UPDATE users SET enabled=false WHERE id=$1", x.user)
			}
			_, _, e = c.Read(ctx)
			if e == nil || ctx.Err() != nil {
				t.Fatal("session did not close on policy", test, e)
			}
			if test == "session_timeout" {
				replay := dial()
				defer replay.CloseNow()
				if e = wsjson.Read(ctx, replay, &message); websocket.CloseStatus(e) != websocket.StatusPolicyViolation {
					t.Fatal("ticket reused", e)
				}
			}
		})
	}
}
