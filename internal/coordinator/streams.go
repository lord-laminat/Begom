package coordinator

import (
	"context"
	"errors"
	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/jackc/pgx/v5"
	"github.com/rychkin/Begom/internal/auth"
	"github.com/rychkin/Begom/internal/db"
	"github.com/rychkin/Begom/internal/model"
	"github.com/rychkin/Begom/internal/nodecontrol"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

func (s *Server) snapshotValue(ctx context.Context, u auth.User) (any, error) {
	jobs, e := s.Store.ListJobs(ctx, u.ID, u.Admin())
	if e != nil {
		return nil, e
	}
	workers, e := s.allowedWorkers(ctx, u)
	if e != nil {
		return nil, e
	}
	v := map[string]any{"server_time": time.Now().UTC(), "jobs": jobs, "workers": workers}
	if u.Admin() {
		v["dispatcher"] = s.Dispatch.Stats()
		v["source_profile"] = map[string]any{"source_id": s.Config.Source.ID, "mock": s.Config.Source.Mock}
	}
	return v, nil
}
func (s *Server) snapshot(w http.ResponseWriter, r *http.Request, u auth.User) {
	v, e := s.snapshotValue(r.Context(), u)
	if e != nil {
		s.fail(w, r, e)
		return
	}
	s.json(w, 200, v)
}
func (s *Server) eventStream(w http.ResponseWriter, r *http.Request, u auth.User) {
	origin, _ := url.Parse(s.Config.PublicOrigin)
	c, e := websocket.Accept(w, r, &websocket.AcceptOptions{OriginPatterns: []string{origin.Host}})
	if e != nil {
		return
	}
	defer c.CloseNow()
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	ctx = c.CloseRead(ctx)
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		current, e := s.Auth.Authenticate(r, false)
		if e != nil {
			c.Close(websocket.StatusPolicyViolation, "session expired")
			return
		}
		v, e := s.snapshotValue(ctx, current)
		if e != nil {
			c.Close(websocket.StatusInternalError, "state unavailable")
			return
		}
		qctx, stop := context.WithTimeout(ctx, 3*time.Second)
		e = wsjson.Write(qctx, c, v)
		stop()
		if e != nil {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}
func (s *Server) controlStream(w http.ResponseWriter, r *http.Request, worker string) {
	c, e := websocket.Accept(w, r, &websocket.AcceptOptions{})
	if e != nil {
		return
	}
	defer c.CloseNow()
	ctx := c.CloseRead(r.Context())
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		cert := r.TLS.PeerCertificates[0]
		var revoked bool
		if !time.Now().Before(cert.NotAfter) {
			return
		}
		if e := s.Store.Pool.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM revoked_certificates WHERE serial=$1)", cert.SerialNumber.String()).Scan(&revoked); e != nil || revoked {
			return
		}
		var session bool
		if e := s.Store.Pool.QueryRow(ctx, "SELECT session_id=$2 AND session_id<>'' FROM workers WHERE id=$1", worker, r.Header.Get("Agent-Session")).Scan(&session); e != nil || !session {
			return
		}
		rows, e := s.Store.Pool.Query(ctx, "SELECT c.id,c.attempt_id,c.lease_expires_at,j.state FROM range_chunks c JOIN jobs j ON j.id=c.job_id WHERE c.leased_by=$1 AND c.status='leased' AND c.lease_expires_at>clock_timestamp()", worker)
		if e != nil {
			return
		}
		leases := map[string]any{}
		for rows.Next() {
			var chunk, attempt, state string
			var expiry time.Time
			if e = rows.Scan(&chunk, &attempt, &expiry, &state); e != nil {
				rows.Close()
				return
			}
			leases[chunk] = map[string]any{"attempt_id": attempt, "state": state, "lease_expires_at": expiry}
		}
		rows.Close()
		if rows.Err() != nil {
			return
		}
		message := map[string]any{"server_time": time.Now().UTC(), "epoch": s.Dispatch.Epoch.ID, "leases": leases, "ready": s.Dispatch.Epoch.Ready()}
		qctx, cancel := context.WithTimeout(ctx, time.Second)
		e = wsjson.Write(qctx, c, message)
		cancel()
		if e != nil {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
func (s *Server) renewIdentities(ctx context.Context) {
	workers, e := s.Store.Workers(ctx)
	if e != nil {
		return
	}
	refs := []string{s.Config.Bao.Prefix + "/tls"}
	for _, w := range workers {
		refs = append(refs, "begom/agents/"+w.ID+"/tls")
	}
	for _, ref := range refs {
		changed, e := s.Secrets.RenewIdentity(ctx, ref, false)
		if e != nil {
			s.Log.Warn("identity renewal deferred", "operation", "certificate.renew", "result", "retry", "error_code", "identity_dependency_unavailable")
			continue
		}
		if changed {
			s.Log.Info("identity renewed", "operation", "certificate.renew", "result", "ok")
		}
	}
}
func (s *Server) revoke(w http.ResponseWriter, r *http.Request, u auth.User) {
	if !u.Admin() {
		s.denied(w, r)
		return
	}
	var in struct {
		Serial string `json:"serial"`
	}
	b, e := s.decode(w, r, &in)
	if e != nil {
		s.fail(w, r, e)
		return
	}
	if in.Serial == "" || len(in.Serial) > 100 || strings.Trim(in.Serial, "0123456789") != "" {
		s.fail(w, r, db.Bad("invalid_serial", "Use the certificate serial number"))
		return
	}
	s.mutate(w, r, u.ID, b, func(tx pgx.Tx) (any, int, error) {
		_, e := tx.Exec(r.Context(), "INSERT INTO revoked_certificates(serial) VALUES($1) ON CONFLICT DO NOTHING", in.Serial)
		if e != nil {
			return nil, 0, e
		}
		e = db.Audit(r.Context(), tx, u.ID, "certificate.revoke", r.PathValue("workerId"), r.Header.Get("X-Request-ID"), in)
		return map[string]bool{"revoked": true}, 200, e
	})
}
func (s *Server) rotate(w http.ResponseWriter, r *http.Request, u auth.User) {
	if !u.Admin() {
		s.denied(w, r)
		return
	}
	id := r.PathValue("workerId")
	if _, e := s.Store.GetWorker(r.Context(), id); e != nil {
		s.fail(w, r, e)
		return
	}
	var in struct{}
	b, e := s.decode(w, r, &in)
	if e != nil {
		s.fail(w, r, e)
		return
	}
	s.mutate(w, r, u.ID, b, func(tx pgx.Tx) (any, int, error) {
		if _, e := s.Secrets.RenewIdentity(r.Context(), "begom/agents/"+id+"/tls", true); e != nil {
			return nil, 0, e
		}
		e := db.Audit(r.Context(), tx, u.ID, "certificate.rotate", id, r.Header.Get("X-Request-ID"), nil)
		return map[string]bool{"rotated": true}, 200, e
	})
}
func (s *Server) actions(w http.ResponseWriter, r *http.Request, u auth.User) {
	if !u.Admin() {
		s.denied(w, r)
		return
	}
	rows, e := s.Store.Pool.Query(r.Context(), "SELECT id,action,state,error_code,created_at FROM worker_actions WHERE worker_id=$1 ORDER BY created_at DESC LIMIT 50", r.PathValue("workerId"))
	if e != nil {
		s.fail(w, r, e)
		return
	}
	defer rows.Close()
	out := []map[string]any{}
	for rows.Next() {
		var id, action, state, code string
		var created time.Time
		if e = rows.Scan(&id, &action, &state, &code, &created); e != nil {
			s.fail(w, r, e)
			return
		}
		out = append(out, map[string]any{"action_id": id, "action": action, "state": state, "error_code": code, "created_at": created})
	}
	s.json(w, 200, map[string]any{"actions": out})
}
func (s *Server) action(w http.ResponseWriter, r *http.Request, u auth.User) {
	if !u.Admin() {
		s.denied(w, r)
		return
	}
	var in struct {
		Action  string `json:"action"`
		Version string `json:"version"`
	}
	b, e := s.decode(w, r, &in)
	if e != nil {
		s.fail(w, r, e)
		return
	}
	switch in.Action {
	case "check", "start", "stop", "restart", "install", "update":
	default:
		s.fail(w, r, db.Bad("invalid_action", "Unknown node action"))
		return
	}
	worker, e := s.Store.GetWorker(r.Context(), r.PathValue("workerId"))
	if e != nil {
		s.fail(w, r, e)
		return
	}
	var actionID string
	committed := s.mutate(w, r, u.ID, b, func(tx pgx.Tx) (any, int, error) {
		actionID = model.NewID()
		_, e := tx.Exec(r.Context(), "INSERT INTO worker_actions(id,worker_id,actor_id,action,state) VALUES($1,$2,$3,$4,'pending')", actionID, worker.ID, u.ID, in.Action)
		if e != nil {
			return nil, 0, e
		}
		e = db.Audit(r.Context(), tx, u.ID, "ssh."+in.Action, worker.ID, r.Header.Get("X-Request-ID"), map[string]string{"action_id": actionID, "version": in.Version})
		return map[string]string{"action_id": actionID, "state": "pending"}, 202, e
	})
	if !committed || actionID == "" {
		return
	}
	go s.runAction(actionID, worker, in.Action, in.Version)
}
func (s *Server) runAction(id string, w model.Worker, action, version string) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	if !s.Dispatch.Epoch.Owns() {
		return
	}
	tx, claimErr := s.Store.Pool.Begin(ctx)
	if claimErr != nil {
		return
	}
	defer tx.Rollback(ctx)
	if _, claimErr = tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtextextended($1,0))", "ssh:"+w.ID); claimErr != nil {
		return
	}
	tag, claimErr := tx.Exec(ctx, "UPDATE worker_actions a SET state='running',updated_at=clock_timestamp() WHERE id=$1 AND state='pending' AND NOT EXISTS(SELECT 1 FROM worker_actions b WHERE b.worker_id=a.worker_id AND (b.state='running' OR b.state='pending' AND b.created_at<a.created_at))", id)
	if claimErr != nil || tag.RowsAffected() != 1 {
		return
	}
	if tx.Commit(ctx) != nil {
		return
	}
	_, e := s.SSH.Run(ctx, w.WorkerConfig, action, version)
	state, code := "completed", ""
	if e == nil && (action == "start" || action == "restart" || action == "install" || action == "update") {
		start := time.Now()
		timer := time.NewTicker(500 * time.Millisecond)
		defer timer.Stop()
		for {
			fresh, err := s.Store.GetWorker(ctx, w.ID)
			if err == nil && fresh.LastHeartbeat != nil && fresh.LastHeartbeat.After(start) && fresh.Version == model.Version {
				break
			}
			select {
			case <-ctx.Done():
				e = errors.New("agent did not register")
			case <-timer.C:
			}
			if e != nil {
				break
			}
		}
	}
	if e != nil {
		state = "failed"
		code = "ssh_operation_failed"
	}
	qctx, stop := context.WithTimeout(context.Background(), 3*time.Second)
	defer stop()
	_, err := s.Store.Mutate(qctx, "system", "ssh.result", id, []byte(state), func(tx pgx.Tx) (any, int, error) {
		_, err := tx.Exec(qctx, "UPDATE worker_actions SET state=$2,error_code=$3,updated_at=clock_timestamp() WHERE id=$1", id, state, code)
		if err != nil {
			return nil, 0, err
		}
		err = db.Audit(qctx, tx, "system", "ssh.result", w.ID, "", map[string]string{"action_id": id, "state": state, "error_code": code})
		return map[string]string{"state": state}, 200, err
	})
	if err != nil {
		s.Log.Warn("SSH result awaits database", "operation", "ssh.result", "result", "error", "error_code", "database_unavailable")
	}
}
func (s *Server) recoverActions(ctx context.Context) error {
	tx, e := s.Store.Pool.Begin(ctx)
	if e != nil {
		return e
	}
	defer tx.Rollback(ctx)
	rows, e := tx.Query(ctx, "UPDATE worker_actions SET state='failed',error_code='outcome_unknown',updated_at=clock_timestamp() WHERE state='running' AND updated_at<(SELECT created_at FROM coordinator_epochs WHERE id=$1) RETURNING id,worker_id", s.Dispatch.Epoch.ID)
	if e != nil {
		return e
	}
	type oldAction struct{ id, worker string }
	var interrupted []oldAction
	for rows.Next() {
		var x oldAction
		if e = rows.Scan(&x.id, &x.worker); e != nil {
			rows.Close()
			return e
		}
		interrupted = append(interrupted, x)
	}
	rows.Close()
	for _, x := range interrupted {
		if e = db.Audit(ctx, tx, "system", "ssh.outcome_unknown", x.worker, "", map[string]string{"action_id": x.id}); e != nil {
			return e
		}
	}
	if e = tx.Commit(ctx); e != nil {
		return e
	}
	rows, e = s.Store.Pool.Query(ctx, "SELECT a.id,a.worker_id,a.action,COALESCE((SELECT context->>'version' FROM audit_log l WHERE l.context->>'action_id'=a.id AND l.action='ssh.'||a.action ORDER BY l.id DESC LIMIT 1),'') FROM worker_actions a WHERE state='pending' ORDER BY created_at LIMIT 50")
	if e != nil {
		return e
	}
	type pending struct{ id, worker, action, version string }
	var actions []pending
	for rows.Next() {
		var a pending
		if e = rows.Scan(&a.id, &a.worker, &a.action, &a.version); e != nil {
			rows.Close()
			return e
		}
		actions = append(actions, a)
	}
	rows.Close()
	for _, a := range actions {
		w, e := s.Store.GetWorker(ctx, a.worker)
		if e != nil {
			return e
		}
		go s.runAction(a.id, w, a.action, a.version)
	}
	return nil
}
func (s *Server) sshSession(w http.ResponseWriter, r *http.Request, u auth.User) {
	if !u.Admin() {
		s.denied(w, r)
		return
	}
	var empty struct{}
	b, e := s.decode(w, r, &empty)
	if e != nil {
		s.fail(w, r, e)
		return
	}
	id := r.PathValue("workerId")
	if _, e = s.Store.GetWorker(r.Context(), id); e != nil {
		s.fail(w, r, e)
		return
	}
	s.mutate(w, r, u.ID, b, func(tx pgx.Tx) (any, int, error) {
		session, ticket := model.NewID(), model.NewID()+model.NewID()
		_, e := tx.Exec(r.Context(), "INSERT INTO ssh_sessions(id,user_id,worker_id,ticket_hash,expires_at) VALUES($1,$2,$3,$4,clock_timestamp()+interval '30 seconds')", session, u.ID, id, auth.Digest(ticket))
		if e != nil {
			return nil, 0, e
		}
		e = db.Audit(r.Context(), tx, u.ID, "ssh.session.create", id, r.Header.Get("X-Request-ID"), map[string]string{"session_id": session})
		return map[string]any{"session_id": session, "ticket": ticket, "expires_in_seconds": 30}, 201, e
	})
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
	c.SetReadLimit(1024)
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Minute)
	defer cancel()
	var first struct {
		Ticket string `json:"ticket"`
	}
	qctx, stop := context.WithTimeout(ctx, 5*time.Second)
	e = wsjson.Read(qctx, c, &first)
	stop()
	if e != nil {
		return
	}
	var sessionID string
	e = s.Store.Pool.QueryRow(ctx, "UPDATE ssh_sessions SET used=true WHERE ticket_hash=$1 AND user_id=$2 AND worker_id=$3 AND NOT used AND expires_at>clock_timestamp() RETURNING id", auth.Digest(first.Ticket), u.ID, worker.ID).Scan(&sessionID)
	if e != nil {
		c.Close(websocket.StatusPolicyViolation, "ticket rejected")
		return
	}
	client, e := s.SSH.Connect(ctx, worker.WorkerConfig)
	if e != nil {
		c.Close(websocket.StatusInternalError, "SSH unavailable")
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
	out, e := session.StdoutPipe()
	if e != nil {
		return
	}
	if e = session.Start("begom-node-helper console"); e != nil {
		return
	}
	defer func() {
		qctx, c := context.WithTimeout(context.Background(), time.Second)
		defer c()
		s.Store.Mutate(qctx, u.ID, "ssh.close", model.NewID(), []byte(sessionID), func(tx pgx.Tx) (any, int, error) {
			e := db.Audit(qctx, tx, u.ID, "ssh.session.close", worker.ID, "", map[string]string{"session_id": sessionID})
			return map[string]bool{"ok": true}, 200, e
		})
	}()
	var writeMu sync.Mutex
	go func() {
		buffer := make([]byte, 4096)
		total := 0
		for {
			n, e := out.Read(buffer)
			if n > 0 {
				total += n
				if total > 2<<20 {
					cancel()
					return
				}
				writeMu.Lock()
				qctx, stop := context.WithTimeout(ctx, 3*time.Second)
				we := wsjson.Write(qctx, c, map[string]string{"output": string(buffer[:n])})
				stop()
				writeMu.Unlock()
				if we != nil {
					cancel()
					return
				}
			}
			if e != nil {
				cancel()
				return
			}
		}
	}()
	for {
		var command struct {
			Command string `json:"command"`
			ID      string `json:"command_id"`
		}
		qctx, stop := context.WithTimeout(ctx, 2*time.Minute)
		e = wsjson.Read(qctx, c, &command)
		stop()
		if e != nil {
			return
		}
		current, e := s.Auth.Authenticate(r, false)
		if e != nil || !current.Admin() {
			return
		}
		parsed, e := nodecontrol.Parse(command.Command)
		if e != nil {
			writeMu.Lock()
			wsjson.Write(ctx, c, map[string]string{"output": "Команда запрещена\n"})
			writeMu.Unlock()
			continue
		}
		if !model.ValidID(command.ID) {
			return
		}
		confirmation, err := s.Store.Mutate(ctx, u.ID, "ssh.command:"+sessionID, command.ID, db.JSON(command), func(tx pgx.Tx) (any, int, error) {
			e := db.Audit(ctx, tx, u.ID, "ssh.command", worker.ID, "", map[string]any{"session_id": sessionID, "command": parsed.Action, "lines": parsed.Lines})
			return map[string]bool{"accepted": true}, 200, e
		})
		if err != nil {
			return
		}
		if confirmation.Replayed {
			continue
		}
		if _, e = io.WriteString(in, strings.TrimSpace(command.Command)+"\n"); e != nil {
			return
		}
	}
}
