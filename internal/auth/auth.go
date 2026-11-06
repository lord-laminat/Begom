package auth

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/rychkin/Begom/internal/db"
	"github.com/rychkin/Begom/internal/model"
	"golang.org/x/crypto/argon2"
	"net/http"
	"strconv"
	"strings"
)

const SessionCookie = "begom_session"
const CSRFCookie = "begom_csrf"

type User struct {
	ID       string   `json:"user_id"`
	Username string   `json:"username"`
	Roles    []string `json:"roles"`
	Enabled  bool     `json:"enabled"`
}

func (u User) Admin() bool    { return u.Has("administrator") }
func (u User) Operator() bool { return u.Admin() || u.Has("operator") }
func (u User) Has(role string) bool {
	for _, r := range u.Roles {
		if r == role {
			return true
		}
	}
	return false
}

type UserInput struct {
	Username string   `json:"username"`
	Password string   `json:"password,omitempty"`
	Roles    []string `json:"roles"`
	Enabled  bool     `json:"enabled"`
}

func Digest(s string) string { h := sha256.Sum256([]byte(s)); return hex.EncodeToString(h[:]) }
func Hash(password string) (string, error) {
	if len(password) < 12 || len(password) > 1024 {
		return "", errors.New("password must contain 12 to 1024 bytes")
	}
	salt := make([]byte, 16)
	if _, e := rand.Read(salt); e != nil {
		return "", e
	}
	key := argon2.IDKey([]byte(password), salt, 2, 32*1024, 2, 32)
	return fmt.Sprintf("$argon2id$v=19$m=32768,t=2,p=2$%s$%s", base64.RawStdEncoding.EncodeToString(salt), base64.RawStdEncoding.EncodeToString(key)), nil
}
func Verify(encoded, password string) bool {
	if len(password) > 1024 {
		return false
	}
	p := strings.Split(encoded, "$")
	if len(p) != 6 || p[1] != "argon2id" || p[2] != "v=19" || p[3] != "m=32768,t=2,p=2" {
		return false
	}
	salt, e := base64.RawStdEncoding.DecodeString(p[4])
	if e != nil || len(salt) != 16 {
		return false
	}
	want, e := base64.RawStdEncoding.DecodeString(p[5])
	if e != nil || len(want) != 32 {
		return false
	}
	got := argon2.IDKey([]byte(password), salt, 2, 32*1024, 2, 32)
	return subtle.ConstantTimeCompare(got, want) == 1
}

type Service struct {
	Store  *db.Store
	Origin string
}

func (s *Service) Bootstrap(ctx context.Context, name, password string) error {
	var n int
	if e := s.Store.Pool.QueryRow(ctx, "SELECT count(*) FROM users").Scan(&n); e != nil {
		return e
	}
	if n > 0 {
		return nil
	}
	hash, e := Hash(password)
	if e != nil {
		return e
	}
	tx, e := s.Store.Pool.Begin(ctx)
	if e != nil {
		return e
	}
	defer tx.Rollback(ctx)
	if _, e = tx.Exec(ctx, "SELECT pg_advisory_xact_lock(82939102)"); e != nil {
		return e
	}
	if e = tx.QueryRow(ctx, "SELECT count(*) FROM users").Scan(&n); e != nil {
		return e
	}
	if n == 0 {
		id := model.NewID()
		if _, e = tx.Exec(ctx, "INSERT INTO users(id,username,password_hash) VALUES($1,$2,$3)", id, name, hash); e != nil {
			return e
		}
		if _, e = tx.Exec(ctx, "INSERT INTO user_roles VALUES($1,'administrator')", id); e != nil {
			return e
		}
		if e = db.Audit(ctx, tx, "bootstrap", "user.bootstrap", id, "", nil); e != nil {
			return e
		}
	}
	return tx.Commit(ctx)
}
func (s *Service) Login(ctx context.Context, name, password string) (User, string, string, error) {
	var id, hash string
	e := s.Store.Pool.QueryRow(ctx, "SELECT id,password_hash FROM users WHERE username=$1 AND enabled", name).Scan(&id, &hash)
	if e != nil && !errors.Is(e, pgx.ErrNoRows) {
		return User{}, "", "", e
	}
	if errors.Is(e, pgx.ErrNoRows) {
		dummy, _ := Hash("unknown-user-password")
		Verify(dummy, password)
		return User{}, "", "", &db.Error{Status: 401, Code: "invalid_credentials", Message: "Invalid credentials"}
	}
	if !Verify(hash, password) {
		return User{}, "", "", &db.Error{Status: 401, Code: "invalid_credentials", Message: "Invalid credentials"}
	}
	u, e := s.User(ctx, id)
	if e != nil {
		return u, "", "", e
	}
	token, csrf := model.NewID()+model.NewID(), model.NewID()+model.NewID()
	tx, e := s.Store.Pool.Begin(ctx)
	if e != nil {
		return u, "", "", e
	}
	defer tx.Rollback(ctx)
	_, e = tx.Exec(ctx, "INSERT INTO sessions(token_hash,user_id,csrf_hash,expires_at) VALUES($1,$2,$3,clock_timestamp()+interval '8 hours')", Digest(token), id, Digest(csrf))
	if e != nil {
		return u, "", "", e
	}
	if e = db.Audit(ctx, tx, id, "user.login", id, "", nil); e != nil {
		return u, "", "", e
	}
	return u, token, csrf, tx.Commit(ctx)
}
func (s *Service) User(ctx context.Context, id string) (User, error) {
	var u User
	e := s.Store.Pool.QueryRow(ctx, "SELECT u.id,u.username,u.enabled,COALESCE(array_agg(r.role) FILTER(WHERE r.role IS NOT NULL),'{}') FROM users u LEFT JOIN user_roles r ON r.user_id=u.id WHERE u.id=$1 GROUP BY u.id", id).Scan(&u.ID, &u.Username, &u.Enabled, &u.Roles)
	return u, e
}
func (s *Service) Authenticate(r *http.Request, mutate bool) (User, error) {
	c, e := r.Cookie(SessionCookie)
	if e != nil {
		return User{}, &db.Error{Status: 401, Code: "authentication_required", Message: "Authentication required"}
	}
	var id, csrf string
	e = s.Store.Pool.QueryRow(r.Context(), "SELECT s.user_id,s.csrf_hash FROM sessions s JOIN users u ON u.id=s.user_id WHERE s.token_hash=$1 AND s.expires_at>clock_timestamp() AND u.enabled", Digest(c.Value)).Scan(&id, &csrf)
	if errors.Is(e, pgx.ErrNoRows) {
		return User{}, &db.Error{Status: 401, Code: "session_expired", Message: "Session expired"}
	}
	if e != nil {
		return User{}, e
	}
	if mutate {
		if r.Header.Get("Origin") != "" && r.Header.Get("Origin") != s.Origin {
			return User{}, &db.Error{Status: 403, Code: "invalid_origin", Message: "Invalid request origin"}
		}
		if subtle.ConstantTimeCompare([]byte(csrf), []byte(Digest(r.Header.Get("X-CSRF-Token")))) != 1 {
			return User{}, &db.Error{Status: 403, Code: "invalid_csrf", Message: "Invalid CSRF token"}
		}
	}
	return s.User(r.Context(), id)
}
func Cookies(w http.ResponseWriter, token, csrf string) {
	for _, v := range []struct {
		name, value string
		httpOnly    bool
	}{{SessionCookie, token, true}, {CSRFCookie, csrf, false}} {
		maxAge := 8 * 3600
		if v.value == "" {
			maxAge = -1
		}
		http.SetCookie(w, &http.Cookie{Name: v.name, Value: v.value, Path: "/", MaxAge: maxAge, Secure: true, HttpOnly: v.httpOnly, SameSite: http.SameSiteStrictMode})
	}
}
func (s *Service) Logout(ctx context.Context, token string) error {
	_, e := s.Store.Pool.Exec(ctx, "DELETE FROM sessions WHERE token_hash=$1", Digest(token))
	return e
}
func (s *Service) SaveUser(ctx context.Context, tx pgx.Tx, actor, request, id string, in UserInput) (any, int, error) {
	if len(in.Username) < 3 || len(in.Username) > 64 || strings.ContainsAny(in.Username, "\r\n\t ") {
		return nil, 0, db.Bad("invalid_username", "Invalid username")
	}
	if len(in.Roles) == 0 {
		return nil, 0, db.Bad("invalid_roles", "At least one role is required")
	}
	seen := map[string]bool{}
	for _, r := range in.Roles {
		if r != "viewer" && r != "operator" && r != "administrator" || seen[r] {
			return nil, 0, db.Bad("invalid_roles", "Invalid roles")
		}
		seen[r] = true
	}
	if _, e := tx.Exec(ctx, "SELECT pg_advisory_xact_lock(82939103)"); e != nil {
		return nil, 0, e
	}
	status := 200
	if id == "" {
		id = model.NewID()
		hash, e := Hash(in.Password)
		if e != nil {
			return nil, 0, db.Bad("invalid_password", e.Error())
		}
		if _, e = tx.Exec(ctx, "INSERT INTO users(id,username,password_hash,enabled) VALUES($1,$2,$3,$4)", id, in.Username, hash, in.Enabled); e != nil {
			return nil, 0, e
		}
		status = 201
	} else {
		var previousAdmin bool
		if e := tx.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM user_roles WHERE user_id=$1 AND role='administrator')", id).Scan(&previousAdmin); e != nil {
			return nil, 0, e
		}
		if previousAdmin && (!seen["administrator"] || !in.Enabled) {
			var others int
			if e := tx.QueryRow(ctx, "SELECT count(*) FROM user_roles r JOIN users u ON u.id=r.user_id WHERE role='administrator' AND u.enabled AND u.id<>$1", id).Scan(&others); e != nil {
				return nil, 0, e
			}
			if others == 0 {
				return nil, 0, db.Conflict("last_administrator")
			}
		}
		tag, e := tx.Exec(ctx, "UPDATE users SET username=$2,enabled=$3 WHERE id=$1", id, in.Username, in.Enabled)
		if e != nil {
			return nil, 0, e
		}
		if tag.RowsAffected() != 1 {
			return nil, 0, db.Forbidden()
		}
		if in.Password != "" {
			hash, e := Hash(in.Password)
			if e != nil {
				return nil, 0, db.Bad("invalid_password", e.Error())
			}
			if _, e = tx.Exec(ctx, "UPDATE users SET password_hash=$2 WHERE id=$1", id, hash); e != nil {
				return nil, 0, e
			}
		}
		if _, e = tx.Exec(ctx, "DELETE FROM user_roles WHERE user_id=$1", id); e != nil {
			return nil, 0, e
		}
		if _, e = tx.Exec(ctx, "DELETE FROM sessions WHERE user_id=$1", id); e != nil {
			return nil, 0, e
		}
	}
	for _, role := range in.Roles {
		if _, e := tx.Exec(ctx, "INSERT INTO user_roles VALUES($1,$2)", id, role); e != nil {
			return nil, 0, e
		}
	}
	e := db.Audit(ctx, tx, actor, "user.save", id, request, map[string]any{"roles": in.Roles, "enabled": in.Enabled})
	return User{ID: id, Username: in.Username, Roles: in.Roles, Enabled: in.Enabled}, status, e
}
func ParseLimit(s string, max int) int {
	n, e := strconv.Atoi(s)
	if e != nil || n < 1 || n > max {
		return max
	}
	return n
}
