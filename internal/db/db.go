package db

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rychkin/Begom/internal/model"
	"sort"
	"strconv"
	"strings"
	"time"
)

//go:embed migrations/*.sql
var migrations embed.FS

const SchemaVersion = 4

type Store struct {
	Pool        *pgxpool.Pool
	Lease       time.Duration
	Heartbeat   time.Duration
	SourceID    string
	MetadataKey []byte
}
type Queryer interface {
	QueryRow(context.Context, string, ...any) pgx.Row
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
}

func (s *Store) SealURL(raw string) ([]byte, error) {
	if raw == "" {
		return nil, nil
	}
	block, e := aes.NewCipher(s.MetadataKey)
	if e != nil {
		return nil, errors.New("metadata encryption key unavailable")
	}
	a, e := cipher.NewGCM(block)
	if e != nil {
		return nil, e
	}
	nonce := make([]byte, a.NonceSize())
	if _, e = rand.Read(nonce); e != nil {
		return nil, e
	}
	return a.Seal(nonce, nonce, []byte(raw), []byte(s.SourceID)), nil
}

type Error struct {
	Status        int
	Code, Message string
}

func (e *Error) Error() string       { return e.Code + ": " + e.Message }
func Bad(code, message string) error { return &Error{422, code, message} }
func Conflict(code string) error     { return &Error{409, code, "Operation conflicts with current state"} }
func Forbidden() error               { return &Error{404, "not_found", "Object not found"} }
func Open(ctx context.Context, url string) (*Store, error) {
	return OpenLimit(ctx, url, 32)
}
func OpenLimit(ctx context.Context, url string, limit int32) (*Store, error) {
	cfg, e := pgxpool.ParseConfig(url)
	if e != nil {
		return nil, errors.New("invalid database configuration")
	}
	cfg.MaxConns = limit
	cfg.MinConns = 2
	cfg.MaxConnIdleTime = time.Minute
	s := &Store{}
	s.Pool, e = pgxpool.NewWithConfig(ctx, cfg)
	if e != nil {
		return nil, e
	}
	return s, s.Pool.Ping(ctx)
}
func (s *Store) Migrate(ctx context.Context) error {
	c, e := s.Pool.Acquire(ctx)
	if e != nil {
		return e
	}
	defer c.Release()
	if _, e = c.Exec(ctx, "SELECT pg_advisory_lock(82939101)"); e != nil {
		return e
	}
	defer c.Exec(context.Background(), "SELECT pg_advisory_unlock(82939101)")
	entries, e := migrations.ReadDir("migrations")
	if e != nil {
		return e
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	var exists bool
	if e = c.QueryRow(ctx, "SELECT to_regclass('schema_migrations') IS NOT NULL").Scan(&exists); e != nil {
		return e
	}
	for _, entry := range entries {
		v, _ := strconv.Atoi(strings.Split(entry.Name(), "_")[0])
		b, _ := migrations.ReadFile("migrations/" + entry.Name())
		sum := sha256.Sum256(b)
		digest := hex.EncodeToString(sum[:])
		if exists {
			var old string
			e = c.QueryRow(ctx, "SELECT checksum FROM schema_migrations WHERE version=$1", v).Scan(&old)
			if e == nil {
				if old != digest {
					return errors.New("migration checksum mismatch")
				}
				continue
			}
			if !errors.Is(e, pgx.ErrNoRows) {
				return e
			}
		}
		tx, e := c.Begin(ctx)
		if e != nil {
			return e
		}
		if _, e = tx.Exec(ctx, string(b)); e == nil {
			_, e = tx.Exec(ctx, "INSERT INTO schema_migrations(version,checksum) VALUES($1,$2)", v, digest)
		}
		if e != nil {
			tx.Rollback(ctx)
			return e
		}
		if e = tx.Commit(ctx); e != nil {
			return e
		}
		exists = true
	}
	return nil
}

type Result struct {
	Status   int
	Body     json.RawMessage
	Replayed bool
}
type Mutation func(pgx.Tx) (any, int, error)

func (s *Store) Mutate(ctx context.Context, actor, op, key string, body []byte, fn Mutation) (Result, error) {
	if !model.ValidID(key) {
		return Result{}, &Error{400, "idempotency_key_required", "A valid Idempotency-Key is required"}
	}
	tx, e := s.Pool.Begin(ctx)
	if e != nil {
		return Result{}, e
	}
	defer tx.Rollback(ctx)
	if _, e = tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtextextended($1,0))", actor+"/"+op+"/"+key); e != nil {
		return Result{}, e
	}
	h := sha256.Sum256(body)
	hash := hex.EncodeToString(h[:])
	var previous string
	var r Result
	e = tx.QueryRow(ctx, "SELECT body_hash,status_code,response FROM idempotency_keys WHERE actor_id=$1 AND operation=$2 AND key=$3", actor, op, key).Scan(&previous, &r.Status, &r.Body)
	if e == nil {
		if previous != hash {
			return Result{}, Conflict("idempotency_conflict")
		}
		r.Replayed = true
		return r, tx.Commit(ctx)
	}
	if !errors.Is(e, pgx.ErrNoRows) {
		return r, e
	}
	v, status, e := fn(tx)
	if e != nil {
		return r, e
	}
	r.Status = status
	r.Body, e = json.Marshal(v)
	if e != nil {
		return r, e
	}
	if e = tx.QueryRow(ctx, "INSERT INTO idempotency_keys(actor_id,operation,key,body_hash,status_code,response) VALUES($1,$2,$3,$4,$5,$6) RETURNING response", actor, op, key, hash, r.Status, r.Body).Scan(&r.Body); e != nil {
		return r, e
	}
	return r, tx.Commit(ctx)
}
func Audit(ctx context.Context, tx pgx.Tx, actor, action, object, request string, data any) error {
	b, e := json.Marshal(data)
	if e != nil {
		return e
	}
	_, e = tx.Exec(ctx, "INSERT INTO audit_log(actor_id,action,object_id,request_id,context) VALUES($1,$2,$3,$4,$5)", actor, action, object, request, b)
	return e
}
func Event(ctx context.Context, tx pgx.Tx, job, worker, chunk, attempt, op, result, code string, id *model.Number) error {
	var external any
	if id != nil {
		external = id.String()
	}
	_, e := tx.Exec(ctx, "INSERT INTO events(component,job_id,worker_id,chunk_id,attempt_id,external_id,operation,result,error_code) VALUES('coordinator',NULLIF($1,''),NULLIF($2,''),NULLIF($3,''),NULLIF($4,''),$5,$6,$7,$8)", job, worker, chunk, attempt, external, op, result, code)
	return e
}
func Number(s string) model.Number { v, _ := strconv.ParseUint(s, 10, 64); return model.Number(v) }
func JSON(v any) []byte {
	b, e := json.Marshal(v)
	if e != nil {
		panic(fmt.Sprintf("internal JSON type: %T", v))
	}
	return b
}
func (s *Store) CheckAccess(ctx context.Context, user, job string, admin, manage bool) error {
	if admin {
		var b bool
		e := s.Pool.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM jobs WHERE id=$1)", job).Scan(&b)
		if e != nil {
			return e
		}
		if b {
			return nil
		}
		return Forbidden()
	}
	var ok bool
	e := s.Pool.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM job_access WHERE job_id=$1 AND user_id=$2 AND (NOT $3 OR permission='manage'))", job, user, manage).Scan(&ok)
	if e != nil {
		return e
	}
	if !ok {
		return Forbidden()
	}
	return nil
}
func (s *Store) SeedWorkers(ctx context.Context, workers []model.WorkerConfig) error {
	for _, w := range workers {
		if e := w.Validate(); e != nil {
			return e
		}
		if _, e := s.Pool.Exec(ctx, "INSERT INTO workers(id,config,enabled) VALUES($1,$2,$3) ON CONFLICT(id) DO NOTHING", w.ID, JSON(w), w.Enabled); e != nil {
			return e
		}
	}
	return nil
}
