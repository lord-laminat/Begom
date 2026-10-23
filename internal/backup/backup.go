// Package backup creates encrypted, independently restorable SQL/file sets.
package backup

import (
	"archive/tar"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/rychkin/Begom/internal/config"
	"github.com/rychkin/Begom/internal/db"
	"github.com/rychkin/Begom/internal/model"
	"github.com/rychkin/Begom/internal/storage"
	"io"
	"net/url"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"
	"time"
)

type Config struct {
	DatabaseURLFile string         `json:"database_url_file" yaml:"database_url_file"`
	Storage         config.Storage `json:"storage" yaml:"storage"`
	Repository      string         `json:"repository" yaml:"repository"`
	PasswordFile    string         `json:"password_file" yaml:"password_file"`
	KnownHostsFile  string         `json:"known_hosts_file" yaml:"known_hosts_file"`
	SSHKeyFile      string         `json:"ssh_key_file" yaml:"ssh_key_file"`
	WorkDir         string         `json:"work_dir" yaml:"work_dir"`
}
type File struct {
	ID       string `json:"file_id"`
	Path     string `json:"path"`
	Size     uint64 `json:"size"`
	Checksum string `json:"sha256"`
}
type Manifest struct {
	ID           string    `json:"backup_id"`
	Version      string    `json:"version"`
	Schema       int       `json:"schema_version"`
	OwnerRole    string    `json:"owner_role"`
	CreatedAt    time.Time `json:"created_at"`
	VolumeID     string    `json:"volume_id"`
	SQLChecksum  string    `json:"sql_sha256"`
	Files        []File    `json:"files"`
	Sidecars     []File    `json:"sidecars"`
	KeyReference string    `json:"key_reference"`
}
type Result struct {
	BackupID    string    `json:"backup_id"`
	StartedAt   time.Time `json:"started_at"`
	FinishedAt  time.Time `json:"finished_at"`
	Files       int       `json:"files"`
	State       string    `json:"state"`
	Snapshot    string    `json:"snapshot,omitempty"`
	Database    string    `json:"database,omitempty"`
	RestoreRoot string    `json:"restore_root,omitempty"`
	RPOSeconds  float64   `json:"rpo_seconds,omitempty"`
	RTOSeconds  float64   `json:"rto_seconds,omitempty"`
}

func credentials(raw, dir string) (string, []string, error) {
	u, e := url.Parse(raw)
	if e != nil || u.User == nil {
		return "", nil, errors.New("invalid database profile")
	}
	password, _ := u.User.Password()
	username := u.User.Username()
	u.User = url.User(username)
	escape := func(s string) string { return strings.NewReplacer("\\", "\\\\", ":", "\\:").Replace(s) }
	port := u.Port()
	if port == "" {
		port = "5432"
	}
	pgpass := filepath.Join(dir, "pgpass")
	line := escape(u.Hostname()) + ":" + escape(port) + ":*:" + escape(username) + ":" + escape(password) + "\n"
	if e = os.WriteFile(pgpass, []byte(line), 0600); e != nil {
		return "", nil, e
	}
	return u.String(), append(os.Environ(), "PGPASSFILE="+pgpass), nil
}
func (c Config) restic(ctx context.Context, args ...string) *exec.Cmd {
	cmd := exec.CommandContext(ctx, "restic", append([]string{"--no-cache"}, args...)...)
	cmd.Env = append(os.Environ(), "RESTIC_REPOSITORY="+c.Repository, "RESTIC_PASSWORD_FILE="+c.PasswordFile)
	if c.KnownHostsFile != "" && c.SSHKeyFile != "" {
		cmd.Args = append(cmd.Args, "-o", "sftp.command=ssh -i "+c.SSHKeyFile+" -o UserKnownHostsFile="+c.KnownHostsFile+" -o StrictHostKeyChecking=yes -o BatchMode=yes -s sftp")
	}
	return cmd
}
func run(cmd *exec.Cmd) error {
	if e := cmd.Run(); e != nil {
		return errors.New("backup tool failed")
	}
	return nil
}
func Run(ctx context.Context, c Config) (Result, error) {
	result := Result{BackupID: model.NewID(), StartedAt: time.Now().UTC(), State: "failed"}
	if e := ValidateConfig(c); e != nil {
		return result, e
	}
	if c.Repository == "" || c.PasswordFile == "" {
		return result, errors.New("restic profile missing")
	}
	volume := &storage.Volume{Config: c.Storage}
	root, e := volume.OpenRoot()
	if e != nil {
		return result, e
	}
	defer root.Close()
	work, e := os.MkdirTemp(c.WorkDir, "begom-backup-")
	if e != nil {
		return result, e
	}
	defer os.RemoveAll(work)
	raw, e := config.Secret(c.DatabaseURLFile)
	if e != nil {
		return result, e
	}
	store, e := db.Open(ctx, raw)
	if e != nil {
		return result, e
	}
	defer store.Pool.Close()
	tx, e := store.Pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if e != nil {
		return result, e
	}
	defer tx.Rollback(ctx)
	var snapshot string
	if e = tx.QueryRow(ctx, "SELECT pg_export_snapshot()").Scan(&snapshot); e != nil {
		return result, e
	}
	m := Manifest{ID: result.BackupID, Version: model.Version, CreatedAt: result.StartedAt, VolumeID: c.Storage.VolumeID, Files: []File{}, KeyReference: "begom/coordinator/runtime:metadata_key"}
	if e = tx.QueryRow(ctx, "SELECT max(version) FROM schema_migrations").Scan(&m.Schema); e != nil {
		return result, e
	}
	if e = tx.QueryRow(ctx, "SELECT pg_get_userbyid(relowner) FROM pg_class WHERE oid='jobs'::regclass").Scan(&m.OwnerRole); e != nil {
		return result, e
	}
	rows, e := tx.Query(ctx, "SELECT id,storage_path,size_bytes::text,checksum FROM files WHERE ready ORDER BY storage_path")
	if e != nil {
		return result, e
	}
	for rows.Next() {
		var f File
		var n string
		if e = rows.Scan(&f.ID, &f.Path, &n, &f.Checksum); e != nil {
			rows.Close()
			return result, e
		}
		if !storage.ValidPath(f.Path) {
			rows.Close()
			return result, errors.New("invalid catalog path")
		}
		f.Size = uint64(db.Number(n))
		m.Files = append(m.Files, f)
	}
	rows.Close()
	if e = rows.Err(); e != nil {
		return result, e
	}
	seenDirs := map[string]bool{}
	for _, video := range m.Files {
		dir := path.Dir(video.Path)
		if seenDirs[dir] {
			continue
		}
		seenDirs[dir] = true
		for _, name := range []string{"metadata.json", "publication.json"} {
			rel := path.Join(dir, name)
			size, hash, e := storage.Hash(root, path.Join("published", rel))
			if e != nil {
				return result, e
			}
			m.Sidecars = append(m.Sidecars, File{Path: rel, Size: size, Checksum: hash})
		}
	}
	safeURL, env, e := credentials(raw, work)
	if e != nil {
		return result, e
	}
	dumpPath := filepath.Join(work, "database.dump")
	dump := exec.CommandContext(ctx, "pg_dump", "--format=custom", "--no-owner", "--no-privileges", "--snapshot="+snapshot, "--file="+dumpPath, safeURL)
	dump.Env = env
	if e = run(dump); e != nil {
		return result, e
	}
	if e = tx.Commit(ctx); e != nil {
		return result, e
	}
	f, e := os.Open(dumpPath)
	if e != nil {
		return result, e
	}
	h := sha256.New()
	_, e = io.Copy(h, f)
	f.Close()
	if e != nil {
		return result, e
	}
	m.SQLChecksum = hex.EncodeToString(h.Sum(nil))
	// Probe without reinitializing a broken existing repository.
	probe := c.restic(ctx, "cat", "config")
	if probe.Run() != nil {
		return result, errors.New("restic repository unavailable; initialize it explicitly")
	}
	pipeR, pipeW := io.Pipe()
	cmd := c.restic(ctx, "backup", "--stdin", "--stdin-filename", "begom.tar", "--tag", m.ID, "--json")
	cmd.Stdin = pipeR
	var output strings.Builder
	cmd.Stdout = &output
	if e = cmd.Start(); e != nil {
		return result, e
	}
	writeErr := make(chan error, 1)
	go func() {
		tw := tar.NewWriter(pipeW)
		add := func(name string, reader io.Reader, size int64) error {
			if e := tw.WriteHeader(&tar.Header{Name: name, Mode: 0600, Size: size, Typeflag: tar.TypeReg, ModTime: m.CreatedAt}); e != nil {
				return e
			}
			_, e := io.CopyN(tw, reader, size)
			return e
		}
		manifest, _ := json.Marshal(m)
		e := add("manifest.json", strings.NewReader(string(manifest)), int64(len(manifest)))
		if e == nil {
			d, err := os.Open(dumpPath)
			if err != nil {
				e = err
			} else {
				st, _ := d.Stat()
				e = add("database.dump", d, st.Size())
				d.Close()
			}
		}
		allFiles := append(append([]File{}, m.Files...), m.Sidecars...)
		for _, file := range allFiles {
			if e != nil {
				break
			}
			if _, e = volume.Check(); e != nil {
				break
			}
			size, hash, err := storage.Hash(root, path.Join("published", file.Path))
			if err != nil || size != file.Size || hash != file.Checksum {
				e = errors.New("ready file integrity failed during backup")
				break
			}
			fd, err := root.Open(path.Join("published", file.Path))
			if err != nil {
				e = err
				break
			}
			e = add(path.Join("published", file.Path), fd, int64(file.Size))
			fd.Close()
		}
		if e == nil {
			e = tw.Close()
		}
		pipeW.CloseWithError(e)
		writeErr <- e
	}()
	processErr := cmd.Wait()
	pipeR.Close()
	e = <-writeErr
	if e != nil {
		return result, e
	}
	if processErr != nil {
		return result, errors.New("restic backup failed")
	}
	result.State = "completed"
	result.Files = len(m.Files)
	result.FinishedAt = time.Now().UTC()
	for _, line := range strings.Split(output.String(), "\n") {
		var summary struct {
			Type     string `json:"message_type"`
			Snapshot string `json:"snapshot_id"`
		}
		if json.Unmarshal([]byte(line), &summary) == nil && summary.Type == "summary" {
			result.Snapshot = summary.Snapshot
		}
	}
	return result, nil
}
func Init(ctx context.Context, c Config) error {
	if e := ValidateConfig(c); e != nil {
		return e
	}
	return run(c.restic(ctx, "init"))
}
func Restore(ctx context.Context, c Config, snapshot, target, newDatabase string) (Result, error) {
	result := Result{StartedAt: time.Now().UTC(), State: "failed", Snapshot: snapshot}
	if e := ValidateConfig(c); e != nil {
		return result, e
	}
	if !strings.HasPrefix(newDatabase, "begom_restore_") || !model.ValidID(newDatabase) || snapshot == "" {
		return result, errors.New("restore requires a new begom_restore_* database and an explicit snapshot")
	}
	if e := os.Mkdir(target, 0700); e != nil {
		return result, errors.New("restore target must be a new empty directory")
	}
	root, e := os.OpenRoot(target)
	if e != nil {
		return result, e
	}
	defer root.Close()
	cmd := c.restic(ctx, "dump", snapshot, "begom.tar")
	out, e := cmd.StdoutPipe()
	if e != nil {
		return result, e
	}
	if e = cmd.Start(); e != nil {
		return result, e
	}
	defer func() {
		if cmd.ProcessState == nil {
			cmd.Process.Kill()
			cmd.Wait()
		}
	}()
	tr := tar.NewReader(out)
	var manifest Manifest
	seen := map[string]bool{}
	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			cmd.Process.Kill()
			cmd.Wait()
			return result, err
		}
		if !storage.ValidPath(header.Name) || header.Typeflag != tar.TypeReg || header.Size < 0 || seen[header.Name] || !(header.Name == "manifest.json" || header.Name == "database.dump" || strings.HasPrefix(header.Name, "published/")) {
			cmd.Process.Kill()
			cmd.Wait()
			return result, errors.New("invalid restore archive")
		}
		seen[header.Name] = true
		if header.Name == "manifest.json" {
			if header.Size > 16<<20 {
				cmd.Process.Kill()
				cmd.Wait()
				return result, errors.New("manifest too large")
			}
			b, err := io.ReadAll(io.LimitReader(tr, header.Size))
			if err != nil || json.Unmarshal(b, &manifest) != nil {
				cmd.Process.Kill()
				cmd.Wait()
				return result, errors.New("invalid manifest")
			}
			if err = storage.WriteJSON(root, "manifest.json", manifest); err != nil {
				return result, err
			}
			continue
		}
		if !seen["manifest.json"] {
			cmd.Process.Kill()
			cmd.Wait()
			return result, errors.New("manifest must precede files")
		}
		if e = root.MkdirAll(path.Dir(header.Name), 0700); e != nil {
			return result, e
		}
		f, e := root.OpenFile(header.Name, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if e != nil {
			return result, e
		}
		_, e = io.CopyN(f, tr, header.Size)
		if e == nil {
			e = f.Sync()
		}
		f.Close()
		if e != nil {
			return result, e
		}
	}
	if e = cmd.Wait(); e != nil {
		return result, errors.New("restic restore failed")
	}
	if !seen["database.dump"] || manifest.Schema < 1 || manifest.Schema > db.SchemaVersion || manifest.VolumeID == "" {
		return result, errors.New("incompatible backup manifest")
	}
	_, hash, e := storage.Hash(root, "database.dump")
	if e != nil || hash != manifest.SQLChecksum {
		return result, errors.New("SQL dump checksum mismatch")
	}
	allFiles := append(append([]File{}, manifest.Files...), manifest.Sidecars...)
	for _, f := range allFiles {
		if !storage.ValidPath(f.Path) {
			return result, errors.New("invalid manifest path")
		}
		size, hash, e := storage.Hash(root, path.Join("published", f.Path))
		if e != nil || size != f.Size || hash != f.Checksum {
			return result, errors.New("restored file integrity failed")
		}
	}
	work, e := os.MkdirTemp(c.WorkDir, "begom-restore-")
	if e != nil {
		return result, e
	}
	defer os.RemoveAll(work)
	raw, e := config.Secret(c.DatabaseURLFile)
	if e != nil {
		return result, e
	}
	store, e := db.Open(ctx, raw)
	if e != nil {
		return result, e
	}
	defer store.Pool.Close()
	if _, e = store.Pool.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{newDatabase}.Sanitize()); e != nil {
		return result, errors.New("cannot create isolated restore database")
	}
	u, e := url.Parse(raw)
	if e != nil {
		return result, e
	}
	u.Path = "/" + newDatabase
	if !model.ValidID(manifest.OwnerRole) {
		return result, errors.New("invalid application owner role")
	}
	isolated, e := db.Open(ctx, u.String())
	if e != nil {
		return result, e
	}
	_, e = isolated.Pool.Exec(ctx, "GRANT USAGE,CREATE ON SCHEMA public TO "+pgx.Identifier{manifest.OwnerRole}.Sanitize())
	isolated.Pool.Close()
	if e != nil {
		return result, errors.New("application role must exist on the isolated restore cluster")
	}
	safe, env, e := credentials(u.String(), work)
	if e != nil {
		return result, e
	}
	restore := exec.CommandContext(ctx, "pg_restore", "--no-owner", "--no-privileges", "--exit-on-error", "--role="+manifest.OwnerRole, "--dbname="+safe, filepath.Join(target, "database.dump"))
	restore.Env = env
	if e = run(restore); e != nil {
		return result, e
	}
	if e = storage.WriteJSON(root, "restore-isolated.json", map[string]any{"database": newDatabase, "volume_id": manifest.VolumeID, "agents_must_remain_disconnected": true}); e != nil {
		return result, e
	}
	if e = os.WriteFile(filepath.Join(target, ".begom-volume"), []byte(manifest.VolumeID), 0444); e != nil {
		return result, e
	}
	result.BackupID = manifest.ID
	result.State = "completed"
	result.Database = newDatabase
	result.RestoreRoot = target
	result.Files = len(manifest.Files)
	result.FinishedAt = time.Now().UTC()
	result.RTOSeconds = result.FinishedAt.Sub(result.StartedAt).Seconds()
	result.RPOSeconds = result.StartedAt.Sub(manifest.CreatedAt).Seconds()
	return result, nil
}
func ValidateConfig(c Config) error {
	if c.Repository == "" || c.PasswordFile == "" || c.DatabaseURLFile == "" {
		return fmt.Errorf("incomplete backup configuration")
	}
	for _, p := range []string{c.SSHKeyFile, c.KnownHostsFile} {
		if strings.ContainsAny(p, " \t\n\r'\"`$") {
			return errors.New("invalid SSH credential path")
		}
	}
	return nil
}
