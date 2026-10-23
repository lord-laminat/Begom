package bootstrap

import (
	"context"
	"errors"
	"github.com/jackc/pgx/v5"
	"github.com/rychkin/Begom/internal/config"
)

// InitDatabase runs inside the PostgreSQL entrypoint's temporary Unix server.
func InitDatabase(ctx context.Context) error {
	c, e := pgx.Connect(ctx, "host=/var/run/postgresql user=begom_admin dbname=begom sslmode=disable")
	if e != nil {
		return errors.New("database bootstrap unavailable")
	}
	defer c.Close(ctx)
	tx, e := c.Begin(ctx)
	if e != nil {
		return e
	}
	defer tx.Rollback(ctx)
	for role, file := range map[string]string{"begom": "app-db-password", "begom_backup": "backup-db-password", "begom_metrics": "metrics-db-password"} {
		password, e := config.Secret("/run/secrets/" + file)
		if e != nil {
			return e
		}
		var exists bool
		if e = tx.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname=$1)", role).Scan(&exists); e != nil {
			return e
		}
		if !exists {
			if _, e = tx.Exec(ctx, "CREATE ROLE "+pgx.Identifier{role}.Sanitize()+" LOGIN"); e != nil {
				return errors.New("role bootstrap failed")
			}
		}
		var literal string
		if e = tx.QueryRow(ctx, "SELECT quote_literal($1)", password).Scan(&literal); e != nil {
			return e
		}
		if _, e = tx.Exec(ctx, "ALTER ROLE "+pgx.Identifier{role}.Sanitize()+" PASSWORD "+literal+" NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION"); e != nil {
			return errors.New("role credentials rejected")
		}
	}
	for _, statement := range []string{"ALTER DATABASE begom OWNER TO begom", "GRANT ALL ON SCHEMA public TO begom", "GRANT pg_read_all_data TO begom_backup", "GRANT pg_monitor TO begom_metrics"} {
		if _, e = tx.Exec(ctx, statement); e != nil {
			return errors.New("database grants failed")
		}
	}
	return tx.Commit(ctx)
}
