package main

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"flag"
	"github.com/rychkin/Begom/internal/auth"
	"github.com/rychkin/Begom/internal/config"
	"github.com/rychkin/Begom/internal/coordinator"
	"github.com/rychkin/Begom/internal/db"
	"github.com/rychkin/Begom/internal/dispatch"
	"github.com/rychkin/Begom/internal/logging"
	"github.com/rychkin/Begom/internal/publication"
	"github.com/rychkin/Begom/internal/runtimeutil"
	"github.com/rychkin/Begom/internal/secrets"
	"github.com/rychkin/Begom/internal/sshcontrol"
	"github.com/rychkin/Begom/internal/storage"
	"github.com/rychkin/Begom/internal/wire"
	"net/http"
	"os"
	"time"
)

func main() {
	log := logging.New("coordinator")
	file := flag.String("config", "/config/coordinator.json", "configuration file")
	flag.Parse()
	var cfg config.Coordinator
	if e := config.Read(*file, &cfg); e != nil {
		log.Error("configuration rejected", "error_code", "invalid_config")
		os.Exit(1)
	}
	cfg.Defaults()
	if e := cfg.Validate(); e != nil {
		log.Error("configuration rejected", "error_code", "invalid_config")
		os.Exit(1)
	}
	ctx, cancel := runtimeutil.Context()
	defer cancel()
	bao, e := secrets.New(cfg.Bao)
	if e != nil {
		log.Error("secret service configuration rejected", "error_code", "invalid_secret_config")
		os.Exit(1)
	}
	runtimeSecrets, e := bao.Get(ctx, cfg.Bao.Prefix+"/runtime")
	if e != nil {
		log.Error("runtime secrets unavailable", "error_code", "secret_service_unavailable")
		os.Exit(1)
	}
	databaseURL := runtimeSecrets["database_url"]
	key, e := base64.StdEncoding.DecodeString(runtimeSecrets["metadata_key"])
	if e != nil || len(key) != 32 || databaseURL == "" {
		log.Error("runtime secret profile invalid", "error_code", "invalid_runtime_secrets")
		os.Exit(1)
	}
	store, e := db.Open(ctx, databaseURL)
	if e != nil {
		log.Error("database unavailable", "error_code", "database_unavailable")
		os.Exit(1)
	}
	defer store.Pool.Close()
	store.Lease = cfg.LeaseDuration()
	store.Heartbeat = time.Duration(cfg.HeartbeatMS) * time.Millisecond
	store.SourceID = cfg.Source.ID
	store.MetadataKey = key
	if e = store.Migrate(ctx); e != nil {
		log.Error("migration failed", "error_code", "migration_failed")
		os.Exit(1)
	}
	if e = store.BindProfile(ctx, cfg.Source.Mock); e != nil {
		log.Error("database donor profile rejected", "error_code", "source_profile_mismatch")
		os.Exit(1)
	}
	if e = store.SeedWorkers(ctx, cfg.Workers); e != nil {
		log.Error("worker profile rejected", "error_code", "invalid_workers")
		os.Exit(1)
	}
	a := &auth.Service{Store: store, Origin: cfg.PublicOrigin}
	if e = a.Bootstrap(ctx, cfg.BootstrapUser, runtimeSecrets["admin_password"]); e != nil {
		log.Error("user bootstrap failed", "error_code", "bootstrap_failed")
		os.Exit(1)
	}
	quota, e := store.QuotaState(ctx)
	if e != nil {
		log.Error("quota state unavailable", "error_code", "database_unavailable")
		os.Exit(1)
	}
	startupLimits := cfg.Source.Limits
	startupLimits.RPS *= quota.Factor
	if e = store.RecordQuotaHorizon(ctx, store.Pool, startupLimits); e != nil {
		log.Error("quota horizon unavailable", "error_code", "database_unavailable")
		return
	}
	quota, e = store.QuotaState(ctx)
	if e != nil {
		log.Error("quota state unavailable", "error_code", "database_unavailable")
		return
	}
	epoch, e := store.AcquireEpoch(ctx, max(dispatch.DrainInterval, quota.Horizon+dispatch.ConnectionLease+dispatch.PermitTTL+2*dispatch.ClockMargin))
	if e != nil {
		log.Error("dispatcher ownership unavailable", "error_code", "epoch_unavailable")
		os.Exit(1)
	}
	go func() { epoch.Run(ctx); cancel() }()
	controlStore, e := db.OpenLimit(ctx, databaseURL, 4)
	if e != nil {
		log.Error("quota pool unavailable", "error_code", "database_unavailable")
		cancel()
		return
	}
	defer controlStore.Pool.Close()
	controlStore.SourceID = store.SourceID
	controlStore.Lease = store.Lease
	controlStore.Heartbeat = store.Heartbeat
	d := dispatch.New(controlStore, epoch, cfg.Source.Limits)
	if e = d.Restore(ctx); e != nil {
		log.Error("quota recovery failed", "error_code", "database_unavailable")
		cancel()
		return
	}
	go d.Run(ctx)
	volume := &storage.Volume{Config: cfg.Storage}
	d.StorageCheck = func() error { _, e := volume.Check(); return e }
	p := &publication.Service{Store: store, Volume: volume, Log: log}
	if e = p.Reconcile(ctx); e != nil {
		log.Warn("initial storage reconciliation deferred", "error_code", "storage_unavailable")
	}
	go p.Run(ctx)
	s := &coordinator.Server{Store: store, Config: cfg, Auth: a, Dispatch: d, Publisher: p, Volume: volume, SSH: &sshcontrol.Service{Bao: bao, PackageDir: cfg.NodePackageDir}, Log: log, Secrets: bao}
	go s.Run(ctx)
	tc, e := wire.TLS(cfg.AgentTLS, false)
	if e != nil {
		log.Error("TLS configuration rejected", "error_code", "invalid_tls")
		os.Exit(1)
	}
	tc.ClientAuth = tls.RequireAndVerifyClientCert
	tc.GetCertificate = func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
		qctx, c := context.WithTimeout(context.Background(), 3*time.Second)
		defer c()
		cert, e := bao.Certificate(qctx, cfg.Bao.Prefix+"/tls")
		return &cert, e
	}
	human := &http.Server{Addr: cfg.Listen, Handler: s.Human(), ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 32768}
	agents := &http.Server{Addr: cfg.AgentListen, Handler: s.Agents(), TLSConfig: tc, ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 32768}
	go runtimeutil.Shutdown(ctx, human, agents)
	go func() {
		if e := agents.ListenAndServeTLS("", ""); e != nil && e != http.ErrServerClosed {
			log.Error("agent listener failed", "error_code", "listener_failed")
			cancel()
		}
	}()
	log.Info("coordinator starting", "operation", "start", "result", "ok", "epoch", epoch.ID, "quota_guard_until", epoch.GuardUntil)
	if e = human.ListenAndServe(); e != nil && e != http.ErrServerClosed {
		log.Error("web listener failed", "error_code", "listener_failed")
		cancel()
	}
}
