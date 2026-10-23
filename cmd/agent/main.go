package main

import (
	"context"
	"crypto/tls"
	"flag"
	"github.com/rychkin/Begom/internal/agent"
	"github.com/rychkin/Begom/internal/config"
	"github.com/rychkin/Begom/internal/logging"
	"github.com/rychkin/Begom/internal/runtimeutil"
	"github.com/rychkin/Begom/internal/secrets"
	"github.com/rychkin/Begom/internal/wire"
	"net/http"
	"os"
	"time"
)

func main() {
	log := logging.New("agent")
	file := flag.String("config", "/config/agent.json", "configuration file")
	flag.Parse()
	var cfg config.Agent
	if config.Read(*file, &cfg) != nil {
		log.Error("configuration rejected", "error_code", "invalid_config")
		os.Exit(1)
	}
	if e := cfg.Validate(); e != nil {
		log.Error("configuration rejected", "error_code", "invalid_config")
		os.Exit(1)
	}
	ctx, cancel := runtimeutil.Context()
	defer cancel()
	a, e := agent.New(cfg, log)
	if e != nil {
		log.Error("agent identity unavailable", "error_code", "invalid_identity")
		os.Exit(1)
	}
	bao, e := secrets.New(cfg.Bao)
	if e != nil {
		log.Error("secret profile rejected", "error_code", "invalid_secret_config")
		os.Exit(1)
	}
	get := func() (*tls.Certificate, error) {
		qctx, c := context.WithTimeout(context.Background(), 3*time.Second)
		defer c()
		pair, e := bao.Certificate(qctx, cfg.Bao.Prefix+"/tls")
		return &pair, e
	}
	transport := a.Client.HTTP.Transport.(*http.Transport)
	transport.TLSClientConfig.Certificates = nil
	transport.TLSClientConfig.GetClientCertificate = func(*tls.CertificateRequestInfo) (*tls.Certificate, error) { return get() }
	tc, e := wire.TLS(cfg.TLS, false)
	if e != nil {
		log.Error("TLS profile invalid", "error_code", "invalid_tls")
		os.Exit(1)
	}
	tc.ClientAuth = tls.RequireAndVerifyClientCert
	tc.GetCertificate = func(*tls.ClientHelloInfo) (*tls.Certificate, error) { return get() }
	if cfg.MetricsListen == "" {
		cfg.MetricsListen = ":9101"
	}
	m := http.NewServeMux()
	m.HandleFunc("GET /metrics", a.Metrics)
	m.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("ok")) })
	server := &http.Server{Addr: cfg.MetricsListen, Handler: m, TLSConfig: tc, ReadHeaderTimeout: 5 * time.Second}
	go runtimeutil.Shutdown(ctx, server)
	go func() {
		if e := server.ListenAndServeTLS("", ""); e != nil && e != http.ErrServerClosed {
			cancel()
		}
	}()
	if e = a.Run(ctx); e != nil && ctx.Err() == nil {
		log.Error("agent stopped", "error_code", "agent_failed")
		os.Exit(1)
	}
}
