package main

import (
	"flag"
	"github.com/rychkin/Begom/internal/config"
	"github.com/rychkin/Begom/internal/logging"
	"github.com/rychkin/Begom/internal/mockdonor"
	"github.com/rychkin/Begom/internal/runtimeutil"
	"net/http"
	"time"
)

func main() {
	listen := flag.String("listen", ":8090", "listen address")
	secret := flag.String("control-token-file", "/run/secrets/mock-token", "control token file")
	flag.Parse()
	token, e := config.Secret(*secret)
	log := logging.New("mockdonor")
	if e != nil {
		log.Error("control identity missing", "error_code", "invalid_config")
		return
	}
	ctx, cancel := runtimeutil.Context()
	defer cancel()
	s := &http.Server{Addr: *listen, Handler: mockdonor.New(token).Handler(), ReadHeaderTimeout: 5 * time.Second}
	go runtimeutil.Shutdown(ctx, s)
	log.Info("mock donor started", "operation", "start", "result", "ok")
	if e = s.ListenAndServe(); e != nil && e != http.ErrServerClosed {
		log.Error("listener failed", "error_code", "listener_failed")
	}
}
