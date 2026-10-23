package runtimeutil

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func Context() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
}
func Shutdown(ctx context.Context, servers ...*http.Server) {
	<-ctx.Done()
	qctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, s := range servers {
		s.Shutdown(qctx)
	}
}
