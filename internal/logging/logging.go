// Package logging deliberately uses codes rather than raw dependency errors.
package logging

import (
	"log/slog"
	"os"
	"regexp"
	"strings"
	"time"
)

var urlQuery = regexp.MustCompile(`([a-zA-Z][a-zA-Z0-9+.-]*://[^\s?]+)\?[^\s]+`)
var urlCredentials = regexp.MustCompile(`([a-zA-Z][a-zA-Z0-9+.-]*://)[^/\s@]+@`)
var headerSecret = regexp.MustCompile(`(?i)(authorization|cookie|set-cookie)\s*:\s*[^\r\n]+`)
var bearer = regexp.MustCompile(`(?i)\bbearer\s+[^\s,;]+`)
var keyValue = regexp.MustCompile(`(?i)\b(password|token|cookie|secret|authorization|dsn)(["']?\s*[:=]\s*["']?|\s+)[^\s,;}"']+`)

func Redact(s string) string {
	s = urlQuery.ReplaceAllString(s, "$1?[redacted]")
	s = urlCredentials.ReplaceAllString(s, "${1}[redacted]@")
	s = headerSecret.ReplaceAllString(s, "$1: [redacted]")
	s = bearer.ReplaceAllString(s, "Bearer [redacted]")
	s = keyValue.ReplaceAllString(s, "$1=[redacted]")
	if strings.Contains(s, "PRIVATE KEY") {
		return "[redacted private key]"
	}
	return s
}
func New(component string) *slog.Logger {
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{ReplaceAttr: func(_ []string, a slog.Attr) slog.Attr {
		if a.Key == slog.TimeKey {
			return slog.String("timestamp", a.Value.Time().UTC().Format(time.RFC3339Nano))
		}
		if a.Key == slog.LevelKey {
			a.Key = "severity"
		}
		if a.Value.Kind() == slog.KindString {
			a.Value = slog.StringValue(Redact(a.Value.String()))
		}
		return a
	}})).With("component", component)
}
