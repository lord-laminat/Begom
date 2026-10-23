package logging

import (
	"strings"
	"testing"
)

func TestNoSecretsInLogs(t *testing.T) {
	for _, s := range []string{"https://cdn/video?sign=secret123&expires=9", "Authorization: Bearer secret123", "password=secret123", "-----BEGIN PRIVATE KEY-----\nsecret123", "postgres://user:secret123@host/db", "Cookie: one=abc; two=secret123", `{"password":"secret123"}`} {
		if strings.Contains(Redact(s), "secret123") {
			t.Fatal("secret survives", Redact(s))
		}
	}
}
