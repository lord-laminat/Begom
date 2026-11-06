package bootstrap

import (
	"crypto/x509"
	"encoding/pem"
	"github.com/rychkin/Begom/internal/config"
	"os"
	"path/filepath"
	"testing"
)

func TestLinuxProfileHasDedicatedSSHAndValidWorkerDNS(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "bundle")
	p := LinuxProfile{PublicHost: "begom.example.internal", Coordinator: "control.example.internal", Postgres: "control.example.internal", OpenBao: "control.example.internal", Workers: []string{"worker1.example.internal", "worker2.example.internal", "worker3.example.internal"}}
	if e := InitLinux(dir, p); e != nil {
		t.Fatal(e)
	}
	var c config.Coordinator
	if e := config.Read(filepath.Join(dir, "config/coordinator.json"), &c); e != nil {
		t.Fatal(e)
	}
	for _, w := range c.Workers {
		if w.SSHPort != 2222 || w.StoragePath != "/srv/begom" {
			t.Fatal("management profile conflicts with provisioning SSH or NAS", w.ID)
		}
		data, e := os.ReadFile(filepath.Join(dir, "secrets", w.ID+".crt"))
		if e != nil {
			t.Fatal(e)
		}
		block, _ := pem.Decode(data)
		cert, e := x509.ParseCertificate(block.Bytes)
		if e != nil || cert.VerifyHostname(w.Host) != nil {
			t.Fatal("worker metrics certificate does not cover inventory DNS", w.ID, e)
		}
	}
	if e := InitLinux(dir, p); e == nil {
		t.Fatal("existing private bundle was overwritten")
	}
}
