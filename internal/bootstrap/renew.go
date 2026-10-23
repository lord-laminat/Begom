package bootstrap

import (
	"crypto/x509"
	"encoding/pem"
	"errors"
	"github.com/rychkin/Begom/internal/model"
	"github.com/rychkin/Begom/internal/pki"
	"os"
	"path/filepath"
	"time"
)

// RenewLeaf preserves the pinned CA, SANs, owner and filesystem permissions.
// Runtime agent identities in KV are rotated through the coordinator instead.
func RenewLeaf(dir, name string) error {
	if !model.ValidID(name) {
		return errors.New("invalid certificate name")
	}
	cert, e := os.ReadFile(filepath.Join(dir, "secrets/ca.crt"))
	if e != nil {
		return e
	}
	key, e := os.ReadFile(filepath.Join(dir, "bootstrap/ca.key"))
	if e != nil {
		return e
	}
	ca, e := pki.Parse(cert, key)
	if e != nil {
		return e
	}
	leafBytes, e := os.ReadFile(filepath.Join(dir, "secrets", name+".crt"))
	if e != nil {
		return e
	}
	block, _ := pem.Decode(leafBytes)
	if block == nil {
		return errors.New("invalid leaf")
	}
	leaf, e := x509.ParseCertificate(block.Bytes)
	if e != nil {
		return e
	}
	hosts := append([]string{}, leaf.DNSNames...)
	for _, ip := range leaf.IPAddresses {
		hosts = append(hosts, ip.String())
	}
	identity := ""
	if len(leaf.URIs) > 0 {
		identity = leaf.URIs[0].String()
	}
	client := false
	for _, usage := range leaf.ExtKeyUsage {
		if usage == x509.ExtKeyUsageClientAuth {
			client = true
		}
	}
	lifetime := 48 * time.Hour
	if !client || name == "metrics" {
		lifetime = 90 * 24 * time.Hour
	}
	next, nextKey, e := ca.IssueFor(name, identity, hosts, client, lifetime)
	if e != nil {
		return e
	}
	for suffix, data := range map[string][]byte{"crt": next, "key": nextKey} {
		path := filepath.Join(dir, "secrets", name+"."+suffix)
		if e = os.WriteFile(path, data, 0600); e != nil {
			return e
		}
	}
	return nil
}
