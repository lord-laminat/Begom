package secrets

import (
	"context"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"github.com/rychkin/Begom/internal/pki"
	"time"
)

func (b *Bao) RenewIdentity(ctx context.Context, ref string, force bool) (bool, error) {
	values, e := b.Get(ctx, ref)
	if e != nil {
		return false, e
	}
	block, _ := pem.Decode([]byte(values["cert"]))
	if block == nil {
		return false, errors.New("invalid leaf certificate")
	}
	leaf, e := x509.ParseCertificate(block.Bytes)
	if e != nil {
		return false, e
	}
	if !force && time.Until(leaf.NotAfter) > 24*time.Hour {
		return false, nil
	}
	root, e := b.Get(ctx, b.Config.Prefix+"/pki")
	if e != nil {
		return false, e
	}
	ca, e := pki.Parse([]byte(root["cert"]), []byte(root["key"]))
	if e != nil {
		return false, e
	}
	hosts := append([]string{}, leaf.DNSNames...)
	for _, ip := range leaf.IPAddresses {
		hosts = append(hosts, ip.String())
	}
	identity := ""
	if len(leaf.URIs) > 0 {
		identity = leaf.URIs[0].String()
	}
	cert, key, e := ca.Issue(leaf.Subject.CommonName, identity, hosts, true)
	if e != nil {
		return false, e
	}
	if e = b.Put(ctx, ref, map[string]string{"cert": string(cert), "key": string(key)}); e != nil {
		return false, e
	}
	return true, nil
}
