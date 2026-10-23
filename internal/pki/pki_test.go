package pki

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"testing"
)

func TestBrowserAndSCRAMCompatibleCertificates(t *testing.T) {
	ca, e := New()
	if e != nil {
		t.Fatal(e)
	}
	if ca.Certificate.SignatureAlgorithm != x509.ECDSAWithSHA256 {
		t.Fatal("CA signature needs a channel-binding hash")
	}
	cert, key, e := ca.Issue("web", "", []string{"localhost", "127.0.0.1"}, false)
	if e != nil {
		t.Fatal(e)
	}
	block, _ := pem.Decode(cert)
	leaf, e := x509.ParseCertificate(block.Bytes)
	if e != nil {
		t.Fatal(e)
	}
	if leaf.PublicKeyAlgorithm != x509.ECDSA {
		t.Fatal("browser TLS signature compatibility")
	}
	if _, e = tls.X509KeyPair(cert, key); e != nil {
		t.Fatal(e)
	}
	pool := x509.NewCertPool()
	pool.AddCert(ca.Certificate)
	if _, e = leaf.Verify(x509.VerifyOptions{DNSName: "localhost", Roots: pool}); e != nil {
		t.Fatal(e)
	}
	parsed, e := Parse(ca.CertPEM, ca.KeyPEM)
	if e != nil || parsed.Certificate.SerialNumber.Cmp(ca.Certificate.SerialNumber) != 0 {
		t.Fatal("authority reload", e)
	}
}
