package pki

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"net/url"
	"time"
)

type Authority struct {
	Certificate *x509.Certificate
	Key         crypto.Signer
	CertPEM     []byte
	KeyPEM      []byte
}

func serial() (*big.Int, error) { return rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128)) }
func New() (Authority, error) {
	key, e := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if e != nil {
		return Authority{}, e
	}
	n, e := serial()
	if e != nil {
		return Authority{}, e
	}
	template := &x509.Certificate{SerialNumber: n, Subject: pkix.Name{CommonName: "Begom local authority"}, NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().AddDate(1, 0, 0), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageCRLSign}
	der, e := x509.CreateCertificate(rand.Reader, template, template, key.Public(), key)
	if e != nil {
		return Authority{}, e
	}
	keyDER, e := x509.MarshalPKCS8PrivateKey(key)
	if e != nil {
		return Authority{}, e
	}
	cert, e := x509.ParseCertificate(der)
	return Authority{Certificate: cert, Key: key, CertPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), KeyPEM: pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})}, e
}
func Parse(cert, key []byte) (Authority, error) {
	cb, _ := pem.Decode(cert)
	kb, _ := pem.Decode(key)
	if cb == nil || kb == nil {
		return Authority{}, errors.New("invalid authority")
	}
	c, e := x509.ParseCertificate(cb.Bytes)
	if e != nil {
		return Authority{}, e
	}
	k, e := x509.ParsePKCS8PrivateKey(kb.Bytes)
	if e != nil {
		return Authority{}, e
	}
	private, ok := k.(crypto.Signer)
	if !ok {
		return Authority{}, errors.New("invalid authority key")
	}
	return Authority{Certificate: c, Key: private, CertPEM: cert, KeyPEM: key}, nil
}
func (a Authority) Issue(name, identity string, hosts []string, client bool) ([]byte, []byte, error) {
	return a.IssueFor(name, identity, hosts, client, 48*time.Hour)
}
func (a Authority) IssueFor(name, identity string, hosts []string, client bool, lifetime time.Duration) ([]byte, []byte, error) {
	key, e := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if e != nil {
		return nil, nil, e
	}
	n, e := serial()
	if e != nil {
		return nil, nil, e
	}
	if lifetime < time.Hour || lifetime > 90*24*time.Hour {
		return nil, nil, errors.New("invalid certificate lifetime")
	}
	expires := time.Now().Add(lifetime)
	if expires.After(a.Certificate.NotAfter) {
		expires = a.Certificate.NotAfter
	}
	t := &x509.Certificate{SerialNumber: n, Subject: pkix.Name{CommonName: name}, NotBefore: time.Now().Add(-time.Minute), NotAfter: expires, KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	if client {
		t.ExtKeyUsage = append(t.ExtKeyUsage, x509.ExtKeyUsageClientAuth)
	}
	if identity != "" {
		u, e := url.Parse(identity)
		if e != nil {
			return nil, nil, e
		}
		t.URIs = []*url.URL{u}
	}
	for _, h := range hosts {
		if ip := net.ParseIP(h); ip != nil {
			t.IPAddresses = append(t.IPAddresses, ip)
		} else {
			t.DNSNames = append(t.DNSNames, h)
		}
	}
	der, e := x509.CreateCertificate(rand.Reader, t, a.Certificate, key.Public(), a.Key)
	if e != nil {
		return nil, nil, e
	}
	keyDER, e := x509.MarshalPKCS8PrivateKey(key)
	if e != nil {
		return nil, nil, e
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}), nil
}
