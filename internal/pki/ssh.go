package pki

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"golang.org/x/crypto/ssh"
)

func SSH() ([]byte, []byte, error) {
	pub, key, e := ed25519.GenerateKey(rand.Reader)
	if e != nil {
		return nil, nil, e
	}
	der, e := x509.MarshalPKCS8PrivateKey(key)
	if e != nil {
		return nil, nil, e
	}
	public, e := ssh.NewPublicKey(pub)
	if e != nil {
		return nil, nil, e
	}
	return pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}), ssh.MarshalAuthorizedKey(public), nil
}
