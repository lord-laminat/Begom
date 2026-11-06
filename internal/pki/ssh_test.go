package pki

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"testing"

	"golang.org/x/crypto/ssh"
)

func TestOpenSSHHostKeyAndLegacyConversionPreserveIdentity(t *testing.T) {
	private, public, e := SSH()
	if e != nil {
		t.Fatal(e)
	}
	block, _ := pem.Decode(private)
	if block == nil || block.Type != "OPENSSH PRIVATE KEY" {
		t.Fatal("OpenSSH host key required")
	}
	signer, e := ssh.ParsePrivateKey(private)
	if e != nil || string(ssh.MarshalAuthorizedKey(signer.PublicKey())) != string(public) {
		t.Fatal("host key identity mismatch", e)
	}
	_, key, e := ed25519.GenerateKey(rand.Reader)
	if e != nil {
		t.Fatal(e)
	}
	old, e := x509.MarshalPKCS8PrivateKey(key)
	if e != nil {
		t.Fatal(e)
	}
	converted, e := OpenSSHPrivate(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: old}))
	if e != nil {
		t.Fatal(e)
	}
	signer, e = ssh.ParsePrivateKey(converted)
	if e != nil {
		t.Fatal(e)
	}
	want, _ := ssh.NewPublicKey(key.Public())
	if string(signer.PublicKey().Marshal()) != string(want.Marshal()) {
		t.Fatal("conversion rotated identity")
	}
	again, e := OpenSSHPrivate(converted)
	if e != nil || string(again) != string(converted) {
		t.Fatal("conversion is not idempotent", e)
	}
}
