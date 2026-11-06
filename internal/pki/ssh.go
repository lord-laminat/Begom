package pki

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/pem"
	"golang.org/x/crypto/ssh"
)

func SSH() ([]byte, []byte, error) {
	pub, key, e := ed25519.GenerateKey(rand.Reader)
	if e != nil {
		return nil, nil, e
	}
	block, e := ssh.MarshalPrivateKey(key, "Begom")
	if e != nil {
		return nil, nil, e
	}
	public, e := ssh.NewPublicKey(pub)
	if e != nil {
		return nil, nil, e
	}
	return pem.EncodeToMemory(block), ssh.MarshalAuthorizedKey(public), nil
}

// OpenSSHPrivate preserves the key identity while converting legacy PKCS#8
// encoding, which OpenSSH does not accept for Ed25519 host keys.
func OpenSSHPrivate(data []byte) ([]byte, error) {
	block, _ := pem.Decode(data)
	if block != nil && block.Type == "OPENSSH PRIVATE KEY" {
		return data, nil
	}
	key, e := ssh.ParseRawPrivateKey(data)
	if e != nil {
		return nil, e
	}
	block, e = ssh.MarshalPrivateKey(key, "Begom")
	if e != nil {
		return nil, e
	}
	return pem.EncodeToMemory(block), nil
}
