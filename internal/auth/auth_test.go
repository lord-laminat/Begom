package auth

import (
	"strings"
	"testing"
)

func TestPasswordHash(t *testing.T) {
	hash, e := Hash("a-long-test-password")
	if e != nil || !strings.HasPrefix(hash, "$argon2id$") || !Verify(hash, "a-long-test-password") || Verify(hash, "wrong") {
		t.Fatal("password hash", e)
	}
	for _, s := range []string{"", "$argon2id$v=19$m=999999999,t=2,p=2$AAAA$BBBB", hash + "$extra"} {
		if Verify(s, "anything") {
			t.Fatal("malformed hash accepted")
		}
	}
	if _, e = Hash("short"); e == nil {
		t.Fatal("short password accepted")
	}
}
