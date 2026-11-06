package target

import (
	"context"
	"github.com/rychkin/Begom/internal/config"
	"net/netip"
	"testing"
)

func TestHTTPSDialReachesAddressValidation(t *testing.T) {
	p := Policy{Source: config.Source{AllowedHosts: []string{"127.0.0.1"}}}
	_, e := p.DialContext(context.Background(), "tcp", "127.0.0.1:443")
	if e == nil || e.Error() != "donor address not allowed" {
		t.Fatal("HTTPS was rejected before the SSRF address check", e)
	}
}

func TestURLAndAddressPolicy(t *testing.T) {
	p := Policy{Source: config.Source{AllowedHosts: []string{"kinescope.io", "*.kinescopecdn.net"}}}
	for _, s := range []string{"https://kinescope.io/1", "https://edge-ams-1.kinescopecdn.net/video"} {
		if _, e := p.URL(s); e != nil {
			t.Fatal(s, e)
		}
	}
	for _, s := range []string{"https://kinescope.io.evil/1", "https://kinescopecdn.net.evil/1", "file:///etc/passwd", "http://user:secret@kinescope.io/1", "http://kinescope.io:8200/1", "http://127.0.0.1/", "http://kinescope.io/#fragment"} {
		if _, e := p.URL(s); e == nil {
			t.Error("accepted", s)
		}
	}
	for _, s := range []string{"127.0.0.1", "::1", "::ffff:127.0.0.1", "169.254.169.254", "10.0.0.1", "172.16.0.1", "192.168.0.1", "fc00::1", "100.64.0.1", "0.0.0.0", "224.0.0.1", "2001:db8::1"} {
		if p.IP(netip.MustParseAddr(s)) {
			t.Error("accepted private/service address", s)
		}
	}
	if !p.IP(netip.MustParseAddr("8.8.8.8")) {
		t.Fatal("rejected public address")
	}
	p.Source.Mock = true
	p.Source.MockCIDRs = []string{"127.0.0.0/8"}
	if !p.IP(netip.MustParseAddr("127.0.0.1")) || p.IP(netip.MustParseAddr("10.0.0.1")) {
		t.Fatal("mock allowlist")
	}
}
