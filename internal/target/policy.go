package target

import (
	"context"
	"errors"
	"github.com/rychkin/Begom/internal/config"
	"net"
	"net/netip"
	"net/url"
	"strings"
)

type Policy struct {
	Source   config.Source
	Resolver *net.Resolver
}

func (p Policy) URL(raw string) (*url.URL, error) {
	u, e := url.Parse(raw)
	if e != nil || len(raw) > 16384 || u.User != nil || u.Hostname() == "" || u.Opaque != "" || u.Fragment != "" || (u.Scheme != "http" && u.Scheme != "https") {
		return nil, errors.New("invalid donor URL")
	}
	host := strings.ToLower(u.Hostname())
	if !p.Source.Mock && u.Scheme != "https" {
		return nil, errors.New("donor HTTPS required")
	}
	allowed := false
	for _, h := range p.Source.AllowedHosts {
		h = strings.ToLower(h)
		if strings.HasPrefix(h, "*.") {
			suffix := h[1:]
			if strings.HasSuffix(host, suffix) && len(host) > len(suffix) {
				allowed = true
			}
		} else if host == h {
			allowed = true
		}
	}
	if !allowed {
		return nil, errors.New("donor host not allowed")
	}
	port := u.Port()
	if !p.Source.Mock && port != "" && port != "80" && port != "443" {
		return nil, errors.New("donor port not allowed")
	}
	return u, nil
}
func (p Policy) IP(a netip.Addr) bool {
	a = a.Unmap()
	public := a.IsGlobalUnicast() && !a.IsPrivate() && !a.IsLoopback() && !a.IsLinkLocalUnicast() && !a.IsLinkLocalMulticast() && !a.IsMulticast()
	if public {
		for _, r := range []string{"100.64.0.0/10", "192.0.0.0/24", "192.0.2.0/24", "198.18.0.0/15", "198.51.100.0/24", "203.0.113.0/24", "240.0.0.0/4", "2001:db8::/32"} {
			if netip.MustParsePrefix(r).Contains(a) {
				public = false
				break
			}
		}
	}
	if public {
		return true
	}
	if p.Source.Mock {
		for _, s := range p.Source.MockCIDRs {
			cidr, e := netip.ParsePrefix(s)
			if e == nil && cidr.Contains(a) {
				return true
			}
		}
	}
	return false
}
func (p Policy) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	host, port, e := net.SplitHostPort(address)
	if e != nil {
		return nil, e
	}
	// The transport calls this hook for HTTPS too. Revalidate the host with
	// HTTPS; using http here would reject every real donor before DNS lookup.
	if _, e = p.URL("https://" + net.JoinHostPort(host, port)); e != nil {
		return nil, e
	}
	resolver := p.Resolver
	if resolver == nil {
		resolver = net.DefaultResolver
	}
	ips, e := resolver.LookupNetIP(ctx, "ip", host)
	if e != nil || len(ips) == 0 {
		return nil, errors.New("donor DNS resolution failed")
	}
	for _, a := range ips {
		if !p.IP(a) {
			return nil, errors.New("donor address not allowed")
		}
	}
	var d net.Dialer
	var last error
	for _, a := range ips {
		c, e := d.DialContext(ctx, network, net.JoinHostPort(a.String(), port))
		if e == nil {
			return c, nil
		}
		last = e
	}
	return nil, last
}
