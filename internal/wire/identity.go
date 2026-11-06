package wire

import (
	"net/http"
	"time"
)

func MetricsIdentity(r *http.Request) bool {
	if r.TLS == nil || len(r.TLS.VerifiedChains) == 0 || len(r.TLS.PeerCertificates) == 0 {
		return false
	}
	leaf := r.TLS.PeerCertificates[0]
	if time.Now().Before(leaf.NotBefore) || !time.Now().Before(leaf.NotAfter) {
		return false
	}
	for _, u := range r.TLS.PeerCertificates[0].URIs {
		if u.String() == "spiffe://begom/service/prometheus" {
			return true
		}
	}
	return false
}
