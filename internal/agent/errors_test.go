package agent

import (
	"context"
	"crypto/x509"
	"errors"
	"io"
	"net/url"
	"testing"
)

func TestDonorTransportErrorsDoNotExposeSignedLinks(t *testing.T) {
	for _, tt := range []struct {
		err  error
		code string
	}{{io.EOF, "donor_connection_closed"}, {context.Canceled, "donor_request_cancelled"}, {context.DeadlineExceeded, "donor_timeout"}, {x509.UnknownAuthorityError{}, "donor_certificate_invalid"}, {errors.New("donor address not allowed"), "donor_address_not_allowed"}, {errors.New("donor DNS resolution failed"), "donor_dns_failed"}} {
		err := &url.Error{Op: "Get", URL: "https://example.com/private-path?token=must-not-leak", Err: tt.err}
		if got := transportErrorCode(err); got != tt.code {
			t.Errorf("got %q, want %q", got, tt.code)
		}
		if got := downloadErrorCode(errors.New(tt.code)); got != tt.code {
			t.Errorf("lost transport diagnostic %q", got)
		}
	}
}
