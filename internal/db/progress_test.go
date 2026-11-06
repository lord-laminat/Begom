package db

import "testing"

func TestPublicSourceURL(t *testing.T) {
	for _, tt := range []struct{ in, want string }{
		{"https://user:private@example.com/video?token=private#secret", "https://example.com/video"},
		{"http://mockdonor:8090/123", "http://mockdonor:8090/123"},
		{"javascript:alert(1)", ""},
		{"data:text/html,<script>alert(1)</script>", ""},
		{"//example.com/video", ""},
		{"file:///etc/passwd", ""},
		{"/relative", ""},
	} {
		if got := SafeURL(tt.in); got != tt.want {
			t.Errorf("SafeURL(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}
