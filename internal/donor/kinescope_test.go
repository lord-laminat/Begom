package donor

import (
	"encoding/json"
	"github.com/rychkin/Begom/internal/model"
	"net/http"
	"testing"
	"time"
)

func TestPublicPage(t *testing.T) {
	p, e := ParsePage([]byte(`<html><script src="x"></script><script>// prefix
 const playerOptions = {"playlist":[{"id":"5b8f06df-ea4f-47fa-9c74-c4741a5ca325","title":"Видео \u003cscript\u003e","downloadLink":"/download-data"}],"unrelated":{"title":"}"}}; throw new Error("must not run");</script></html>`), "https://kinescope.io/123")
	if e != nil || p.Metadata.CanonicalID == "" || p.DownloadLink != "/download-data" || p.Metadata.Title != "Видео <script>" {
		t.Fatal(p, e)
	}
}
func TestMissingDownloadPermission(t *testing.T) {
	p, e := ParsePage([]byte(`<script>const playerOptions={"playlist":[{"id":"a","title":"Public playback","sources":{"hls":{"src":"/master.m3u8"}},"files":{"list":[{"mime":"application/pdf","url":"/attachment"}]}}]};</script>`), "https://kinescope.io/1")
	if e != nil || p.DownloadLink != "" {
		t.Fatal(p, e)
	}
}
func TestRejectUnknownAndExecutableSchemas(t *testing.T) {
	for _, s := range []string{`<script>const playerOptions = fetch("/secret");</script>`, `<script>const playerOptions = {playlist: [{id: "x"}]};</script>`, `<html>Missing</html>`, `<script>const text = 'const playerOptions={"playlist":[{"id":"fake"}]}';</script>`, `<script>/* const playerOptions={"playlist":[{"id":"fake"}]} */</script>`} {
		if _, e := ParsePage([]byte(s), "https://kinescope.io/1"); e == nil {
			t.Error("accepted unsupported schema", s)
		}
	}
}
func TestAssetsAndBest(t *testing.T) {
	body := []byte(`{"assets":[{"quality":"360p","size":10,"url":"/a"},{"quality":"720p","size":"20","url":"/b"},{"quality":"original","size":999,"url":"/c"}],"transcripts":[{"url":"/transcript"}]}`)
	assets, e := ParseAssets(body, "https://kinescope.io/downloads")
	if e != nil {
		t.Fatal(e)
	}
	best, ok := Best(assets, 30)
	if !ok || best.Quality != "720p" || best.URL != "https://kinescope.io/b" {
		t.Fatal(best)
	}
	if _, ok = Best(assets, 1); ok {
		t.Fatal("oversize asset selected")
	}
}
func TestAttachmentsAndStreamsNeverBecomeVideoFiles(t *testing.T) {
	assets := []Asset{{Quality: "original", URL: "https://example.com/presentation.pdf", Size: 1}, {Quality: "original", URL: "https://example.com/master.m3u8", Size: 1}, {Quality: "720p", URL: "https://example.com/video.mp4", Size: 2}}
	best, ok := Best(assets, 10)
	if !ok || best.Quality != "720p" {
		t.Fatal(best)
	}
	if _, e := ParseAssets([]byte(`{"new_schema":[]}`), "https://example.com"); e == nil {
		t.Fatal("unknown schema silently accepted")
	}
	counts := Resources([]byte(`{"sources":["/master.m3u8","/master.m3u8"],"files":[{"mime":"application/pdf","url":"/attachment"}],"transcripts":[{},{}]}`))
	if counts["streams"] != 1 || counts["pdf_attachments"] != 1 || counts["transcripts"] != 2 {
		t.Fatal(counts)
	}
}
func TestRetryPolicy(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	if RetryAfter("5", now) != 5*time.Second || RetryAfter(now.Add(10*time.Second).Format(http.TimeFormat), now) != 10*time.Second {
		t.Fatal("Retry-After")
	}
	p := model.RetryPolicy{BaseMS: 100, MaxMS: 1000}
	if RetryDelay(p, 20, 17*time.Millisecond) != 1017*time.Millisecond {
		t.Fatal("retry cap")
	}
	for status, want := range map[int]string{200: "found", 404: "not_found", 410: "not_found", 401: "forbidden", 403: "forbidden", 429: "rate_limited", 500: "retry_wait"} {
		got, _ := Classify(status)
		if got != want {
			t.Fatal(status, got)
		}
	}
}
func FuzzPlayerJSON(f *testing.F) {
	f.Add(`const playerOptions={"playlist":[]};`)
	f.Fuzz(func(t *testing.T, s string) {
		b, e := PlayerJSON([]byte(s))
		if e == nil && !json.Valid(b) {
			t.Fatal("accepted non-JSON")
		}
	})
}
