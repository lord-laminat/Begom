// Package donor interprets public player metadata without executing page scripts.
package donor

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/rychkin/Begom/internal/model"
	"golang.org/x/net/html"
	"io"
	"mime"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"time"
	"unicode"
)

type Page struct {
	Metadata     model.Metadata
	DownloadLink string
}
type player struct {
	Playlist []struct {
		ID           string `json:"id"`
		Title        string `json:"title"`
		DownloadLink string `json:"downloadLink"`
	} `json:"playlist"`
}

func ParsePage(body []byte, pageURL string) (Page, error) {
	z := html.NewTokenizer(bytes.NewReader(body))
	inScript := false
	for {
		switch z.Next() {
		case html.ErrorToken:
			if z.Err() != io.EOF {
				return Page{}, z.Err()
			}
			return Page{}, errors.New("unsupported_page_schema")
		case html.StartTagToken:
			t := z.Token()
			inScript = t.Data == "script"
		case html.EndTagToken:
			if z.Token().Data == "script" {
				inScript = false
			}
		case html.TextToken:
			if !inScript {
				continue
			}
			literal, e := PlayerJSON(z.Text())
			if e != nil {
				continue
			}
			var p player
			if e = json.Unmarshal(literal, &p); e != nil || len(p.Playlist) != 1 || p.Playlist[0].ID == "" || len(p.Playlist[0].Title) > 4096 {
				continue
			}
			item := p.Playlist[0]
			if len(item.ID) > 100 {
				return Page{}, errors.New("unsupported_page_schema")
			}
			return Page{Metadata: model.Metadata{CanonicalID: item.ID, Title: item.Title, PageURL: pageURL, FoundAt: time.Now().UTC(), Resources: Resources(literal)}, DownloadLink: item.DownloadLink}, nil
		}
	}
}

// PlayerJSON recognizes a declaration followed by a JSON object, skipping JS
// strings and comments. Function calls, templates and expression evaluation fail.
func PlayerJSON(script []byte) ([]byte, error) {
	previous := ""
	for i := 0; i < len(script); {
		c := script[i]
		if c == '"' || c == '\'' || c == '`' {
			quote := c
			i++
			for i < len(script) {
				if script[i] == '\\' {
					i += 2
					continue
				}
				if script[i] == quote {
					i++
					break
				}
				i++
			}
			previous = ""
			continue
		}
		if c == '/' && i+1 < len(script) && script[i+1] == '/' {
			for i < len(script) && script[i] != '\n' {
				i++
			}
			continue
		}
		if c == '/' && i+1 < len(script) && script[i+1] == '*' {
			i += 2
			for i+1 < len(script) && !(script[i] == '*' && script[i+1] == '/') {
				i++
			}
			i += 2
			continue
		}
		if c == '_' || c == '$' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' {
			start := i
			i++
			for i < len(script) && (script[i] == '_' || script[i] == '$' || script[i] >= 'a' && script[i] <= 'z' || script[i] >= 'A' && script[i] <= 'Z' || script[i] >= '0' && script[i] <= '9') {
				i++
			}
			word := string(script[start:i])
			if word == "playerOptions" && (previous == "const" || previous == "var" || previous == "let") {
				j := i
				for j < len(script) && unicode.IsSpace(rune(script[j])) {
					j++
				}
				if j < len(script) && script[j] == '=' {
					j++
					for j < len(script) && unicode.IsSpace(rune(script[j])) {
						j++
					}
					if j < len(script) && script[j] == '{' {
						d := json.NewDecoder(bytes.NewReader(script[j:]))
						var raw json.RawMessage
						if e := d.Decode(&raw); e == nil {
							return raw, nil
						}
					}
				}
			}
			previous = word
			continue
		}
		if !unicode.IsSpace(rune(c)) {
			previous = ""
		}
		i++
	}
	return nil, errors.New("unsupported_player_initializer")
}

type Asset struct {
	Quality  string       `json:"quality"`
	Size     model.Number `json:"size"`
	URL      string       `json:"url"`
	Filename string       `json:"download_filename"`
}

func ParseAssets(body []byte, base string) ([]Asset, error) {
	var response struct {
		Assets json.RawMessage `json:"assets"`
	}
	if e := json.Unmarshal(body, &response); e != nil {
		return nil, errors.New("unsupported_download_schema")
	}
	if len(response.Assets) == 0 || string(response.Assets) == "null" {
		return nil, errors.New("unsupported_download_schema")
	}
	var assets []Asset
	if e := json.Unmarshal(response.Assets, &assets); e != nil {
		return nil, errors.New("unsupported_download_schema")
	}
	if len(assets) > 100 {
		return nil, errors.New("too_many_assets")
	}
	out := []Asset{}
	u, e := url.Parse(base)
	if e != nil {
		return nil, e
	}
	for _, a := range assets {
		link, e := url.Parse(a.URL)
		if e != nil || link.String() == "" {
			continue
		}
		a.URL = u.ResolveReference(link).String()
		out = append(out, a)
	}
	return out, nil
}
func Best(assets []Asset, maxBytes uint64) (Asset, bool) {
	var best Asset
	rank := -1
	for _, a := range assets {
		if !videoAsset(a) {
			continue
		}
		if uint64(a.Size) > maxBytes {
			continue
		}
		n := QualityRank(a.Quality)
		if n > rank {
			best = a
			rank = n
		}
	}
	return best, rank >= 0
}
func videoAsset(a Asset) bool {
	ext := strings.ToLower(path.Ext(a.Filename))
	if ext == "" {
		u, e := url.Parse(a.URL)
		if e != nil {
			return false
		}
		ext = strings.ToLower(path.Ext(u.Path))
	}
	switch ext {
	case ".mp4", ".m4v", ".mov", ".webm", ".mkv", ".avi":
		return true
	case "":
		return QualityRank(a.Quality) > 0
	default:
		return false
	}
}

// Only counts are exposed; attachment URLs may contain expiring credentials.
func Resources(body []byte) map[string]int {
	var value any
	if json.Unmarshal(body, &value) != nil {
		return nil
	}
	out := map[string]int{}
	seen := map[string]bool{}
	var walk func(any, string)
	walk = func(v any, key string) {
		switch x := v.(type) {
		case map[string]any:
			if x["mime"] == "application/pdf" || x["mime_type"] == "application/pdf" {
				out["pdf_attachments"]++
			}
			for k, v := range x {
				walk(v, k)
			}
		case []any:
			if key == "transcripts" {
				out["transcripts"] += len(x)
			}
			for _, v := range x {
				walk(v, key)
			}
		case string:
			u, e := url.Parse(x)
			if e != nil || seen[x] {
				return
			}
			switch strings.ToLower(path.Ext(u.Path)) {
			case ".m3u8", ".mpd":
				out["streams"]++
				seen[x] = true
			case ".pdf":
				if key != "mime" && key != "mime_type" {
					out["pdf_links"]++
					seen[x] = true
				}
			}
		}
	}
	walk(value, "")
	return out
}
func QualityRank(s string) int {
	if strings.EqualFold(s, "original") {
		return 100000
	}
	s = strings.TrimSuffix(strings.ToLower(s), "p")
	n, _ := strconv.Atoi(s)
	return n
}
func MIME(a Asset) string {
	ext := strings.ToLower(path.Ext(a.Filename))
	if ext == "" {
		u, _ := url.Parse(a.URL)
		if u != nil {
			ext = strings.ToLower(path.Ext(u.Path))
		}
	}
	m := mime.TypeByExtension(ext)
	if strings.HasPrefix(m, "video/") {
		return m
	}
	return "video/mp4"
}
func Classify(status int) (string, bool) {
	switch {
	case status == 404 || status == 410:
		return "not_found", false
	case status == 401 || status == 403:
		return "forbidden", false
	case status == 429:
		return "rate_limited", true
	case status >= 500 || status == 408:
		return "retry_wait", true
	case status >= 200 && status < 300:
		return "found", false
	default:
		return "failed", false
	}
}
func RetryAfter(h string, now time.Time) time.Duration {
	if n, e := strconv.ParseUint(strings.TrimSpace(h), 10, 32); e == nil {
		return time.Duration(n) * time.Second
	}
	if t, e := http.ParseTime(h); e == nil && t.After(now) {
		return t.Sub(now)
	}
	return 0
}
func RetryDelay(p model.RetryPolicy, attempt int, jitter time.Duration) time.Duration {
	if attempt > 20 {
		attempt = 20
	}
	base := int64(p.BaseMS)
	max := int64(p.MaxMS)
	delay := base * (1 << attempt)
	if delay > max {
		delay = max
	}
	return time.Duration(delay)*time.Millisecond + jitter
}
func PageURL(template string, id model.Number) (string, error) {
	if strings.Count(template, "{id}") != 1 {
		return "", fmt.Errorf("source template must contain one {id}")
	}
	return strings.Replace(template, "{id}", id.String(), 1), nil
}
