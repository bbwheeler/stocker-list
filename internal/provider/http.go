package provider

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strings"
	"unicode/utf8"
)

const (
	// maxBodySize caps how much of a single response body we will read.
	// A bounded read prevents a huge HTML error page from exhausting memory.
	maxBodySize = 20 << 20
	// bodyPreviewLimit is the maximum number of body bytes included in errors.
	bodyPreviewLimit = 512
	// maxRedirects bounds a redirect chain: the initial request plus at most
	// maxRedirects-1 followed redirects before the chain is refused.
	maxRedirects = 5
	// browserUserAgent is required by api.nasdaq.com and accepted by TMX.
	browserUserAgent = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

// getJSON performs a GET, verifies the response is a successful JSON response,
// and decodes it into dst. Errors include the URL, status, Content-Type, and a
// short body preview so upstream HTML error pages are diagnosable instead of
// surfacing as "invalid character '<'".
//
// The caller owns the client and its timeout and redirect policy.
func getJSON(ctx context.Context, client *http.Client, url string, dst any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return fmt.Errorf("build request for %s: %w", url, err)
	}
	req.Header.Set("User-Agent", browserUserAgent)
	req.Header.Set("Accept", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("request %s: %w", url, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBodySize+1))
	if err != nil {
		return fmt.Errorf("read response from %s: %w", url, err)
	}
	if len(body) > maxBodySize {
		return fmt.Errorf("response from %s exceeds %d bytes", url, maxBodySize)
	}

	contentType := resp.Header.Get("Content-Type")
	switch {
	case resp.StatusCode != http.StatusOK:
		return fmt.Errorf("unexpected HTTP %d from %s (Content-Type %q): %s",
			resp.StatusCode, url, contentType, bodyPreview(body))
	case contentType != "" && !isJSONContentType(contentType):
		return fmt.Errorf("unexpected Content-Type %q from %s (HTTP %d): %s",
			contentType, url, resp.StatusCode, bodyPreview(body))
	}

	if err := json.Unmarshal(body, dst); err != nil {
		return fmt.Errorf("decode JSON from %s (HTTP %d, Content-Type %q): %w: %s",
			url, resp.StatusCode, contentType, err, bodyPreview(body))
	}
	return nil
}

// isJSONContentType reports whether v is a JSON media type. It accepts
// application/json, text/json, and structured suffixes such as
// application/problem+json.
func isJSONContentType(v string) bool {
	mediaType, _, err := mime.ParseMediaType(v)
	if err != nil {
		return false
	}
	return mediaType == "application/json" ||
		mediaType == "text/json" ||
		strings.HasSuffix(mediaType, "+json")
}

// bodyPreview returns a single-line, whitespace-collapsed preview of body,
// truncated to bodyPreviewLimit bytes. Truncated previews end in "...".
// The truncation point is snapped back to a UTF-8 rune boundary so a
// multi-byte character is never cut in half.
func bodyPreview(body []byte) string {
	truncated := len(body) > bodyPreviewLimit
	if truncated {
		body = trimPartialRune(body[:bodyPreviewLimit])
	}
	s := strings.Join(strings.Fields(string(body)), " ")
	if truncated {
		return s + "..."
	}
	return s
}

// trimPartialRune drops the incomplete trailing UTF-8 rune left behind when
// b is a byte prefix cut at an arbitrary offset. It only inspects the final
// rune fragment, so at most utf8.UTFMax-1 bytes are removed and a body that
// was already invalid UTF-8 is left otherwise untouched.
func trimPartialRune(b []byte) []byte {
	if len(b) == 0 || b[len(b)-1] < utf8.RuneSelf {
		return b // An ASCII byte always ends a complete rune.
	}
	// Walk back over the trailing continuation bytes to the lead byte.
	i := len(b) - 1
	for i >= 0 && b[i]&0xC0 == 0x80 {
		i--
	}
	if i < 0 {
		return b // Continuation bytes only: the source is not valid UTF-8.
	}
	var want int
	switch c := b[i]; {
	case c&0xE0 == 0xC0:
		want = 2
	case c&0xF0 == 0xE0:
		want = 3
	case c&0xF8 == 0xF0:
		want = 4
	default:
		return b // Not a UTF-8 lead byte; the source is not valid UTF-8.
	}
	if len(b)-i >= want {
		return b // The final rune is complete.
	}
	return b[:i] // The cut landed inside the final rune; drop the fragment.
}

// limitRedirects is an http.Client CheckRedirect policy that refuses the
// maxRedirects-th redirect, so a chain never exceeds maxRedirects requests
// (the initial request plus at most maxRedirects-1 followed redirects) and
// fails with "stopped after N redirects" — the same convention as Go's
// default policy. Go's default rules still apply otherwise, including
// dropping sensitive headers when a redirect changes hosts.
func limitRedirects(_ *http.Request, via []*http.Request) error {
	if len(via) >= maxRedirects {
		return fmt.Errorf("stopped after %d redirects", maxRedirects)
	}
	return nil
}
