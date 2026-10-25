package provider

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestGetJSON_HTMLResponse(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("<!DOCTYPE html><html><body>Our websites are currently unavailable.</body></html>"))
	}))
	defer srv.Close()

	var dst map[string]any
	err := getJSON(context.Background(), srv.Client(), srv.URL, &dst)
	if err == nil {
		t.Fatal("getJSON succeeded for an HTML response; want an error")
	}
	for _, want := range []string{
		srv.URL,
		"unexpected Content-Type",
		"text/html",
		"HTTP 200",
		"Our websites are currently unavailable",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not contain %q", err, want)
		}
	}
	if strings.Contains(err.Error(), "invalid character '<'") {
		t.Errorf("error still surfaces the raw JSON syntax error: %v", err)
	}
}

func TestGetJSON_Non200(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("<html>maintenance</html>"))
	}))
	defer srv.Close()

	var dst map[string]any
	err := getJSON(context.Background(), srv.Client(), srv.URL, &dst)
	if err == nil {
		t.Fatal("getJSON succeeded for HTTP 503; want an error")
	}
	for _, want := range []string{srv.URL, "unexpected HTTP 503", "text/html", "maintenance"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not contain %q", err, want)
		}
	}
}

func TestGetJSON_HappyPath(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Accept"); got != "application/json" {
			t.Errorf("Accept = %q, want application/json", got)
		}
		if got := r.Header.Get("User-Agent"); got != browserUserAgent {
			t.Errorf("User-Agent = %q, want %q", got, browserUserAgent)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer srv.Close()

	var dst struct {
		OK bool `json:"ok"`
	}
	if err := getJSON(context.Background(), srv.Client(), srv.URL, &dst); err != nil {
		t.Fatalf("getJSON: %v", err)
	}
	if !dst.OK {
		t.Error("dst.OK = false, want true")
	}
}

func TestGetJSON_InvalidJSON(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"broken":`))
	}))
	defer srv.Close()

	var dst map[string]any
	err := getJSON(context.Background(), srv.Client(), srv.URL, &dst)
	if err == nil {
		t.Fatal("getJSON succeeded for malformed JSON; want an error")
	}
	for _, want := range []string{srv.URL, "decode JSON", "application/json", `{"broken":`} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not contain %q", err, want)
		}
	}
}

func TestIsJSONContentType(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want bool
	}{
		{"application/json", true},
		{"application/json; charset=utf-8", true},
		{"application/problem+json", true},
		{"text/json", true},
		{"text/html", false},
		{"application/xml", false},
		{"", false},
		{"not a mime type", false},
	} {
		if got := isJSONContentType(tc.in); got != tc.want {
			t.Errorf("isJSONContentType(%q) = %v, want %v", tc.in, got, tc.want)
		}
	}
}

func TestBodyPreview(t *testing.T) {
	if got := bodyPreview([]byte("  <html>\n\t<body>hi</body>  </html> ")); got != "<html> <body>hi</body> </html>" {
		t.Errorf("bodyPreview collapsed whitespace incorrectly: %q", got)
	}

	preview := bodyPreview([]byte(strings.Repeat("a", bodyPreviewLimit+100)))
	if !strings.HasSuffix(preview, "...") {
		t.Errorf("bodyPreview did not mark truncation: %q", preview)
	}
	if got := len(preview); got > bodyPreviewLimit+len("...") {
		t.Errorf("bodyPreview returned %d bytes, want at most %d", got, bodyPreviewLimit+len("..."))
	}
}

func TestLimitRedirects(t *testing.T) {
	via := make([]*http.Request, maxRedirects)
	if err := limitRedirects(nil, via); err == nil {
		t.Fatal("limitRedirects allowed maxRedirects redirects")
	}
	if err := limitRedirects(nil, via[:maxRedirects-1]); err != nil {
		t.Fatalf("limitRedirects rejected %d redirects: %v", maxRedirects-1, err)
	}
}
