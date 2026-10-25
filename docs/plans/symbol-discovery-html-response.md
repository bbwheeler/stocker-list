# Symbol Discovery HTML Response Handling Implementation Plan

**Goal:** Stop a single stock-symbol provider from terminating the whole service when it returns an HTML page instead of JSON, and make such failures diagnosable with the HTTP status, Content-Type, URL, and a short body preview.

**Architecture:** The two providers (TSX and US) currently decode response bodies directly with `encoding/json`, so an HTML error page surfaces as `invalid character '<'`. This plan adds one shared HTTP/JSON helper that validates status and Content-Type before decoding, sets explicit `User-Agent` and `Accept` headers, limits redirects, and includes a body preview in errors. The refresher is changed to log a provider failure, continue with the healthy providers, and keep the refresh loop running instead of exiting.

**Tech Stack:** Go 1.25, standard library `net/http`, `net/http/httptest`, `encoding/json`, `mime`, `log/slog`.

**Spec:** This document. The investigation evidence below is the specification for the fix.

## Global Constraints

- Do not change the TSX or NASDAQ endpoint constants as part of this fix. The TSX failure is an upstream outage, not a stale URL (evidence below).
- No new third-party dependencies. Use the Go standard library only.
- Preserve the existing `provider.Provider` interface and both provider public APIs.
- Keep `KAFKA_BROKERS` empty as a valid no-op configuration; do not introduce Kafka coupling into provider tests.
- Every step must leave the module buildable and `go test ./...` green.
- Follow the repository's existing style: small files, table-driven tests where useful, no logging inside provider clients.

---

## Problem Summary

The service fails on startup with this journald error:

```
{"time":"2026-10-25T01:47:52.851221412Z","level":"ERROR","msg":"failed to run",
 "error":"symbol discovery failed: provider failed to list symbols: decode response: invalid character '<' looking for beginning of value"}
```

The message means Go's JSON decoder received a body whose first non-whitespace byte was `<`. That is the signature of an HTML document, not JSON. The wrapping chain identifies the exact path:

1. `cmd/server/main.go` starts `refresher.Refresher.Run`.
2. `refresher.Run` calls `refresher.tick`.
3. `refresher.tick` calls `refresher.discoverAllSymbols`.
4. `discoverAllSymbols` iterates providers in order: TSX first, US second.
5. `provider.Client.ListSymbols` (TSX) performs the GET and calls `json.Decoder.Decode`.
6. The decoder returns `invalid character '<' looking for beginning of value`.

Because the TSX provider is first and its error aborts the loop, the US provider never runs.

### Verified root cause

**TMX is serving a site-wide maintenance page with HTTP 200 and `Content-Type: text/html`.** The TSX JSON endpoint is currently unavailable. The page says:

> Our websites are currently unavailable. Due to security vulnerabilities related to PHP, TMX has undertaken measures to mitigate potential security risks. TMX trading and clearing systems are operating normally.

The key point is that the response is **HTTP 200 OK**, so the existing status check in `tsx.go` passes. The code never checks `Content-Type`, so it hands the HTML body to the JSON decoder.

#### TSX evidence

Command:

```
curl -sS -D - -o /tmp/tsx_body.txt \
  'https://www.tsx.com/json/company-directory/search/tsx/*'
```

Response headers (abridged):

```
HTTP/2 200
content-type: text/html
content-length: 70171
server: AmazonS3
last-modified: Sat, 24 Oct 2026 01:08:50 GMT
x-cache: Error from cloudfront
via: 1.1 ...cloudfront.net (CloudFront)
```

There is no `Location` header and no redirect chain. The response is a direct 200.

Body preview:

```
<!DOCTYPE html>
...
Our websites are currently unavailable. Due to security vulnerabilities related to PHP,
TMX has undertaken measures to mitigate potential security risks.
TMX trading and clearing systems are operating normally.
```

The same body (same ETag `"72f99d70df112b87350db9a2fc260d6d"`, same 70,171-byte length) is returned for every path tested:

- `https://www.tsx.com/`
- `https://www.tsx.com/en`
- `https://www.tsx.com/en/listings/listing-with-us/listed-company-directory`
- `https://www.tsx.com/json/company-directory/search/tsx/*`
- `https://www.tsx.com/json/company-directory/search/tsx/AC`
- `https://www.tsx.com/company-directory/search/tsx/*`

This rules out a stale endpoint path, a missing trailing wildcard, and a path capitalization problem: the host is serving one static maintenance document for all routes.

A request from a different network (the `allorigins` HTTP proxy) returned the same HTML page, so this is not specific to the machine that ran the investigation. The maintenance page's `last-modified` timestamp (2026-10-24 01:08:50 UTC) is consistent with the journald error at 2026-10-25 01:47:52 UTC.

Additional controls that did **not** change the response:

- Browser-like `User-Agent`
- `Accept: application/json`
- `X-Requested-With: XMLHttpRequest`
- `Referer: https://www.tsx.com/en/listings/listing-with-us/listed-company-directory`
- Cookies from a prior request to `https://www.tsx.com/`
- HTTP/1.1 instead of HTTP/2
- Cache-busting query string

So missing headers are not the cause of this incident. Adding headers is still correct defensive practice and is part of this plan.

#### US evidence (the healthy provider)

Command:

```
curl -sS -D - -o /tmp/nasdaq_body.txt \
  -H 'User-Agent: Mozilla/5.0 ... Chrome/120.0.0.0 Safari/537.36' \
  -H 'Accept: application/json' \
  'https://api.nasdaq.com/api/screener/stocks?limit=250&offset=0&exchange=NASDAQ'
```

Response headers (abridged):

```
HTTP/2 200
content-type: application/json; charset=utf-8
server: Kestrel
```

Body preview:

```
{"data":{"filters":null,"table":{"asOf":null,"headers":{...},
 "rows":[{"symbol":"NVDA","name":"NVIDIA Corporation Common Stock",...},
         {"symbol":"AAPL","name":"Apple Inc. Common Stock",...}, ...]}}
```

The first page contained 250 rows and decoded to the existing `nasdaqResponse` shape. The US provider is working.

#### Endpoint configuration

- TSX URL: compile-time constant `defaultBaseURL` in `internal/provider/tsx.go:15`.
- US URL: compile-time constant `nasdaqBaseURL` in `internal/provider/us.go:19`.
- No environment variable, config file, deploy template, or systemd unit sets either URL. `.env.example`, `.env.podman`, `podman-compose.yml`, `deploy/quadlet/stocker-list.container`, and `Makefile` contain no provider URL or API key.
- `internal/config/config.go` only exposes `REFRESH_CHECK_INTERVAL`, `KAFKA_BROKERS`, and `KAFKA_TOPIC`. There is no stale provider configuration to change.

### What is not the cause

- **Not a stale endpoint path.** Every TSX path returns the same maintenance page.
- **Not missing `User-Agent` or `Accept`.** Requests with browser headers and cookies return the same page.
- **Not a redirect.** The response is a direct 200; there is no `Location`.
- **Not TLS or a local proxy.** The certificate is a valid `*.tsx.com` Amazon certificate, and an external proxy saw the same page.
- **Not configuration.** No environment variable or deploy template sets a provider URL or API key. The URLs are compile-time constants in `internal/provider/tsx.go` and `internal/provider/us.go`.

### Unverified / limits of this investigation

- It is unverified when TMX will restore the endpoint, or whether TMX will publish a replacement API.
- No alternative public TSX symbol-list endpoint was verified during the outage. This plan therefore does not guess a new URL.
- If TMX restores the existing endpoint, no URL change is needed and the TSX provider resumes automatically on the next refresh cycle.
- If TMX permanently retires the endpoint, replacing `defaultBaseURL` is a separate follow-up. This plan makes that failure non-fatal and clearly logged in the meantime.

### Existing docs and plans

| Document | Contents | Is it a symbol-discovery plan? |
|---|---|---|
| `design.md` | Nine-line project description: pulls stock data and publishes to Kafka; protobuf message shape. | No |
| `README.md` | Architecture, Kafka integration, provider descriptions, environment variables, local/Podman/Quadlet run instructions. | No |
| `docs/kafka-go-producer-rewrite.md` | Complete, detailed implementation plan for the historical sarama → segmentio/kafka-go rewrite. It is already implemented at HEAD (`go.mod` uses kafka-go; config has only `KAFKA_BROKERS`/`KAFKA_TOPIC`). | No |
| `docs/plans/` | Did not exist before this document. | No |

There is no existing plan for symbol discovery or provider error handling. This document is the first one.

---

## Affected Code

| File | Function / symbol | Current behavior |
|---|---|---|
| `internal/provider/tsx.go` | `defaultBaseURL` (line 15) | TSX endpoint constant: `https://www.tsx.com/json/company-directory/search/tsx/*`. No env/config override. |
| `internal/provider/tsx.go` | `NewClient`, `NewClientForTest` | Build `http.Client` with a timeout only. Default redirect policy (up to 10). No `CheckRedirect`. |
| `internal/provider/tsx.go` | `ListSymbols` (lines 49–85) | Creates a GET with no `User-Agent` or `Accept`. Checks `StatusCode != 200`. Then decodes with `json.Decoder` without checking `Content-Type`. Wraps decode failure as `decode response: %w`, which produces the reported `invalid character '<'`. |
| `internal/provider/us.go` | `nasdaqBaseURL` (line 19) | US endpoint constant: `https://api.nasdaq.com/api/screener/stocks`. No env/config override. |
| `internal/provider/us.go` | `userAgent` (lines 35–37), `maxBodySize` (lines 39–40) | Package-level constants currently live in `us.go` but are general HTTP concerns. |
| `internal/provider/us.go` | `NewUSClient`, `NewUSClientForTest` | Build `http.Client` with a timeout only. Default redirect policy. No `CheckRedirect`. |
| `internal/provider/us.go` | `fetchPage` (lines 137–160) | Sets `User-Agent` and `Accept: application/json`. Checks `StatusCode != 200`. Then decodes with `json.Decoder` without checking `Content-Type`. Error is `decode response: %w`. |
| `internal/refresher/refresher.go` | `tick` (lines 57–67) | Calls `discoverAllSymbols`; any error is returned as `symbol discovery failed`, which makes `Run` return and the service exit. |
| `internal/refresher/refresher.go` | `discoverAllSymbols` (lines 71–95) | Iterates providers and returns on the first `ListSymbols` error. TSX failing prevents US from running and prevents publishing any symbols. |
| `cmd/server/main.go` | `run` (lines 28–73) | Starts `ref.Run` in a goroutine. On `Run` error it logs `failed to run` and cancels the context. It does not need a change for this fix; the refresher will stop returning provider errors. |
| `internal/config/config.go` | `Config`, `Load` | Only refresh interval and Kafka settings. No provider URL/key fields. No change required. |
| `.env.example`, `.env.podman`, `podman-compose.yml`, `deploy/quadlet/stocker-list.container`, `Makefile` | — | No provider URL or key values. No stale configuration to fix. |
| `README.md` | Providers section (lines ~43–61) | Documents the TSX endpoint and says it is always active. Needs a short note about graceful degradation and response validation. |

---

## Proposed Fix

### 1. One shared HTTP/JSON helper

Create `internal/provider/http.go` with:

- `getJSON(ctx, client, url, dst)` — builds the request, sets `User-Agent` and `Accept: application/json`, performs the GET, reads at most `maxBodySize+1` bytes, validates status and Content-Type, and unmarshals JSON.
- `isJSONContentType` — accepts `application/json`, `text/json`, and `+json` media types via `mime.ParseMediaType`.
- `bodyPreview` — truncates to 512 bytes and collapses whitespace for error messages.
- `limitRedirects` — stops after 5 redirects.
- Constants `maxBodySize = 20 << 20`, `bodyPreviewLimit = 512`, `maxRedirects = 5`, and `browserUserAgent`.

An empty Content-Type is allowed through to `json.Unmarshal`; if the body is HTML, the unmarshal error still includes the status, Content-Type, and body preview. A present non-JSON Content-Type is rejected before decoding.

### 2. Provider changes

- `tsx.go` uses `getJSON` instead of its own request/decode block. Both constructors set `CheckRedirect: limitRedirects`. The `defaultBaseURL` constant is unchanged.
- `us.go` uses `getJSON` in `fetchPage`. Both constructors set `CheckRedirect: limitRedirects`. The duplicated `userAgent` and `maxBodySize` constants move to `http.go`.
- Both providers wrap helper errors with their provider context (`fetch TSX symbols: ...`, `fetch US symbols: ...`).

### 3. Refresher changes

`discoverAllSymbols` changes from "return on first provider error" to:

- Log each provider error at error level with the provider type and the full error.
- Continue to the next provider.
- Publish the symbols returned by the healthy providers.
- Return the companies, the number of failed providers, and a fatal error only for Kafka publish failures.

`tick` logs a warning when `failed > 0` and returns `nil`, so `Run` keeps the loop alive and retries on the next interval. If every provider fails, the service stays up, logs the failure, publishes nothing, and retries instead of entering a systemd restart loop.

### 4. What does not change

- The TSX and NASDAQ URLs.
- The `provider.Provider` interface.
- The Kafka producer API and no-op behavior.
- Environment variables and deploy templates. No provider URL or key is configurable today, and none is added.
- The 24-hour default refresh interval.

### Why this addresses the incident

The immediate cause of the crash is that an upstream maintenance page with HTTP 200 and `text/html` passes the status check and reaches the JSON decoder. The helper rejects that response before decoding and reports the real reason. The refresher then treats the TSX outage as a provider failure rather than a fatal service failure, so the healthy US provider continues to publish symbols. When TMX restores the endpoint, the unchanged TSX URL starts working again on the next cycle.

---

## Test Plan

### Unit tests (new and updated)

| Test | File | What it proves |
|---|---|---|
| `TestGetJSON_HTMLResponse` | `internal/provider/http_test.go` | A 200 `text/html` body is rejected; the error contains the URL, status, Content-Type, and body preview. |
| `TestGetJSON_Non200` | `internal/provider/http_test.go` | A 503 with an HTML body produces a status-aware error, not a JSON syntax error. |
| `TestGetJSON_HappyPath` | `internal/provider/http_test.go` | JSON with `application/json` decodes; `User-Agent` and `Accept` are sent. |
| `TestIsJSONContentType` | `internal/provider/http_test.go` | `application/json`, `text/json`, and `+json` are accepted; `text/html`, `application/xml`, empty, and malformed values are rejected. |
| `TestLimitRedirects` | `internal/provider/http_test.go` | The redirect policy allows fewer than 5 redirects and rejects the 5th. |
| `TestListSymbols_HTMLInsteadOfJSON` | `internal/provider/fmp_test.go` (existing TSX test file) | Reproduces the production failure and asserts the error is not `invalid character '<'`. |
| `TestListSymbols_RedirectLimit` | `internal/provider/fmp_test.go` | A redirect loop returns an error mentioning redirects instead of hanging. |
| `TestUS_HTMLInsteadOfJSON` | `internal/provider/us_test.go` | The same HTML response is rejected by the US provider. |
| `TestUS_RedirectToHTML` | `internal/provider/us_test.go` | A redirect to an HTML page is followed (within the limit) and then rejected by the Content-Type check. |
| `TestRun_ProviderFailureDoesNotStopLoop` | `internal/refresher/refresher_test.go` | One failing provider does not terminate `Run`; the healthy provider is still used and the failure is logged. |

Existing happy-path, empty-response, pagination, deduplication, HTTP-error, invalid-JSON, and context-cancellation tests must keep passing.

### End-to-end verification

1. `gofmt -l .` — expect no output.
2. `go vet ./...` — expect no output.
3. `go test ./... -count=1` — all packages pass.
4. `go build ./...` — succeeds.
5. Run `go run ./cmd/server` with `KAFKA_BROKERS` empty and the default refresh interval. Expected observations:
   - The process stays running instead of exiting.
   - Logs contain `provider failed to list symbols` for `*provider.Client` with an error such as:
     `fetch TSX symbols: unexpected Content-Type "text/html" from https://www.tsx.com/json/company-directory/search/tsx/* (HTTP 200): <!DOCTYPE html> ...`
     The body preview is the first 512 bytes of the response, so for the current TMX page it begins with `<!DOCTYPE html>` rather than the maintenance sentence itself.
   - Logs contain `refresh cycle completed with provider failures` with `symbols=<US symbol count>` and `failed_providers=1`.
   - No log contains `symbol discovery failed` and no log contains `invalid character '<'`.
6. Optional service check after rebuilding the image:
   - Rebuild and push the image per `deploy/push.sh`.
   - `systemctl --user restart stocker-list`
   - `systemctl --user status stocker-list`
   - `journalctl --user -u stocker-list -f`
   - Confirm the service is active (not restarting every 10 seconds) and the warning is visible.
7. When TMX restores the endpoint, re-run `go run ./cmd/server` or restart the service and confirm the TSX symbols are published again with no code change.

### Plan validation note

The code in this plan was exercised in a throwaway copy of the repository while the plan was being written. `go vet ./...` and `go test ./...` passed, and a live run produced the expected `unexpected Content-Type "text/html"` TSX error, published 6,977 US symbols, logged `refresh cycle completed with provider failures`, and kept the process running. The real repository working tree contains only this document; no application source was changed.

---

## Implementation Steps

### Step 1: Add the shared HTTP/JSON helper and its tests

**Files:**
- Create: `internal/provider/http.go`
- Create: `internal/provider/http_test.go`

**Do:**

1. Write `internal/provider/http_test.go` first with these tests:

```go
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
	for _, want := range []string{"unexpected HTTP 503", "text/html", "maintenance"} {
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
		if got := r.Header.Get("User-Agent"); got == "" {
			t.Error("User-Agent is empty")
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

func TestLimitRedirects(t *testing.T) {
	via := make([]*http.Request, maxRedirects)
	if err := limitRedirects(nil, via); err == nil {
		t.Fatal("limitRedirects allowed maxRedirects redirects")
	}
	if err := limitRedirects(nil, via[:maxRedirects-1]); err != nil {
		t.Fatalf("limitRedirects rejected %d redirects: %v", maxRedirects-1, err)
	}
}
```

2. Run the tests. They must fail because `getJSON`, `isJSONContentType`, `limitRedirects`, and `maxRedirects` do not exist:

```
go test ./internal/provider -run 'TestGetJSON|TestIsJSON|TestLimitRedirects' -count=1
```

3. Implement `internal/provider/http.go`:

```go
package provider

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strings"
)

const (
	// maxBodySize caps how much of a single response body we will read.
	maxBodySize = 20 << 20
	// bodyPreviewLimit is the maximum number of body bytes included in errors.
	bodyPreviewLimit = 512
	// maxRedirects is the maximum number of redirects followed per request.
	maxRedirects = 5
	// browserUserAgent is required by api.nasdaq.com and accepted by TMX.
	browserUserAgent = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

// getJSON performs a GET, verifies the response is a successful JSON response,
// and decodes it into dst. Errors include the URL, status, Content-Type, and a
// short body preview so upstream HTML error pages are diagnosable instead of
// surfacing as "invalid character '<'".
func getJSON(ctx context.Context, client *http.Client, url string, dst any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	req.Header.Set("User-Agent", browserUserAgent)
	req.Header.Set("Accept", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return err
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

func isJSONContentType(v string) bool {
	mediaType, _, err := mime.ParseMediaType(v)
	if err != nil {
		return false
	}
	return mediaType == "application/json" ||
		mediaType == "text/json" ||
		strings.HasSuffix(mediaType, "+json")
}

func bodyPreview(body []byte) string {
	truncated := len(body) > bodyPreviewLimit
	if truncated {
		body = body[:bodyPreviewLimit]
	}
	s := strings.Join(strings.Fields(string(body)), " ")
	if truncated {
		return s + "..."
	}
	return s
}

func limitRedirects(_ *http.Request, via []*http.Request) error {
	if len(via) >= maxRedirects {
		return fmt.Errorf("stopped after %d redirects", maxRedirects)
	}
	return nil
}
```

4. Run the tests again:

```
gofmt -w internal/provider/http.go internal/provider/http_test.go
go test ./internal/provider -run 'TestGetJSON|TestIsJSON|TestLimitRedirects' -count=1
```

**Verify:** `go test ./internal/provider -run 'TestGetJSON|TestIsJSON|TestLimitRedirects' -count=1` passes.

**Done when:** The helper tests pass; a 200 `text/html` response produces an error containing the status, Content-Type, and body preview, and does not surface `invalid character '<'`.

---

### Step 2: Refactor the TSX provider onto the helper

**Files:**
- Modify: `internal/provider/tsx.go`
- Modify: `internal/provider/fmp_test.go` (the existing TSX provider tests)

**Do:**

1. Add these two tests to `internal/provider/fmp_test.go`:

```go
func TestListSymbols_HTMLInsteadOfJSON(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, "<!DOCTYPE html><html><body>Our websites are currently unavailable.</body></html>")
	}))
	defer srv.Close()

	c := NewClientForTest(srv.URL)
	_, err := c.ListSymbols(context.Background())
	if err == nil {
		t.Fatal("ListSymbols succeeded for an HTML response; want an error")
	}
	for _, want := range []string{
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

func TestListSymbols_RedirectLimit(t *testing.T) {
	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, srv.URL, http.StatusFound)
	}))
	defer srv.Close()

	c := NewClientForTest(srv.URL)
	_, err := c.ListSymbols(context.Background())
	if err == nil {
		t.Fatal("ListSymbols succeeded for a redirect loop; want an error")
	}
	if !strings.Contains(err.Error(), "redirect") {
		t.Errorf("error %q does not mention redirects", err)
	}
}
```

Add `fmt` and `strings` to the test file's imports if they are not already present.

2. In `internal/provider/tsx.go`, remove the `encoding/json` and `io` imports. Keep `context`, `fmt`, `net/http`, and `time`.

3. Set the redirect policy in both constructors:

```go
func NewClient() *Client {
	return &Client{
		baseURL: defaultBaseURL,
		httpClient: &http.Client{
			Timeout:       30 * time.Second,
			CheckRedirect: limitRedirects,
		},
	}
}

func NewClientForTest(baseURL string) *Client {
	return &Client{
		baseURL: baseURL,
		httpClient: &http.Client{
			Timeout:       15 * time.Second,
			CheckRedirect: limitRedirects,
		},
	}
}
```

4. Replace the request/status/decode block in `ListSymbols` with:

```go
func (c *Client) ListSymbols(ctx context.Context) ([]Company, error) {
	var tsxResp tsxResponse
	if err := getJSON(ctx, c.httpClient, c.baseURL, &tsxResp); err != nil {
		return nil, fmt.Errorf("fetch TSX symbols: %w", err)
	}

	out := make([]Company, 0, len(tsxResp.Results))
	for _, e := range tsxResp.Results {
		if e.Symbol == "" {
			continue
		}
		out = append(out, Company{
			Symbol:   e.Symbol,
			Name:     e.Name,
			Exchange: "TSX",
			Currency: "CAD",
		})
	}
	return out, nil
}
```

5. Run the TSX tests:

```
gofmt -w internal/provider/tsx.go internal/provider/fmp_test.go
go test ./internal/provider -run 'TestListSymbols' -count=1
```

**Verify:** All `TestListSymbols_*` tests pass, including the existing success, empty-response, HTTP-error, invalid-JSON, context-cancelled, and empty-symbol tests.

**Done when:** `internal/provider/tsx.go` no longer imports `encoding/json` or `io`; its request path goes through `getJSON`; the HTML and redirect tests pass.

---

### Step 3: Refactor the US provider onto the helper

**Files:**
- Modify: `internal/provider/us.go`
- Modify: `internal/provider/us_test.go`

**Do:**

1. Add these two tests to `internal/provider/us_test.go`:

```go
func TestUS_HTMLInsteadOfJSON(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, "<!DOCTYPE html><html><body>Access denied</body></html>")
	}))
	defer srv.Close()

	c := NewUSClientForTest(srv.URL)
	_, err := c.ListSymbols(context.Background())
	if err == nil {
		t.Fatal("ListSymbols succeeded for an HTML response; want an error")
	}
	for _, want := range []string{
		"unexpected Content-Type",
		"text/html",
		"HTTP 200",
		"Access denied",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not contain %q", err, want)
		}
	}
}

func TestUS_RedirectToHTML(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/blocked" {
			w.Header().Set("Content-Type", "text/html")
			w.WriteHeader(http.StatusOK)
			fmt.Fprint(w, "<!DOCTYPE html><html><body>blocked</body></html>")
			return
		}
		http.Redirect(w, r, "/blocked", http.StatusFound)
	}))
	defer srv.Close()

	c := NewUSClientForTest(srv.URL)
	_, err := c.ListSymbols(context.Background())
	if err == nil {
		t.Fatal("ListSymbols succeeded for a redirect to HTML; want an error")
	}
	for _, want := range []string{"unexpected Content-Type", "text/html", "blocked"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not contain %q", err, want)
		}
	}
}
```

2. In `internal/provider/us.go`, remove the `encoding/json` and `io` imports. Remove the package-level `userAgent` and `maxBodySize` constants; they now live in `http.go`. Keep `strings` for `TrimSpace`.

3. Set the redirect policy in both constructors:

```go
func NewUSClient() *USClient {
	return &USClient{
		baseURL:   nasdaqBaseURL,
		exchanges: defaultExchanges,
		httpClient: &http.Client{
			Timeout:       30 * time.Second,
			CheckRedirect: limitRedirects,
		},
	}
}

func NewUSClientForTest(baseURL string) *USClient {
	return &USClient{
		baseURL:   baseURL,
		exchanges: defaultExchanges,
		httpClient: &http.Client{
			Timeout:       15 * time.Second,
			CheckRedirect: limitRedirects,
		},
	}
}
```

4. Replace `fetchPage` with:

```go
// fetchPage performs a single screener GET and decodes the page of rows.
func (c *USClient) fetchPage(ctx context.Context, url string) ([]nasdaqRow, error) {
	var pag nasdaqResponse
	if err := getJSON(ctx, c.httpClient, url, &pag); err != nil {
		return nil, fmt.Errorf("fetch US symbols: %w", err)
	}
	return pag.Data.Table.Rows, nil
}
```

5. Run the US tests and the full provider package:

```
gofmt -w internal/provider/us.go internal/provider/us_test.go
go test ./internal/provider -count=1
```

**Verify:** All provider tests pass, including the existing pagination, deduplication, trim, empty-response, HTTP-error, and invalid-JSON tests.

**Done when:** `internal/provider/us.go` no longer imports `encoding/json` or `io`; `fetchPage` uses `getJSON`; the HTML and redirect-to-HTML tests pass.

---

### Step 4: Make the refresher tolerate individual provider failures

**Files:**
- Modify: `internal/refresher/refresher.go`
- Modify: `internal/refresher/refresher_test.go`

**Do:**

1. Add this test to `internal/refresher/refresher_test.go`:

```go
type errorProvider struct {
	err error
}

func (p errorProvider) ListSymbols(context.Context) ([]provider.Company, error) {
	return nil, p.err
}

type countingProvider struct {
	calls int
}

func (p *countingProvider) ListSymbols(context.Context) ([]provider.Company, error) {
	p.calls++
	return []provider.Company{{Symbol: "AC", Exchange: "TSX"}}, nil
}

func TestRun_ProviderFailureDoesNotStopLoop(t *testing.T) {
	var logs bytes.Buffer
	log := slog.New(slog.NewTextHandler(&logs, nil))
	healthy := &countingProvider{}
	providers := []provider.Provider{
		errorProvider{err: errors.New(`unexpected Content-Type "text/html"`)},
		healthy,
	}
	ref := refresher.New(testConfig(), nil, providers, log, &kafka.Producer{})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		ref.Run(ctx)
		close(done)
	}()

	// Give the immediate tick time to run. The failing provider must not make
	// Run exit.
	time.Sleep(200 * time.Millisecond)
	select {
	case <-done:
		t.Fatal("Run exited after one provider failed; want it to keep running")
	default:
	}

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not exit after context cancellation")
	}

	if healthy.calls == 0 {
		t.Error("healthy provider was not called")
	}
	if !strings.Contains(logs.String(), "provider failed to list symbols") {
		t.Errorf("logs do not mention the provider failure: %s", logs.String())
	}
}
```

Add `bytes`, `errors`, and `strings` to the test file's imports.

2. In `internal/refresher/refresher.go`, replace `tick` with:

```go
func (r *Refresher) tick(ctx context.Context) error {
	r.log.Info("refresh cycle starting")

	companies, failed, err := r.discoverAllSymbols(ctx)
	if err != nil {
		return fmt.Errorf("symbol discovery failed: %w", err)
	}
	if failed > 0 {
		r.log.Warn("refresh cycle completed with provider failures",
			"symbols", len(companies), "failed_providers", failed)
	} else {
		r.log.Info("refresh cycle complete", "symbols", len(companies))
	}
	return nil
}
```

3. Replace `discoverAllSymbols` with:

```go
// discoverAllSymbols pulls the current symbol list from all providers and
// publishes a StockUpdate message to Kafka for every symbol discovered.
// A failure from one provider is logged and does not prevent the remaining
// providers from being used; the cycle still succeeds.
func (r *Refresher) discoverAllSymbols(ctx context.Context) ([]provider.Company, int, error) {
	var allCompanies []provider.Company
	var failed int

	for _, p := range r.providers {
		companies, err := p.ListSymbols(ctx)
		if err != nil {
			failed++
			r.log.Error("provider failed to list symbols",
				"provider", fmt.Sprintf("%T", p), "error", err)
			continue
		}
		allCompanies = append(allCompanies, companies...)
	}

	if r.producer != nil {
		for _, c := range allCompanies {
			msg := &kafkastockv1.Stock{
				Symbol:   c.Symbol,
				Exchange: c.Exchange,
			}
			if err := r.producer.PublishStockUpdate(ctx, msg); err != nil {
				return nil, failed, fmt.Errorf("failed to publish update for symbol %s on exchange %s: %w", c.Symbol, c.Exchange, err)
			}
		}
	}

	return allCompanies, failed, nil
}
```

4. Run the refresher tests and the full package list:

```
gofmt -w internal/refresher/refresher.go internal/refresher/refresher_test.go
go test ./internal/refresher -count=1
go test ./... -count=1
```

**Verify:** The refresher tests pass; `TestRun_ProviderFailureDoesNotStopLoop` proves `Run` stays alive and the failure is logged.

**Done when:** A single provider failure no longer aborts the cycle or exits the process; the healthy provider's symbols are still published; Kafka publish failures still return an error.

---

### Step 5: Document the new provider behavior

**Files:**
- Modify: `README.md`

**Do:**

1. In the `## Providers` section, after the TSX paragraph, add a short paragraph:

```markdown
Both providers validate the HTTP status and `Content-Type` before decoding. If a
provider is unavailable or returns a non-JSON response (for example, an HTML
maintenance page served with HTTP 200), the refresh cycle logs
`provider failed to list symbols` with the provider, URL, status, Content-Type,
and a short body preview. The cycle continues with the remaining providers and
retries on the next interval; it does not terminate the service.
```

2. Leave the endpoint URLs and environment-variable tables unchanged. No provider URL or key is configurable, and this fix does not add one.

**Verify:**

- `grep -n "provider failed to list symbols" README.md` shows the new paragraph.
- `git diff --stat` shows only the intended documentation change.

**Done when:** The README explains the graceful-degradation behavior and the diagnostic log message without documenting transient outage details as permanent behavior.

---

### Step 6: Full verification and commit

**Files:** No source changes in this step.

**Do:**

1. Format and verify the whole module:

```
gofmt -l .
go vet ./...
go test ./... -count=1
go build ./...
```

Expected: `gofmt -l .` prints nothing; `go vet` and `go test` are clean; the build succeeds.

2. Run the binary with Kafka disabled and observe the startup behavior:

```
KAFKA_BROKERS= go run ./cmd/server
```

Expected:

- The process keeps running.
- The TSX failure is logged as `provider failed to list symbols` with `unexpected Content-Type "text/html"` and a body preview beginning with `<!DOCTYPE html>`.
- `refresh cycle completed with provider failures` appears with a non-zero US symbol count and `failed_providers=1`.
- No `invalid character '<'` error appears.
- Ctrl-C shuts the process down.

3. Optional service verification:

```
(cd deploy && ./push.sh)
systemctl --user daemon-reload
systemctl --user restart stocker-list
systemctl --user status stocker-list
journalctl --user -u stocker-list -f
```

Confirm the service remains active instead of restarting every 10 seconds.

4. Commit and push the implementation:

```
git add internal/provider/http.go internal/provider/http_test.go \
        internal/provider/tsx.go internal/provider/fmp_test.go \
        internal/provider/us.go internal/provider/us_test.go \
        internal/refresher/refresher.go internal/refresher/refresher_test.go \
        README.md
git commit -m "fix: handle HTML responses from symbol providers"
git push
```

**Verify:** `git status --short` is clean after the push; `go test ./... -count=1` still passes.

**Done when:** The full verification passes, the manual run shows graceful degradation, and the change is committed and pushed.

---

## Out of Scope

- Finding or implementing a replacement TSX data source while TMX is unavailable. The endpoint is unchanged; a follow-up is needed only if TMX permanently retires it.
- Changing the default 24-hour refresh interval or adding per-provider retry/backoff.
- Adding provider URL or enable/disable environment variables.
- Fixing the pre-existing `main.run` exit-code behavior on fatal errors or the `context canceled` log message on normal shutdown.
- Changing Kafka producer behavior or the `provider.Provider` interface.
