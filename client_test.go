package dalgo2cloudflarekv

import (
	"bytes"
	"context"
	"crypto/x509"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"runtime"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

func fixtureOptions() Options {
	return Options{AccountID: "account", NamespaceID: "namespace", CollectionAlias: "kv", AllowedPrefixes: []string{"data/"}, Token: func(context.Context) (string, error) { return "owned-read-token", nil }}
}
func testClient(t *testing.T, options Options, handler http.HandlerFunc) *Client {
	t.Helper()
	server := httptest.NewTLSServer(handler)
	t.Cleanup(server.Close)
	pool := x509.NewCertPool()
	pool.AddCert(server.Certificate())
	options.RootCAs = pool
	options.Endpoint = server.URL + "/client/v4"
	c, err := New(options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := c.Close(); err != nil {
			t.Error(err)
		}
	})
	return c
}
func checkRequest(t *testing.T, r *http.Request) {
	t.Helper()
	if r.Method != "GET" || r.Header.Get("Authorization") != "Bearer owned-read-token" || r.Header.Get("Cookie") != "" || r.Header.Get("Accept-Encoding") != "identity" || r.TLS == nil || r.ProtoMajor != 1 {
		t.Error("verified read-only transport policy changed")
	}
}
func write(t *testing.T, w http.ResponseWriter, r *http.Request, data []byte) {
	t.Helper()
	if _, err := w.Write(data); err != nil {
		if (!errors.Is(err, syscall.EPIPE) && !errors.Is(err, syscall.ECONNRESET)) || !errors.Is(r.Context().Err(), context.Canceled) {
			t.Error("unexpected owned response failure")
		}
	}
}
func code(err error) string {
	var e *Error
	if errors.As(err, &e) {
		return e.Code
	}
	return ""
}
func await(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal("owned acknowledgement timed out")
	}
}
func waitIdle(t *testing.T, c *Client) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		c.mu.Lock()
		n := len(c.active)
		c.mu.Unlock()
		if n == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("physical lease did not settle")
		}
		runtime.Gosched()
	}
}

type transportFunc func(*http.Request) (*http.Response, error)

func (f transportFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func response(body io.ReadCloser) *http.Response {
	return &http.Response{StatusCode: 200, Header: make(http.Header), Body: body}
}

func TestOwnedTLSRawBytesOriginalEncodedKeysAndNoReuse(t *testing.T) {
	keys := []string{"data/a/b%2F😀", "data/space key", "data/\x00", "data/empty"}
	var calls atomic.Int32
	c := testClient(t, fixtureOptions(), func(w http.ResponseWriter, r *http.Request) {
		checkRequest(t, r)
		calls.Add(1)
		key := strings.TrimPrefix(r.URL.Path, "/client/v4/accounts/account/storage/kv/namespaces/namespace/values/")
		if r.URL.EscapedPath() != "/client/v4/accounts/account/storage/kv/namespaces/namespace/values/"+url.PathEscape(key) || r.URL.RawQuery != "" {
			t.Error("key was cleaned/double-decoded/not one segment")
		}
		w.Header().Set("Expiration", "18446744073709551615")
		if key != "data/empty" {
			write(t, w, r, []byte{0, 255, 'n', 'u', 'l', 'l'})
		}
	})
	if c.owned == nil || c.owned.Proxy != nil || !c.owned.DisableKeepAlives || !c.owned.DisableCompression || c.owned.Protocols == nil || !c.owned.Protocols.HTTP1() || c.owned.Protocols.HTTP2() || c.owned.ForceAttemptHTTP2 {
		t.Fatal("owned GET retry/proxy/protocol policy changed")
	}
	for _, key := range keys {
		row, err := c.Get(context.Background(), key)
		if err != nil || row.Key != key || row.Value == nil || row.Expiration == nil || *row.Expiration != ^uint64(0) {
			t.Fatal("native raw observation", err)
		}
		if key == "data/empty" {
			if len(row.Value) != 0 {
				t.Fatal("empty bytes not present")
			}
		} else if !bytes.Equal(row.Value, []byte{0, 255, 'n', 'u', 'l', 'l'}) {
			t.Fatal("raw bytes coerced")
		}
	}
	if calls.Load() != 4 {
		t.Fatal("unexpected retry")
	}
}
func TestAll404AreErrorsNeverFalseExistsOrMissingBatch(t *testing.T) {
	for _, status := range []int{401, 403, 404, 429, 500} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			var calls atomic.Int32
			c := testClient(t, fixtureOptions(), func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.WriteHeader(status)
				write(t, w, r, []byte(`{"errors":[{"code":10009,"message":"PRIVATE_TOKEN_KEY_CAUSE"}]}`))
			})
			if _, err := c.Get(context.Background(), "data/a"); err == nil || strings.Contains(err.Error(), "PRIVATE") {
				t.Fatal("failure changed into missing/leaked cause")
			}
			if present, err := c.Exists(context.Background(), "data/a"); present || err == nil {
				t.Fatal("ambiguous status became false,nil")
			}
			if rows, err := c.GetMany(context.Background(), []string{"data/a", "data/b"}); err == nil || rows != nil {
				t.Fatal("batch fabricated missing/partial success")
			}
			if calls.Load() != 3 {
				t.Fatal("failed read retried or dispatched later batch key")
			}
		})
	}
}
func TestOrderedBatchSnapshotsKeysIndependentCopiesAndAggregateBudgets(t *testing.T) {
	keys := []string{"data/a", "data/b", "data/a"}
	opts := fixtureOptions()
	opts.Token = func(context.Context) (string, error) { keys[1] = "foreign"; return "owned-read-token", nil }
	var calls atomic.Int32
	c := testClient(t, opts, func(w http.ResponseWriter, r *http.Request) {
		checkRequest(t, r)
		calls.Add(1)
		write(t, w, r, []byte(strings.TrimPrefix(r.URL.Path, "/client/v4/accounts/account/storage/kv/namespaces/namespace/values/")))
	})
	rows, err := c.GetMany(context.Background(), keys)
	if err != nil || len(rows) != 3 || rows[1].Key != "data/b" || rows[2].Key != "data/a" {
		t.Fatal("batch admission changed", err)
	}
	rows[0].Value[0] = 'X'
	if rows[2].Value[0] != 'd' {
		t.Fatal("duplicates share storage")
	}
	before := calls.Load()
	if empty, err := c.GetMany(context.Background(), nil); err != nil || len(empty) != 0 || calls.Load() != before {
		t.Fatal("empty batch performed I/O")
	}
	opts = fixtureOptions()
	opts.MaxResponseBytes = 5
	c2 := testClient(t, opts, func(w http.ResponseWriter, r *http.Request) { calls.Add(1); write(t, w, r, []byte("abc")) })
	before = calls.Load()
	if rows, err := c2.GetMany(context.Background(), []string{"data/a", "data/b", "data/c"}); code(err) != "bounds" || rows != nil || calls.Load() != before+2 {
		t.Fatal("aggregate bytes did not stop later dispatch", err)
	}
}
func TestInputConfigurationEscapingAdmissionBeforeTokenAndHTTP(t *testing.T) {
	var tokens atomic.Int32
	options := fixtureOptions()
	options.Token = func(context.Context) (string, error) { tokens.Add(1); return "owned-read-token", nil }
	options.RoundTripper = transportFunc(func(*http.Request) (*http.Response, error) {
		t.Error("invalid input dispatched HTTP")
		return nil, errors.New("PRIVATE")
	})
	c, err := New(options)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	for _, key := range []string{"", ".", "..", "foreign/a", string([]byte{0xff}), "data/" + strings.Repeat("a", 513)} {
		if _, err := c.Get(context.Background(), key); err == nil {
			t.Fatal("bad native key admitted")
		}
	}
	if _, err := c.ListKeys(context.Background(), ListOptions{Prefix: "foreign", Limit: 10}); err == nil {
		t.Fatal("foreign prefix admitted")
	}
	for _, n := range []int{1, 9, 1001} {
		if _, err := c.ListKeys(context.Background(), ListOptions{Prefix: "data/", Limit: n}); code(err) != "input" {
			t.Fatal("REST minimum ignored")
		}
	}
	small := options
	small.MaxRequestBytes = 10
	c2, err := New(small)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c2.Close() }()
	if _, err := c2.Get(context.Background(), "data/a"); code(err) != "bounds" {
		t.Fatal("escaped URL not admitted preauth")
	}
	if _, err := c2.GetMany(context.Background(), []string{"data/a", "data/b"}); code(err) != "bounds" {
		t.Fatal("aggregate URLs not admitted preauth")
	}
	if tokens.Load() != 0 {
		t.Fatal("invalid descriptors invoked token")
	}
	for _, change := range []func(*Options){func(o *Options) { o.AccountID = "../other" }, func(o *Options) { o.NamespaceID = "" }, func(o *Options) { o.Token = nil }, func(o *Options) { o.AllowedPrefixes = nil }, func(o *Options) { o.AllowedPrefixes = []string{""} }, func(o *Options) { o.AllowedPrefixes = []string{string([]byte{0xff})} }, func(o *Options) { o.Endpoint = "http://127.0.0.1/client/v4" }, func(o *Options) { o.Endpoint = "https://user:secret@example.test/client/v4" }, func(o *Options) { o.Endpoint = "https://example.test/client/v4?secret" }, func(o *Options) { o.Timeout = -1 }, func(o *Options) { o.MaxValueBytes = 26 << 20 }, func(o *Options) { o.CursorTTL = time.Hour }} {
		o := fixtureOptions()
		change(&o)
		if c, err := New(o); err == nil {
			_ = c.Close()
			t.Fatal("unsafe config admitted")
		}
	}
	o := fixtureOptions()
	o.AllowedPrefixes = []string{""}
	o.AllowNamespaceAccess = true
	all, err := New(o)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = all.Close() }()
	o.AllowedPrefixes[0] = "foreign"
	if !all.scoped("data/a") {
		t.Fatal("prefix authority slice not captured")
	}
}
