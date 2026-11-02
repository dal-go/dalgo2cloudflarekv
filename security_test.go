package dalgo2cloudflarekv

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestOwnedVerifiedTLSNegativeAndRedirectNeverFollowed(t *testing.T) {
	var targets atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { targets.Add(1); w.WriteHeader(500) }))
	t.Cleanup(target.Close)
	c := testClient(t, fixtureOptions(), func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Location", target.URL+"/PRIVATE_TOKEN")
		w.WriteHeader(307)
	})
	if _, err := c.Get(context.Background(), "data/a"); code(err) != "http" || strings.Contains(err.Error(), "PRIVATE") {
		t.Fatal("redirect accepted/leaked", err)
	}
	if targets.Load() != 0 {
		t.Fatal("redirect followed")
	}
	untrusted := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { targets.Add(1); w.WriteHeader(500) }))
	t.Cleanup(untrusted.Close)
	o := fixtureOptions()
	o.Endpoint = untrusted.URL + "/client/v4"
	other, err := New(o)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = other.Close() }()
	if _, err := other.Get(context.Background(), "data/a"); code(err) != "transport" || targets.Load() != 0 {
		t.Fatal("untrusted CA reached HTTP", err)
	}
}
func TestRawWireHeadersCapsAndCredentialFailuresAreRedacted(t *testing.T) {
	for _, tc := range []struct{ header, value, body, expected string }{{"Content-Encoding", "gzip", "raw", "protocol"}, {"Content-Length", "-1", "raw", "protocol"}, {"Content-Length", "4", "raw", "bounds"}, {"Expiration", "1.5", "raw", "protocol"}, {"Expiration", "18446744073709551616", "raw", "protocol"}, {"", "", "four", "bounds"}} {
		o := fixtureOptions()
		o.MaxValueBytes = 3
		var closed atomic.Int32
		o.RoundTripper = transportFunc(func(*http.Request) (*http.Response, error) {
			r := response(closeCallback{Reader: strings.NewReader(tc.body), close: func() error { closed.Add(1); return nil }})
			if tc.header != "" {
				r.Header.Set(tc.header, tc.value)
			}
			return r, nil
		})
		c, err := New(o)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := c.Get(context.Background(), "data/a"); code(err) != tc.expected || closed.Load() != 1 {
			t.Fatal("wire cap/header/owned cleanup", err)
		}
		_ = c.Close()
	}
	var calls atomic.Int32
	for _, token := range []string{"", "Bearer token", "bad\nPRIVATE", strings.Repeat("x", 8193)} {
		o := fixtureOptions()
		o.Token = func(context.Context) (string, error) { return token, nil }
		o.RoundTripper = transportFunc(func(*http.Request) (*http.Response, error) { calls.Add(1); return nil, errors.New("PRIVATE") })
		c, err := New(o)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := c.Get(context.Background(), "data/a"); code(err) != "authentication" || strings.Contains(err.Error(), "PRIVATE") {
			t.Fatal("unsafe token admitted")
		}
		_ = c.Close()
	}
	if calls.Load() != 0 {
		t.Fatal("invalid credentials dispatched")
	}
}

type closeCallback struct {
	io.Reader
	close func() error
}

func (c closeCallback) Close() error { return c.close() }
func TestReadAndCursorFinalCleanupCancellationHasNoLateDeliveryOrACK(t *testing.T) {
	var calls atomic.Int32
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	o := fixtureOptions()
	o.RoundTripper = transportFunc(func(*http.Request) (*http.Response, error) {
		n := calls.Add(1)
		next := "one"
		if n > 1 {
			next = "two"
		}
		return response(closeCallback{Reader: strings.NewReader(`{"success":true,"result":[],"result_info":{"cursor":"` + next + `"}}`), close: func() error {
			if n == 2 {
				cancel(errors.New("PRIVATE_CLEANUP"))
			}
			return nil
		}}), nil
	})
	c, err := New(o)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	first, err := c.ListKeys(context.Background(), ListOptions{Prefix: "data/", Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	request := ListOptions{Prefix: "data/", Limit: 10, Cursor: first.NextCursor}
	if page, err := c.ListKeys(ctx, request); !errors.Is(err, context.Canceled) || page.NextCursor != "" {
		t.Fatal("cleanup cancellation ACKed successor", err)
	}
	waitIdle(t, c)
	c.mu.Lock()
	s := c.cursors[first.NextCursor]
	if s == nil || s.requests != 2 || s.pages != 2 {
		t.Error("cleanup failure lost old handle/spent work")
	}
	c.mu.Unlock()
	if page, err := c.ListKeys(context.Background(), request); err != nil || page.NextCursor == "" {
		t.Fatal("old cursor not retryable after cleanup cancellation", err)
	}
}
func TestCallerAndExpiredDeadlineBeforeAnyCredentialOrHTTP(t *testing.T) {
	var calls atomic.Int32
	o := fixtureOptions()
	o.Token = func(context.Context) (string, error) { calls.Add(1); return "owned-read-token", nil }
	c, err := New(o)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	caller, cancel := context.WithCancelCause(context.Background())
	cancel(errors.New("PRIVATE"))
	expired, stop := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer stop()
	for _, tc := range []struct {
		ctx      context.Context
		expected error
	}{{caller, context.Canceled}, {expired, context.DeadlineExceeded}} {
		if _, err := c.Get(tc.ctx, "data/a"); !errors.Is(err, tc.expected) {
			t.Fatal(err)
		}
		if _, err := c.GetMany(tc.ctx, []string{"data/a"}); !errors.Is(err, tc.expected) {
			t.Fatal(err)
		}
		if _, err := c.ListKeys(tc.ctx, ListOptions{Prefix: "data/", Limit: 10}); !errors.Is(err, tc.expected) {
			t.Fatal(err)
		}
	}
	if calls.Load() != 0 {
		t.Fatal("preaborted caller invoked credentials")
	}
}
