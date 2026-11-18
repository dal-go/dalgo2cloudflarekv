package dalgo2cloudflarekv

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// The callback observes a real response-close event and the ACK critical
// section, not an Err-call count. It waits for the actual original expiry and
// still returns the real parent error (nil); the per-call deadline remains later.
type expiryAtACKContext struct {
	context.Context
	client     *Client
	cleaned    *atomic.Bool
	expires    time.Time
	entered    atomic.Bool
	expired    atomic.Bool
	boundedOut atomic.Bool
	once       sync.Once
}

func (c *expiryAtACKContext) Err() error {
	if c.cleaned.Load() {
		if c.client.mu.TryLock() {
			c.client.mu.Unlock()
		} else {
			c.once.Do(func() {
				c.entered.Store(true)
				timer := time.NewTimer(time.Until(c.expires))
				defer timer.Stop()
				bound := time.NewTimer(2 * time.Second)
				defer bound.Stop()
				select {
				case <-timer.C:
					c.expired.Store(!time.Now().Before(c.expires))
				case <-bound.C:
					c.boundedOut.Store(true)
				}
			})
		}
	}
	return c.Context.Err()
}

func TestFinalACKRechecksOriginalJourneyExpiryAfterCallerContextCallback(t *testing.T) {
	for _, terminal := range []bool{false, true} {
		name := "successor"
		if terminal {
			name = "terminal"
		}
		t.Run(name, func(t *testing.T) {
			var calls, closes atomic.Int32
			var cleaned atomic.Bool
			options := fixtureOptions()
			options.Timeout = 3 * time.Second
			options.CursorTTL = 500 * time.Millisecond
			options.RoundTripper = transportFunc(func(*http.Request) (*http.Response, error) {
				n := calls.Add(1)
				payload := `{"success":true,"result":[]}`
				if n == 1 {
					payload = `{"success":true,"result":[{"name":"data/a"}],"result_info":{"cursor":"one"}}`
				}
				if n == 2 {
					next := `"two"`
					if terminal {
						next = `""`
					}
					payload = `{"success":true,"result":[{"name":"data/b"}],"result_info":{"cursor":` + next + `}}`
				}
				return response(closeCallback{Reader: strings.NewReader(payload), close: func() error {
					if n == 2 {
						closes.Add(1)
						cleaned.Store(true)
					}
					return nil
				}}), nil
			})
			client, err := New(options)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = client.Close() }()
			first, err := client.ListKeys(context.Background(), ListOptions{Prefix: "data/", Limit: 10})
			if err != nil || len(first.Keys) != 1 || first.Keys[0].Name != "data/a" || first.NextCursor == "" {
				t.Fatal("initial acknowledged page", err)
			}
			client.mu.Lock()
			state := client.cursors[first.NextCursor]
			if state == nil {
				client.mu.Unlock()
				t.Fatal("original journal unavailable")
			}
			expires := state.expires
			client.mu.Unlock()
			ctx := &expiryAtACKContext{Context: context.Background(), client: client, cleaned: &cleaned, expires: expires}
			page, err := client.ListKeys(ctx, ListOptions{Prefix: "data/", Limit: 10, Cursor: first.NextCursor})
			if code(err) != "cursor" || page.Keys != nil || page.NextCursor != "" || page.Complete || !ctx.entered.Load() || !ctx.expired.Load() || ctx.boundedOut.Load() || ctx.Context.Err() != nil {
				t.Fatal("expired final successor/terminal ACK delivered, or real boundary not entered", err)
			}
			if closes.Load() != 1 || calls.Load() != 2 {
				t.Fatal("cleanup repeated or hidden retry")
			}
			client.mu.Lock()
			if state.expires != expires || state.rows != 1 || state.last != "data/a" || state.server != "one" || state.seen["data/b"] || state.tokens["two"] || state.pages != 2 || state.requests != 2 || state.responseBytes == 0 {
				t.Error("late refusal changed original expiry/ACK or refunded spent work")
			}
			if len(client.cursors) != 0 || client.cursorBytes != 0 || client.cursorNodes != 0 {
				t.Error("settled expired journal not refunded")
			}
			client.mu.Unlock()
			if _, err := client.ListKeys(context.Background(), ListOptions{Prefix: "data/", Limit: 10, Cursor: first.NextCursor}); code(err) != "cursor" {
				t.Fatal("expired original handle resurrected")
			}
			fresh, err := client.ListKeys(context.Background(), ListOptions{Prefix: "data/", Limit: 10})
			if err != nil || !fresh.Complete || len(fresh.Keys) != 0 || calls.Load() != 3 {
				t.Fatal("fresh journey did not reopen after expiry refusal", err)
			}
		})
	}
}

func TestBusyExpiredACKedJournalRetainsGlobalQuotaUntilPhysicalSettlement(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	unblock := releaseOnCleanup(t, release)
	var calls atomic.Int32
	late := &ownedBody{reader: strings.NewReader(`{"success":true,"result":[{"name":"data/b"}],"result_info":{"cursor":"two"}}`)}
	options := fixtureOptions()
	options.MaxConcurrent = 2
	options.MaxCursors = 2
	options.MaxCursorBytes = 450
	options.MaxCursorNodes = 4
	options.CursorTTL = 500 * time.Millisecond
	options.RoundTripper = transportFunc(func(*http.Request) (*http.Response, error) {
		switch calls.Add(1) {
		case 1:
			return response(io.NopCloser(strings.NewReader(`{"success":true,"result":[{"name":"data/a"}],"result_info":{"cursor":"one"}}`))), nil
		case 2:
			close(entered)
			<-release
			return response(late), nil
		default:
			return response(io.NopCloser(strings.NewReader(`{"success":true,"result":[{"name":"data/a"}],"result_info":{"cursor":""}}`))), nil
		}
	})
	client, err := New(options)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	first, err := client.ListKeys(context.Background(), ListOptions{Prefix: "data/", Limit: 10})
	if err != nil || first.NextCursor == "" || len(first.Keys) != 1 || first.Keys[0].Name != "data/a" {
		t.Fatal("initial acknowledged native page", err)
	}
	client.mu.Lock()
	state := client.cursors[first.NextCursor]
	if state == nil {
		client.mu.Unlock()
		t.Fatal("ACKed journal expired before fixture admission")
	}
	expires := state.expires
	beforeBytes, beforeNodes := client.cursorBytes, client.cursorNodes
	client.mu.Unlock()
	request := ListOptions{Prefix: "data/", Limit: 10, Cursor: first.NextCursor}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, e := client.ListKeys(ctx, request); done <- e }()
	await(t, entered)
	cancel()
	select {
	case e := <-done:
		if !errors.Is(e, context.Canceled) {
			t.Fatal(e)
		}
	case <-time.After(time.Second):
		t.Fatal("pending resume blocked public cancellation")
	}
	// Wait for the actual original journal deadline, without modifying its clock,
	// TTL, quota or last ACKed key. The ignored transport remains physically live.
	timer := time.NewTimer(time.Until(expires))
	defer timer.Stop()
	<-timer.C
	if _, err := client.ListKeys(context.Background(), ListOptions{Prefix: "data/", Limit: 10}); code(err) != "bounds" {
		t.Fatal("expired busy journal quota reused by another journey", err)
	}
	client.mu.Lock()
	held := client.cursors[first.NextCursor]
	if held != state || !state.busy || state.expires != expires || state.last != "data/a" || state.rows != 1 || state.requests != 2 || client.cursorBytes != beforeBytes || client.cursorNodes != beforeNodes {
		t.Error("expiry refunded retained state, advanced ACK or reset spent work")
	}
	client.mu.Unlock()
	if calls.Load() != 2 {
		t.Fatal("refused new journey dispatched HTTP")
	}
	unblock()
	waitIdle(t, client)
	if late.closed.Load() != 1 {
		t.Fatal("late response body not closed exactly once")
	}
	client.mu.Lock()
	if len(client.cursors) != 0 || client.cursorBytes != 0 || client.cursorNodes != 0 {
		t.Error("settled expired journal quota not refunded")
	}
	client.mu.Unlock()
	if _, err := client.ListKeys(context.Background(), request); code(err) != "cursor" {
		t.Fatal("expired handle resurrected")
	}
	fresh, err := client.ListKeys(context.Background(), ListOptions{Prefix: "data/", Limit: 10})
	if err != nil || !fresh.Complete || len(fresh.Keys) != 1 || fresh.Keys[0].Name != "data/a" || calls.Load() != 3 {
		t.Fatal("new journey did not reopen after physical settlement", err)
	}
}

func TestAbandonedFirstPageAndCloseRetireOnlyAfterPhysicalSettlement(t *testing.T) {
	for _, closeClient := range []bool{false, true} {
		name := "abandoned"
		if closeClient {
			name = "closed"
		}
		t.Run(name, func(t *testing.T) {
			entered, release := make(chan struct{}), make(chan struct{})
			unblock := releaseOnCleanup(t, release)
			var calls atomic.Int32
			late := &ownedBody{reader: strings.NewReader(`{"success":true,"result":[],"result_info":{"cursor":"one"}}`)}
			options := fixtureOptions()
			options.MaxConcurrent = 2
			options.MaxCursors = 2
			options.MaxCursorBytes = 450
			options.RoundTripper = transportFunc(func(*http.Request) (*http.Response, error) {
				if calls.Add(1) == 1 {
					close(entered)
					<-release
					return response(late), nil
				}
				return response(io.NopCloser(strings.NewReader(`{"success":true,"result":[]}`))), nil
			})
			client, err := New(options)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = client.Close() }()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() { _, e := client.ListKeys(ctx, ListOptions{Prefix: "data/", Limit: 10}); done <- e }()
			await(t, entered)
			cancel()
			select {
			case e := <-done:
				if !errors.Is(e, context.Canceled) {
					t.Fatal(e)
				}
			case <-time.After(time.Second):
				t.Fatal("first-page cancellation blocked")
			}
			if closeClient {
				if err := client.Close(); err != nil {
					t.Fatal(err)
				}
			} else if _, err := client.ListKeys(context.Background(), ListOptions{Prefix: "data/", Limit: 10}); code(err) != "bounds" {
				t.Fatal("invisible pending journal quota reused", err)
			}
			client.mu.Lock()
			if len(client.cursors) != 1 || client.cursorBytes == 0 || client.cursorNodes == 0 {
				t.Error("pending retired journal was refunded early")
			}
			client.mu.Unlock()
			unblock()
			waitIdle(t, client)
			client.mu.Lock()
			if len(client.cursors) != 0 || client.cursorBytes != 0 || client.cursorNodes != 0 {
				t.Error("retired journal leaked after physical settlement")
			}
			client.mu.Unlock()
			if late.closed.Load() != 1 {
				t.Fatal("late first-page body cleanup count")
			}
			if !closeClient {
				if page, err := client.ListKeys(context.Background(), ListOptions{Prefix: "data/", Limit: 10}); err != nil || !page.Complete {
					t.Fatal("abandoned reservation did not reopen", err)
				}
			}
		})
	}
}
