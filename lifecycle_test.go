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

func releaseOnCleanup(t *testing.T, ch chan struct{}) func() {
	t.Helper()
	var once sync.Once
	f := func() { once.Do(func() { close(ch) }) }
	t.Cleanup(f)
	return f
}

func TestOwnedTLSDispatchedCallerDeadlineDisconnectAndReopen(t *testing.T) {
	for _, deadline := range []bool{false, true} {
		name := "caller"
		if deadline {
			name = "deadline"
		}
		t.Run(name, func(t *testing.T) {
			entered, disconnected, cleanup := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var slow atomic.Bool
			slow.Store(true)
			c := testClient(t, fixtureOptions(), func(w http.ResponseWriter, r *http.Request) {
				checkRequest(t, r)
				if slow.Swap(false) {
					close(entered)
					select {
					case <-r.Context().Done():
						close(disconnected)
					case <-cleanup:
					}
					return
				}
				write(t, w, r, []byte{0, 255})
			})
			t.Cleanup(func() { close(cleanup) })
			ctx, cancel := context.WithCancelCause(context.Background())
			defer cancel(nil)
			if deadline {
				c.options.Timeout = 300 * time.Millisecond
			}
			done := make(chan error, 1)
			go func() { _, err := c.Get(ctx, "data/a"); done <- err }()
			await(t, entered)
			start := time.Now()
			if !deadline {
				cancel(errors.New("PRIVATE_CALLER_CAUSE"))
			}
			select {
			case err := <-done:
				expected := context.Canceled
				if deadline {
					expected = context.DeadlineExceeded
				}
				if !errors.Is(err, expected) || strings.Contains(err.Error(), "PRIVATE") {
					t.Fatal("dispatched classification", err)
				}
			case <-time.After(750 * time.Millisecond):
				t.Fatal("cancellation delayed")
			}
			if time.Since(start) > 750*time.Millisecond {
				t.Fatal("prompt budget exceeded")
			}
			await(t, disconnected)
			waitIdle(t, c)
			if row, err := c.Get(context.Background(), "data/a"); err != nil || len(row.Value) != 2 {
				t.Fatal("read after cancellation", err)
			}
		})
	}
}
func TestIgnoredTokenRetainsAdmissionAndNeverDispatchesLateCredentials(t *testing.T) {
	for _, deadline := range []bool{false, true} {
		started, release := make(chan struct{}), make(chan struct{})
		unblock := releaseOnCleanup(t, release)
		var tokens, httpCalls atomic.Int32
		o := fixtureOptions()
		o.MaxConcurrent = 1
		if deadline {
			o.Timeout = 25 * time.Millisecond
		}
		o.Token = func(context.Context) (string, error) {
			if tokens.Add(1) == 1 {
				close(started)
				<-release
			}
			return "owned-read-token", nil
		}
		o.RoundTripper = transportFunc(func(*http.Request) (*http.Response, error) {
			httpCalls.Add(1)
			return response(io.NopCloser(strings.NewReader("bytes"))), nil
		})
		c, err := New(o)
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { _, e := c.Get(ctx, "data/a"); done <- e }()
		await(t, started)
		if !deadline {
			cancel()
		}
		select {
		case err := <-done:
			expected := context.Canceled
			if deadline {
				expected = context.DeadlineExceeded
			}
			if !errors.Is(err, expected) {
				t.Fatal(err)
			}
		case <-time.After(time.Second):
			t.Fatal("ignored token blocked public cancellation")
		}
		for range 3 {
			if _, err := c.Get(context.Background(), "data/a"); code(err) != "busy" {
				t.Fatal("physical credential admission released", err)
			}
		}
		if tokens.Load() != 1 || httpCalls.Load() != 0 {
			t.Fatal("extra credentials/HTTP admitted")
		}
		unblock()
		waitIdle(t, c)
		if _, err := c.Get(context.Background(), "data/a"); err != nil || httpCalls.Load() != 1 {
			t.Fatal("admission did not reopen", err)
		}
		cancel()
		_ = c.Close()
	}
}

type ownedBody struct {
	reader       io.Reader
	readStarted  chan struct{}
	readRelease  <-chan struct{}
	closeStarted chan struct{}
	closeRelease <-chan struct{}
	readOnce     sync.Once
	closeOnce    sync.Once
	closed       atomic.Int32
	closeErr     error
}

func (b *ownedBody) Read(p []byte) (int, error) {
	if b.readStarted != nil {
		b.readOnce.Do(func() { close(b.readStarted) })
	}
	if b.readRelease != nil {
		<-b.readRelease
	}
	return b.reader.Read(p)
}
func (b *ownedBody) Close() error {
	b.closed.Add(1)
	if b.closeStarted != nil {
		b.closeOnce.Do(func() { close(b.closeStarted) })
	}
	if b.closeRelease != nil {
		<-b.closeRelease
	}
	return b.closeErr
}
func TestLateRoundTripOwnsExactBodyAndKeepsBusyUntilSettlement(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	unblock := releaseOnCleanup(t, release)
	body := &ownedBody{reader: strings.NewReader("late")}
	var calls atomic.Int32
	o := fixtureOptions()
	o.MaxConcurrent = 1
	o.RoundTripper = transportFunc(func(*http.Request) (*http.Response, error) {
		if calls.Add(1) == 1 {
			close(started)
			<-release
			return response(body), nil
		}
		return response(io.NopCloser(strings.NewReader("ok"))), nil
	})
	c, err := New(o)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, e := c.Get(ctx, "data/a"); done <- e }()
	await(t, started)
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("late transport blocked cancellation")
	}
	if _, err := c.Get(context.Background(), "data/a"); code(err) != "busy" {
		t.Fatal("late transport lost physical lease")
	}
	unblock()
	waitIdle(t, c)
	if body.closed.Load() != 1 {
		t.Fatal("exact late body not closed once")
	}
	if _, err := c.Get(context.Background(), "data/a"); err != nil {
		t.Fatal(err)
	}
}
func TestIgnoredReadAndCloseStayAdmittedWithoutBlockingDeadlineOrClose(t *testing.T) {
	for _, blockClose := range []bool{false, true} {
		name := "read"
		if blockClose {
			name = "close"
		}
		t.Run(name, func(t *testing.T) {
			started, release := make(chan struct{}), make(chan struct{})
			unblock := releaseOnCleanup(t, release)
			body := &ownedBody{reader: strings.NewReader("ok")}
			if blockClose {
				body.closeStarted = started
				body.closeRelease = release
			} else {
				body.readStarted = started
				body.readRelease = release
			}
			o := fixtureOptions()
			o.MaxConcurrent = 1
			o.RoundTripper = transportFunc(func(*http.Request) (*http.Response, error) { return response(body), nil })
			c, err := New(o)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() { _, e := c.Get(ctx, "data/a"); done <- e }()
			await(t, started)
			cancel()
			select {
			case err := <-done:
				if !errors.Is(err, context.Canceled) {
					t.Fatal(err)
				}
			case <-time.After(time.Second):
				t.Fatal("hostile read/cleanup delayed return")
			}
			if _, err := c.Get(context.Background(), "data/a"); code(err) != "busy" {
				t.Fatal("hostile physical work admission was reused")
			}
			closed := make(chan struct{})
			go func() { _ = c.Close(); close(closed) }()
			await(t, closed)
			unblock()
			waitIdle(t, c)
			if body.closed.Load() != 1 {
				t.Fatal("body cleanup repeated")
			}
			if _, err := c.Get(context.Background(), "data/a"); code(err) != "closed" {
				t.Fatal("closed client reopened")
			}
		})
	}
}
func TestFailureCleanupCancellationAndSecretRedaction(t *testing.T) {
	for _, mode := range []string{"token", "transport", "status", "body", "close"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancelCause(context.Background())
			defer cancel(nil)
			o := fixtureOptions()
			if mode == "token" {
				o.Token = func(context.Context) (string, error) {
					cancel(errors.New("PRIVATE_TOKEN_CAUSE"))
					return "", errors.New("PRIVATE")
				}
			}
			body := &ownedBody{reader: strings.NewReader("ok")}
			if mode == "close" {
				body.closeErr = errors.New("PRIVATE_CLOSE_CAUSE")
			}
			o.RoundTripper = transportFunc(func(*http.Request) (*http.Response, error) {
				switch mode {
				case "transport":
					cancel(errors.New("PRIVATE"))
					return response(body), errors.New("PRIVATE_TRANSPORT")
				case "status":
					r := response(body)
					r.StatusCode = 404
					return r, nil
				case "body":
					return response(errorReader{}), nil
				}
				return response(body), nil
			})
			c, err := New(o)
			if err != nil {
				t.Fatal(err)
			}
			_, err = c.Get(ctx, "data/a")
			if err == nil || strings.Contains(err.Error(), "PRIVATE") {
				t.Fatal("failure leaked cause or succeeded")
			}
			if (mode == "token" || mode == "transport") && !errors.Is(err, context.Canceled) {
				t.Fatal("caller priority lost", err)
			}
			waitIdle(t, c)
			_ = c.Close()
		})
	}
}

type errorReader struct{}

func (errorReader) Read([]byte) (int, error) { return 0, errors.New("PRIVATE_READ_ERROR") }
func (errorReader) Close() error             { return nil }
