package dalgo2cloudflarekv

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func listReply(t *testing.T, w http.ResponseWriter, r *http.Request, rows, cursor string) {
	t.Helper()
	write(t, w, r, []byte(`{"success":true,"errors":[],"messages":[],"result":`+rows+`,"result_info":{"cursor":`+cursor+`}}`))
}
func TestRESTWholePagesRawMetadataUTF8OrderAndEmptyProgress(t *testing.T) {
	var calls atomic.Int32
	c := testClient(t, fixtureOptions(), func(w http.ResponseWriter, r *http.Request) {
		checkRequest(t, r)
		if r.URL.Path != "/client/v4/accounts/account/storage/kv/namespaces/namespace/keys" || r.URL.Query().Get("prefix") != "data/" || r.URL.Query().Get("limit") != "10" {
			t.Error("list prefix/minimum/authority changed")
		}
		switch calls.Add(1) {
		case 1:
			if r.URL.Query().Get("cursor") != "" {
				t.Error("first cursor not empty")
			}
			listReply(t, w, r, `[{"name":"data/a","metadata":{"wide":9007199254740993.000,"__proto__":{"constructor":"own"},"prototype":true}},{"name":"data/b","metadata":null,"expiration":18446744073709551615}]`, `"private/one%😀"`)
		case 2:
			if r.URL.Query().Get("cursor") != "private/one%😀" {
				t.Error("opaque cursor changed")
			}
			listReply(t, w, r, `[]`, `"private/two"`)
		case 3:
			if r.URL.Query().Get("cursor") != "private/two" {
				t.Error("empty page lost position")
			}
			listReply(t, w, r, `[{"name":"data/\ue000","metadata":[true,null,1.2300E+004]},{"name":"data/😀"}]`, `null`)
		default:
			t.Error("unexpected retry/page")
		}
	})
	first, err := c.ListKeys(context.Background(), ListOptions{Prefix: "data/", Limit: 10})
	if err != nil || first.Complete || first.NextCursor == "" || len(first.Keys) != 2 {
		t.Fatal("short unfinished REST page lost", err)
	}
	if string(first.Keys[0].Metadata) != `{"wide":9007199254740993.000,"__proto__":{"constructor":"own"},"prototype":true}` || string(first.Keys[1].Metadata) != "null" || first.Keys[1].Expiration == nil || *first.Keys[1].Expiration != ^uint64(0) {
		t.Fatal("raw metadata/expiration coerced")
	}
	if strings.Contains(first.NextCursor, "private") {
		t.Fatal("provider cursor exposed")
	}
	second, err := c.ListKeys(context.Background(), ListOptions{Prefix: "data/", Limit: 10, Cursor: first.NextCursor})
	if err != nil || second.Complete || len(second.Keys) != 0 {
		t.Fatal("empty unfinished page treated as EOF", err)
	}
	third, err := c.ListKeys(context.Background(), ListOptions{Prefix: "data/", Limit: 10, Cursor: second.NextCursor})
	if err != nil || !third.Complete || third.NextCursor != "" || len(third.Keys) != 2 || third.Keys[1].Metadata != nil || string(third.Keys[0].Metadata) != `[true,null,1.2300E+004]` {
		t.Fatal("UTF8 ordering/presence/terminal cursor", err)
	}
	if _, err := c.ListKeys(context.Background(), ListOptions{Prefix: "data/", Limit: 10, Cursor: first.NextCursor}); code(err) != "cursor" {
		t.Fatal("old ACKed handle reusable")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.cursors) != 0 || c.cursorBytes != 0 || c.cursorNodes != 0 {
		t.Fatal("terminal journal not refunded")
	}
}
func TestRefusedResumedPageRetainsPriorHandleLastKeyAndSpentAttempts(t *testing.T) {
	for _, bad := range []string{`[{"name":"foreign/secret"}]`, `[{"name":"data/a"}]`, `[{"name":"data/0"}]`} {
		t.Run(bad, func(t *testing.T) {
			var calls atomic.Int32
			c := testClient(t, fixtureOptions(), func(w http.ResponseWriter, r *http.Request) {
				switch calls.Add(1) {
				case 1:
					listReply(t, w, r, `[{"name":"data/a"}]`, `"one"`)
				case 2:
					listReply(t, w, r, bad, `"two"`)
				case 3:
					listReply(t, w, r, `[{"name":"data/b"}]`, `""`)
				default:
					t.Error("hidden retry")
				}
			})
			first, err := c.ListKeys(context.Background(), ListOptions{Prefix: "data/", Limit: 10})
			if err != nil {
				t.Fatal(err)
			}
			request := ListOptions{Prefix: "data/", Limit: 10, Cursor: first.NextCursor}
			if _, err := c.ListKeys(context.Background(), request); err == nil {
				t.Fatal("foreign/backward/duplicate page delivered")
			}
			c.mu.Lock()
			state := c.cursors[first.NextCursor]
			if state == nil || state.requests != 2 || state.pages != 2 || state.rows != 1 || state.last != "data/a" || state.responseBytes == 0 {
				t.Error("failed ACK lost prior position/spent work")
			}
			c.mu.Unlock()
			page, err := c.ListKeys(context.Background(), request)
			if err != nil || !page.Complete || page.Keys[0].Name != "data/b" {
				t.Fatal("prior handle not retryable", err)
			}
		})
	}
}
func TestRepeatedRESTCursorAndBudgetRefusalDoNotACK(t *testing.T) {
	var calls atomic.Int32
	o := fixtureOptions()
	o.MaxPages = 2
	c := testClient(t, o, func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			listReply(t, w, r, `[]`, `"same"`)
		} else {
			listReply(t, w, r, `[]`, `"same"`)
		}
	})
	first, err := c.ListKeys(context.Background(), ListOptions{Prefix: "data/", Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	request := ListOptions{Prefix: "data/", Limit: 10, Cursor: first.NextCursor}
	if _, err := c.ListKeys(context.Background(), request); code(err) != "cursor" {
		t.Fatal("nonprogressing provider cursor accepted", err)
	}
	if _, err := c.ListKeys(context.Background(), request); code(err) != "bounds" || calls.Load() != 2 {
		t.Fatal("journey work reset on failed ACK", err)
	}
	o = fixtureOptions()
	o.MaxRows = 10
	c2 := testClient(t, o, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		listReply(t, w, r, `[{"name":"data/a"}]`, `"one"`)
	})
	first, err = c2.ListKeys(context.Background(), ListOptions{Prefix: "data/", Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	before := calls.Load()
	if _, err := c2.ListKeys(context.Background(), ListOptions{Prefix: "data/", Limit: 10, Cursor: first.NextCursor}); code(err) != "bounds" || calls.Load() != before {
		t.Fatal("remaining rows below REST minimum dispatched or sliced", err)
	}
}
func TestMalformedRESTEnvelopesAndSuccessorRequestRefuseWholePage(t *testing.T) {
	var overRows []map[string]string
	for i := range 11 {
		overRows = append(overRows, map[string]string{"name": "data/" + string(rune('a'+i))})
	}
	over, _ := json.Marshal(overRows)
	for _, body := range []string{
		`{"success":true,"result":` + string(over) + `}`,
		`{"success":true,"success":true,"result":[]}`, `{"success":true,"result":[{"name":"data/\ud800"}]}`, `{"success":true,"errors":[{}],"result":[]}`, `{"success":true,"result":null}`, `{"success":true,"result":[],"result_info":{"count":1}}`,
		`{"success":true,"result":[],"result_info":{"cursor":3}}`, `{"success":true,"result":[{"name":"data/a","expiration":1.5}]}`, `{"success":true,"result":[{"name":"data/a","metadata":"` + strings.Repeat("x", 1025) + `"}]}`,
		`{"success":true,"result":[{"name":"data/a","other":true}]}`, `{"success":true,"result":[],"other":true}`, `{"success":true,"result":[]} {}`, string([]byte{0xff}),
	} {
		c := testClient(t, fixtureOptions(), func(w http.ResponseWriter, r *http.Request) { write(t, w, r, []byte(body)) })
		if _, err := c.ListKeys(context.Background(), ListOptions{Prefix: "data/", Limit: 10}); err == nil {
			t.Fatal("malformed listing accepted")
		}
		c.mu.Lock()
		if len(c.cursors) != 0 || c.cursorBytes != 0 || c.cursorNodes != 0 {
			t.Error("failed firstpage retained invisible journal")
		}
		c.mu.Unlock()
	}
	o := fixtureOptions()
	o.MaxRequestBytes = 300
	c := testClient(t, o, func(w http.ResponseWriter, r *http.Request) {
		cursor, _ := json.Marshal(strings.Repeat("?", 200))
		listReply(t, w, r, `[]`, string(cursor))
	})
	if _, err := c.ListKeys(context.Background(), ListOptions{Prefix: "data/", Limit: 10}); code(err) != "bounds" {
		t.Fatal("unusable successor ACKed", err)
	}
}
func TestCursorAuthorityAndPhysicalBusyUntilIgnoredResumeSettles(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	unblock := releaseOnCleanup(t, release)
	var calls atomic.Int32
	var rotated atomic.Bool
	o := fixtureOptions()
	o.Token = func(context.Context) (string, error) {
		if rotated.Load() {
			return "rotated-scope", nil
		}
		return "owned-read-token", nil
	}
	o.MaxConcurrent = 2
	o.RoundTripper = transportFunc(func(*http.Request) (*http.Response, error) {
		n := calls.Add(1)
		if n == 2 {
			close(entered)
			<-release
		}
		cursor := "one"
		if n >= 2 {
			cursor = "two"
		}
		return response(io.NopCloser(strings.NewReader(`{"success":true,"result":[],"result_info":{"cursor":"` + cursor + `"}}`))), nil
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
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, e := c.ListKeys(ctx, request); done <- e }()
	await(t, entered)
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("ignored resume blocked publiccancel")
	}
	if _, err := c.ListKeys(context.Background(), request); code(err) != "cursor" || calls.Load() != 2 {
		t.Fatal("inflight cursor reused after cancellation")
	}
	unblock()
	waitIdle(t, c)
	next, err := c.ListKeys(context.Background(), request)
	if err != nil || next.NextCursor == "" {
		t.Fatal("prior handle did not reopen", err)
	}
	wrong := request
	wrong.Cursor = next.NextCursor
	wrong.Limit = 11
	if _, err := c.ListKeys(context.Background(), wrong); code(err) != "cursor" {
		t.Fatal("page size rebinding accepted")
	}
	rotated.Store(true)
	wrong = request
	wrong.Cursor = next.NextCursor
	if _, err := c.ListKeys(context.Background(), wrong); code(err) != "cursor" {
		t.Fatal("authscope rebinding accepted", err)
	}
}
