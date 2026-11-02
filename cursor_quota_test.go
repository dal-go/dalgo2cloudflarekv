package dalgo2cloudflarekv

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestGlobalCursorByteAndNodeRefusalPreservePriorAndRefundTerminal(t *testing.T) {
	for _, nodes := range []bool{false, true} {
		name := "bytes"
		if nodes {
			name = "nodes"
		}
		t.Run(name, func(t *testing.T) {
			o := fixtureOptions()
			if nodes {
				o.MaxCursorNodes = 3
			} else {
				o.MaxCursorBytes = 450
			}
			var calls atomic.Int32
			c := testClient(t, o, func(w http.ResponseWriter, r *http.Request) {
				switch calls.Add(1) {
				case 1:
					listReply(t, w, r, `[]`, `"one"`)
				case 2:
					row, _ := json.Marshal([]map[string]string{{"name": "data/" + strings.Repeat("x", 100)}})
					listReply(t, w, r, string(row), `"two"`)
				case 3:
					listReply(t, w, r, `[]`, `""`)
				default:
					t.Error("hidden quota retry")
				}
			})
			first, err := c.ListKeys(context.Background(), ListOptions{Prefix: "data/", Limit: 10})
			if err != nil {
				t.Fatal(err)
			}
			request := ListOptions{Prefix: "data/", Limit: 10, Cursor: first.NextCursor}
			c.mu.Lock()
			beforeBytes, beforeNodes := c.cursorBytes, c.cursorNodes
			c.mu.Unlock()
			if _, err := c.ListKeys(context.Background(), request); code(err) != "bounds" {
				t.Fatal("retained global quota omitted", err)
			}
			c.mu.Lock()
			state := c.cursors[first.NextCursor]
			if state == nil || state.requests != 2 || c.cursorBytes != beforeBytes || c.cursorNodes != beforeNodes || state.rows != 0 {
				t.Error("quota refusal lost prior/refunded spent work")
			}
			c.mu.Unlock()
			page, err := c.ListKeys(context.Background(), request)
			if err != nil || !page.Complete {
				t.Fatal("rejected successor consumed prior", err)
			}
			c.mu.Lock()
			defer c.mu.Unlock()
			if len(c.cursors) != 0 || c.cursorBytes != 0 || c.cursorNodes != 0 {
				t.Fatal("terminal quota leaked")
			}
		})
	}
}
func TestAtomicConcurrentACKQuotaAndCloseRefund(t *testing.T) {
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	unblock := releaseOnCleanup(t, release)
	o := fixtureOptions()
	o.MaxCursors = 2
	o.MaxCursorNodes = 5
	c := testClient(t, o, func(w http.ResponseWriter, r *http.Request) {
		entered <- struct{}{}
		select {
		case <-release:
		case <-r.Context().Done():
			return
		}
		listReply(t, w, r, `[]`, `"one"`)
	})
	type result struct {
		page ListPage
		err  error
	}
	done := make(chan result, 2)
	for range 2 {
		go func() {
			p, e := c.ListKeys(context.Background(), ListOptions{Prefix: "data/", Limit: 10})
			done <- result{p, e}
		}()
	}
	await(t, entered)
	await(t, entered)
	unblock()
	success, refused := 0, 0
	for range 2 {
		select {
		case r := <-done:
			if r.err == nil {
				success++
				if r.page.NextCursor == "" {
					t.Fatal("cursor not delivered")
				}
			} else if code(r.err) == "bounds" {
				refused++
			} else {
				t.Fatal(r.err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("concurrent ACK hung")
		}
	}
	if success != 1 || refused != 1 {
		t.Fatal("global quota check and ACK were not atomic")
	}
	c.mu.Lock()
	if len(c.cursors) != 1 || c.cursorNodes != 3 {
		t.Error("failed concurrent reservation leaked")
	}
	c.mu.Unlock()
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.cursors) != 0 || c.cursorBytes != 0 || c.cursorNodes != 0 {
		t.Fatal("Close did not refund exact member charges")
	}
}
func TestNetACKReplacementAndFixedTTLRefund(t *testing.T) {
	o := fixtureOptions()
	o.MaxCursors = 1
	o.MaxCursorBytes = 450
	var calls atomic.Int32
	c := testClient(t, o, func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			listReply(t, w, r, `[]`, `"one"`)
		} else {
			listReply(t, w, r, `[]`, `"two"`)
		}
	})
	first, err := c.ListKeys(context.Background(), ListOptions{Prefix: "data/", Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	second, err := c.ListKeys(context.Background(), ListOptions{Prefix: "data/", Limit: 10, Cursor: first.NextCursor})
	if err != nil || second.NextCursor == first.NextCursor {
		t.Fatal("net replacement counted both old/new journals", err)
	}
	if _, err := c.ListKeys(context.Background(), ListOptions{Prefix: "data/", Limit: 10}); code(err) != "bounds" {
		t.Fatal("cursor count ignored")
	}
	o = fixtureOptions()
	o.CursorTTL = 200 * time.Millisecond
	c2 := testClient(t, o, func(w http.ResponseWriter, r *http.Request) { listReply(t, w, r, `[]`, `"one"`) })
	expired, err := c2.ListKeys(context.Background(), ListOptions{Prefix: "data/", Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	timer := time.NewTimer(o.CursorTTL)
	defer timer.Stop()
	<-timer.C
	if _, err := c2.ListKeys(context.Background(), ListOptions{Prefix: "data/", Limit: 10, Cursor: expired.NextCursor}); code(err) != "cursor" {
		t.Fatal("fixed original TTL extended", err)
	}
	c2.mu.Lock()
	defer c2.mu.Unlock()
	if len(c2.cursors) != 0 || c2.cursorBytes != 0 || c2.cursorNodes != 0 {
		t.Fatal("expiry did not refund")
	}
}
