package dalgo2cloudflarekv

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"strings"
	"time"
)

// Metadata retains JSON number lexemes and presence: nil is absent, "null" is
// present JSON null. It is not an atomic observation of a separate value read.
type KeyInfo struct {
	Name       string
	Expiration *uint64
	Metadata   json.RawMessage
}
type ListOptions struct {
	Prefix string
	Limit  int
	Cursor string
}
type ListPage struct {
	Keys       []KeyInfo
	Complete   bool
	NextCursor string
}
type cursor struct {
	prefix        string
	limit         int
	fingerprint   [32]byte
	server        string
	expires       time.Time
	pages         int
	rows          int
	requests      int
	requestBytes  int
	responseBytes int
	work          int
	last          string
	seen          map[string]bool
	tokens        map[string]bool
	busy          bool
	retire        bool
	retainedBytes int
	retainedNodes int
}

func newHandle() (string, error) {
	var b [24]byte
	if _, e := rand.Read(b[:]); e != nil {
		return "", failure("cursor")
	}
	return base64.RawURLEncoding.EncodeToString(b[:]), nil
}
func retention(handle, prefix, server string, seen, tokens map[string]bool) (int, int) {
	bytes, nodes := 256+len(handle)+len(prefix)+len(server)+32, 2
	for key := range seen {
		bytes += 32 + len(key)
		nodes++
	}
	for token := range tokens {
		bytes += 32 + len(token)
		nodes++
	}
	return bytes, nodes
}
func (c *Client) dropCursorLocked(handle string, s *cursor) {
	if c.cursors[handle] == s {
		delete(c.cursors, handle)
		c.cursorBytes -= s.retainedBytes
		c.cursorNodes -= s.retainedNodes
	}
}

// Busy journals remain charged while an admitted operation or abandoned physical
// read still holds their maps. Retirement never extends their original TTL.
func (c *Client) expireLocked() {
	now := time.Now()
	for h, s := range c.cursors {
		if !s.busy && (s.retire || c.closed || !now.Before(s.expires)) {
			c.dropCursorLocked(h, s)
		}
	}
}
func (o *operation) reserveList(request ListOptions) (string, *cursor, bool, error) {
	c := o.client
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := o.check(); err != nil {
		return "", nil, false, err
	}
	c.expireLocked()
	if request.Cursor != "" {
		s, ok := c.cursors[request.Cursor]
		if !ok || s.busy || s.prefix != request.Prefix || s.limit != request.Limit {
			return "", nil, false, failure("cursor")
		}
		s.busy = true
		o.state = s
		return request.Cursor, s, false, nil
	}
	if len(c.cursors) >= c.options.MaxCursors {
		return "", nil, false, failure("bounds")
	}
	handle, err := newHandle()
	if err != nil {
		return "", nil, false, err
	}
	if c.cursors[handle] != nil {
		return "", nil, false, failure("cursor")
	}
	s := &cursor{prefix: strings.Clone(request.Prefix), limit: request.Limit, expires: time.Now().Add(c.options.CursorTTL), seen: make(map[string]bool), tokens: make(map[string]bool), busy: true}
	s.retainedBytes, s.retainedNodes = retention(handle, s.prefix, "", s.seen, s.tokens)
	if s.retainedBytes > c.options.MaxCursorBytes-c.cursorBytes || s.retainedNodes > c.options.MaxCursorNodes-c.cursorNodes {
		return "", nil, false, failure("bounds")
	}
	c.cursors[handle] = s
	c.cursorBytes += s.retainedBytes
	c.cursorNodes += s.retainedNodes
	o.state = s
	return handle, s, true, nil
}
func closedFields(value any, names ...string) (map[string]any, error) {
	m, ok := value.(map[string]any)
	if !ok {
		return nil, failure("protocol")
	}
	for key := range m {
		found := false
		for _, name := range names {
			if key == name {
				found = true
				break
			}
		}
		if !found {
			return nil, failure("protocol")
		}
	}
	return m, nil
}
func parseList(o *operation, raw []byte, limit int) ([]KeyInfo, string, error) {
	value, err := strictJSON(o, raw)
	if err != nil {
		return nil, "", err
	}
	top, err := closedFields(value, "success", "errors", "messages", "result", "result_info")
	if err != nil {
		return nil, "", err
	}
	if top["success"] != true {
		return nil, "", failure("protocol")
	}
	if e, ok := top["errors"]; ok {
		items, ok := e.([]any)
		if !ok || len(items) != 0 {
			return nil, "", failure("protocol")
		}
	}
	if m, ok := top["messages"]; ok {
		if _, ok = m.([]any); !ok {
			return nil, "", failure("protocol")
		}
	}
	values, ok := top["result"].([]any)
	if !ok || len(values) > limit {
		return nil, "", failure("protocol")
	}
	cursorText := ""
	if info, ok := top["result_info"]; ok {
		m, e := closedFields(info, "count", "cursor")
		if e != nil {
			return nil, "", e
		}
		if count, ok := m["count"]; ok {
			n, ok := count.(json.Number)
			if !ok {
				return nil, "", failure("protocol")
			}
			size, e := unsigned(string(n))
			if e != nil || size != uint64(len(values)) {
				return nil, "", failure("protocol")
			}
		}
		if token, ok := m["cursor"]; ok && token != nil {
			cursorText, ok = token.(string)
			if !ok || !validCursor(cursorText) {
				return nil, "", failure("protocol")
			}
		}
	}
	// Preserve each ORIGINAL metadata JSON value after strict whole-wire duplicate,
	// UTF8, depth and node validation. No float64 decoding or JSON round trip.
	var rawTop map[string]json.RawMessage
	if json.Unmarshal(raw, &rawTop) != nil {
		return nil, "", failure("protocol")
	}
	var rawKeys []json.RawMessage
	if json.Unmarshal(rawTop["result"], &rawKeys) != nil {
		return nil, "", failure("protocol")
	}
	out := make([]KeyInfo, len(values))
	for i, value := range values {
		if err := o.check(); err != nil {
			return nil, "", err
		}
		m, e := closedFields(value, "name", "expiration", "metadata")
		if e != nil {
			return nil, "", e
		}
		name, ok := m["name"].(string)
		if !ok || !validKey(name) {
			return nil, "", failure("protocol")
		}
		out[i].Name = name
		if expiration, ok := m["expiration"]; ok {
			n, ok := expiration.(json.Number)
			if !ok {
				return nil, "", failure("protocol")
			}
			v, e := unsigned(string(n))
			if e != nil {
				return nil, "", e
			}
			out[i].Expiration = &v
		}
		if _, ok := m["metadata"]; ok {
			var fields map[string]json.RawMessage
			if json.Unmarshal(rawKeys[i], &fields) != nil {
				return nil, "", failure("protocol")
			}
			metadata := fields["metadata"]
			if len(metadata) > 1024 {
				return nil, "", failure("bounds")
			}
			out[i].Metadata = append(json.RawMessage(nil), metadata...)
		}
	}
	return out, cursorText, nil
}

// ListKeys returns WHOLE REST pages. There is no logical-limit slicing, value
// lookup, snapshot guarantee, public server cursor, or key-list-based Exists.
func (c *Client) ListKeys(ctx context.Context, request ListOptions) (ListPage, error) {
	var zero ListPage
	o, err := c.begin(ctx)
	if err != nil {
		return zero, err
	}
	defer o.finish()
	if request.Limit == 0 {
		request.Limit = 100
	}
	if request.Limit < 10 || request.Limit > 1000 || !validText(request.Cursor, 128, true) {
		return zero, failure("input")
	}
	if err := c.prefix(request.Prefix); err != nil {
		return zero, err
	}
	handle, state, first, err := o.reserveList(request)
	if err != nil {
		return zero, err
	}
	committed := false
	defer func() {
		if first && !committed {
			c.mu.Lock()
			state.retire = true
			c.expireLocked()
			c.mu.Unlock()
		}
	}()
	c.mu.Lock()
	allowed := state.pages < c.options.MaxPages && state.requests < c.options.MaxRequests && request.Limit <= c.options.MaxRows-state.rows && request.Limit <= c.options.MaxWork-state.work
	c.mu.Unlock()
	if !allowed {
		return zero, failure("bounds")
	}
	endpoint := c.listURL(request.Prefix, request.Limit, state.server)
	if err = o.preflightURLs([]string{endpoint}); err != nil {
		return zero, err
	}
	token, auth, err := o.auth()
	if err != nil {
		return zero, err
	}
	fingerprint := hashScope("list", c.root, request.Prefix, string(auth[:]))
	c.mu.Lock()
	if !first && state.fingerprint != fingerprint {
		c.mu.Unlock()
		return zero, failure("cursor")
	}
	if first {
		state.fingerprint = fingerprint
	}
	state.pages++
	c.mu.Unlock()
	raw, err := o.readURL(endpoint, token, c.options.MaxResponseBytes, request.Limit)
	if err != nil {
		return zero, err
	}
	keys, next, err := parseList(o, raw.body, request.Limit)
	if err != nil {
		return zero, err
	}
	c.mu.Lock()
	last := state.last
	seen := state.seen
	tokens := state.tokens
	repeated := next != "" && (next == state.server || tokens[next])
	c.mu.Unlock()
	if repeated {
		return zero, failure("cursor")
	}
	for _, key := range keys {
		if !strings.HasPrefix(key.Name, request.Prefix) || !c.scoped(key.Name) || last != "" && key.Name <= last || seen[key.Name] {
			return zero, failure("protocol")
		}
		last = key.Name
	}
	successor := ""
	if next != "" {
		if err := o.preflightURLs([]string{c.listURL(request.Prefix, request.Limit, next)}); err != nil {
			return zero, err
		}
		successor, err = newHandle()
		if err != nil {
			return zero, err
		}
	}
	// Calculate net retained cost BEFORE allocating cloned journal maps. Only
	// successfully ACKed names/tokens enter the replacement; spent work stays.
	nextBytes, nextNodes := retention(successor, state.prefix, next, seen, tokens)
	for _, key := range keys {
		nextBytes += 32 + len(key.Name)
		nextNodes++
	}
	if next != "" && !tokens[next] {
		nextBytes += 32 + len(next)
		nextNodes++
	}
	page := ListPage{Keys: keys, Complete: next == "", NextCursor: successor}
	if err := o.check(); err != nil {
		return zero, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.cursors[handle] != state || !time.Now().Before(state.expires) || successor != "" && c.cursors[successor] != nil {
		return zero, failure("cursor")
	}
	if next != "" {
		if nextBytes > c.options.MaxCursorBytes-(c.cursorBytes-state.retainedBytes) || nextNodes > c.options.MaxCursorNodes-(c.cursorNodes-state.retainedNodes) {
			return zero, failure("bounds")
		}
		nextSeen := make(map[string]bool, len(seen)+len(keys))
		for key := range seen {
			nextSeen[key] = true
		}
		for _, key := range keys {
			nextSeen[key.Name] = true
		}
		nextTokens := make(map[string]bool, len(tokens)+1)
		for token := range tokens {
			nextTokens[token] = true
		}
		nextTokens[next] = true
		// Copies and caller context callbacks can cross either deadline.
		if err := o.checkACKLocked(handle, state, successor); err != nil {
			return zero, err
		}
		c.dropCursorLocked(handle, state)
		state.seen = nextSeen
		state.tokens = nextTokens
		state.server = next
		state.last = last
		state.rows += len(keys)
		state.retainedBytes = nextBytes
		state.retainedNodes = nextNodes
		c.cursors[successor] = state
		c.cursorBytes += nextBytes
		c.cursorNodes += nextNodes
	} else {
		if err := o.checkACKLocked(handle, state, ""); err != nil {
			return zero, err
		}
		c.dropCursorLocked(handle, state)
	}
	committed = true
	return page, nil
}

// checkACKLocked is the last fallible boundary before either successor or
// terminal acknowledgement. Observe the caller first, then the ORIGINAL journey
// expiry/membership; a context callback itself may wait across that expiry.
func (o *operation) checkACKLocked(handle string, state *cursor, successor string) error {
	if err := o.check(); err != nil {
		return err
	}
	c := o.client
	if c.cursors[handle] != state || !time.Now().Before(state.expires) || successor != "" && c.cursors[successor] != nil {
		return failure("cursor")
	}
	return nil
}
