package dalgo2cloudflarekv

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

// Error contains only a safe local classification and optional HTTP status.
// It never retains provider text, credentials, keys, URLs or cancellation causes.
type Error struct {
	Code   string
	Status int
}

func (e *Error) Error() string  { return "cloudflare KV read: " + e.Code }
func failure(code string) error { return &Error{Code: code} }

type TokenProvider func(context.Context) (string, error)
type Options struct {
	AccountID            string
	NamespaceID          string
	CollectionAlias      string
	AllowedPrefixes      []string
	AllowNamespaceAccess bool
	Token                TokenProvider
	Timeout              time.Duration
	MaxConcurrent        int
	MaxKeys              int
	MaxRows              int
	MaxPages             int
	MaxRequests          int
	MaxValueBytes        int
	MaxResponseBytes     int
	MaxRequestBytes      int
	MaxWork              int
	MaxCursors           int
	MaxCursorBytes       int
	MaxCursorNodes       int
	CursorTTL            time.Duration
	RootCAs              *x509.CertPool
	// Endpoint is trusted infrastructure, default https://api.cloudflare.com/client/v4.
	// Requests cannot supply or override it. Only a verified HTTPS root is accepted.
	Endpoint string
	// RoundTripper is borrowed. Its implementation determines TLS, proxy, retry
	// and remote-cancellation behavior; Close never destroys a borrowed transport.
	RoundTripper http.RoundTripper
}
type Client struct {
	options     Options
	root        string
	transport   http.RoundTripper
	owned       *http.Transport
	mu          sync.Mutex
	closed      bool
	active      map[*operation]struct{}
	cursors     map[string]*cursor
	cursorBytes int
	cursorNodes int
}

func identifier(s string) bool {
	if len(s) < 1 || len(s) > 32 {
		return false
	}
	for _, c := range s {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_' || c == '-') {
			return false
		}
	}
	return true
}
func defaultInt(v *int, def, max int) bool {
	if *v == 0 {
		*v = def
	}
	return *v > 0 && *v <= max
}
func validText(s string, max int, empty bool) bool {
	return (empty || s != "") && len(s) <= max && utf8.ValidString(s)
}
func validKey(s string) bool { return validText(s, 512, false) && s != "." && s != ".." }

// New snapshots trusted configuration. It performs no token lookup or I/O.
func New(options Options) (*Client, error) {
	if !identifier(options.AccountID) || !identifier(options.NamespaceID) || options.Token == nil {
		return nil, failure("configuration")
	}
	options.AccountID = strings.Clone(options.AccountID)
	options.NamespaceID = strings.Clone(options.NamespaceID)
	if options.CollectionAlias == "" {
		options.CollectionAlias = "kv"
	}
	if !validText(options.CollectionAlias, 128, false) {
		return nil, failure("configuration")
	}
	if len(options.AllowedPrefixes) == 0 || len(options.AllowedPrefixes) > 128 {
		return nil, failure("configuration")
	}
	options.AllowedPrefixes = append([]string(nil), options.AllowedPrefixes...)
	for i, p := range options.AllowedPrefixes {
		if !validText(p, 512, true) || p == "" && !options.AllowNamespaceAccess {
			return nil, failure("configuration")
		}
		options.AllowedPrefixes[i] = strings.Clone(p)
	}
	if options.Timeout == 0 {
		options.Timeout = 30 * time.Second
	}
	if options.Timeout <= 0 || options.Timeout > 2*time.Minute {
		return nil, failure("configuration")
	}
	if options.CursorTTL == 0 {
		options.CursorTTL = 5 * time.Minute
	}
	if options.CursorTTL <= 0 || options.CursorTTL > 10*time.Minute {
		return nil, failure("configuration")
	}
	if !defaultInt(&options.MaxConcurrent, 4, 16) || !defaultInt(&options.MaxKeys, 100, 1000) || !defaultInt(&options.MaxRows, 1000, 10000) || !defaultInt(&options.MaxPages, 100, 1000) || !defaultInt(&options.MaxRequests, 100, 1000) || !defaultInt(&options.MaxValueBytes, 4<<20, 25<<20) || !defaultInt(&options.MaxResponseBytes, 16<<20, 32<<20) || !defaultInt(&options.MaxRequestBytes, 64<<10, 1<<20) || !defaultInt(&options.MaxWork, 10000, 100000) || !defaultInt(&options.MaxCursors, 128, 1024) || !defaultInt(&options.MaxCursorBytes, 16<<20, 16<<20) || !defaultInt(&options.MaxCursorNodes, 65536, 65536) {
		return nil, failure("configuration")
	}
	if options.Endpoint == "" {
		options.Endpoint = "https://api.cloudflare.com/client/v4"
	}
	if !validText(options.Endpoint, 2048, false) {
		return nil, failure("configuration")
	}
	options.Endpoint = strings.Clone(options.Endpoint)
	u, err := url.Parse(options.Endpoint)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.Path != "/client/v4" || u.RawPath != "" {
		return nil, failure("configuration")
	}
	transport := options.RoundTripper
	var owned *http.Transport
	if transport == nil {
		tc := &tls.Config{MinVersion: tls.VersionTLS12, NextProtos: []string{"http/1.1"}}
		if options.RootCAs != nil {
			tc.RootCAs = options.RootCAs.Clone()
		}
		// No reused connections: net/http can transparently retry eligible GETs on
		// stale pooled connections, even when invoking RoundTrip directly.
		protocols := new(http.Protocols)
		protocols.SetHTTP1(true)
		owned = &http.Transport{Proxy: nil, DialContext: (&net.Dialer{Timeout: options.Timeout}).DialContext, TLSClientConfig: tc, Protocols: protocols, ForceAttemptHTTP2: false, DisableKeepAlives: true, DisableCompression: true, MaxConnsPerHost: options.MaxConcurrent, MaxResponseHeaderBytes: 64 << 10, TLSHandshakeTimeout: options.Timeout, ResponseHeaderTimeout: options.Timeout}
		transport = owned
	}
	root := u.String() + "/accounts/" + options.AccountID + "/storage/kv/namespaces/" + options.NamespaceID
	return &Client{options: options, root: root, transport: transport, owned: owned, active: make(map[*operation]struct{}), cursors: make(map[string]*cursor)}, nil
}
func (c *Client) scoped(key string) bool {
	for _, p := range c.options.AllowedPrefixes {
		if strings.HasPrefix(key, p) {
			return true
		}
	}
	return false
}
func (c *Client) key(key string) error {
	if !validKey(key) {
		return failure("input")
	}
	if !c.scoped(key) {
		return failure("permission")
	}
	return nil
}
func (c *Client) prefix(prefix string) error {
	if !validText(prefix, 512, true) {
		return failure("input")
	}
	if !c.scoped(prefix) {
		return failure("permission")
	}
	return nil
}

type operation struct {
	client        *Client
	parent        context.Context
	ctx           context.Context
	cancel        context.CancelFunc
	deadline      time.Time
	physical      int
	finished      bool
	released      bool
	requests      int
	requestBytes  int
	responseBytes int
	state         *cursor
}

func (c *Client) begin(parent context.Context) (*operation, error) {
	if parent == nil {
		return nil, failure("input")
	}
	if err := parentError(parent); err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, failure("closed")
	}
	if len(c.active) >= c.options.MaxConcurrent {
		return nil, failure("busy")
	}
	ctx, cancel := context.WithTimeout(parent, c.options.Timeout)
	deadline, _ := ctx.Deadline()
	o := &operation{client: c, parent: parent, ctx: ctx, cancel: cancel, deadline: deadline}
	c.active[o] = struct{}{}
	return o, nil
}
func parentError(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		if err == context.DeadlineExceeded {
			return context.DeadlineExceeded
		}
		return context.Canceled
	}
	return nil
}
func (o *operation) check() error {
	if err := parentError(o.parent); err != nil {
		return err
	}
	if !time.Now().Before(o.deadline) {
		return context.DeadlineExceeded
	}
	if o.ctx.Err() != nil {
		return context.Canceled
	}
	return nil
}
func (o *operation) releaseLocked() {
	if o.finished && o.physical == 0 && !o.released {
		o.released = true
		delete(o.client.active, o)
		if o.state != nil {
			o.state.busy = false
		}
		o.client.expireLocked()
	}
}
func (o *operation) finish() {
	o.client.mu.Lock()
	o.finished = true
	o.releaseLocked()
	o.client.mu.Unlock()
	o.cancel()
}
func (o *operation) track() { o.client.mu.Lock(); o.physical++; o.client.mu.Unlock() }
func (o *operation) settled() {
	o.client.mu.Lock()
	o.physical--
	o.releaseLocked()
	o.client.mu.Unlock()
}

type foreignResult[T any] struct {
	value T
	err   error
}

func foreign[T any](o *operation, fn func() (T, error)) (T, error) {
	var zero T
	if err := o.check(); err != nil {
		return zero, err
	}
	o.track()
	result := make(chan foreignResult[T], 1)
	go func() { v, e := fn(); o.settled(); result <- foreignResult[T]{v, e} }()
	select {
	case <-o.ctx.Done():
		return zero, o.check()
	case r := <-result:
		if err := o.check(); err != nil {
			return zero, err
		}
		return r.value, r.err
	}
}
func (o *operation) auth() (string, [32]byte, error) {
	var zero [32]byte
	token, err := foreign(o, func() (string, error) { return o.client.options.Token(o.ctx) })
	if err != nil {
		if final := o.check(); final != nil {
			return "", zero, final
		}
		return "", zero, failure("authentication")
	}
	if len(token) == 0 || len(token) > 8192 {
		return "", zero, failure("authentication")
	}
	for _, c := range token {
		if c < 33 || c > 126 {
			return "", zero, failure("authentication")
		}
	}
	token = strings.Clone(token)
	digest := hashScope("auth", token)
	if err := o.check(); err != nil {
		return "", zero, err
	}
	return token, digest, nil
}
func hashScope(domain string, fields ...string) [32]byte {
	h := sha256.New()
	for _, s := range append([]string{"dalgo-cloudflare-kv-v1", domain}, fields...) {
		var size [8]byte
		binary.BigEndian.PutUint64(size[:], uint64(len(s)))
		_, _ = h.Write(size[:])
		_, _ = h.Write([]byte(s))
	}
	var sum [32]byte
	copy(sum[:], h.Sum(nil))
	return sum
}
func (c *Client) Close() error {
	c.mu.Lock()
	if !c.closed {
		c.closed = true
		for o := range c.active {
			o.cancel()
		}
		c.expireLocked()
	}
	c.mu.Unlock()
	if c.owned != nil {
		c.owned.CloseIdleConnections()
	}
	return nil
}
