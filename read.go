package dalgo2cloudflarekv

import (
	"context"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
)

// Entry is a successful raw REST observation, never a guessed missing result.
// Empty Value is present; every non-200 status, including404, is an error.
type Entry struct {
	Key        string
	Value      []byte
	Expiration *uint64
}

func (o *operation) admitURL(endpoint string, work int) error {
	if err := o.check(); err != nil {
		return err
	}
	n := len(endpoint)
	c := o.client
	c.mu.Lock()
	defer c.mu.Unlock()
	if n > c.options.MaxRequestBytes-o.requestBytes || o.requests >= c.options.MaxRequests {
		return failure("bounds")
	}
	if s := o.state; s != nil {
		if n > c.options.MaxRequestBytes-s.requestBytes || s.requests >= c.options.MaxRequests || work > c.options.MaxWork-s.work {
			return failure("bounds")
		}
		s.requestBytes += n
		s.requests++
		s.work += work
	}
	o.requestBytes += n
	o.requests++
	return nil
}
func (o *operation) preflightURLs(urls []string) error {
	total := 0
	for _, u := range urls {
		if err := o.check(); err != nil {
			return err
		}
		if len(u) > o.client.options.MaxRequestBytes-total {
			return failure("bounds")
		}
		total += len(u)
	}
	o.client.mu.Lock()
	defer o.client.mu.Unlock()
	if total > o.client.options.MaxRequestBytes-o.requestBytes || len(urls) > o.client.options.MaxRequests-o.requests {
		return failure("bounds")
	}
	if s := o.state; s != nil {
		if total > o.client.options.MaxRequestBytes-s.requestBytes || len(urls) > o.client.options.MaxRequests-s.requests {
			return failure("bounds")
		}
	}
	return nil
}
func (o *operation) remaining(maximum int) (int, error) {
	o.client.mu.Lock()
	defer o.client.mu.Unlock()
	n := min(maximum, o.client.options.MaxResponseBytes-o.responseBytes)
	if s := o.state; s != nil {
		n = min(n, o.client.options.MaxResponseBytes-s.responseBytes)
	}
	if n <= 0 {
		return 0, failure("bounds")
	}
	return n, nil
}
func (o *operation) chargeBytes(n int) error {
	o.client.mu.Lock()
	defer o.client.mu.Unlock()
	o.responseBytes += n
	if s := o.state; s != nil {
		s.responseBytes += n
	}
	if o.responseBytes > o.client.options.MaxResponseBytes || o.state != nil && o.state.responseBytes > o.client.options.MaxResponseBytes {
		return failure("bounds")
	}
	return nil
}

type rawResponse struct {
	body       []byte
	expiration *uint64
}

func (o *operation) readURL(endpoint, token string, maximum, work int) (rawResponse, error) {
	var zero rawResponse
	if err := o.admitURL(endpoint, work); err != nil {
		return zero, err
	}
	limit, err := o.remaining(maximum)
	if err != nil {
		return zero, err
	}
	result, err := foreign(o, func() (rawResponse, error) {
		if err := o.check(); err != nil {
			return zero, err
		}
		req, err := http.NewRequestWithContext(o.ctx, http.MethodGet, endpoint, nil)
		if err != nil {
			return zero, failure("input")
		}
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Accept-Encoding", "identity")
		response, err := o.client.transport.RoundTrip(req)
		if response == nil || response.Body == nil {
			if err != nil {
				return zero, failure("transport")
			}
			return zero, failure("protocol")
		}
		var once sync.Once
		var closeErr error
		closeBody := func() { once.Do(func() { closeErr = response.Body.Close() }) }
		done, settled := make(chan struct{}), make(chan struct{})
		go func() {
			defer close(settled)
			select {
			case <-o.ctx.Done():
				closeBody()
			case <-done:
			}
		}()
		// This foreign lease includes BOTH the read and exact owned-body close. A
		// noncooperative transport/read/close stays admitted after public cancellation.
		defer func() { close(done); closeBody(); <-settled }()
		if err != nil {
			return zero, failure("transport")
		}
		if err := o.check(); err != nil {
			return zero, err
		}
		if response.StatusCode != http.StatusOK {
			code := "http"
			if response.StatusCode == 401 {
				code = "authentication"
			}
			if response.StatusCode == 403 {
				code = "permission"
			}
			return zero, &Error{Code: code, Status: response.StatusCode}
		}
		if encoding := response.Header.Get("Content-Encoding"); encoding != "" && encoding != "identity" {
			return zero, failure("protocol")
		}
		if raw := response.Header.Get("Content-Length"); raw != "" {
			n, e := unsigned(raw)
			if e != nil {
				return zero, failure("protocol")
			}
			if n > uint64(limit) {
				return zero, failure("bounds")
			}
		}
		bytes, readErr := io.ReadAll(io.LimitReader(response.Body, int64(limit)+1))
		if err := o.chargeBytes(len(bytes)); err != nil {
			return zero, err
		}
		if len(bytes) > limit {
			return zero, failure("bounds")
		}
		if readErr != nil {
			return zero, failure("transport")
		}
		result := rawResponse{body: bytes}
		if raw := response.Header.Get("Expiration"); raw != "" {
			n, e := unsigned(raw)
			if e != nil {
				return zero, failure("protocol")
			}
			result.expiration = &n
		}
		closeBody()
		if closeErr != nil {
			return zero, failure("transport")
		}
		return result, nil
	})
	if final := o.check(); final != nil {
		return zero, final
	}
	return result, err
}
func unsigned(raw string) (uint64, error) {
	if raw == "" {
		return 0, failure("protocol")
	}
	for _, c := range raw {
		if c < '0' || c > '9' {
			return 0, failure("protocol")
		}
	}
	n, e := strconv.ParseUint(raw, 10, 64)
	if e != nil {
		return 0, failure("protocol")
	}
	return n, nil
}
func (c *Client) valueURL(key string) string { return c.root + "/values/" + url.PathEscape(key) }
func (o *operation) readPoint(key, endpoint, token string) (Entry, error) {
	r, err := o.readURL(endpoint, token, o.client.options.MaxValueBytes, 1)
	if err != nil {
		return Entry{}, err
	}
	if err := o.check(); err != nil {
		return Entry{}, err
	}
	value := append([]byte{}, r.body...)
	return Entry{Key: strings.Clone(key), Value: value, Expiration: r.expiration}, nil
}
func (c *Client) Get(ctx context.Context, key string) (Entry, error) {
	o, err := c.begin(ctx)
	if err != nil {
		return Entry{}, err
	}
	defer o.finish()
	if err = c.key(key); err != nil {
		return Entry{}, err
	}
	endpoint := c.valueURL(key)
	if err = o.preflightURLs([]string{endpoint}); err != nil {
		return Entry{}, err
	}
	token, _, err := o.auth()
	if err != nil {
		return Entry{}, err
	}
	row, err := o.readPoint(key, endpoint, token)
	if final := o.check(); final != nil {
		return Entry{}, final
	}
	return row, err
}

// Exists can confirm presence, but never returns false,nil for ambiguous REST404.
func (c *Client) Exists(ctx context.Context, key string) (bool, error) {
	_, err := c.Get(ctx, key)
	return err == nil, err
}
func (c *Client) GetMany(ctx context.Context, keys []string) ([]Entry, error) {
	o, err := c.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer o.finish()
	if len(keys) > c.options.MaxKeys || len(keys) > c.options.MaxRows || len(keys) > c.options.MaxWork {
		return nil, failure("bounds")
	}
	keys = append([]string(nil), keys...)
	urls := make([]string, len(keys))
	for i, key := range keys {
		if err := o.check(); err != nil {
			return nil, err
		}
		if err := c.key(key); err != nil {
			return nil, err
		}
		urls[i] = c.valueURL(key)
	}
	if err = o.preflightURLs(urls); err != nil {
		return nil, err
	}
	rows := make([]Entry, len(keys))
	if len(keys) == 0 {
		if err := o.check(); err != nil {
			return nil, err
		}
		return rows, nil
	}
	token, _, err := o.auth()
	if err != nil {
		return nil, err
	}
	for i, key := range keys {
		rows[i], err = o.readPoint(key, urls[i], token)
		if err != nil {
			return nil, err
		}
	}
	if err := o.check(); err != nil {
		return nil, err
	}
	return rows, nil
}

func (c *Client) listURL(prefix string, limit int, cursor string) string {
	values := url.Values{"prefix": {prefix}, "limit": {strconv.Itoa(limit)}}
	if cursor != "" {
		values.Set("cursor", cursor)
	}
	return c.root + "/keys?" + values.Encode()
}
func validCursor(s string) bool {
	return validText(s, 16384, true) && !strings.ContainsAny(s, "\x00\r\n")
}
