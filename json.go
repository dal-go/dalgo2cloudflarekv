package dalgo2cloudflarekv

import (
	"bytes"
	"encoding/json"
	"io"
	"strconv"
	"unicode/utf8"
)

func validJSONStrings(b []byte) bool {
	if !utf8.Valid(b) {
		return false
	}
	for i := 0; i < len(b); i++ {
		if b[i] != '"' {
			continue
		}
		i++
		for ; i < len(b) && b[i] != '"'; i++ {
			if b[i] != '\\' {
				continue
			}
			i++
			if i >= len(b) {
				return false
			}
			if b[i] != 'u' {
				continue
			}
			if i+4 >= len(b) {
				return false
			}
			n, err := strconv.ParseUint(string(b[i+1:i+5]), 16, 16)
			if err != nil {
				return false
			}
			i += 4
			if n >= 0xdc00 && n <= 0xdfff {
				return false
			}
			if n >= 0xd800 && n <= 0xdbff {
				if i+6 >= len(b) || b[i+1] != '\\' || b[i+2] != 'u' {
					return false
				}
				m, err := strconv.ParseUint(string(b[i+3:i+7]), 16, 16)
				if err != nil || m < 0xdc00 || m > 0xdfff {
					return false
				}
				i += 6
			}
		}
	}
	return true
}

func strictJSON(o *operation, data []byte) (any, error) {
	if !validJSONStrings(data) {
		return nil, failure("protocol")
	}
	d := json.NewDecoder(bytes.NewReader(data))
	d.UseNumber()
	nodes := 0
	var read func(int) (any, error)
	read = func(depth int) (any, error) {
		nodes++
		if err := o.check(); err != nil {
			return nil, err
		}
		if depth > 32 || nodes > 65536 {
			return nil, failure("bounds")
		}
		t, err := d.Token()
		if err != nil {
			return nil, failure("protocol")
		}
		delim, ok := t.(json.Delim)
		if !ok {
			return t, nil
		}
		switch delim {
		case '{':
			out := map[string]any{}
			for d.More() {
				key, err := d.Token()
				if err != nil {
					return nil, failure("protocol")
				}
				k, ok := key.(string)
				if !ok {
					return nil, failure("protocol")
				}
				if _, duplicate := out[k]; duplicate {
					return nil, failure("protocol")
				}
				v, err := read(depth + 1)
				if err != nil {
					return nil, err
				}
				out[k] = v
			}
			end, err := d.Token()
			if err != nil || end != json.Delim('}') {
				return nil, failure("protocol")
			}
			return out, nil
		case '[':
			out := []any{}
			for d.More() {
				v, err := read(depth + 1)
				if err != nil {
					return nil, err
				}
				out = append(out, v)
			}
			end, err := d.Token()
			if err != nil || end != json.Delim(']') {
				return nil, failure("protocol")
			}
			return out, nil
		default:
			return nil, failure("protocol")
		}
	}
	x, err := read(0)
	if err != nil {
		return nil, err
	}
	if _, err := d.Token(); err != io.EOF {
		return nil, failure("protocol")
	}
	return x, nil
}
