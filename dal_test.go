package dalgo2cloudflarekv

import (
	"bytes"
	"context"
	"errors"
	"github.com/dal-go/record"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type callbackRecord struct {
	record.Record
	key   func()
	data  func()
	state func(error)
}

func (r *callbackRecord) Key() *record.Key {
	if r.key != nil {
		r.key()
	}
	return r.Record.Key()
}
func (r *callbackRecord) Data() any {
	if r.data != nil {
		r.data()
	}
	return r.Record.Data()
}
func (r *callbackRecord) SetError(err error) record.Record {
	r.Record.SetError(err)
	if r.state != nil {
		r.state(err)
	}
	return r
}

func TestDALgoRawByteAndEntryBridgeNeverParsesJSONOr404(t *testing.T) {
	c := testClient(t, fixtureOptions(), func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/client/v4/accounts/account/storage/kv/namespaces/namespace/values/data/missing" {
			w.WriteHeader(404)
			return
		}
		write(t, w, r, []byte{0, 255, 'n', 'u', 'l', 'l'})
	})
	var target []byte
	r := record.NewRecordWithData(record.NewKeyWithID("kv", "data/a"), &target)
	if err := c.DB().Get(context.Background(), r); err != nil || !r.Exists() || !bytes.Equal(target, []byte{0, 255, 'n', 'u', 'l', 'l'}) {
		t.Fatal("raw target was coerced", err)
	}
	var entry Entry
	r2 := record.NewRecordWithData(record.NewKeyWithID("kv", "data/b"), &entry)
	if err := c.DB().Get(context.Background(), r2); err != nil || entry.Key != "data/b" || !bytes.Equal(entry.Value, target) {
		t.Fatal("native Entry target", err)
	}
	missing := record.NewRecordWithData(record.NewKeyWithID("kv", "data/missing"), &entry)
	if err := c.DB().Get(context.Background(), missing); code(err) != "http" || record.IsNotFound(err) {
		t.Fatal("REST404 falsely marked notfound", err)
	}
	if exists, err := c.DB().Exists(context.Background(), record.NewKeyWithID("kv", "data/missing")); exists || err == nil || record.IsNotFound(err) {
		t.Fatal("REST404 falsely marked absent", err)
	}
	bad := record.NewRecordWithData(record.NewKeyWithID("kv", "data/a"), map[string]any{})
	if err := c.DB().Get(context.Background(), bad); code(err) != "target" {
		t.Fatal("generic JSON decoding was invented", err)
	}
}
func TestDALgoAdmittedRecordsKeysAndTargetsSurviveCallerSliceMutation(t *testing.T) {
	for _, replace := range []bool{false, true} {
		c := testClient(t, fixtureOptions(), func(w http.ResponseWriter, r *http.Request) { write(t, w, r, []byte(r.URL.Path)) })
		var a, b, substitute []byte
		key := record.NewKeyWithID("kv", "data/a")
		originalB := record.NewRecordWithData(record.NewKeyWithID("kv", "data/b"), &b)
		records := []record.Record{nil, originalB}
		first := &callbackRecord{Record: record.NewRecordWithData(key, &a)}
		first.data = func() {
			key.ID = "foreign"
			if replace {
				records[1] = record.NewRecordWithData(record.NewKeyWithID("kv", "foreign"), &substitute)
			} else {
				records[1] = nil
			}
		}
		records[0] = first
		if err := c.DB().GetMulti(context.Background(), records); err != nil || !strings.HasSuffix(string(a), "/values/data/a") || !strings.HasSuffix(string(b), "/values/data/b") || len(substitute) != 0 || !originalB.Exists() {
			t.Fatal("callback replaced admitted identity/target", err)
		}
	}
}
func TestDALgoFreshnessAfterEveryKeyStateAndDataCallback(t *testing.T) {
	for _, stage := range []string{"key", "state", "data"} {
		t.Run(stage, func(t *testing.T) {
			var tokens atomic.Int32
			o := fixtureOptions()
			o.Token = func(context.Context) (string, error) { tokens.Add(1); return "owned-read-token", nil }
			c, err := New(o)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = c.Close() }()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var a, b []byte
			laterKeys, dataCalls := 0, 0
			first := &callbackRecord{Record: record.NewRecordWithData(record.NewKeyWithID("kv", "data/a"), &a)}
			second := &callbackRecord{Record: record.NewRecordWithData(record.NewKeyWithID("kv", "data/b"), &b), key: func() { laterKeys++ }, data: func() { t.Error("later data callback after known cancellation") }}
			first.data = func() {
				dataCalls++
				if stage == "data" {
					cancel()
				}
			}
			switch stage {
			case "key":
				first.key = cancel
			case "state":
				first.state = func(err error) {
					if err == nil {
						cancel()
					}
				}
			case "data":
			}
			if err := c.DB().GetMulti(ctx, []record.Record{first, second}); !errors.Is(err, context.Canceled) {
				t.Fatal("callback cancellation lost", err)
			}
			if tokens.Load() != 0 || stage == "key" && laterKeys != 0 || stage == "state" && dataCalls != 0 {
				t.Fatal("later user callback/auth followed observed cancellation")
			}
		})
	}
}
func TestDALgoFinalStateCallbackCancellationAndDeadlineActuallyEntered(t *testing.T) {
	for _, deadline := range []bool{false, true} {
		o := fixtureOptions()
		c := testClient(t, o, func(w http.ResponseWriter, r *http.Request) { write(t, w, r, []byte("native")) })
		ctx, cancel := context.WithCancel(context.Background())
		var stop context.CancelFunc
		if deadline {
			ctx, stop = context.WithTimeout(ctx, 200*time.Millisecond)
			defer stop()
		}
		var target []byte
		entered := false
		r := &callbackRecord{Record: record.NewRecordWithData(record.NewKeyWithID("kv", "data/a"), &target)}
		r.state = func(err error) {
			if err == nil && len(target) > 0 {
				entered = true
				if deadline {
					<-ctx.Done()
				} else {
					cancel()
				}
			}
		}
		err := c.DB().Get(ctx, r)
		cancel()
		expected := context.Canceled
		if deadline {
			expected = context.DeadlineExceeded
		}
		if !entered || !errors.Is(err, expected) || !errors.Is(r.Error(), expected) {
			t.Fatal("final record callback not observed/reclassified", err)
		}
	}
}
func TestDALgoFailureRestorationCanCancelWithoutCauseLeak(t *testing.T) {
	o := fixtureOptions()
	o.Token = func(context.Context) (string, error) { return "", errors.New("PRIVATE_TOKEN") }
	c, err := New(o)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	var target []byte
	entered := false
	r := &callbackRecord{Record: record.NewRecordWithData(record.NewKeyWithID("kv", "data/a"), &target)}
	r.state = func(e error) {
		if e != nil {
			entered = true
			cancel(errors.New("PRIVATE_RECORD_CALLBACK"))
		}
	}
	if err := c.DB().Get(ctx, r); !entered || !errors.Is(err, context.Canceled) || !errors.Is(r.Error(), context.Canceled) {
		t.Fatal("error callback cancellation priority", err)
	}
}
func TestDALgoCallbackCloseStopsLaterDataAndAuthentication(t *testing.T) {
	var tokens atomic.Int32
	o := fixtureOptions()
	o.Token = func(context.Context) (string, error) { tokens.Add(1); return "owned-read-token", nil }
	c, err := New(o)
	if err != nil {
		t.Fatal(err)
	}
	var target []byte
	dataCalled := false
	r := &callbackRecord{Record: record.NewRecordWithData(record.NewKeyWithID("kv", "data/a"), &target), data: func() { dataCalled = true }}
	r.state = func(e error) {
		if e == nil {
			if err := c.Close(); err != nil {
				t.Error(err)
			}
		}
	}
	if err := c.DB().Get(context.Background(), r); !errors.Is(err, context.Canceled) || dataCalled || tokens.Load() != 0 {
		t.Fatal("Close callback followed by Data/auth", err)
	}
}
func TestDALgoUnsupportedTargetsKeysAndReadOnlyCapabilities(t *testing.T) {
	c, err := New(fixtureOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	var data []byte
	for _, key := range []*record.Key{record.NewKeyWithID("other", "data/a"), record.NewKeyWithID("kv", 1), record.NewKeyWithID("kv", "foreign")} {
		r := record.NewRecordWithData(key, &data)
		if err := c.DB().Get(context.Background(), r); err == nil {
			t.Fatal("invalid native key admitted")
		}
	}
	b := &backend{client: c}
	if b.Schema() != nil || b.ID() == "" {
		t.Fatal("configured metadata changed")
	}
	if err := b.RunReadonlyTransaction(context.Background(), nil); err == nil {
		t.Fatal("snapshot TX invented")
	}
	if err := b.RunReadwriteTransaction(context.Background(), nil); err == nil {
		t.Fatal("write TX invented")
	}
	if _, err := b.ExecuteQueryToRecordsReader(context.Background(), nil); err == nil {
		t.Fatal("SQL/query compiler invented")
	}
	if _, err := b.ExecuteQueryToRecordsetReader(context.Background(), nil); err == nil {
		t.Fatal("SQL/query compiler invented")
	}
}
