package dalgo2cloudflarekv

import (
	"context"
	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/dalgo/recordset"
	"github.com/dal-go/record"
)

// DB preserves original raw bytes through explicit *[]byte or *Entry targets.
// Record callbacks run synchronously on the caller goroutine, never abandoned
// after timeout. Arbitrarily blocking caller callbacks can delay public return.
func (c *Client) DB() dal.DB { return dal.NewDB(&backend{client: c}) }

type backend struct {
	dal.ConcurrencyAvailable
	client *Client
}

var _ dal.Backend = (*backend)(nil)

func (*backend) ID() string           { return "cloudflare-kv:configured-namespace" }
func (*backend) Adapter() dal.Adapter { return dal.NewAdapter("dalgo2cloudflarekv", "unreleased") }
func (*backend) Schema() dal.Schema   { return nil }

type recordAdmission struct {
	record record.Record
	key    string
	bytes  *[]byte
	entry  *Entry
}

func (c *Client) admitRecord(o *operation, r record.Record) (recordAdmission, error) {
	a := recordAdmission{record: r}
	if err := o.check(); err != nil {
		return a, err
	}
	if r == nil {
		return a, failure("input")
	}
	key := r.Key()
	if err := o.check(); err != nil {
		return a, err
	}
	if key == nil || key.Parent() != nil || key.Collection() != c.options.CollectionAlias {
		return a, failure("key")
	}
	id, ok := key.ID.(string)
	if !ok {
		return a, failure("key")
	}
	if err := c.key(id); err != nil {
		return a, err
	}
	a.key = id
	return a, nil
}
func (a *recordAdmission) target(o *operation) error {
	if err := o.check(); err != nil {
		return err
	}
	a.record.SetError(nil)
	if err := o.check(); err != nil {
		return err
	}
	value := a.record.Data()
	if err := o.check(); err != nil {
		return err
	}
	switch target := value.(type) {
	case *[]byte:
		if target == nil {
			return failure("target")
		}
		a.bytes = target
	case *Entry:
		if target == nil {
			return failure("target")
		}
		a.entry = target
	default:
		return failure("target")
	}
	return nil
}
func (b *backend) Get(ctx context.Context, r record.Record) error {
	return b.getRecords(ctx, []record.Record{r})
}
func (b *backend) GetMulti(ctx context.Context, records []record.Record) error {
	return b.getRecords(ctx, records)
}
func (b *backend) Exists(ctx context.Context, key *record.Key) (bool, error) {
	o, err := b.client.begin(ctx)
	if err != nil {
		return false, err
	}
	defer o.finish()
	if key == nil || key.Parent() != nil || key.Collection() != b.client.options.CollectionAlias {
		return false, failure("key")
	}
	id, ok := key.ID.(string)
	if !ok {
		return false, failure("key")
	}
	if err := b.client.key(id); err != nil {
		return false, err
	}
	endpoint := b.client.valueURL(id)
	if err := o.preflightURLs([]string{endpoint}); err != nil {
		return false, err
	}
	token, _, err := o.auth()
	if err != nil {
		return false, err
	}
	_, err = o.readPoint(id, endpoint, token)
	if final := o.check(); final != nil {
		return false, final
	}
	return err == nil, err
}
func (b *backend) getRecords(ctx context.Context, records []record.Record) error {
	o, err := b.client.begin(ctx)
	if len(records) > b.client.options.MaxKeys || len(records) > b.client.options.MaxRows || len(records) > b.client.options.MaxWork {
		if err != nil {
			return err
		}
		o.finish()
		return failure("bounds")
	}
	records = append([]record.Record(nil), records...)
	if err != nil {
		return finalizeRecords(records, err, func() error {
			if ctx == nil {
				return nil
			}
			return parentError(ctx)
		})
	}
	defer o.finish()
	finish := func(e error) error { return finalizeRecords(records, e, o.check) }
	admitted := make([]recordAdmission, len(records))
	urls := make([]string, len(records))
	for i, r := range records {
		admitted[i], err = b.client.admitRecord(o, r)
		if err != nil {
			return finish(err)
		}
		urls[i] = b.client.valueURL(admitted[i].key)
	}
	if err = o.preflightURLs(urls); err != nil {
		return finish(err)
	}
	for i := range admitted {
		if err = admitted[i].target(o); err != nil {
			return finish(err)
		}
	}
	if len(records) == 0 {
		return finish(nil)
	}
	token, _, err := o.auth()
	if err != nil {
		return finish(err)
	}
	entries := make([]Entry, len(records))
	for i, a := range admitted {
		entries[i], err = o.readPoint(a.key, urls[i], token)
		if err != nil {
			return finish(err)
		}
	}
	if err := o.check(); err != nil {
		return finish(err)
	}
	for i, a := range admitted {
		if a.bytes != nil {
			*a.bytes = entries[i].Value
		} else {
			*a.entry = entries[i]
		}
	}
	return finish(nil)
}

// Error state callbacks may themselves cancel/Close. Reclassify only while the
// real monotonic cancellation priority advances, with a fixed five-pass bound.
func finalizeRecords(records []record.Record, result error, check func() error) error {
	for pass := 0; pass < 5; pass++ {
		if final := check(); final != nil {
			result = final
		}
		changed := false
		for _, r := range records {
			if r == nil {
				continue
			}
			if final := check(); final != nil && final != result {
				result = final
				changed = true
			}
			r.SetError(result)
			if final := check(); final != nil && final != result {
				result = final
				changed = true
			}
		}
		if final := check(); final != nil && final != result {
			result = final
			changed = true
		}
		if !changed {
			return result
		}
	}
	return result
}
func (*backend) RunReadonlyTransaction(context.Context, dal.ROTxWorker, ...dal.TransactionOption) error {
	return dal.ErrNotSupported
}
func (*backend) RunReadwriteTransaction(context.Context, dal.RWTxWorker, ...dal.TransactionOption) error {
	return dal.ErrNotSupported
}
func (*backend) ExecuteQueryToRecordsReader(context.Context, dal.Query) (dal.RecordsReader, error) {
	return nil, dal.ErrNotSupported
}
func (*backend) ExecuteQueryToRecordsetReader(context.Context, dal.Query, ...recordset.Option) (dal.RecordsetReader, error) {
	return nil, dal.ErrNotSupported
}
