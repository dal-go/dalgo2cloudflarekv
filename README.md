# DALgo Cloudflare Workers KV REST reads

Read an **existing ordinary Workers KV namespace** through Cloudflare's native
REST API. This is separate from D1, KV Instant beta, a Workers binding and a
DALgo-managed document layout. The adapter does not create accounts, namespaces,
schemas or hosted credentials.

```go
client, err := dalgo2cloudflarekv.New(dalgo2cloudflarekv.Options{
    AccountID: "your-account-id",
    NamespaceID: "your-existing-namespace-id",
    CollectionAlias: "kv",
    AllowedPrefixes: []string{"tenant-a/"},
    Token: func(ctx context.Context) (string, error) {
        return os.Getenv("CLOUDFLARE_KV_READ_TOKEN"), nil
    },
})
if err != nil { return err }
defer client.Close()
entry, err := client.Get(ctx, "tenant-a/original-key")
// entry.Value contains exact bytes, including empty values and invalid UTF8.
```

Use a narrowly scoped `Workers KV Storage Read` bearer token. Token lookup occurs
once per public operation, including a resumed list call, inside its original
context. Constructors perform no authentication or network I/O. There is no
anonymous, Global API key/email, admin or alternate endpoint fallback.

## Native data and absence

`Get` returns `Entry{Key, Value, Expiration}` on HTTP 200. `Value` is an independent
raw `[]byte`: empty bytes remain present, and JSON text, JSON `null`, binary,
NULs and arbitrary UTF8 are never automatically decoded or normalized.
`Expiration` is an optional observed unsigned integer number of Unix seconds,
without a float64 round trip. It is not a guarantee that a cached observation
remains current.

**Every REST 404 remains an error.** The pinned REST documentation and official
Wrangler helper do not establish an authoritative numeric missing-key code that
separates key absence from namespace/authorization failures. There is no invented
`10009` interpretation, error-message matching or translation to
`record.ErrRecordNotFound`. `Exists` returns `true, nil` only after a successful
bounded raw GET. It never returns a successful `false, nil`; an unsuccessful
observation returns a nonnil error. List observations are not used to manufacture
authoritative point absence.

`GetMany` snapshots all original keys before authentication, reads sequentially
under one operation deadline and aggregate budget, and preserves input order and
duplicates with independent byte storage. Empty batches perform no token lookup
or HTTP request. Any failed read aborts the batch without returning partial data
or fabricated missing positions.

## Literal scope and request policy

Account and namespace are fixed trusted constructor configuration. Allowed
literal prefixes are copied, enforced on original keys before token lookup, and
checked again on every listed key. A namespace-wide empty prefix requires an
explicit `AllowNamespaceAccess: true`. These checks are a local boundary, not
proof that a hosted token has a prefix-level ACL.

Native keys retain their exact UTF8 bytes and spelling, including slash, percent,
astral characters and whitespace. Invalid UTF8, empty keys, exact `.` or `..`, and
keys longer than 512 bytes are rejected. Keys are encoded exactly once as one URL
segment; a literal `%2F` is sent as `%252F`. No path cleaning or key rewriting is
performed. The native binding's broad key grammar and the REST reference's
printable/non-whitespace wording differ; account-free wire fixtures do not prove
hosted acceptance of every native key spelling.

The default API base is `https://api.cloudflare.com/client/v4`. Optional `Endpoint`
and `RootCAs` are trusted infrastructure for verified HTTPS fixtures; descriptors
cannot override authority. URL credentials, query, fragment and alternate paths
are refused. The owned HTTP/1-only transport verifies TLS, disables keep-alives,
ambient proxies and transparent compression, and sends no cookies. It invokes
one `RoundTrip`, follows no redirects and schedules no application retries.
Disabling reuse also avoids Go's automatic eligible-GET retry on stale pooled
connections. A supplied `RoundTripper` is borrowed trusted infrastructure: its
internal retry/proxy/TLS behavior cannot be certified by this adapter, and it is
never closed or destroyed.

## Whole REST list pages

```go
page, err := client.ListKeys(ctx, dalgo2cloudflarekv.ListOptions{
    Prefix: "tenant-a/", Limit: 100,
})
// Resume on this same live client, with the exact prefix and limit:
next, err := client.ListKeys(ctx, dalgo2cloudflarekv.ListOptions{
    Prefix: "tenant-a/", Limit: 100, Cursor: page.NextCursor,
})
```

The REST page limit is **10 through 1,000** (default 100). Listing returns names,
optional expiration and optional JSON metadata; it does not return values.
Metadata is validated as bounded strict JSON and retained as `json.RawMessage`,
preserving number lexemes and valid own field names such as `__proto__`,
`constructor` and `prototype`. Nil metadata means absent; raw `null` means present
JSON null. This is not an atomic observation of a separately read value. A
separate metadata endpoint and `getWithMetadata` profile are not implemented.

The API returns complete original pages, never a truncated subset with a cursor
that silently skips the remaining rows. If the remaining row budget cannot admit
the whole requested page, the next request is refused before dispatch. Empty or
short pages can be unfinished. REST completion follows absent, null or empty
`result_info.cursor`, rather than result count or native binding `list_complete`.
Over-return, malformed envelopes, duplicate JSON fields, foreign prefixes,
repeated provider cursors, repeated keys and backward UTF8-byte key order fail
the whole page before delivery or acknowledgement.

Returned cursor handles are opaque, single-use and local to this client instance.
They bind the fixed account/namespace, exact prefix/limit and token fingerprint.
Raw provider cursor text and tokens are never exposed in handles or errors.
Page/request/byte/work charges persist across retries; failure preserves the
previous acknowledged handle when further work remains admissible. A pending
physical request keeps its handle busy even after prompt public cancellation.
Only final successful delivery atomically replaces its handle and retained quota.
Terminal journals refund their own retained charges. Expired, abandoned first-page
and closed journals keep their retained charge while an admitted operation or
physical request still holds them, then refund at actual settlement. Expiry is
never extended, and an expired handle cannot be acknowledged or resumed.

Ordinary Workers KV is eventually consistent. Listing is not a snapshot and
cannot guarantee absence, complete coverage under concurrent mutation, or
exactly-once delivery across a network/consumer failure. The local acknowledgement
boundary is a successful SDK return, not proof a remote caller received output.

## Deadlines, ownership and bounds

An operation starts before key copying, authentication, I/O, parsing or DALgo
callbacks. The default local timeout is 30 seconds (maximum 120); an earlier
caller deadline wins. Caller cancellation causes are redacted to the standard
`context.Canceled` or `context.DeadlineExceeded` sentinels. Original monotonic
deadline checks run after owned response cleanup and caller callbacks before
success. A list journey has a separate fixed five-minute lifetime (maximum ten),
which is never extended on resume; spent journey counters also persist.

Pending token, transport, body read and exact body close work retains bounded
physical admission until it actually settles. Public cancellation and client
`Close` do not wait for an uncooperative borrowed operation. Exact late response
bodies are closed once. Repeated abandoned calls return `busy` rather than
accumulating unbounded work. This is local cancellation and resource admission,
not proof Cloudflare stopped executing a request.

Defaults and local maximums are explicit in `Options`:

| Bound | Default | Maximum |
| --- | --- | --- |
| Concurrent admitted operations | 4 | 16 |
| Batch keys | 100 | 1,000 |
| List journey rows | 1,000 | 10,000 |
| List journey pages / operation requests | 100 each | 1,000 each |
| Individual raw value | 4 MiB | 25 MiB |
| Aggregate raw response bytes | 16 MiB | 32 MiB |
| Aggregate escaped request URLs | 64 KiB | 1 MiB |
| Journey requested-row work | 10,000 | 100,000 |
| Live cursor handles | 128 | 1,024 |
| Cursor retained logical bytes, all journals | 16 MiB | 16 MiB |
| Cursor retained logical nodes, all journals | 65,536 | 65,536 |

Responses are capped before JSON parsing or raw value materialization. Metadata
is capped at 1,024 original JSON bytes per key; parsed JSON has depth 32 and 65,536
nodes per page. Server cursors are capped at 16 KiB and must fit the next escaped
request before acknowledgement. Cursor quotas include handles, prefixes, seen
names/tokens and fixed/container charges, with atomic net replacement across
concurrent journeys. They bound logical retained state, not total Go heap,
Cloudflare billing, server scan work or rate-limit consumption.

## DALgo facade and capability limits

`client.DB()` uses `CollectionAlias` (default `kv`) and original string IDs. Pass
`*[]byte` or `*Entry` record targets; there is no JSON/float64 round trip, hidden
document envelope or managed key layout. `Get`, `GetMulti` and presence-only
`Exists` share the native bounds and 404 failure policy. Admitted record slices,
keys and targets are captured before authentication/target delivery, and each
`Key`, `Data` and `SetError` callback is followed by a fresh guard. Error-state
restoration can itself observe cancellation and reclassify within a fixed bound.
Callbacks run synchronously on the caller goroutine and are never abandoned;
an arbitrarily blocking caller callback can delay return, so a hard return-time
guarantee cannot include hostile synchronous caller code. A callback failure may
leave target bytes populated but the record error state prevents successful data
access; targets are not a transactional rollback mechanism.

Provider-supported writes are **not implemented** by this adapter. Generic DALgo
query compilation and dynamic schema discovery are also not implemented. The
exact native KV REST protocol has no SQL query interface or cross-key atomic
snapshot/read-write transaction; the facade reports `dal.ErrNotSupported` for
those operations. This is not a claim that the provider cannot write a key.

Source tests use owned verified TLS and adversarial borrowed transports. Until
controller execution and publication receipts exist, they are source evidence
only. No hosted account, token permission, real Workers runtime, browser/CORS,
installed CLI, registry release, Apps integration or deployment is claimed.

Primary contracts: [raw REST reads](https://developers.cloudflare.com/api/resources/kv/subresources/namespaces/subresources/values/methods/get/),
[REST listing](https://developers.cloudflare.com/api/resources/kv/subresources/namespaces/subresources/keys/methods/list/),
[native key grammar](https://developers.cloudflare.com/kv/api/write-key-value-pairs/),
[limits](https://developers.cloudflare.com/kv/platform/limits/), and
[eventual consistency](https://developers.cloudflare.com/kv/concepts/how-kv-works/).
