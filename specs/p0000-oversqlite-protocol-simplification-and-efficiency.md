# p0000: Oversqlite Protocol Simplification and Efficiency

> Current durable-recovery authority: [r0001](r0001-oversqlite-durable-recovery.md) and the active `swagger/two_way_sync.yaml` supersede conflicting lifecycle/envelope, request-count, source-sequence and pending-source-retargeting statements below. The sole marker remains `v1`; every sync request now requires its exact protocol header. All terminal outcomes consume source sequence, durable local disposition precedes ack, and ack precedes the successor. Old acceptance/evidence records below retain their historical status. Existing typed codecs, request/bundle hashes and unrelated contracts remain in force. Runtime phases after C01 remain pending.

Status: **REMEDIATED AFTER BLOCKING REREVIEW; INDEPENDENT REREVIEW REQUIRED;
NOT OWNER-ACCEPTED; SQLITENOW K0 NOT AUTHORIZED**

Created: 2026-09-19

Proposed FK/resolution amendment (2026-09-25):
[p0001](p0001-oversqlite-fk-safe-sync-and-automatic-resolution.md) defines the proposed
server-visible FK baseline, safe final-state application, automatic conflict policies and targeted
reconciliation work. Its section 1.1 names the exact subjects it would supersede after acceptance,
including conservative unknown-baseline grouping in the convergent-sync addendum. Other p0000
requirements remain. This cross-reference does not accept p0001, authorize runtime changes, or
rewrite the historical status and acceptance records of earlier p0000 candidates. OpenAPI/corpus
changes remain a mandatory subsequent contract-settlement gate before runtime consumers.

Repository: `/Users/pochkin/Projects/my/go-oversync`

SQLiteNow companion:
[Oversqlite protocol simplification and efficiency for Kotlin Multiplatform](../../sqlitenow-kmp/specs/oversqlite-protocol-simplification-and-efficiency.md)

## 1. Authority and independence

This is an independent protocol specification. It is not a child of
`m0000-oversqlite-schema-evolution.md`, does not amend a schema-migration contract, and is not a
phase of m0010 or any other schema-evolution qualification campaign. A schema generation supplies
the table and column contract consumed by the wire codec, but that dependency does not make
transport shape, request count, compression, or watch scheduling schema-migration concerns.

The `p` prefix identifies this separate protocol-simplification program. This draft may be reviewed
and accepted independently of the `m` series. Existing schema-evolution acceptance records must not
be reopened or rewritten merely because this work changes the transport used by their retained
behaviors.

Planning, owner acceptance, implementation, qualification, independent review, implementation
acceptance, commit, push, deployment, publication, and release remain separate gates. This draft
authorizes none of the later gates.

## 2. Breaking-change posture

Oversqlite is in breaking-change mode. The implementation must replace the current protocol rather
than add a second path beside it.

Normative consequences:

1. The protocol identity remains the existing exact literal `v1`. There is no `v2`, version
   negotiation, version range, fallback, or dual-protocol period.
2. Removed endpoints, JSON objects, fields, and client methods are deleted. They are not retained as
   aliases, deprecated facades, adapters, feature flags, or hidden compatibility readers.
3. Go server, direct Go Oversqlite client, SQLiteNow KMP client, Swift products that delegate to KMP,
   fixtures, examples, documentation, and tests cut over together before the candidate can be
   accepted.
4. Old and new clients are not required to coexist with one server deployment. Rollout is an
   atomic development cutover, not a negotiated production migration.
5. Persisted server or client state that exists only to support the removed wire workflow is removed
   or replaced. If a local or server format must change, the implementation must fail closed on the
   old format; it must not add a compatibility migration solely to preserve unreleased protocol
   state.
6. Existing semantic safety contracts remain unless this specification changes them explicitly:
   source identity, generation proof, atomic bundles, deterministic conflict behavior, idempotent
   replay, canonical hashes, snapshot isolation, bounded work, and restart recovery are preserved.

Changing the protocol while retaining the literal `v1` is deliberate. `v1` names the sole supported
pre-release contract; it is not a promise that an earlier development build remains compatible.

## 3. Objective

Make the common Oversqlite synchronization path cheap in both round trips and bytes while reducing
the number of protocol concepts that implementations must maintain.

The target steady-state paths, after opening and contract admission, are:

| Operation | Required sequential HTTP exchanges |
| --- | ---: |
| no local changes, ordinary incremental sync | 1 (`pull`) |
| one local bundle, then ordinary incremental sync | 2 (`push`, then `pull`) |
| retry after an ambiguous completed push | 2 (`push` replay, then `pull`) |

Recovery, conflict resolution, source replacement, snapshot adoption, and pagination may require
more exchanges. They must not impose their costs on every ordinary one-row or no-change sync.

The design priorities, in order, are:

1. fewer required round trips;
2. fewer protocol states and endpoints;
3. removal of repeated row-envelope text;
4. bounded transfer size and memory;
5. compression only where it has a realistic chance to save bytes; and
6. deterministic cross-runtime behavior and recovery.

## 4. Current repository observations

These observations explain the replacement scope but do not override normative decisions:

- `swagger/two_way_sync.yaml` exposes separate push-session creation, chunk, commit, deletion, and
  committed-row retrieval operations.
- `oversync/rest_models.go` repeats `schema`, `table`, `key`, `op`, version, and `payload` member names
  in every row object. Pull and snapshot rows repeat the same envelope.
- capabilities expose both wire limits and server implementation-tuning values, including default
  chunk sizes and concurrency/materialization settings.
- the direct Go client implements durable push-session continuation and separate committed-row
  settlement because the transport splits one logical push across several endpoints.
- canonical request and committed-bundle hashes already operate on logical rows. Their semantic
  inputs can remain stable even when the transport packs those rows differently.

The SQLiteNow companion records the corresponding KMP observations. These are baseline facts, not
requirements to preserve the existing shapes.

## 5. Replacement endpoint set

### 5.1 Retained or reshaped operations

The supported client-facing data plane consists of:

- `GET /sync/generations/{generation_id}`: the single discovery and admitted-contract response;
- `POST /sync/connect`: source lifecycle admission and initialization coordination;
- `POST /sync/push`: one complete, bounded, atomic, idempotent local bundle;
- `GET /sync/pull`: incremental authoritative bundles;
- `GET /sync/watch`: an optional wake-up hint stream;
- `POST /sync/snapshot-sessions`: create a bounded snapshot session;
- `GET /sync/snapshot-sessions/{snapshot_id}`: retrieve a bounded snapshot chunk; and
- `DELETE /sync/snapshot-sessions/{snapshot_id}`: explicitly release a snapshot session.

Existing health and operator-status endpoints remain outside this client data-plane redesign.

### 5.2 Removed operations

The replacement deletes:

- `GET /sync/capabilities`;
- `POST /sync/push-sessions`;
- `POST /sync/push-sessions/{push_id}/chunks`;
- `POST /sync/push-sessions/{push_id}/commit`;
- `DELETE /sync/push-sessions/{push_id}`; and
- `GET /sync/committed-bundles/{bundle_seq}/rows`.

No fast-path endpoint is added beside these routes. `POST /sync/push` replaces the complete
push-session and committed-row workflow.

### 5.3 Discovery is the sole contract response

The generation-discovery response becomes the only opening-time response that describes the wire
contract. It retains the exact `protocol_version: "v1"` and schema-generation identity and table
contract. It also carries only client-relevant hard limits and whether watch is enabled.

It must not expose implementation-tuning details such as server materialization batch sizes,
internal concurrency counts, or recommended row counts. The retained limit vocabulary is:

- `max_push_rows`;
- `max_push_decoded_bytes`;
- `max_pull_bundles`;
- `max_pull_decoded_bytes`;
- `max_snapshot_rows_per_chunk`;
- `max_snapshot_decoded_bytes_per_chunk`;
- `max_snapshot_rows`;
- `max_snapshot_logical_bytes`; and
- `max_row_decoded_bytes`.

All limits are positive integers. `max_push_decoded_bytes`, `max_pull_decoded_bytes`, and
`max_snapshot_decoded_bytes_per_chunk` count the complete decoded compact JSON document, including
its top-level envelope, group headers, tuples, punctuation, and emitted UTF-8 string escapes, but not
HTTP framing or compressed bytes. `max_snapshot_logical_bytes` instead bounds the
pagination-independent snapshot logical total defined in section 8.2. `max_push_decoded_bytes`
bounds each push request and each successful push response. `max_row_decoded_bytes` bounds the
canonical expanded key plus payload for one logical row.

Server configuration and every bundle-producing path enforce these representation invariants before
publishing a ready contract or committing data:

- one maximum-sized valid push row plus its complete request envelope, and its possible complete
  authoritative success response, fit in `max_push_decoded_bytes`;
- every committed bundle, including a server-originated bundle, fits as the only bundle in a
  complete pull response within `max_pull_decoded_bytes`; and
- every materialized snapshot row fits as the only row in a complete compact chunk response within
  `max_snapshot_decoded_bytes_per_chunk`.

Invalid static configuration makes the service not ready. A commit path that cannot satisfy the
pull invariant returns typed `bundle_projection_too_large` and rolls back before publishing
history; the HTTP push mapping is `422`. A non-HTTP/server-originated writer receives the equivalent
typed application error. Snapshot creation that finds an unrepresentable row returns the retained
`snapshot_session_limit_exceeded` family with dimension `row_decoded_body_bytes`, exact actual and
limit values, and no published session. Encountering an already-committed bundle that violates the
invariant is corruption and pull returns terminal `500 committed_bundle_unrepresentable`; it must
not return an empty page with `has_more: true`. The client stops automatic retry and requires
operator repair or an explicit recovery path. The response contains `watch_enabled: true|false`;
no generic feature map is retained for behavior that is mandatory in this specification.

A client validates and caches this response during opening. Once the ready client is published, an
ordinary push, pull, watch reconnect, or snapshot operation must not perform a capabilities or
discovery preflight. Reopening or reattaching obtains a fresh contract. A terminal generation or
source-identity error also returns the client to the existing explicit opening/recovery path rather
than silently refreshing during the failed operation.

## 6. Compact logical-row encoding

### 6.1 Table indices

`schema_generation.tables` in discovery is canonically ordered and covered by the generation
fingerprint. The zero-based position of a table in that exact array is its `table_index` for the
ready client's lifetime. No second table registry or transport-specific table identifier is added.

Within a table contract, key values use the exact `sync_key_columns` order. Payload remains a JSON
object because partial updates require a distinction between an omitted column and a column set to
SQL `NULL`. This specification deliberately does not add positional payload masks or a second
column-dictionary protocol before measurements show that payload member names remain material
after grouping and compression.

### 6.2 Row groups

Every push, committed effect, pull bundle, and snapshot chunk represents rows as ordered groups:

```json
[
  [
    0,
    0,
    [
      ["i", [101], 0, {"id":101,"title":"first"}],
      ["u", [102], 4, {"title":"renamed"}],
      ["d", [103], 7]
    ]
  ]
]
```

A group is `[table_index, first_row_ordinal, rows]`.

- `table_index` selects the discovered table.
- `first_row_ordinal` is the ordinal of the first row in that group.
- Groups cover contiguous ranges in logical row order. They are ordered by ordinal, contain no gap
  or overlap, and begin at the enclosing body's requested or implied start ordinal.
- A new group begins whenever the next logical row belongs to a different table. Consecutive groups
  may therefore use the same table index only if an intervening group used another table.
- A row is `[`op`, key_values, version]` for a delete and
  `[`op`, key_values, version, payload]` for an insert or update.
- The exact operation codes are `"i"`, `"u"`, and `"d"`; expansion maps them to the retained
  logical operations `INSERT`, `UPDATE`, and `DELETE`, respectively.
- `key_values` has exactly one entry per discovered sync-key column, in discovery order.
- On push, `version` is the base row version. In authoritative effects, pull, and snapshots it is the
  authoritative row version.
- Insert and update payloads are JSON objects. Delete rows have exactly three elements and therefore
  cannot carry a payload.
- Snapshot rows are current full-row after-images. Their tuple operation is always `"i"`, their
  payload is the complete visible row, and their version is the authoritative row version at the
  pinned snapshot checkpoint. Snapshot decoders reject `"u"`, `"d"`, a missing payload, or a
  partial payload. The insert code describes insertion into empty snapshot staging; it does not
  claim that the row was historically created by an insert bundle.

Decoders reject unknown table indices, unknown operation codes, wrong tuple arity, key arity
mismatch, non-canonical table or column names inside payloads, ordinal gaps or overlaps, duplicate
logical targets, hidden scope columns, and values that violate the discovered wire type.

The transport codec expands a row tuple to the existing logical row fields before validation,
hashing, conflict checks, or database application. Consequently:

- `oversync.generation-pinned-push-request.v1` retains its current logical input and domain label;
- the committed-bundle hash retains its current logical input and domain label;
- transfer compression never participates in a hash; and
- transport grouping cannot change logical row order.

Cross-runtime golden vectors must prove that the old object representation and the new tuple
representation expand to the same canonical logical rows and therefore the same retained hashes.
The old object representation is test input only after cutover; it is not accepted on the network.

## 7. Single-request push

### 7.1 Request

`POST /sync/push` carries exactly one complete logical client bundle:

```json
{
  "source_bundle_id": 17,
  "canonical_request_hash": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
  "initialization_id": "optional-only-when-required-by-connect",
  "rows": [
    [0, 0, [["u", [102], 4, {"title":"renamed"}]]]
  ]
}
```

The source ID and generation proof remain exact request headers. `rows` must be non-empty and must
fit both advertised push limits. `planned_row_count`, push IDs, chunk ordinals, client-selected chunk
sizes, and commit requests no longer exist.

The client freezes a bounded bundle before sending it. If more dirty rows exist, it creates later
source bundles; it does not create one oversized logical bundle and transport it through a session.
The client chooses a batch that fits both row and decoded-byte limits. Row count alone is never the
only batching criterion.

A client-local push byte value is a soft batching target; the admitted server value is the hard
protocol maximum. Selection measures the complete compact request incrementally. When the first
pending row plus the envelope exceeds the local target but fits the server hard maximum, the client
must send that row alone so progress is possible. No second row may take the body above the local
target. If the envelope plus first row exceeds the server maximum, the client preserves the dirty
row without freezing or consuming a source bundle ID. A row that itself exceeds
`max_row_decoded_bytes` raises terminal `push_row_exceeds_admitted_limit` and requires data/schema
correction or connection to a server with a sufficient contract. A row within that row limit whose
complete request still exceeds the server push maximum proves the section 5.3 relationship false
and raises terminal `invalid_admitted_contract`. Clients never send an empty push; an envelope that
cannot fit the server maximum likewise makes discovery admission invalid, while an envelope larger
only than the local target uses the same one-row exception.

### 7.2 Commit and response

The server validates the entire request before durable mutation, commits the logical bundle atomically, and
returns the complete generation-visible authoritative effects in the successful response:

```json
{
  "bundle_seq": 81,
  "bundle_hash": "abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd",
  "rows": [
    [0, 0, [["u", [102], 81, {"id":102,"title":"renamed"}]]]
  ]
}
```

HTTP success means committed. A redundant `status`, `source_id`, `source_bundle_id`,
`planned_row_count`, `row_count`, `byte_count`, or echoed request hash is not included. The client
already owns the request identity and verifies the authoritative rows with `bundle_hash`.

The successful decoded response is bounded by `max_push_decoded_bytes`. Server configuration must
make this bound sufficient for every bundle the server admits under its advertised push limits. The
server must reject an inadmissible request before commit and roll back any transaction work used to
derive authoritative effects; it must never commit a bundle and then discover that its required
success response cannot be represented within the advertised bound.

### 7.3 Idempotency and faults

`(authenticated user, source_id, source_bundle_id)` remains the idempotency key and the canonical
request hash remains the replay identity.

- An exact retry after a lost response returns the same `bundle_seq`, `bundle_hash`, and logical
  authoritative rows.
- Reuse with a different hash is rejected as the existing deterministic replay conflict.
- Disconnect or cancellation before the server commit leaves no committed bundle.
- Disconnect after commit may be observationally ambiguous; exact retry resolves it.
- The server retains only the durable committed bundle needed for ordinary history and replay. It
  does not retain a separate upload-session state machine or staged upload rows.
- A client keeps its frozen outbox bundle until the successful response is validated and applied
  atomically. Process death at any point replays the same complete request.

Committed replay is classified before mutable lifecycle admission. After authentication, immutable
source/generation identity and header grammar, body limits and decoding, strict envelope parsing,
tuple/logical validation, and canonical request-hash recomputation, the server looks up
`(authenticated user, source_id, source_bundle_id)` in retained committed history and interprets the
result as follows:

- a matching canonical request hash returns the stored committed result without rerunning current
  source sequence, scope lifecycle, initialization lease/state, conflict detection, application
  hooks, or write transaction work;
- a retained tuple with a different canonical request hash returns the deterministic replay-
  mismatch error;
- if no row is retained but durable source sequence proves that this source-bundle ID was already
  committed and pruned, the server returns `history_pruned` rather than attempting a new write; a
  genuinely new ID continues to normal sequence/lifecycle admission; and
- an unavailable generation projection for a retained replay returns the existing generation-
  retirement recovery outcome rather than attempting a new write.

This rule deliberately lets a first initialization push be replayed after that commit has moved the
scope from initializing to initialized. It also permits an exact read-only replay after later source
lifecycle movement while the authenticated committed history and its projection remain retained.
Authentication, request/source identity, generation proof matching the committed record and needed
to decode its retained projection, body bounds, strict syntax, logical validation, hash equality,
and retained-history availability are never bypassed. Replay does not require that the matched
generation or source still be writable; it requires that their authenticated committed projection
remain available.

This chooses cheap ordinary operation and a small state machine over partial-upload continuation.
The bounded request size limits the cost of retransmitting a failed large request.

## 8. Pull and snapshot response cleanup

### 8.1 Pull

Pull keeps its checkpoint, stable-ceiling, history-pruned, pagination, and atomic-application
semantics. Each returned bundle retains only metadata the client cannot derive safely:

- `bundle_seq`;
- `source_id`;
- `source_bundle_id`;
- `bundle_hash`; and
- compact `rows`.

Per-bundle `row_count`, `byte_count`, and a push-only canonical request hash are absent. The enclosing
response retains `stable_bundle_seq` and `has_more`. The decoded response is bounded by
`max_pull_decoded_bytes` and bundle count by `max_pull_bundles`.

A generation projection may legitimately hide every row in a committed bundle. Pull represents
that case as the real bundle metadata with compact `rows: []`; the client verifies the empty-row
bundle hash and advances its checkpoint atomically without applying rows. This is not an empty page
and must not cause a pagination loop. Push requests and authoritative push responses remain
non-empty. A retained bundle that cannot be encoded or fit as an indivisible pull response still
returns terminal `500 committed_bundle_unrepresentable`.

### 8.2 Snapshots

Snapshot session creation retains the snapshot ID, stable snapshot bundle sequence, total row count,
total `logical_byte_count`, and expiry because these values drive recovery, capacity checks, and
progress.

Snapshot logical bytes use one pagination-independent function named
`oversync.snapshot-logical-row.v1`. For each row, construct this logical accounting value:

```json
["i", table_index, key_values, row_version, payload]
```

`table_index`, `key_values`, `row_version`, and the complete payload use the admitted generation and
the same exact value grammar used for compact tuples. The row's logical byte count is the UTF-8
length of the JCS encoding of that array. No domain-label bytes, separators between rows, HTTP
envelope, group header, ordinal, or compression bytes are counted. Session
`logical_byte_count` is the checked sum over all rows in row-ordinal order. Each chunk returns the
same checked sum for only its rows, and clients sum chunk values and require exact equality with the
session total before final adoption. Pagination and retry therefore cannot change the total.

A chunk response does not repeat the snapshot ID or snapshot bundle sequence already bound to the
URL and durable client state. It contains compact row groups, `next_row_ordinal`, and `has_more`.
Existing staging invisibility, final atomic replacement, checkpoint recovery, expiry, source
replacement, and bounded-memory semantics remain.

The chunk request `max_bytes` and `max_snapshot_decoded_bytes_per_chunk` continue to bound the
complete decoded compact HTTP body, not logical bytes. If the next row fits the server hard maximum
but not the request's smaller byte target, `snapshot_chunk_too_small` returns
`required_decoded_body_bytes`: the exact decoded UTF-8 length of the minimal successful chunk body
containing that one row, its group and envelope, the correct `next_row_ordinal`, and the correct
`has_more`. The client retries the same ordinal with `max_rows=1` and that byte value, bounded by the
admitted server maximum. This required value is never added to snapshot logical totals. If a row
cannot fit the server hard maximum, snapshot creation must already have failed and no session may be
published.

## 9. HTTP compression

### 9.1 Required behavior

Bounded JSON request and response bodies use HTTP `gzip` selectively:

1. Clients with a raw/native HTTP transport send `Accept-Encoding: gzip` for bounded JSON
   responses. A Fetch-managed browser transport relies on the user agent's forbidden-header and
   content-decoding behavior and does not attempt to set or recover `Accept-Encoding` itself.
2. A client considers request compression only when the compact UTF-8 JSON body is at least 2,048
   bytes. It sends `Content-Encoding: gzip` only when the encoded result is strictly smaller than
   the identity body.
3. A server considers response compression only when the identity body is at least 2,048 bytes and
   the request accepts gzip. It may send identity when compression is not beneficial.
4. Bodies below 2,048 bytes use identity. In particular, the common small one-row push is not
   compressed merely because gzip is available.
5. `text/event-stream` watch responses are never gzip-compressed by this protocol.
6. Discovery, connect, error, and empty responses follow the same threshold; endpoint names do not
   force compression.
7. `Content-Length`, when present, describes encoded bytes. All advertised protocol byte limits,
   JSON validation, hashes, and row-size checks describe the decoded body or expanded logical rows
   as specified by their owning contract.
8. A receiver that can observe raw response bytes bounds both encoded input and incrementally
   decoded output. Because senders use gzip only when smaller, the encoded-byte ceiling may equal
   the applicable decoded-body ceiling. A decoded body that crosses its limit is aborted and
   rejected even if its gzip body was small.
9. Raw transports reject invalid gzip, concatenated trailing data, unsupported content encodings,
   or multiple content encodings without attempting identity fallback.

Browser Fetch is an explicit enforcement boundary, not a protocol variant. For JS and Wasm browser
clients, the user agent owns response content negotiation, encoded-byte intake, gzip member parsing,
and decoding before application bytes are exposed. The Oversqlite client incrementally bounds the
decoded bytes Fetch exposes and rejects any still-observable unsupported or multiple encoding
metadata, but it does not claim to inspect discarded encoded bytes or gzip member boundaries. A user
agent network/decompression failure is a transport failure and preserves the frozen operation for
retry. Server conformance still requires one identity or single-member gzip representation within
the encoded ceiling; browser live qualification proves that server behavior from an external
network trace, while adversarial encoded-overflow and concatenated-member client rejection apply
only to raw transports. Browser request compression remains mandatory: the client supplies the
already-compressed body with `Content-Encoding: gzip`; a cross-origin deployment must allow that
request header and the resulting preflight. Failure to obtain that permission is an environment
configuration failure, not permission to send an above-threshold compressible request by identity.
This boundary adds no capability bit or alternate JSON contract.

Compression is a transport optimization, not a capability, feature, protocol revision, hash input,
or reason to keep two JSON codecs.

### 9.2 Evidence

Qualification must record identity and on-wire byte counts for at least:

- a small single-row insert;
- a small single-row delete;
- a 200-row one-table push;
- a mixed-table push;
- an ordinary multi-bundle pull;
- a large snapshot chunk; and
- deterministic incompressible content above the threshold.

Small fixtures must prove identity encoding. Compressible larger fixtures must prove fewer on-wire
bytes than compact identity JSON. Incompressible fixtures must prove that gzip is not mandatory when
it does not save bytes. Fixture expectations identify whether they apply to every transport,
raw/native transports only, or server-wire capture for Fetch-managed browsers. Browser evidence
must never relabel application-visible decoded bytes as encoded network bytes.

## 10. Watch is a hint, not a pull queue

Watch events mean only that a pull may make progress. They do not create one required pull per event.

The server may coalesce multiple bundle commits into one event carrying the highest observed bundle
sequence. The client keeps one pending high-water mark:

- if no pull is running, the first newer event schedules one pull-to-stable operation;
- while a pull is running, later events only raise the pending high-water mark;
- after the pull completes, the client schedules at most one trailing pull when its durable
  checkpoint is still below that high-water mark; and
- reconnect performs the same checkpoint comparison rather than an unconditional extra pull when
  the checkpoint is already current.

Watch failure retains polling fallback. Watch delivery remains lossy and reconnectable; correctness
comes from pull checkpoints, not from receiving every event.

## 11. OpenAPI, direct Go client, and shared corpus ownership

The Go repository owns the normative transport contract:

- `swagger/two_way_sync.yaml` defines the replacement endpoints and compact tuple schemas;
- Go HTTP handlers enforce the same contract;
- exported direct-service operations use the same one-bundle push semantics and logical row codec,
  without simulating HTTP compression;
- the direct Go Oversqlite client exercises the same admission cache, batching, replay, response
  application, and watch-conflation behavior.

The existing language-neutral corpus remains in the SQLiteNow repository at
`oversqlite-contracts/`. p0000 governs the required wire meanings and scenarios; the SQLiteNow
companion governs corpus storage, manifest integrity, and consumer wiring. P0 is therefore a
coordinated two-repository fixture change even though production implementation remains closed.
Go tests must accept an explicit corpus root and fail closed when the accepted manifest is absent or
has the wrong hash; they must not silently fall back to copied literals.

The corpus must contain valid, boundary, malformed, fault, and compression metadata cases. Each
consumer must parse and execute the same fixture files; copying fixture content into Kotlin, Swift,
or Go test literals does not qualify as shared-corpus evidence.

## 12. Required implementation sequence

Implementation remains closed until this draft is reviewed and owner-accepted. After acceptance,
the recommended gates are:

1. **P0 — contract freeze:** finalize OpenAPI, compact-codec vectors, request-count scenarios,
   compression fixtures in the existing SQLiteNow corpus, malformed-input precedence, and exact
   retained hash equivalence.
2. **P1 — Go authority:** replace handlers and persisted push-session machinery, update the direct
   service surface, implement bounded gzip, and make the manifest-pinned shared fixtures pass.
3. **P2 — direct Go client:** replace session upload/settlement with bounded whole-bundle replay,
   cache discovery at opening, and conflate watch hints.
4. **P3 — SQLiteNow KMP and Swift:** implement the separately accepted companion against the frozen
   Go authority and shared corpus.
5. **P4 — paired qualification:** run Go package/root/vet and database lanes plus KMP host,
   platform, real-server, Swift, fault, request-count, and byte-evidence lanes against one frozen
   pair of revisions.
6. **P5 — independent review and owner acceptance:** review the complete paired candidate, remediate
   findings, rerun invalidated evidence, and stop for explicit acceptance before delivery.

P0 may add or change specs, OpenAPI, and fixtures only after implementation is authorized. It must
not silently begin production implementation during specification acceptance.

## 13. Acceptance criteria

The paired implementation is eligible for owner acceptance only when all of the following are true:

### 13.1 Simplicity and removal

- only the endpoint set in section 5 remains in the supported protocol;
- no capabilities endpoint, push-session route, committed-row route, compatibility handler, legacy
  request decoder, client fallback, feature flag, or deprecated facade remains;
- server-side push-session tables, staged-row state, cleanup jobs, and configuration exist only if
  another accepted non-protocol contract independently requires them; otherwise they are removed;
- public docs, examples, OpenAPI, generated clients, and runtime errors describe only the new path;
- `protocol_version` is still exactly `v1`, and no `v2` or version-negotiation surface appears.

### 13.2 Exchange counts

- after opening, a no-change sync issues exactly one data-plane request: pull;
- after opening, a successful one-row sync issues exactly two sequential data-plane requests: one
  push and one pull;
- neither path performs discovery or capabilities preflight;
- an exact replay after a lost one-row push response resolves with one repeated push request and the
  normal following pull;
- a bounded multi-row logical bundle uses one push request rather than create/chunk/commit/fetch.

### 13.3 Bytes and compression

- compact encoding removes per-row `schema`, `table`, `key`, `op`, and version member names;
- the agreed golden single-row and 200-row fixtures are smaller as compact identity JSON than their
  current object-envelope equivalents;
- bodies below 2,048 bytes are identity encoded;
- compressible bodies at or above 2,048 bytes demonstrate a smaller transmitted gzip body;
- raw transports fail closed on decoded and encoded limit violations without unbounded buffering;
  Fetch-managed browsers fail closed on decoded overflow and use the section 9.1 user-agent
  boundary for encoded response intake and gzip-member validation;
- identity and gzip encodings produce identical semantic results and hashes.

### 13.4 Semantics and recovery

- compact rows expand deterministically and reproduce retained canonical request and bundle hashes;
- success-response loss, request-body interruption, process death before send, process death after
  commit, exact replay after initialization completion, mismatched replay, conflict, rejection,
  retirement, and initialization faults preserve existing intent and recovery invariants;
- pull and snapshot application remain atomic and bounded;
- no admitted committed bundle or published snapshot row can stall forever behind a complete-body
  byte limit, and snapshot logical totals are pagination-independent;
- 100 watch events arriving during one active pull cause no more than that pull and one trailing
  pull, while the final durable checkpoint reaches the advertised high-water mark;
- disabled or failed watch retains polling correctness.

### 13.5 Cross-runtime evidence

- Go server, direct Go client, KMP JVM, Android, Apple/native, JS, Wasm, and Swift-facing products
  consume the same manifest-pinned codec and fault corpus where those targets are supported;
- live-server lanes prove real HTTP gzip, identity, request counts, redirects, cancellation, body
  limits, and replay rather than relying only on mocked transport; browser evidence distinguishes
  externally captured server-wire behavior from application-visible decoded-byte enforcement;
- the final evidence ledger pins both repository revisions, the accepted spec hashes, OpenAPI hash,
  fixture hashes, commands, environment, and raw byte-count artifacts.

## 14. Closed HTTP decisions

This section closes transport details that an implementation task must not redesign.

### 14.1 Common request rules

- Authentication, exact source ID, generation ID, and generation fingerprint headers retain their
  current names, grammar, and validation behavior.
- JSON requests use `Content-Type: application/json`. Parameters on that media type are allowed only
  when their semantics are UTF-8. Any other media type returns `415 unsupported_media_type`.
- Request `Content-Encoding` is absent/`identity` or exactly `gzip`. Multiple encodings and every
  other encoding return `415 unsupported_content_encoding`.
- JSON decoding is strict: one UTF-8 JSON value, no unknown top-level or envelope object members, no
  duplicate object members, no trailing non-whitespace bytes, and exact integer/string grammar from
  OpenAPI.
- Encoded-byte overflow returns `413 encoded_body_too_large`; decoded-byte overflow returns
  `413 decoded_body_too_large`; malformed gzip returns `400 invalid_gzip`; malformed JSON or an
  invalid top-level/envelope shape returns `400 invalid_request`. After that envelope is accepted,
  every row-group, tuple, ordinal, operation, key, payload, row-version, duplicate-target, hidden
  column, logical-hash, or advertised logical-limit failure in a push returns `400 push_invalid`.
- The push envelope is accepted only when the top level is an object with exactly the required and
  permitted members, each member has its required JSON primitive/container type, and `rows` is an
  array. Envelope examples include a missing `source_bundle_id`, string-valued `rows`, or an unknown
  top-level member and return `invalid_request`. An empty rows array and evaluation of any rows
  element belong to the logical/group/tuple stage; empty rows or a wrong group/tuple arity, type,
  value, or ordering therefore returns `push_invalid`, not `invalid_request`.
- Successful bounded JSON responses use `Content-Type: application/json`. Errors retain the current
  `{ "error": string, "message": string, ...typed fields }` family and are themselves bounded.
  Discovery, connect, push, pull, and snapshot successes and every typed error use the same
  request-aware bounded writer and the section 9.1 2,048-byte selective-gzip rule; specialized
  generation, source-retirement, conflict, and representation-limit bodies are not allowed to
  bypass that boundary. SSE remains identity encoded.
- Redirects are not part of the protocol. Clients reject them and do not forward authorization or
  source/generation proof to another location.

Validation precedence for every bounded JSON write is: method and authentication; immutable source
and generation identity/header grammar; media type and content encoding; encoded limit; gzip decode
and decoded limit; strict JSON/envelope shape; tuple and logical-row validation plus canonical
request-hash recomputation; committed-history lookup by source tuple. An existing tuple then follows
section 7.3 exact-replay or mismatch behavior. Only a non-replay continues through current source
lifecycle and sequence; initialization state/lease; conflict/application hook; and transaction
commit. A higher-precedence failure is returned without evaluating lower-precedence application
work. Replay does not refresh leases, invoke hooks, or mutate lifecycle state.

### 14.2 Exact discovery and connect boundary

Generation discovery returns this top-level shape and no additional members:

```json
{
  "protocol_version": "v1",
  "schema_generation": {"id":"1","fingerprint":"...","tables":[]},
  "limits": {
    "max_push_rows": 200,
    "max_push_decoded_bytes": 1048576,
    "max_pull_bundles": 1000,
    "max_pull_decoded_bytes": 67108864,
    "max_snapshot_rows_per_chunk": 1000,
    "max_snapshot_decoded_bytes_per_chunk": 4194304,
    "max_snapshot_rows": 100000,
    "max_snapshot_logical_bytes": 104857600,
    "max_row_decoded_bytes": 4194304
  },
  "watch_enabled": true
}
```

The numbers above illustrate the shape, not universal server defaults. `schema_generation` retains
the accepted ID, fingerprint, table, key-column, column, type, nullability, and foreign-key grammar.
`app_name`, generic `features`, `bundle_limits`, recommended chunk sizes, TTLs, materialization
batches, and concurrency settings are absent.

Connect retains its current request and lifecycle-resolution response. It does not repeat discovery
or limits. Discovery precedes connect during opening, and the client publishes neither as a ready
contract until connect reaches the existing ready/admitted outcome.

The connect success body has exactly `resolution` plus the resolution-dependent optional
`initialization_id`, `lease_expires_at`, and `retry_after_seconds` members defined by OpenAPI.
`initialization_id`, wherever present in connect or direct push, is an exact lowercase dashed UUID;
neither side trims, normalizes, or case-folds it. The allowed resolution values are
`remote_authoritative`, `initialize_local`, `initialize_empty`, and `retry_later`.

### 14.3 Exact push statuses and errors

`POST /sync/push` returns:

- `200` with the section 7 success body for a new commit or exact committed replay;
- `400 invalid_request` for malformed JSON or an invalid top-level/envelope shape;
- `400 push_invalid` after envelope acceptance for an invalid logical bundle, hash, group, tuple,
  ordinal, operation, key, payload, version, duplicate target, hidden column, or logical limit;
- `409 push_conflict` with the retained typed conflict body;
- `409 push_replay_mismatch` when a committed source tuple exists with a different canonical
  request hash;
- `409 source_sequence_out_of_order`, `source_sequence_changed`, `scope_uninitialized`,
  `scope_initializing`, `initialization_stale`, or `history_pruned` under their retained meanings;
- `410 initialization_expired` or an existing generation/source retirement error;
- `413 encoded_body_too_large` or `decoded_body_too_large` at the transport boundary;
- `422` with the retained application rejection category;
- `422 bundle_projection_too_large` when a new push's authoritative result or future indivisible
  pull representation cannot fit and the transaction is rolled back;
- `503 commit_outcome_unknown` when commit acknowledgement is indeterminate, or
  `503 service_not_ready`; and
- `500 push_failed` only for an otherwise unclassified server failure.

There is no `201`, staging response, push ID, `202`, commit endpoint, or result-page continuation.
`commit_outcome_unknown` instructs the client to retry the same complete request and source tuple.

### 14.4 Retained read query grammar and compact responses

- Pull retains optional `after_bundle_seq`, `max_bundles`, and `target_bundle_seq` query parameters.
  Omitting `after_bundle_seq` means `0`; omitting `max_bundles` uses the existing bounded server
  default; omitting `target_bundle_seq` captures the current committed ceiling. Their current numeric
  grammar, bounds, checkpoint, stable-ceiling, history-pruned, and source-lifecycle errors remain.
  Only bundle row encoding and redundant bundle fields change as specified in section 8.
  An invariant-violating retained bundle returns terminal
  `500 committed_bundle_unrepresentable`, never an empty `has_more` loop.
- Snapshot chunk retrieval retains optional `after_row_ordinal`, `max_rows`, and `max_bytes` query
  parameters. Omitting `after_row_ordinal` means `0`; omitting `max_rows` or `max_bytes` uses the
  corresponding bounded server default. Explicit and defaulted row and byte values remain capped by
  the discovered server maximum. Create/delete, capacity, expiry, authorization, source replacement,
  and recovery meanings remain.
- `snapshot_chunk_too_small.required_decoded_body_bytes` has the exact section 8.2 meaning. The old
  ambiguous `required_byte_count` member is removed.
- Watch retains optional `after_bundle_seq`; omitting it means `0`. A bundle event is exactly
  `event: bundle\ndata: {"bundle_seq":N}\n\n`; `source_id` and `source_bundle_id` are removed from
  the event because they are not used for correctness. Heartbeat comments remain allowed.
- Every unknown query parameter on these client endpoints is rejected as `400 invalid_request`
  rather than ignored.

### 14.5 OpenAPI completion rule

P0 is not complete until `swagger/two_way_sync.yaml` defines every retained request, success, typed
error, header, query parameter, compact tuple arity, gzip behavior, and removed route exactly. No
runtime implementation may intentionally differ from OpenAPI and claim the prose alone as authority.

## 15. Closed persistence and cutover decisions

### 15.1 PostgreSQL

The fresh managed layout removes `sync.push_sessions`, `sync.push_session_rows`, their constraints,
indexes, cleanup work, metrics, limits, configuration, manifest entries, and reserved-relation
references. `sync.bundle_log` and `sync.bundle_rows` remain the durable idempotency and replay source.
An exact replay reconstructs the generation-visible success response from committed history; it
does not need upload-session state.

This is a managed-layout breaking cutover. Bootstrap does not migrate, read, drain, or drop the old
session tables. An existing database whose exact managed layout contains them fails closed under the
ordinary managed-layout mismatch behavior. Before deploying the new server, operators stop every
old server and managed writer and recreate the development database or restore application data into
a freshly bootstrapped supported layout. Mixed old/new servers are unsupported. The retained exact
labels `v1` and `server_postgres_sync_v1` do not create an old-layout compatibility promise.

### 15.2 Direct Go SQLite client

The direct Go client keeps a frozen semantic outbox until compact authoritative effects are
validated and applied. It removes active push IDs, next-upload ordinals, committed-row page cursors,
and any state used only by staged upload. If its current local control schema contains such fields,
the expected schema is replaced directly. An existing database with the obsolete exact control
schema fails closed; there is no dual reader or automatic compatibility migration.

### 15.3 Cutover invariant

No accepted artifact may imply that a pending legacy session can be resumed after cutover. The
operator precondition is an atomic code/database cutover, not session drainage by the new code.

## 16. Go implementation checklist

Every item is intentionally unchecked. The implementation task updates this list or a linked
ledger with evidence; prose claims do not close an item.

### P0 — authority, OpenAPI, and corpus

- [ ] Record explicit owner acceptance and implementation authorization for the exact p0000 hash.
- [ ] Record `git rev-parse HEAD`, branch, worktree status, Go version, Docker version, and the paired
  SQLiteNow revision without treating unrelated pre-existing drift as implementation work.
- [ ] Replace the route and schema surface in `swagger/two_way_sync.yaml`, including removals,
  compact tuples, insert-only snapshot tuples, envelope-aware discovery invariants, snapshot logical
  byte fields, exact errors, gzip/Fetch enforcement profiles, and SSE payload.
- [ ] Update `oversync/protocol_documentation_test.go` and related OpenAPI-presence tests so removed
  paths fail if reintroduced and every required new field/status is asserted.
- [ ] Coordinate the manifest-pinned fixture changes under
  `sqlitenow-kmp/oversqlite-contracts/`; record the corpus root and manifest hash used by Go tests.
- [ ] Freeze vectors for the single-row local-target exception, indivisible pull/snapshot limits,
  snapshot logical-byte accounting and `required_decoded_body_bytes`, initialization replay after
  response loss/restart, insert-only snapshot tuples, and `invalid_request` versus `push_invalid`.
- [ ] Freeze the OpenAPI and fixture hashes before production implementation starts.

### P1 — Go server contract and codec

- [ ] Replace verbose wire models in `oversync/rest_models.go` with strict discovery limits, compact
  row groups, one-shot push request/response, compact pull bundles, compact snapshot chunks, and the
  reduced watch event.
- [ ] Implement one shared compact codec that expands to retained logical rows before validation and
  hashing; add exact tuple-arity, ordinal, table-index, key-order, payload, insert-only snapshot, and
  malformed-input tests.
- [ ] Preserve existing canonical request and committed-bundle hash results with golden equivalence
  vectors; do not add a new hash label.
- [ ] Replace the push handlers/service entrypoints in `oversync/http_handlers.go`,
  `oversync/service.go`, and the owning push implementation with atomic `POST /sync/push` and exact
  replay from committed history before mutable lifecycle admission.
- [ ] Implement the section 14 validation precedence and exact `invalid_request`/`push_invalid`
  split; prove an initialization commit can replay after response loss and restart without renewing
  a lease or rerunning sequence, lifecycle, conflict, hook, or write work.
- [ ] Add bounded request gzip decode and selective bounded response gzip encode; prove encoded and
  decoded ceilings independently on raw transports, keep SSE identity encoded, and expose the exact
  server-wire behavior required for Fetch-managed browser evidence.
- [ ] Move hard client limits and `watch_enabled` into generation discovery; remove
  `GetCapabilities`, `HandleCapabilities`, `CapabilitiesResponse`, `BundleCapabilitiesLimits`, and
  generic mandatory feature negotiation.
- [ ] Enforce the indivisible pull-bundle and snapshot-row representation invariants before commit
  or session publication; never emit an empty `has_more` pull stall.
- [ ] Compact pull projection and insert-only snapshot chunk projection without weakening their
  SQL-side suppression, decoded-response limits, stable ceiling, or snapshot-session guarantees;
  implement the exact snapshot logical-byte function and chunk-too-small required-body count.
- [ ] Emit the reduced coalescible watch event and preserve polling correctness.

### P2 — server persistence and legacy deletion

- [ ] Delete `oversync/push_sessions.go` after its still-required atomic validation, conflict,
  replay, projection, and transaction logic has an explicit new owner.
- [ ] Remove `sync.push_sessions` and `sync.push_session_rows` from `oversync/schema_init.go`,
  `oversync/managed_layout.go`, catalog/adoption/reservation code, cleanup code, status metrics, and
  every schema/layout assertion.
- [ ] Remove push-session TTLs, chunk sizes, committed-row page sizes, configuration fields,
  constants, errors, counters, and background cleanup.
- [ ] Prove fresh bootstrap contains neither removed table and old exact managed layout fails closed
  without mutation.
- [ ] Replace session-specific restart/resource tests with whole-request interruption, response-loss,
  exact replay, mismatch, conflict, and bounded-body tests.

### P3 — direct Go Oversqlite client and examples

- [ ] Replace `oversqlite/push_http.go`, push replay/settlement, wire models, and state-table code with
  one bounded request and atomic response application.
- [ ] Cache the admitted discovery contract for the ready-client lifetime and remove operation-time
  capabilities calls.
- [ ] Select frozen outbox bundles by both admitted row and decoded-byte targets, sending one
  server-admissible first row alone when it exceeds the local soft byte target and failing locally
  only when its complete request exceeds the admitted server hard maximum.
- [ ] Remove persisted push IDs, chunk ordinals, committed-row cursors, and `committed_remote`-style
  receipt state where it exists; preserve exact whole-request replay and newer-edit protection.
- [ ] Classify `bundle_projection_too_large`, `committed_bundle_unrepresentable`, and snapshot
  representation-limit errors as terminal nonmutation outcomes; preserve the frozen outbox or read
  checkpoint and stop automatic retry until explicit correction/recovery.
- [ ] Add request compression and bounded response decoding with the section 9 raw/native behavior;
  direct Go transport must enforce both encoded and decoded response ceilings.
- [ ] Conflate watch high-water notifications to one active and at most one trailing pull.
- [ ] Replace routes, logs, examples, and middleware classification in
  `examples/samplesync_server`, `examples/nethttp_server`, and `integration/mobile_flow`.
- [ ] Update `oversqlite/README.md`, durable docs, and examples to describe only the replacement.

### P4 — removal and author freeze

- [ ] Search production, tests, OpenAPI, examples, and durable docs for `/sync/capabilities`,
  `/sync/push-sessions`, `/sync/committed-bundles`, push IDs, chunk/commit methods, and removed
  capability fields; every remaining hit must be historical fixture input or an explicit
  absence assertion.
- [ ] Run focused tests until stable, review the complete checklist, and freeze production changes.
- [ ] Record changed paths, final Go revision/worktree identity, OpenAPI hash, fixture-manifest hash,
  and invalidation rules in `.artifacts/p0000-protocol-efficiency/go-ledger.md`.

## 17. Go validation checklist

Run from `/Users/pochkin/Projects/my/go-oversync`. Each command records start/end time, exit status,
toolchain, raw log path, and the unchanged candidate identity. A later production change invalidates
the affected focused/package lane and the final aggregate lane.

Set
`OVERSQLITE_CONTRACTS_ROOT=/Users/pochkin/Projects/my/sqlitenow-kmp/oversqlite-contracts` for every
Go test lane that consumes the shared corpus. Consumers must validate `MANIFEST.sha256` before
reading a fixture and must not discover a different fallback root.

- [ ] Focused compact-codec, gzip/Fetch-profile, push, initialization replay, error precedence,
  indivisible pull, snapshot opcode/logical-byte/chunk-too-small, discovery, watch, layout, and
  OpenAPI tests pass with `-count=1`.
- [ ] `scripts/test-lane.sh package` passes.
- [ ] `scripts/test-lane.sh audit` passes.
- [ ] `scripts/test-lane.sh full` passes.
- [ ] `scripts/test-lane.sh mobile` passes.
- [ ] PostgreSQL 16 and 17.10 coverage in the owning suites passes; the ledger names the tests and
  images rather than inferring coverage from Docker availability.
- [ ] `scripts/run-with-timeout.pl 240 -- env GOTOOLCHAIN=go1.25.12 go vet ./...` passes.
- [ ] Every changed Go file matches Go 1.25.12 `gofmt` output.
- [ ] `git diff --check` passes for Git-visible changes.
- [ ] The paired command
  `GO_OVERSYNC_REPO=/Users/pochkin/Projects/my/go-oversync ./gradlew oversqliteCrossRepoConformance`
  passes from the SQLiteNow root against the frozen OpenAPI and manifest.
- [ ] Request traces prove exactly one request for admitted no-change sync and exactly push then pull
  for admitted one-row sync.
- [ ] Byte artifacts cover every section 9.2 workload and state identity bytes, gzip bytes, ratio,
  encoding choice, decoded size, semantic/hash equality, and whether encoded bytes came from a raw
  receiver or external browser network capture.
- [ ] Final residual searches and fresh-bootstrap schema inspection prove legacy protocol and
  upload-session persistence are absent.

Heavy stress and the complete KMP platform/real-server matrix belong to paired qualification after
both implementations freeze; a Go-only pass is not paired acceptance.

## 18. Fresh-session implementation handoff

Use this handoff only after the owner records acceptance and implementation authorization for the
exact p0000 and companion hashes.

**Repository:** `/Users/pochkin/Projects/my/go-oversync`, paired with
`/Users/pochkin/Projects/my/sqlitenow-kmp`.

**Objective:** implement the complete accepted p0000 Go authority, direct Go client, OpenAPI,
fixture-consumer, persistence deletion, examples, tests, and durable documentation. Replace the old
protocol; do not add a fast path or compatibility layer.

**Prerequisites:** re-read both accepted specs completely; verify their hashes and the current
repository identities; inspect and preserve unrelated worktree state; ensure Go 1.25.12, Docker,
PostgreSQL test images, and the paired corpus checkout are available. Stop on any authority/hash
mismatch or unowned overlapping change.

**Execution:** complete sections 16 and 17 in order. P0 freezes OpenAPI and fixtures; P1/P2 implement
the server and fresh layout; P3 implements the direct client and examples; P4 freezes the author
candidate. Use focused tests during development and run expensive final lanes only after production
changes freeze.

**Deliverables:** implementation; updated OpenAPI; manifest-pinned shared fixtures; focused and
aggregate tests; durable docs/examples; `.artifacts/p0000-protocol-efficiency/go-ledger.md`; raw
request-count and byte artifacts; changed-path manifest; exact final hashes; and a concise review
handoff naming any residual risk.

**Exclusions:** no protocol `v2`, compatibility aliases, dual readers/writers, old database
migration, schema-evolution m-series work, unrelated refactoring, release work, staging, commit,
push, deployment, or publication.

**Stop condition:** stop at `IMPLEMENTED: READY FOR PAIRED KMP IMPLEMENTATION AND AUTHOR
QUALIFICATION`. Do not claim independent review, owner acceptance, delivery, or release readiness.

## 19. Non-goals

This specification does not:

- add a new protocol version or a compatibility negotiation mechanism;
- change schema-generation lifecycle, table evolution, backfill, generation retirement, or the
  m0000 child roadmap;
- make m0010 or any schema-evolution qualification campaign responsible for this work;
- combine push and pull into one transaction or one HTTP request;
- replace JSON with Protobuf, CBOR, MessagePack, or a custom binary transport;
- add positional payload values or payload-column masks before evidence justifies them;
- make watch delivery reliable or authoritative;
- remove snapshot sessions, whose bounded materialization and atomic adoption solve a different
  problem from upload chunking;
- weaken authentication, source binding, generation proof, conflict detection, hash verification,
  response limits, redirect refusal, or cancellation; or
- authorize implementation, qualification, staging, commit, push, deployment, publication, or
  release.

## 20. Convergent interactive sync amendment (2026-09-24)

The locally authorized convergent-sync work extends the direct Go client without changing this
specification's wire contract. One public `Sync(ctx)` drains successive bounded push bundles and
then pulls, including new writes that race a push or pull. The shared 64-entry allowance counts
each push-preparation entry, including recursive conflict reentry, frozen-outbox replay, recovery
pushes and all `SyncThenDetach(ctx)` rounds. HTTP retries of one prepared request are separate.
Check before entry 65; attempt 64 may finish and pull if local work is gone. Count confirmed
bundles once by durable source ID and source-bundle ID. Any confirmed bundle makes the aggregate
push outcome `COMMITTED`, even if an outer retry ends with a no-op.

An incomplete sync has current dirty and outbound counts, unique confirmed-bundle count, and
reason `push_attempt_limit`, `no_progress`, or `uploads_paused`. Prove no progress from durable work
identity rather than row counts. Preserve cancellation and terminal error precedence. Each pull
apply checks local dirty and outbox state in its own applying transaction; only a typed local-work
rejection returns to the drain. Check pending state after a zero-bundle or final pull response.
Checkpoint recovery uses recovery-authorized pushes through the same allowance and returns to
that path after a racing write at snapshot final apply. Direct one-bundle `PushPending(ctx)` and
fail-closed `PullToStable(ctx)` remain primitive operations.

Freeze complete FK dependency groups, including deferred cycles. A group may cross the local soft
byte target if its full compact request fits admitted hard row and byte limits. A group exceeding
those hard limits fails with a typed unsendable-group error before freezing an outbox, consuming a
source-bundle ID, or sending HTTP. `Sync(ctx)` stops before another push on upload pause; with
pending work it returns `uploads_paused`, and with no pending work it may enter the download phase.
For FK-bearing dirty rows, the local retained payload records both the current typed capture and
the server-base typed capture from the first unsent write. Subsequent writes before freeze preserve that base;
legacy captures without a trustworthy base are grouped conservatively. Wire pushes use only the
current capture and send no payload for deletes. Preparation pages the initial dirty selection
and expands only needed FK peers, bounded by the admitted hard row limit.
Download pause retains skipped-remote reporting. `SyncThenDetach(ctx)` uses at most three rounds,
shares the 64-entry allowance, and returns a blocked detach with a typed reason and optional last
fully completed sync report on incomplete or paused work. The blocked reason also includes
`sync_round_limit` when all three rounds have started. Authoritative effects and application
triggers preserve dirty business rows even outside the effect stream. `Start(ctx)` retains its own uploader
policy; its polling downloader waits without spinning while paused.

The no-change one-pull and one-row push-then-pull traces in section 13.2 remain exact. No
endpoint, field, protocol version, OpenAPI change, or corpus change is required: multiple pushes
and authoritative effects already use existing messages. The OpenAPI SHA-256 at this amendment is
`94940822a3ba5f4b614a7e95bde1f528ec5f463793eb4feb4fe6ee2de93c21b4`; the corpus
manifest SHA-256 is `0e64ece3dce806b815b9833dac2be9c7835888a149678db3a540c0937f722f3f`.

## Maintenance validation amendment (2026-09-24)

The owner-authorized proportionate-validation implementation adopts the tracked
[Oversqlite validation runbook](../../sqlitenow-kmp/docs-engineering/oversqlite-validation.md) for subsequent maintenance candidates.
Development runs focused regressions. Maintenance reruns affected owning packages,
supported platform scenarios and live boundaries, with manual reviewer-confirmed
reuse of unchanged evidence. The runbook defines invalidation, boundedness/race
triggers, mandatory-aggregate identification and fail-closed execution counts.

Earlier instructions in this document requiring final aggregate reruns after every
source change apply to this document's initial implementation/qualification campaign
and to release profiles requiring them. They are not unconditional requirements for
later maintenance. The full initial protocol/release matrix remains mandatory;
contract changes require their own accepted profile. Historical acceptance records,
failed attempts and source-revision evidence are unchanged. This amendment does not
retroactively qualify convergent-sync remediation or grant review, acceptance or
delivery. Specification hash changes require a new authority record; no OpenAPI,
wire fixture, corpus manifest or runtime behavior changes are made by this amendment.

## Prospective section17 command amendment — 2026-10-26

For future explicitly bound full-profile candidates, [r0001 validation section9](r0001-oversqlite-durable-recovery-validation.md) and its owner-approved manifest supersede section17 command repetition/envelopes with the approved300s normal-entry schedule and explicitly reduce the separate load maximum to25users/25concurrency; old50/100 load claims earn zero current credit. Preserve every protocol/codec/request/transport/legacy-absence/build/packaging obligation and both ordinary/heavy and normal/observed paths. Section9.2 byte evidence remains seven workloads: six valid sync traffic workloads plus the exact opaque incompressible production-compression-selector witness, with no protocol-semantic claim for that opaque body. Fetch uses actual server-wire encoded capture and actual decoded receipts. E41 changes no wire, SDK default, algorithm or qualification outcome.

### Approved E42 repair amendment — 2026-10-27

The owner approved the [combined repair amendment](../.artifacts/qualification-efficiency-e42-20261026T110553+0200/approved-repair-20261027T0338Z/owner-approval-and-plan.json). The current version2
profile binds its [successor review packet](../.artifacts/qualification-efficiency-e42-20261026T110553+0200/approved-repair-20261027T0338Z/reviewed-profile/proposal.md):508 declarations,
499 normal entries and9 orchestration controllers. The original80-method local
Node entry is partitioned into32/48 whole-class groups, retaining all methods and
300-second wall /270-second work /30-second cleanup caps. Other entry membership,
source workload sizes, body deadlines and the25-user maximum remain unchanged.

The three identified stale fixture files use causal cascade-observation phases
and exclusive-lifetime migration ownership. The worker client immediately owns
its internal readiness rejection while preserving caller-visible errors and
bounded cleanup; only its client asset size/hash changes in the manifest.
These are approved fixture/ownership repairs, with no wire/schema/provider or
SDK-default change. Original coverage columns remain frozen baseline evidence;
additive current fixture bindings identify the four repaired method bodies.

Historical receipts and predecessor seals retain their original identities.
Reuse requires reviewed dependency nonreach; affected web Worker/Fetch/Browser
isolation evidence must be refreshed after the client repair. Linuxx64/ARM64
retain97selected methods each and remain owner-blocked. Approval does not grant
native qualification, E43 acceptance, commit, push, release or deployment.
