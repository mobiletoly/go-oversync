# Rollout retirement contract, version 1

This family extends the existing client-upgrade, maintenance-conflicts v2 and
table-evolution contracts. Those fixtures are unchanged. This family qualifies
the Go SDK and reference application; it does not qualify Kotlin, Dart or Swift.

The manifest binds exact input bytes and names actual Go consumers. `schema.json`
describes the portable documents. The independent Python guard validates structure,
hashes, scenario coverage and wire/preservation semantics; its mutation tests prove
that updating a hash cannot turn retirement into a retryable response or permit a
pending upgrade to proceed. Use `--execute` to require successful named consumers.

| Input | Behavior exercised |
| --- | --- |
| `retired-client.json` | Automatic upload, download and watch stop; local intent survives. |
| `http.json` | All 12 HTTP routes, authenticated never-bound old source, authentication precedence. |
| `intent.json` | Unresolved conflict and committed-but-unacknowledged request survive client reconstruction. |
| `boundaries.json` | Maintenance versus retirement at create, chunk, commit and replay boundaries. |
| `journeys.json` | Two same-file mobile contraction journeys, pending/empty upgrades, process restart, supported source replacement and a later generation. |

Exact retired proof produces HTTP 426, `retired_schema_generation`, `retryable:false`
and `upgrade_application`. This intentionally precedes source/session lookup, including
for a previously unseen source. A mismatched fingerprint remains a mismatch. Ordinary
authentication, header grammar and request parsing remain in force.

An empty retired client can use the existing supported-generation upgrade after the
full local drain check. A queued, conflicted or uncertain request is not empty. Its
source, request hash, sequence, receipt and business intent cannot be discarded,
rebound or replayed through a retired-generation recovery route.

```sh
python3 scripts/check-rollout-retirement-conformance.py
python3 -m unittest discover -s scripts -p 'test_check_rollout_retirement_conformance.py'
python3 scripts/check-rollout-retirement-conformance.py --execute
```

Execution requires PostgreSQL 16 through `TEST_DATABASE_URL` for the nested mobile
module. PostgreSQL 17 equivalence is a separate explicitly selected lane. See the
application rollout documentation and qualification ledger for commands and limits.
