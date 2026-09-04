# Maintenance and ordinary-conflict supplement v1

This is the transition behavior supplement to the unchanged `client-upgrade/v1`
corpus. The baseline contract sections 13.1–13.2 govern shared behavior. It adds no generation
alias, conversion callback, version bypass, or client-side PostgreSQL conversion.

`scenarios.json` gives stable IDs, portable initial states, actions, protocol
categories, durable outcomes, and executable Go test mappings. Exchange bodies
are category examples, not replacements for full core wire schemas. The
application-owned runtime allowlist response is deliberately distinct from
schema-proof validation. Production applications must enforce equivalent
independently authenticated admission before migrating the server.

Local request and revision tokens are opaque and runtime-specific. Preserve the
whole frozen request and every newer edit. `accept_server` can discard only the
selected frozen intent; it cannot discard newer edits. The Go reference rejects
keep/merge of an older frozen row while a newer dirty edit exists, requiring a
fresh explicit decision. Cancellation, stale decisions and failing application
triggers leave the transaction unchanged. Do not map a maintenance response to
success or automatically reset, replace a source, migrate, or overwrite a file.

Run `python3 scripts/check-maintenance-conformance.py --execute` from the Go
repository. Go 1.25.12 and PostgreSQL 16 are required; use isolated database
fixtures. The runner checks artifact hashes and requires an actual PASS event
for every mapped test. The two parameterized client tests consume this corpus.
Other mapped tests exercise process death, real HTTP, PostgreSQL, same-file
upgrades and recovery. KMP, Dart/Flutter and Swift execution remains pending
non-Go runtime support; their public and generated APIs must implement equivalent behavior.
