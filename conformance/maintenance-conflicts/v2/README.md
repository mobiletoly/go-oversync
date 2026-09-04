# Maintenance/conflict conformance revision 2

Historical fixture input: `cases.json` and its recorded exchanges describe the
removed push-session, chunk, commit, committed-row, and `committed_remote`
workflow. They are retained only as a pre-P0-P4 durable-layout and migration
reference. They are not current wire conformance, and the direct-protocol test
explicitly skips them rather than teaching compatibility behavior. Current
conflict behavior is exercised by `scenarios.json` and the direct
`POST /sync/push` Go tests.

`cases.json` contains 16 concrete executable client cases. Inputs select the initial
schema generation, row identity/value and queued operation. Expected records every
HTTP request method/path, source/proof headers, request bytes, response status/bytes
or deliberate transport failure, and ordered durable states. Body strings retain
actual JSON bytes, including newlines; row payloads retain their encoded strings.

State order is before upload, after interrupted/rejected upload, after the explicit
application decision (if any), after reopening the same persisted file, and after
successful resumption when applicable. Every case verifies uninstall rejection at
the unresolved boundary. Source and frozen request identities must match exactly.
Conflict IDs are runtime-allocated opaque handles: decisions use the exact handle
returned by the runtime; their bytes are not an additional cross-runtime UUID
algorithm. Revision, request, response, and newer-intent facts are compared.

`runtime-upgrade.json` supplies the real mobile HTTP scenario's keys, queued value,
rejection response and post-upgrade values. Its persisted source/request identities
are runtime allocated; the test captures and compares their exact bytes across
replacement, including a fixture with conflict metadata absent. It does not claim
execution of an older binary.

Scenario exchange references name a manifest artifact. References into `cases.json`
also name one exact case `ID`; standalone artifacts such as `runtime-upgrade.json`
omit the case selector and refer to the complete document.

Go retains `cases.json` behind the explicitly skipped historical-input test and
consumes `scenarios.json` in current direct-protocol tests and the mobile test
named by that file. Other runtimes implement equivalent logical
state inspection; SQLite table names describe the reference representation. KMP,
Dart and Swift execution remains pending for the non-Go runtimes. Existing upgrade wire vectors and
revision-1 artifacts are historical and unchanged. Normal tests never regenerate
expected outcomes. The explicit development recording environment flag is not an
acceptance lane; recorded values require review and a subsequent comparison run.

## Revision 5: exact staging hash echoes

Initial staging responses now echo the frozen create-request hash, matching the production server and OpenAPI. Only the 20 affected response hash fields in `cases.json` change; requests and expected durable states remain unchanged. The production reader rejects empty or mismatched echoes at every staging ordinal. `TestPushPending_StagingHashEchoPreservesFrozenIntent` covers initial/resumed empty, missing, null, wrong-type, mismatched and uppercase echoes, with no follow-up upload/cleanup and exact frozen-state preservation before a successful retry.
