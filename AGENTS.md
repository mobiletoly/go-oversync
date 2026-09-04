# Repository Guidelines

## Project Structure & Modules
- `oversync/`: PostgreSQL adapter, HTTP handlers, schema discovery, FK migration, materialization hooks.
- `oversqlite/`: SQLite client SDK with triggers, upload/download loops, conflict resolver.
- `examples/`: Reference servers and simulators (`nethttp_server`, `mobile_flow`, `samplesync_server`).
- `docs/`: Jekyll documentation and architecture notes; `swagger/two_way_sync.yaml` holds the API spec.
- Tests live alongside code (`*_test.go`) within each module.

## Build, Test, and Development Commands
- Go language/consumer minimum: Go 1.25.0. Go 1.24 and earlier are unsupported.
- Reproducible development, validation, benchmark, and memory-evidence toolchain: Go 1.25.12. Run normative Go commands with `GOTOOLCHAIN=go1.25.12`; Go 1.26 is not an acceptance or benchmark lane for the memory-bounded snapshot program. Ensure Docker is available for testcontainers-based tests.
- Quick checks: `go test ./oversync ./oversqlite` (unit/fast integration). Full sweep: `go test ./...` (may pull containers).
- Run example server: `DATABASE_URL=... go run ./examples/nethttp_server`.
- Mobile simulator: `go run ./examples/mobile_flow -scenario all -cleanup=false` (uses Postgres + HTTP server).

## Coding Style & Naming Conventions
- Go style with `gofmt` (tabs, canonical imports). Use clear, lower_snake schema/table names in configs.
- Keep HTTP/JSON models in `rest_models.go` aligned with `swagger/two_way_sync.yaml`.
- Prefer descriptive logger keys; avoid leaking secrets in logs.
- Payload helpers expect lower_snake JSON keys; match server/client symmetry.
- Do not trim IDs, hashes, keys, or other protocol tokens as a substitute for validation. Treat them as exact values; check for `""` when testing presence unless a specific grammar says otherwise.
- When refactoring duplicated validation with slightly different error text, prefer a shared helper that returns normalized field errors and wrap those errors with operation-specific context at the caller. Avoid callback-heavy validators just to preserve wording differences.

## Complexity Guidelines
- Measure cognitive complexity across production Go code to identify review hotspots.
- New or materially changed functions should have cognitive complexity below 30. Prefer substantially lower complexity for orchestration functions.
- Treat pre-existing functions above the threshold as incremental cleanup candidates, not unrelated blockers.
- Use `gocognit`, `gocyclo`, and `funlen` diagnostically. Cognitive-complexity compliance does not require satisfying unrelated default line or statement thresholds.
- Do not reduce scores by merely distributing one difficult routine across thin helpers. Extract cohesive phases with explicit responsibilities and preserved invariants.
- Transaction boundaries, SQL and hook ordering, error precedence, cleanup behavior, and protocol clarity take priority over metric improvements.
- Document and justify any intentional exception when a materially changed function cannot remain below the threshold without obscuring correctness.

## Testing Guidelines
- Framework: Go `testing` + `testify` assertions; some tests rely on `testcontainers` (Docker required).
- Name tests `TestThing_Subject`; keep table-driven where possible.
- For DB-backed tests, clean up resources and respect existing timeouts.
- Before PRs, provide current evidence for changed owning packages and affected consumers: Oversqlite for client-only changes, Oversync plus required database/example flows for server/schema changes, and both for shared dependencies or contracts. Explicit initial-protocol/release acceptance commands still take precedence.

## Proportionate Oversqlite validation

Use the paired tracked runbook `../sqlitenow-kmp/docs-engineering/oversqlite-validation.md`. Distinguish development, maintenance-candidate, and initial-protocol/release profiles. Maintenance uses manually selected affected packages, runtime scenarios and live boundaries with reviewer-confirmed reuse. Record fresh, reused, subsumed, not-applicable and blocked lanes. A mandatory aggregate names its governing section, exact command and trigger. Initial protocol/release matrices and historical acceptance records remain intact; no automatic reuse acceptance is introduced.

## Validation Efficiency
- Before implementation, turn the governing specification into a checklist covering exact CLI/API behavior, required scenarios, owning tests, validation lanes, deadlines, and environment/storage requirements.
- Use focused tests while developing. Run expensive package, root, vet, and nested-module lanes only after the implementation is stable.
- Before expensive validation, review the complete implementation against that checklist and finish focused checks. Resolve missing requirements before freezing the candidate.
- Before final validation, freeze production changes and define a validation ledger identifying required, focused, superseded, and rerun-required lanes.
- Do not rerun an expensive lane when a later successful broader lane covers the same unchanged code.
- Record which changes invalidate which results. After a late change, rerun the directly affected focused/package lanes and the final aggregate when required by the governing acceptance contract or affected behavior. Documentation-only changes do not invalidate database suites; run their document/link/identity checks instead.
- After the first suite timeout, diagnose it with focused evidence before another aggregate run. A retry needs a stated cause, a corrective change or changed environment, and evidence that it addresses the cause. Do not repeat an aggregate merely to see whether it passes this time.
- Retry bounded failures only when needed to distinguish implementation failure from environmental or timing failure. Record every failed attempt and retry; do not extend prescribed deadlines.
- The managed core PostgreSQL fixture defaults to durable Docker-volume storage on macOS to avoid host-filesystem sharing latency. Set `OVERSYNC_TEST_DOCKER_VOLUME=0` to require a host bind mount, or `=1` to require a Docker volume on any platform. Other platforms retain the bind-mount default. Keep normal PostgreSQL durability settings and clean only run-owned resources.
- For work crossing 45–60 minutes, give an explicit overrun checkpoint with completed work, remaining scope, blockers, and a revised estimate; do not let repeated validation silently consume hours.
- Mandatory acceptance commands still take precedence when they explicitly require separate lanes.

## Paired Oversqlite Contract Workflow

- Treat the governing Go and SQLiteNow specifications, `swagger/two_way_sync.yaml`, and the shared `oversqlite-contracts` corpus as one versioned contract boundary. When both repositories are authorized, capture complete tracked and untracked baselines and verify both identities before editing; stop on external drift or ambiguous overlap.
- Reconcile normative wire decisions in the specifications and OpenAPI before changing runtime consumers. Update active fixtures, explicit fixture classifications, the sorted corpus manifest, and pinned hashes together only after the contract settles.
- Active legacy references are allowed only as explicit absence assertions or clearly classified historical fixture input. Go and JVM corpus consumers must fail closed when the configured corpus root is absent or mismatched, and must execute every fixture family they claim to support.
- Run the fast contract gate before database-heavy validation: specification and OpenAPI hashes, manifest integrity, fixture consumers, compact-codec tests, and focused HTTP boundary tables. JVM contract checks validate the shared wire boundary; they do not substitute for Go server/client, platform, or live-server evidence.
- Give database-backed families independent budgets. Do not run suites concurrently against shared PostgreSQL resources unless their database, listener, and application identities are isolated. Before choosing serial or parallel execution, confirm that the lane's outer wrapper and per-package deadlines can accommodate that mode.
- Treat an aggregate timeout as inconclusive until the affected family is isolated. Do not retry an unchanged aggregate merely to seek a pass. If the prescribed wrapper cannot contain a stable serial matrix, run the same cleanup-aware constituent command with an explicitly recorded budget only when the qualification contract permits it, and retain both the wrapper failure and constituent evidence.
- Keep the validation ledger current across both repositories. After a late change, rerun only the directly invalidated focused or package lanes plus any mandatory final aggregate; a contract or corpus change invalidates paired consumers, while an instruction-only change does not invalidate runtime suites.

## Specification & Fresh-Session Handoffs
- Treat planning, review, acceptance, implementation, qualification, commit, push, and release as separate authorization gates.
- A fresh-session planning request must identify the repository, governing authority, objective, scope, prerequisites, exclusions, deliverable, validation expectations, and stop conditions.
- Make planning handoffs detailed about authority and process while leaving design to the receiving session. The receiving session inspects the repository and chooses APIs, implementation boundaries, and test ownership unless the governing specification fixes them.
- Record normative decisions in the governing specification before handoff. Present unresolved observations as evidence or hypotheses for the receiving session to verify.
- Use frozen repository identities and hashes for snapshot-dependent work. Stop if the receiving session cannot reproduce them.

## Commit & Pull Request Guidelines
- Commits: concise, imperative subject lines (e.g., “Add FK deferrable migration guard”).
- PRs should include: summary of changes, impact on sync protocol/schema, testing commands run, and any breaking considerations.
- Link issues when applicable; include screenshots only if UI artifacts change (rare here).

## Security & Configuration Tips
- Never commit secrets; `JWT_SECRET` and `DATABASE_URL` must come from env.
- When running integration sims, isolate databases per run to avoid cross-test contamination.
- FK migration tools operate on registered tables only; double-check `RegisteredTables` before enabling auto-migration in production.
