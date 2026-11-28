# Integration Harnesses

This directory contains repository-owned validation infrastructure, not application starter code.

- `nethttp_server/` provides the HTTP server used by integration, fault, and load tests.
- `mobile_flow/` is the nested-module end-to-end simulator and client migration harness.
- `schema_evolution/` owns PostgreSQL migration, rollout, recovery, and qualification fixtures.

These paths may change with repository test contracts and have no compatibility guarantee.
