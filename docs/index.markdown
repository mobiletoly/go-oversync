---
layout: default
title: Home
---

# go-oversync

go-oversync is a Go library suite for bundle-based sync between local SQLite clients and
PostgreSQL servers.

## Current Model

The verified contract in this repository is:

- PostgreSQL business tables are authoritative
- clients push one logical dirty set through one bounded `POST /sync/push` request
- clients replay the authoritative committed bundle returned by the server
- clients pull complete committed bundles only
- retained-history gaps, fresh installs, and source recovery rebuild through frozen snapshot
  sessions

## Repository Surface

- `oversync/`: PostgreSQL adapter, bundle capture, schema validation, HTTP handlers
- `oversqlite/`: SQLite client SDK with trigger-based dirty capture
- `examples/samplesync_server/`: maintained application-facing sample server
- `integration/`: repository-owned server, simulator, schema-evolution, and qualification harnesses
- `swagger/two_way_sync.yaml`: OpenAPI description of the HTTP surface

## Design Constraints

The runtime is intentionally fail-closed.

- registered PostgreSQL tables must define exactly one visible sync key column of type `uuid` or
  `text`
- registered PostgreSQL tables must define `_sync_scope_id TEXT NOT NULL` and scope-inclusive
  identity / foreign keys
- registered and managed table sets must be FK-closed
- unsupported key shapes and unsupported FK shapes fail during bootstrap
- one `oversqlite.Client` owns one SQLite database at a time
- one SQLite database maps to one configured remote schema
- retained bundle history is bounded; clients below the retained floor recover through snapshot
  rebuild

## Start Here

- Read [Core Concepts](documentation/core-concepts/) for the vocabulary and end-to-end model.
- Use [Getting Started](getting-started.html) to stand up a server and client.
- Run the maintained server example in `examples/samplesync_server/`.
- Use [Server Deployment and Operations](documentation/server-deployment-and-operations/) to
  configure capacity, monitoring, shutdown, upgrades, and recovery.
- Use [HTTP API](documentation/api/) for the wire contract.
