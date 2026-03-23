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
- clients push one logical dirty set through staged push sessions
- clients replay the authoritative committed bundle returned by the server
- clients pull complete committed bundles only
- fresh installs and prune recovery rebuild through frozen snapshot sessions

## Repository Surface

- `oversync/`: PostgreSQL adapter, bundle capture, schema validation, HTTP handlers
- `oversqlite/`: SQLite client SDK with trigger-based dirty capture
- `examples/nethttp_server/`: reference `net/http` server
- `examples/mobile_flow/`: simulator for the current sync contract
- `swagger/two_way_sync.yaml`: OpenAPI description of the HTTP surface

## Design Constraints

The runtime is intentionally fail-closed.

- registered PostgreSQL tables must use a single-column UUID sync key
- registered and managed table sets must be FK-closed
- unsupported key shapes and unsupported FK shapes fail during bootstrap
- one `oversqlite.Client` owns one SQLite database at a time
- one SQLite database maps to one configured remote schema

## Start Here

- Read [Core Concepts](documentation/core-concepts/) for the vocabulary and end-to-end model.
- Use [Getting Started](getting-started.html) to stand up a server and client.
- Use [HTTP API](documentation/api/) for the wire contract.
