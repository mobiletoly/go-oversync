---
layout: default
title: Advanced Concepts
permalink: /documentation/advanced-concepts/
---

# Advanced Concepts

This page describes the supported bundle-based sync model.

## Authoritative state

Registered business tables are the source of truth. Sync metadata is derived from committed
business-table effects rather than projected afterward from a separate authoritative log.

## Bundles

A bundle is the smallest durable sync unit.

- One committed business-table transaction becomes one committed sync bundle.
- `sync.bundle_log` records committed bundle metadata.
- `sync.bundle_rows` records normalized row effects for that bundle.
- Clients apply complete bundles only.

## Structured keys

Rows are identified by structured sync keys, not by an implicit single `pk` field on the wire.
In the current supported envelope, sync keys are still single-column UUID keys, but the protocol
surface is shaped around structured keys so bootstrap can fail closed on unsupported shapes.

## Row state

`sync.row_state` stores the authoritative per-row replicated state for conflict detection,
idempotent replay, and snapshot rebuilds.

- `row_version` is the authoritative version seen by clients.
- `deleted` distinguishes live rows from tombstoned rows.
- the key is `(user_id, schema_name, table_name, key_json)`

## Push

Push is all-or-nothing at bundle level.

- The client sends one logical dirty set with `source_id`, `source_bundle_id`, and `base_bundle_seq`.
- The server validates the whole request and either rejects it or commits one bundle.
- Retrying the same accepted `(user_id, source_id, source_bundle_id)` returns the same committed
  bundle.

## Pull

Pull is frozen to a stable ceiling.

- The first `GET /sync/pull` response returns `stable_bundle_seq`.
- Follow-up requests must keep using that ceiling until it is reached.
- Clients advance `last_bundle_seq_seen` only after durable local bundle apply.

## Snapshot rebuilds

Chunked snapshot sessions return the full current after-image at one exact frozen bundle sequence.

Use it for:

- first hydration
- destructive recovery
- rebuild after `history_pruned`

## Fail-closed contract

The supported envelope is intentionally strict.

- bootstrap fails for unsupported FK/key shapes
- bootstrap fails when the managed or registered table set is not FK-closed
- bootstrap fails when required FK deferrability is missing
- pull/hydrate/recover fail while local dirty rows exist
- malformed server responses are rejected without advancing durable checkpoints

## Supported envelope

The current contract is designed to be reliable for:

- single-column UUID sync keys
- single-column FKs between registered/managed tables
- self-references
- multi-table cycles
- `ON DELETE CASCADE`
- `ON UPDATE CASCADE`

Unsupported shapes must fail at bootstrap rather than degrade into partial runtime behavior.
