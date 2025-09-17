---
layout: default
title: Home
---

# go-oversync — PostgreSQL adapter for multi-device sync

**A set of libraries that add two-way sync between client databases and PostgreSQL
servers.**

[## What is go-oversync?

go-oversync is a Go library suite designed for applications that need **reliable synchronization**
between local client databases and a central PostgreSQL backend, across multiple devices and
platforms.

Use it when you want to:

- Sync changes bi-directionally between a client (e.g. mobile or embedded SQLite) and a server (
  PostgreSQL).
- Allow offline operation and automatic syncing once connectivity is restored.
- Handle conflict resolution out of the box.
- Plug into your existing HTTP server and authentication stack without needing a separate sync
  server.

It includes:

- A **PostgreSQL adapter** that integrates with your server APIs.
- A **Go SQLite client**, for desktop or backend usage.
- **Kotlin Multiplatform client** — For Android/iOS apps
  via [sqlitenow-kmp](https://github.com/mobiletoly/sqlitenow-kmp)


## Why go-oversync?

- **Library, not framework** — Integrate with your existing server architecture
- **Bring your own auth** — Works with any authentication system (JWT, sessions, API keys)
- **Clean architecture** — No invasive columns in your business tables
- **Offline-first** — Works seamlessly with poor connectivity
- **Multi-device** — Perfect sync across phones, tablets, and web

## How It Works

**You control the server:** go-oversync provides libraries that integrate into your existing HTTP
server. You handle routing, middleware, authentication, and business logic.

**Multiple client options:** Use the Go SQLite client for Go apps, or the Kotlin Multiplatform
client for Android/iOS apps. Both sync with the same PostgreSQL backend. More client libraries
are coming.

**Simple integration:** Register your tables with go-oversync, add a few HTTP handlers to your
routes, and the library handles change tracking, conflict resolution, and sync protocol details.

**Flexible authentication:** Works with any auth system — JWT, sessions, API keys, or custom
authentication. You extract user/device IDs and pass them to go-oversync.

## Key Features

- **Conflict Resolution** — Optimistic concurrency with automatic conflict detection
- **Batch Processing** — FK-aware ordering and efficient batch operations
- **User Isolation** — Each user has completely isolated sync streams
- **Multi-Platform** — Go client for servers/desktop, Kotlin Multiplatform for mobile
- **Offline-First** — Works perfectly with intermittent connectivity


## Client Libraries

### Go SQLite Client (This Repository)

- **Location:** `oversqlite/` package in this repo
- **Use case:** Go applications, desktop apps, server-to-server sync
- **Database:** SQLite with automatic trigger-based change tracking
- **Features:** Batch uploads, conflict resolution, offline-first design

### Kotlin Multiplatform Client (Separate Repository)

- **Repository:** [sqlitenow-kmp](https://github.com/mobiletoly/sqlitenow-kmp)
- **Use case:** Android and iOS mobile applications
- **Database:** SQLite with the same sync protocol
- **Features:** Cross-platform mobile support, same sync guarantees

Both clients sync with the same PostgreSQL backend using identical protocols, ensuring perfect
compatibility across platforms.

## Ready to try it?

- First get familiar with [Core Concepts](core-concepts.html) guide to understand the key components
  and vocabulary
- Start with the [Getting Started](getting-started.html) guide to build your first sync-enabled
  server
- Explore the [Documentation](documentation.html) for detailed server, client, and HTTP API
  reference
