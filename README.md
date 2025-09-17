# go-oversync — PostgreSQL adapter for multi-device sync

**A set of libraries that add two-way sync between client databases and PostgreSQL
servers.**

## What is go-oversync?

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

```
┌──────────────────┐    Your HTTP      ┌──────────────────┐
│ Client Database  │    Server with    │   PostgreSQL     │
│ (SQLite, etc.)   │ ◄─ go-oversync ─► │ + go-oversync    │
│ + Client Library │    handlers       │   Library        │
└──────────────────┘    and auth       └──────────────────┘
```


## Quick Start

### 1. Install

```bash
go get github.com/mobiletoly/go-oversync
```

### 2. Try the Example

```bash
# Start the example server
go run ./examples/nethttp_server

# In another terminal, run the mobile simulator
go run ./examples/mobile_flow
```

The mobile simulator runs 11 comprehensive scenarios including multi-device sync, conflict
resolution, and edge cases. All scenarios pass with 100% success rate.

### 3. Integrate with Your Server

```go
// 1. Configure go-oversync for your tables
cfg := &oversync.ServiceConfig{
    MaxSupportedSchemaVersion: 1,
    AppName: "my-app",
    RegisteredTables: []oversync.RegisteredTable{
        {Schema: "business", Table: "users"},
        {Schema: "business", Table: "posts"},
    },
}

svc, _ := oversync.NewSyncService(pool, cfg, logger)

// 2. Create handlers (works with any auth system)
h := oversync.NewSyncHandlers(svc, yourAuthSystem, logger)

// 3. Add to your existing HTTP server
mux.Handle("POST /sync/upload", yourAuthMiddleware(http.HandlerFunc(h.HandleUpload)))
mux.Handle("GET /sync/download", yourAuthMiddleware(http.HandlerFunc(h.HandleDownload)))
```

**Your auth, your rules:** Use JWT, sessions, API keys, or any authentication system. Just extract
`userID` and `deviceID` and pass them to the handlers.


### 4. Client Setup

Both clients sync with the same PostgreSQL backend using identical protocols, ensuring perfect
compatibility across platforms.

#### Option A: Go SQLite Client

- **Location:** `oversqlite/` package in this repo
- **Use case:** Go applications, desktop apps, server-to-server sync
- **Database:** SQLite with automatic trigger-based change tracking
- **Features:** Batch uploads, conflict resolution, offline-first design

#### Option B: Kotlin Multiplatform Client (Android/iOS)

- **Repository:** [sqlitenow-kmp](https://github.com/mobiletoly/sqlitenow-kmp)
- **Use case:** Android and iOS mobile applications
- **Database:** SQLite with the same sync protocol
- **Features:** Cross-platform mobile support, same sync guarantees


## Key Features

- **Conflict Resolution** — Optimistic concurrency with automatic conflict detection
- **Batch Processing** — FK-aware ordering and efficient batch operations
- **User Isolation** — Each user has completely isolated sync streams
- **Multi-Platform** — Go client for servers/desktop, Kotlin Multiplatform for mobile
- **Offline-First** — Works perfectly with intermittent connectivity


## Documentation

- **[Documentation](https://mobiletoly.github.io/go-oversync/)** - Documentation home page

## Examples

- **[nethttp_server](examples/nethttp_server/)** — Server reference implementation
- **[mobile_flow](examples/mobile_flow/)** — Comprehensive sync simulator with 11 scenarios
- **[samplesync_server](examples/samplesync_server/)** — Multi-tenant server example to
  to be used with Kotlin Multiplatform sample app
  [samplesync-kmp](https://github.com/mobiletoly/sqlitenow-kmp/tree/main/samplesync-kmp)


# License

```
Copyright 2025 Toly Pochkin

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
```
