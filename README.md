# go-oversync — Production-Ready Multi-Device Sync

**A set of libraries that add bulletproof two-way sync between client databases and PostgreSQL servers.**

## What is go-oversync?

go-oversync is **not a server** — it's a collection of libraries that you integrate into your
existing applications:

- **PostgreSQL server adapter** — Plugs into your HTTP server with any authentication system
- **Go SQLite client** — For Go applications that need to sync with PostgreSQL (included in this
  repo)
- **Kotlin Multiplatform client** — For Android/iOS apps
  via [sqlitenow-kmp](https://github.com/mobiletoly/sqlitenow-kmp)
- **Flexible integration** — Works with your existing routes, middleware, and auth

## Why go-oversync?

- **Library, not framework** — Integrate with your existing server architecture
- **Bring your own auth** — Works with any authentication system (JWT, sessions, API keys)
- **Battle-tested** — Production-ready with comprehensive conflict resolution
- **Clean architecture** — No invasive columns in your business tables
- **Offline-first** — Works seamlessly with poor connectivity
- **Multi-device** — Perfect sync across phones, tablets, and web


## How It Works

```
┌──────────────────┐    Your HTTP      ┌──────────────────┐
│ Client Database  │    Server with    │   PostgreSQL     │
│ (SQLite, etc.)   │ ◄─ go-oversync ─► │ + go-oversync    │
│ + Client Library │    handlers       │   Library        │
└──────────────────┘    and auth       └──────────────────┘
```

**You control the server:** go-oversync provides libraries that integrate into your existing HTTP
server. You handle routing, middleware, authentication, and business logic.

**Multiple client options:** Use the Go SQLite client for Go apps, or the Kotlin Multiplatform
client for Android/iOS apps. Both sync with the same PostgreSQL backend.

**Simple integration:** Register your tables with go-oversync, add a few HTTP handlers to your
routes, and the library handles change tracking, conflict resolution, and sync protocol details.

**Flexible authentication:** Works with any auth system — JWT, sessions, API keys, or custom
authentication. You extract user/device IDs and pass them to go-oversync.

## Quick Start

### 1. Install

```bash
go get github.com/mobiletoly/go-oversync
```

### 2. Try the Example

```bash
# Start the example server (PostgreSQL required)
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

```go
// Configure sync for your tables
cfg := oversqlite.DefaultConfig("business", []oversqlite.SyncTable{
    {TableName: "users"},
    {TableName: "posts"},
})
client, _ := oversqlite.NewClient(db, serverURL, userID, deviceID, tokenProvider, cfg)

// Sync in one line
client.UploadOnce(ctx)
client.DownloadOnce(ctx, 1000)
```

#### Option B: Kotlin Multiplatform Client (Android/iOS)

- **Repository:** [sqlitenow-kmp](https://github.com/mobiletoly/sqlitenow-kmp)
- **Use case:** Android and iOS mobile applications
- **Database:** SQLite with the same sync protocol
- **Features:** Cross-platform mobile support, same sync guarantees

```kotlin
// Kotlin Multiplatform - works on Android & iOS
val syncClient = SyncClient(database, serverUrl, userId, deviceId, tokenProvider)
syncClient.uploadOnce()
syncClient.downloadOnce(limit = 1000)
```

## Key Features

- **Automatic Sync** — Changes tracked via database triggers, no manual instrumentation
- **Conflict Resolution** — Optimistic concurrency with automatic conflict detection
- **Batch Processing** — FK-aware ordering and efficient batch operations
- **User Isolation** — Each user has completely isolated sync streams
- **Multi-Platform** — Go client for servers/desktop, Kotlin Multiplatform for mobile
- **Offline-First** — Works perfectly with intermittent connectivity
- **Production Ready** — Comprehensive test suite with 100% scenario coverage

## Architecture

**Library-based:** go-oversync provides PostgreSQL server adapters and multiple client options that
integrate into your existing applications. You control the server, routes, and authentication.

**Multiple client platforms:** Use the Go SQLite client for Go applications, or the Kotlin
Multiplatform client for Android/iOS mobile apps.

**Clean separation:** Your business tables stay untouched. All sync metadata lives in separate
`sync.*` tables that go-oversync manages.

**Flexible auth:** Works with any authentication system. You extract user/device identifiers and
pass them to go-oversync handlers.

**HTTP API:** Simple JSON upload/download endpoints that you add to your existing server routes.


## Integration Examples

### With Existing Authentication

```go
// Works with any auth system
func (s *MyServer) setupSyncRoutes() {
    // Your existing auth middleware
    authRequired := s.requireAuth()

    // go-oversync handlers
    syncHandlers := oversync.NewSyncHandlers(s.syncService, s.extractUserDevice, s.logger)

    // Add to your existing routes
    s.router.POST("/api/sync/upload", authRequired(syncHandlers.HandleUpload))
    s.router.GET("/api/sync/download", authRequired(syncHandlers.HandleDownload))
}

// Extract user/device from your auth system
func (s *MyServer) extractUserDevice(r *http.Request) (userID, deviceID string, err error) {
    // Your auth logic here - JWT, sessions, API keys, etc.
    user := s.getCurrentUser(r)
    device := s.getDeviceID(r) // from header, JWT claim, etc.
    return user.ID, device, nil
}
```

### With Different Frameworks

- **Gin, Echo, Chi, Gorilla** — Add handlers to your existing router
- **gRPC** — Wrap handlers in gRPC service methods
- **GraphQL** — Call handlers from GraphQL resolvers
- **Custom protocols** — Use the core sync service directly

## Documentation

- **📖 [Quick Start Guide](docs/)** — Get up and running in 10 minutes
- **🔧 [API Reference](docs/pages/api.md)** — Complete HTTP API documentation  
- **📋 [Examples](examples/)** — Working examples and test scenarios
- **📐 [Architecture Specs](specs/)** — Deep-dive technical specifications

## Examples

- **[nethttp_server](examples/nethttp_server/)** — Complete server implementation
- **[mobile_flow](examples/mobile_flow/)** — Comprehensive sync simulator with 11 scenarios
- **[samplesync_server](examples/samplesync_server/)** — Multi-tenant server example


## API Overview

### Upload Changes
```http
POST /sync/upload
Authorization: Bearer <jwt>
Content-Type: application/json

{
  "last_server_seq_seen": 42,
  "changes": [{
    "source_change_id": 1,
    "schema": "business",
    "table": "users",
    "op": "INSERT",
    "pk": "550e8400-e29b-41d4-a716-446655440000",
    "server_version": 0,
    "payload": {"id": "550e...", "name": "John", "email": "john@example.com"}
  }]
}
```

### Download Changes
```http
GET /sync/download?after=0&limit=100&schema=business
Authorization: Bearer <jwt>
```

Returns ordered stream of changes from other devices with conflict detection and snapshot-consistent
paging.

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
