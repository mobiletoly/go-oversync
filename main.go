// Copyright 2025 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
)

func main() {
	fmt.Println("🚀 go-oversync - Two-Way Synchronization Library")
	fmt.Println("================================================")
	fmt.Println()
	fmt.Println("go-oversync provides robust two-way synchronization between clients and servers")
	fmt.Println("with conflict resolution, optimistic concurrency control, and idempotent operations.")
	fmt.Println()

	fmt.Println("📚 Available Examples:")
	fmt.Println()
	fmt.Println("1. 🌐 HTTP Server Example (examples/nethttp_server/)")
	fmt.Println("   A complete two-way sync server using Go's net/http package")
	fmt.Println("   Features: JWT auth, conflict resolution, business table handlers")
	fmt.Println("   Run: cd examples/nethttp_server && go run .")
	fmt.Println()

	fmt.Println("2. 📱 Simple Client Example (examples/simple_client/)")
	fmt.Println("   HTTP client demonstrating two-way sync API usage")
	fmt.Println("   Features: Upload/download, conflict handling, watermark sync")
	fmt.Println("   Run: cd examples/simple_client && go run .")
	fmt.Println()

	fmt.Println("3. 🗄️  SQLite Client Example (examples/sqlite_client/)")
	fmt.Println("   Offline-first SQLite client with automatic change tracking")
	fmt.Println("   Features: SQLite triggers, metadata tables, background sync")
	fmt.Println("   Run: cd examples/sqlite_client && go run .")
	fmt.Println()
}
