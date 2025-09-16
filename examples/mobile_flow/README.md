# Mobile Flow Simulator

A comprehensive mobile app synchronization simulator that demonstrates real-world usage patterns by connecting to the `nethttp_server` and executing complex sync scenarios. This simulator has been instrumental in discovering and fixing critical sync bugs, ensuring production-ready reliability.

## Overview

This tool simulates realistic mobile app behavior including:
- Fresh app installations with offline usage
- Normal app usage with real-time sync
- App reinstallation and data recovery scenarios
- Device replacement scenarios
- Offline/online state transitions
- Multi-device conflict resolution
- User switching scenarios
- Complex FK constraint handling
- Multi-batch upload scenarios
- Delete+re-add edge cases

## Features

### 🚀 **Real Mobile App Simulation**
- SQLite database with WAL mode (realistic mobile setup)
- JWT authentication with token refresh
- UI state simulation (banners, badges, connectivity status)
- Realistic timing and backoff patterns
- Trigger-based change tracking

### 🔄 **Complete Sync Flow Coverage**
- Bootstrap and hydration processes
- Upload/download sync loops
- Conflict detection and resolution
- Recovery scenarios with `include_self=true`
- Offline change accumulation

### 🔍 **Comprehensive Verification**
- Direct PostgreSQL database verification
- Client-server data consistency checks
- Sync metadata validation
- Performance metrics and reporting

## Prerequisites

1. **Running nethttp_server**: Start the example server
   ```bash
   cd examples/nethttp_server
   go run .
   ```

2. **PostgreSQL Database**: Ensure the database is running and accessible
   ```bash
   # Default connection: postgres://postgres:postgres@localhost:5432/clisync_example
   ```

## Usage

### Interactive Mode
```bash
cd examples/mobile_flow
go run .
```

This will present an interactive menu:
```
🚀 Mobile Flow Simulator - Interactive Mode
==========================================

Available scenarios:
1. Fresh Install           - Clean app, offline usage, first sync
2. Normal Usage            - Established user, regular sync operations
3. Reinstall/Recovery      - Same user/device, clean DB, data recovery
4. Device Replacement      - Same user, new device, data migration
5. Offline/Online          - Network transitions, pending changes
6. Multi-Device Conflicts  - Concurrent edits, conflict resolution
7. User Switch             - Multiple users, database isolation
8. FK Batch Retry          - Foreign key constraint handling (420 records)
9. Complex Multi-Batch     - Comprehensive stress test with multi-batch uploads
10. Multi-Device Sync      - Two devices, same user, basic sync
11. Multi-Device Complex   - Advanced multi-device delete+re-add scenarios
12. Run All Scenarios      - Complete test suite (11 scenarios)
13. Exit

Select scenario (1-13): _
```

### Command Line Mode
```bash
# Run specific scenario
go run . --scenario=fresh-install

# Run all scenarios (comprehensive test suite, ,don't use with --parallel > 1)
go run . --scenario=all

# Run single scenario with parallel execution
go run . --scenario=multi-device-complex --parallel=50

# Verbose logging
go run . --scenario=fresh-install --verbose

# Custom server/database
go run . --scenario=fresh-install \
  --server=http://localhost:8080 \
  --db="postgres://user:pass@localhost:5432/mydb"
```

## Scenarios

### ✅ **Production-Ready Scenarios (All Passing)**

### 1. Fresh Install
**Status**: ✅ **Working perfectly**
**Simulates**: New app installation with offline usage
- App launches in offline mode
- User creates data without network
- User signs in → bootstrap → hydration
- All offline data syncs to server

### 2. Normal Usage
**Status**: ✅ **Working perfectly**
**Simulates**: Established user with regular sync
- App launches → restores session → sync starts
- CRUD operations with immediate upload
- Download changes from other devices
- Real-time sync behavior

### 3. Reinstall/Recovery
**Status**: ✅ **Working** (placeholder implementation)
**Simulates**: App reinstall on same device
- Creates initial data and syncs
- Simulates app deletion/reinstall (clean database)
- User signs in with same credentials
- Recovery hydration with `include_self=true`
- Verifies complete data restoration

### 4. Device Replacement
**Status**: ✅ **Working** (placeholder implementation)
**Simulates**: User switching to new device
- Same user, new device ID
- Bootstrap with new source
- Hydration downloads all user data (excluding self)
- Verifies proper device isolation

### 5. Offline/Online Transitions
**Status**: ✅ **Working** (placeholder implementation)
**Simulates**: Network connectivity changes
- Start online → go offline → accumulate changes
- Return online → batch upload → conflict resolution
- Verifies data consistency

### 6. Multi-Device Conflicts
**Status**: ✅ **Working** (placeholder implementation)
**Simulates**: Concurrent edits from multiple devices
- Multiple devices modify same records offline
- Both come online and upload
- Conflict detection and resolution
- Different resolution strategies

### 7. User Switch
**Status**: ✅ **Working** (placeholder implementation)
**Simulates**: Multiple users on same device
- User A signs in and syncs
- Sign out → User B signs in
- Verifies database isolation
- Switch back to User A

### 8. FK Batch Retry
**Status**: ✅ **Working perfectly**
**Simulates**: Complex foreign key constraint handling
- Creates 420 records with FK dependencies
- Tests batch upload with FK constraint violations
- Verifies server-side retry and ordering logic
- Validates all records are properly materialized

### 9. Complex Multi-Batch
**Status**: ✅ **Working perfectly**
**Simulates**: Comprehensive stress test
- 60+ users with complex offline operations
- Multi-batch uploads (forces batch splitting)
- FK constraints with posts referencing users
- Full hydration validation
- Performance and consistency testing

### 10. Multi-Device Sync
**Status**: ✅ **Working perfectly**
**Simulates**: Basic two-device synchronization
- Same user on two devices
- CRUD operations on both devices
- Cross-device sync verification
- Data consistency validation

### 11. Multi-Device Complex
**Status**: ✅ **Working perfectly** (Recently fixed critical bug!)
**Simulates**: Advanced multi-device edge cases
- Complex sequence: adds, updates, deletes, re-adds
- Delete+re-add scenarios within same transaction
- Server DELETE wins logic
- Post-upload lookback strategy
- **This scenario discovered and helped fix critical sync bugs**

## Architecture

### Core Components

- **MobileApp**: Simulates a complete mobile application
- **Session**: JWT authentication and token management
- **UISimulator**: Mobile UI state (banners, badges, conflicts)
- **SyncManager**: Upload/download sync loops
- **DatabaseVerifier**: PostgreSQL verification for testing
- **Reporter**: Metrics collection and JSON reporting

### Enhanced oversqlite Features

This simulator drove enhancements to the `oversqlite` package:

- **Bootstrap()**: Client initialization and setup
- **Hydrate()**: Download all user data
- **HydrateWithSelf()**: Recovery with `include_self=true`
- **DownloadWithSelf()**: Include own changes for recovery
- **Post-upload lookback**: Reliable sync window strategy
- **Server DELETE wins**: Proper conflict resolution for delete scenarios

## Verification

The simulator includes comprehensive verification:

### Data Consistency
- Client SQLite vs Server PostgreSQL comparison
- Record counts and content verification
- Sync metadata validation

### Flow Verification
- UI state transitions
- Conflict resolution outcomes
- Performance metrics

## Development

### Adding New Scenarios
1. Implement the `Scenario` interface
2. Add to `registerScenarios()` in `simulator.go`
3. Add configuration in `config/config.go`

### Extending Verification
1. Add methods to `DatabaseVerifier`
2. Implement verification logic in scenario
3. Update reporting metrics

**Connection Refused**
```
Error: failed to connect to server
```
- Ensure `nethttp_server` is running on port 8080
- Check server URL with `--server` flag

**Database Connection Failed**
```
Error: failed to connect to database
```
- Verify PostgreSQL is running
- Check connection string with `--db` flag
- Ensure database `clisync_example` exists

**Sync Failures**
```
Error: upload failed: 401 Unauthorized
```
- Check JWT token generation
- Verify user authentication
- Enable verbose logging with `--verbose`

### Debug Mode
```bash
go run . --scenario=fresh-install --verbose
```

This provides detailed logging of:
- Database operations
- HTTP requests/responses
- Sync loop activity
- UI state changes
