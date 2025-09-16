# Complex Multi-Batch Scenario Documentation

## Overview

The `complex-multi-batch` scenario is a comprehensive stress test that validates the robustness of the sync system under challenging real-world conditions. This scenario tests the critical user flow of using an app extensively, then deleting and reinstalling it, and having all data correctly restored.

## Scenario Purpose

This scenario addresses the fundamental question: **"Can users trust that their data will be perfectly preserved and restored even after complex offline usage patterns and app reinstallation?"**

### Key Validation Points

1. **Intermediate Change Preservation** - Multiple updates to the same record don't lose intermediate states
2. **FK Constraint Handling** - Child records can be uploaded before parent records with proper retry logic
3. **Multi-Batch Upload Robustness** - Large datasets are processed correctly across multiple upload rounds
4. **DELETE Operation Integrity** - Deleted records are properly marked as deleted and synchronized
5. **Fresh Install Hydration** - Complete data restoration after app reinstallation
6. **Data Consistency** - Final state matches expectations across all devices and server

## Scenario Configuration

```go
"complex-multi-batch": {
    Name:             "Complex Multi-Batch",
    Description:      "Comprehensive stress test with complex offline operations, multi-batch uploads, FK constraints, and full hydration validation",
    UserID:           "user-complex-multi-batch",
    SourceID:         "device-complex-001", 
    DeviceName:       "iPhone Complex Test",
    OfflineMode:      true,
    CleanDatabase:    true,
    InitialRecords:   60,  // 60 users (forces multiple batches)
    UpdateOperations: 30,  // 30 users updated multiple times each
    DeleteOperations: 10,  // 10 users deleted
}
```

## Phase 1: Complex Offline Operations

### 1.1 Initial Data Creation
- **60 Users Created**: Complex User 1-60 with unique emails
- **120 Posts Created**: 2 posts per user with FK dependencies to authors
- **Offline Mode**: All operations performed without network connectivity
- **Trigger Validation**: Each operation captured with real JSON payloads

### 1.2 Multiple Update Operations (Intermediate Change Preservation Test)
- **30 Users Selected**: First 30 users undergo multiple updates
- **4 Updates Per User**: Each user updated 4 times with different names/emails
  - Update #1: "Updated User X (Update #1)"
  - Update #2: "Updated User X (Update #2)" 
  - Update #3: "Updated User X (Update #3)"
  - Update #4: "Updated User X (Update #4)" ← Final state
- **120 Total Updates**: 30 users × 4 updates each
- **Coalescing Test**: Triggers should coalesce to single UPDATE per user

### 1.3 Complex Deletion Operations
- **10 Users Deleted**: Users 51-60 (last 10 users)
- **20 Posts Deleted**: 2 posts per deleted user (to maintain FK integrity)
- **Deletion Order**: Posts deleted first, then users (proper FK constraint handling)
- **Deletion Flag Test**: DELETE operations should capture OLD row data and update deleted flag

### 1.4 Expected Pending Changes
- **Total Operations**: 60 + 120 + 120 + 30 = 330 operations
- **Coalesced Pending**: ~180 changes due to trigger coalescing:
  - 50 Users: INSERT or UPDATE (final state)
  - 10 Users: DELETE
  - 100 Posts: INSERT (remaining posts)
  - 20 Posts: DELETE

## Phase 2: Multi-Batch Upload with FK Constraint Testing

### 2.1 Network Restoration
- **Go Online**: Restore network connectivity
- **Sync Activation**: Trigger upload process
- **Initial State**: 180 pending changes ready for upload

### 2.2 Upload Process Validation
- **Batch Size**: Default 200 changes per batch (should handle all 180 in one batch)
- **FK Constraint Testing**: Posts may reference users in different batch positions
- **Server Processing**: Verify server handles FK dependencies correctly
- **Progress Monitoring**: Track upload rounds and pending count reduction

### 2.3 Expected Upload Behavior
- **Single Batch Success**: All 180 changes should upload in one batch
- **FK Resolution**: Server should handle FK dependencies automatically
- **Status Responses**: Server should return 180 individual status responses
- **Pending Cleanup**: Client should clear all pending changes after successful upload

## Phase 3: Database State Verification

### 3.1 Local SQLite Verification
- **Record Counts**: 
  - Users: 50 (60 created - 10 deleted)
  - Posts: 100 (120 created - 20 deleted)
- **FK Integrity**: All posts have valid author_id references
- **Final State Verification**: Updated users show final state (Update #4), not intermediate states
- **Deletion Verification**: Deleted records not present in business tables

### 3.2 Sync Metadata Verification
- **Row Metadata**: `_sync_row_meta` contains correct server versions
- **Deletion Records**: Deleted records marked with `deleted=1`
- **Version Tracking**: All records have proper server version numbers

## Phase 4: Fresh Install + Hydration Test

### 4.1 App Reinstallation Simulation
- **Sign Out**: Clean logout from current session
- **App Closure**: Close current app instance
- **Fresh Instance**: Create new app with SAME user_id and source_id
- **Empty Database**: Verify fresh SQLite database is completely empty

### 4.2 Hydration Process
- **Sign In**: Same user credentials trigger hydration
- **Full Download**: Download all user data from server
- **Batch Processing**: Handle large dataset download in batches
- **Completion Verification**: Ensure all data is downloaded

### 4.3 Hydration Verification
- **Exact Match**: Hydrated database exactly matches original final state
- **Record Counts**: Same counts as Phase 3 verification
- **FK Relationships**: All foreign key relationships intact
- **No Deleted Records**: Deleted records not present in hydrated database
- **Sync Metadata**: Proper sync metadata established

## Success Criteria

### ✅ Data Integrity
- All 180 changes processed successfully despite complexity
- No intermediate change data lost during multiple updates
- FK constraints properly handled with automatic retry logic
- DELETE operations properly marked as deleted with OLD row data

### ✅ Performance Requirements
- Scenario completes within 30 seconds
- Upload handles 180+ changes efficiently
- No memory leaks or resource exhaustion
- Reasonable server response times

### ✅ Robustness Validation
- FK constraint violations handled gracefully with retries
- Multi-batch uploads work correctly under load
- Fresh install hydration produces identical state
- Complex offline usage patterns supported

### ✅ Real-World Simulation
- Simulates actual user behavior patterns
- Tests critical "delete app → reinstall → restore data" flow
- Validates data preservation across app lifecycle
- Ensures user confidence in data safety

## Technical Implementation Details

### Trigger Payload Capture
```sql
-- INSERT trigger captures NEW row data
INSERT INTO _sync_pending(table_name, pk_uuid, op, base_version, payload)
VALUES ('users', NEW.id, 'INSERT', 0, json_object(
    'id', NEW.id,
    'name', NEW.name,
    'email', NEW.email,
    'created_at', NEW.created_at,
    'updated_at', NEW.updated_at
));
```

### Change Coalescing Logic
- Multiple operations on same record coalesce to single pending change
- Final operation type determines the pending operation
- Payload always reflects the final state at time of last operation
- Intermediate states preserved in server change log

### FK Constraint Resolution
- Server processes upserts in dependency order (parents first)
- FK violations result in "fk_missing" status with retry
- Client retries failed changes in subsequent upload batches
- Eventually consistent when all dependencies are satisfied

## Debugging and Monitoring

### Key Log Messages
- `"Complex offline operations finished"` - Phase 1 completion
- `"Successfully processed upload"` - Server upload success
- `"All changes uploaded successfully"` - Client upload completion
- `"Database state verification passed"` - Phase 3 success
- `"Fresh install and hydration finished"` - Phase 4 completion

### Performance Metrics
- **Creation Time**: ~100ms for 180 record operations
- **Upload Time**: ~100ms for 180 change batch
- **Hydration Time**: ~2-5 seconds for full data download
- **Total Scenario Time**: <30 seconds target

### Failure Indicators
- Pending changes count not decreasing during upload
- FK constraint violations not resolving after retries
- Hydrated database counts not matching expected values
- Scenario timeout (>30 seconds)

## Real-World Benefits

This scenario validates the sync system's ability to handle:

1. **Heavy Offline Usage**: Users creating/editing lots of data offline
2. **App Reinstallation**: Critical user flow for app updates/device changes
3. **Data Reliability**: Users can trust their data will be preserved
4. **Performance Under Load**: System remains responsive with large datasets
5. **Complex Relationships**: FK dependencies handled correctly
6. **Edge Cases**: DELETE operations, multiple updates, batch processing

## Comparison with Other Scenarios

| Scenario | Records | Operations | FK Tests | Hydration | Complexity |
|----------|---------|------------|----------|-----------|------------|
| fresh-install | 4 | Simple | Basic | No | Low |
| fk-batch-retry | 50 | Basic | Yes | No | Medium |
| **complex-multi-batch** | **180** | **Complex** | **Advanced** | **Full** | **High** |

The complex-multi-batch scenario provides the most comprehensive validation of sync system robustness and real-world reliability.

## Implementation Architecture

### Data Flow Diagram

```
Phase 1: Offline Operations
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Business      │    │    Triggers      │    │  _sync_pending  │
│    Tables       │    │                  │    │                 │
│                 │    │                  │    │                 │
│ users: 60 INS   │───▶│ Capture Payloads │───▶│ 60 pending INS  │
│ posts: 120 INS  │    │ json_object()    │    │ 120 pending INS │
│ users: 120 UPD  │    │ Coalesce ops     │    │ 30 pending UPD  │
│ users: 10 DEL   │    │ OLD row data     │    │ 10 pending DEL  │
│ posts: 20 DEL   │    │                  │    │ 20 pending DEL  │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                                                Total: 180 changes

Phase 2: Upload & Sync
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│  _sync_pending  │    │     Server       │    │   PostgreSQL    │
│                 │    │   Processing     │    │    Database     │
│                 │    │                  │    │                 │
│ 180 changes     │───▶│ Batch Upload     │───▶│ 50 users        │
│ Real payloads   │    │ FK Resolution    │    │ 100 posts       │
│ JSON data       │    │ Status Response  │    │ Change log      │
│                 │◄───│ 180 statuses     │    │ Metadata        │
└─────────────────┘    └──────────────────┘    └─────────────────┘

Phase 4: Hydration
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Fresh App     │    │     Server       │    │   Restored      │
│                 │    │   Download       │    │    SQLite       │
│                 │    │                  │    │                 │
│ Empty SQLite    │◄───│ Full Hydration   │◄───│ 50 users        │
│                 │    │ Batch Download   │    │ 100 posts       │
│ Same user_id    │    │ Change Replay    │    │ Exact match     │
│ Same source_id  │    │                  │    │ Original state  │
└─────────────────┘    └──────────────────┘    └─────────────────┘
```

### Critical Technical Details

#### Trigger Payload Capture Implementation
The scenario validates that triggers capture **actual row data** at mutation time:

```sql
-- Example INSERT trigger for users table
CREATE TRIGGER trg_users_ai AFTER INSERT ON users
WHEN COALESCE((SELECT apply_mode FROM _sync_client_info LIMIT 1), 0) = 0
BEGIN
    INSERT INTO _sync_pending(table_name, pk_uuid, op, base_version, payload)
    VALUES ('users', NEW.id, 'INSERT', 0, json_object(
        'id', NEW.id,
        'name', NEW.name,
        'email', NEW.email,
        'created_at', NEW.created_at,
        'updated_at', NEW.updated_at
    ));
END;
```

#### Change Coalescing Behavior
Multiple operations on the same record are coalesced:

```
Record UUID: abc-123
1. INSERT → _sync_pending: (abc-123, INSERT, payload1)
2. UPDATE → _sync_pending: (abc-123, UPDATE, payload2) [overwrites]
3. UPDATE → _sync_pending: (abc-123, UPDATE, payload3) [overwrites]
4. UPDATE → _sync_pending: (abc-123, UPDATE, payload4) [overwrites]
Final: 1 pending change with final payload4
```

#### FK Constraint Resolution Flow
```
Upload Batch: [post1→user1, post2→user2, user1, user2]
Server Processing:
1. Process user1 → SUCCESS (no dependencies)
2. Process user2 → SUCCESS (no dependencies)
3. Process post1 → SUCCESS (user1 now exists)
4. Process post2 → SUCCESS (user2 now exists)
Result: All changes applied successfully
```

## Expected Test Results

### Phase 1 Completion Metrics
```
✅ Phase 1 complete: Complex offline operations finished
   total_users_created=60
   users_updated_multiple_times=30
   users_deleted=10
   posts_deleted=20
   expected_final_users=50
   expected_final_posts=100
```

### Phase 2 Upload Metrics
```
🔍 DEBUG: oversqlite creating change - schema: "business", table: "users", op: "INSERT"
🔍 DEBUG: oversqlite creating change - schema: "business", table: "posts", op: "INSERT"
🔍 DEBUG: oversqlite creating change - schema: "business", table: "users", op: "UPDATE"
🔍 DEBUG: oversqlite creating change - schema: "business", table: "users", op: "DELETE"

Server Response:
{"msg":"Successfully processed upload","changes_count":180,"highest_server_seq":XXXXX}
```

### Phase 3 Verification Results
```
📊 Local database counts: users=50, posts=100
📊 Server database counts: users=50, posts=100
🔍 FK relationships: 100 posts with valid author references
✅ Final state verification: All updated users show "Update #4" names
✅ Deletion verification: No deleted records in business tables
```

### Phase 4 Hydration Results
```
📱 Fresh database confirmed empty: users=0, posts=0
👤 Signing in with same user_id to trigger hydration
🔄 Performing full hydration (download all user data from server)
📊 Hydrated database counts: users=50, posts=100
✅ Hydrated vs server state: EXACT MATCH
```

## Debugging Guide

### Common Issues and Solutions

#### Issue: Pending Changes Not Decreasing
**Symptoms**: Upload appears successful but pending count stays high
**Cause**: Client not processing upload response statuses correctly
**Debug**: Check server logs for "Successfully processed upload" vs client pending count

#### Issue: FK Constraint Violations
**Symptoms**: Some changes stuck with "fk_missing" status
**Cause**: Parent records not uploaded before child records
**Debug**: Check upload order and FK dependency resolution

#### Issue: Hydration Incomplete
**Symptoms**: Hydrated database has fewer records than expected
**Cause**: Download batching not completing all data
**Debug**: Check download batch sizes and "has_more" responses

#### Issue: Intermediate Changes Lost
**Symptoms**: Final user names don't show "Update #4"
**Cause**: Triggers not capturing payloads correctly or coalescing incorrectly
**Debug**: Check _sync_pending payload column for actual JSON data

### Performance Benchmarks

| Metric | Target | Typical | Notes |
|--------|--------|---------|-------|
| Data Creation | <1s | ~100ms | 180 operations offline |
| Upload Processing | <5s | ~100ms | Single batch upload |
| Server Processing | <2s | ~100ms | 180 changes processed |
| Hydration Download | <10s | ~2-5s | Full data restoration |
| Total Scenario | <30s | ~15-20s | End-to-end completion |

## Validation Checklist

### ✅ Pre-Execution Validation
- [ ] Server running and accessible
- [ ] Database clean and initialized
- [ ] JWT authentication configured
- [ ] Network simulation working

### ✅ Phase 1 Validation
- [ ] 60 users created successfully
- [ ] 120 posts created with FK dependencies
- [ ] 30 users updated 4 times each (120 updates)
- [ ] 10 users + 20 posts deleted correctly
- [ ] 180 pending changes in _sync_pending table
- [ ] Real JSON payloads captured in payload column

### ✅ Phase 2 Validation
- [ ] Upload batch contains 180 changes
- [ ] Server processes all changes successfully
- [ ] FK constraints resolved automatically
- [ ] Client receives 180 status responses
- [ ] Pending changes cleared from _sync_pending
- [ ] No upload retries needed (single batch success)

### ✅ Phase 3 Validation
- [ ] Local database: 50 users, 100 posts
- [ ] All FK relationships valid
- [ ] Updated users show final state (Update #4)
- [ ] Deleted records absent from business tables
- [ ] Sync metadata correctly reflects server state

### ✅ Phase 4 Validation
- [ ] Fresh app starts with empty database
- [ ] Hydration downloads all user data
- [ ] Hydrated state exactly matches original final state
- [ ] No data loss during reinstallation flow
- [ ] User experience seamless and reliable

## Real-World Impact

This scenario validates the most critical user experience promise: **"Your data is safe and will be perfectly restored even if you delete and reinstall the app."**

### User Confidence Factors
1. **Data Durability**: Complex offline work is never lost
2. **State Consistency**: Final state is preserved across devices
3. **Relationship Integrity**: FK dependencies maintained correctly
4. **Deletion Handling**: Deleted data properly synchronized
5. **Recovery Reliability**: Fresh installs restore complete state

### Business Value
- **User Retention**: Confidence in data safety increases app usage
- **Support Reduction**: Fewer "lost data" support tickets
- **Platform Trust**: Reliable sync builds user trust
- **Scalability Proof**: System handles realistic data volumes
- **Quality Assurance**: Comprehensive testing reduces production issues
