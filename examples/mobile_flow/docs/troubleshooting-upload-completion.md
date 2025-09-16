# Troubleshooting Upload Completion Issues

## Problem Description

During testing of the complex-multi-batch scenario, we discovered a critical issue where:

1. **Server successfully processes all changes** (180 changes in ~100ms)
2. **Client pending count remains unchanged** (still shows 180 pending)
3. **No subsequent uploads occur** (client thinks upload failed)
4. **Scenario appears to hang** (waiting for pending count to decrease)

## Root Cause Analysis

### Server-Side Evidence (✅ Working Correctly)
```json
{"time":"2025-08-11T21:56:49.744406+03:00","level":"INFO","msg":"Successfully processed upload","source_id":"device-complex-001","changes_count":180,"highest_server_seq":49170}
```

**Server Behavior**: ✅ CORRECT
- Received 180 changes
- Processed all changes successfully  
- Updated highest_server_seq to 49170
- Returned HTTP 200 response

### Client-Side Evidence (❌ Issue Identified)
```
📊 After upload round round=1 pending=180 previous=180
⏳ No progress detected, waiting longer for FK constraint resolution...
```

**Client Behavior**: ❌ PROBLEM
- Sent 180 changes to server
- Received successful response
- **BUT**: Pending count still shows 180
- **BUT**: No changes removed from _sync_pending table

## Technical Investigation

### Expected Upload Response Processing Flow

1. **Client sends upload request** with 180 changes
2. **Server processes changes** and returns UploadResponse
3. **UploadResponse contains**:
   ```go
   type UploadResponse struct {
       Accepted         bool                 `json:"accepted"`           // true
       HighestServerSeq int64                `json:"highest_server_seq"` // 49170
       Statuses         []ChangeUploadStatus `json:"statuses"`           // 180 statuses
   }
   ```
4. **Client processes each status**:
   ```go
   for i, status := range response.Statuses {
       change := changes[i]
       switch status.Status {
       case "applied":
           // Remove from _sync_pending table
           DELETE FROM _sync_pending WHERE table_name = ? AND pk_uuid = ?
       }
   }
   ```

### Potential Root Causes

#### Hypothesis 1: Status Array Length Mismatch
**Problem**: Server returns fewer statuses than changes sent
**Evidence**: `len(response.Statuses) != len(changes)`
**Impact**: Client loop processes fewer changes than expected
**Fix**: Ensure server creates exactly one status per change

#### Hypothesis 2: Status Response Format Issue  
**Problem**: Server returns wrong status values
**Evidence**: `status.Status != "applied"` for successful changes
**Impact**: Client doesn't recognize successful changes
**Fix**: Verify server uses correct status strings

#### Hypothesis 3: Transaction Rollback
**Problem**: Client transaction rolls back after processing statuses
**Evidence**: DELETE operations succeed but transaction fails
**Impact**: Pending changes not actually removed from database
**Fix**: Check transaction commit/rollback logic

#### Hypothesis 4: Response Parsing Error
**Problem**: Client fails to parse server response
**Evidence**: JSON unmarshaling errors or nil response
**Impact**: Status processing never occurs
**Fix**: Add response validation and error handling

## Debugging Steps

### Step 1: Verify Server Response Structure
Add logging to server upload processing:

```go
// In oversync/upload.go ProcessUpload method
logger.Info("Upload response created", 
    "changes_sent", len(req.Changes),
    "statuses_created", len(statuses),
    "accepted", true,
    "highest_seq", response.HighestServerSeq)
```

### Step 2: Verify Client Response Processing
Add logging to client upload processing:

```go
// In oversqlite/sync.go uploadBatch method
logger.Info("Processing upload response",
    "changes_sent", len(changes), 
    "statuses_received", len(response.Statuses),
    "accepted", response.Accepted,
    "highest_seq", response.HighestServerSeq)

for i, status := range response.Statuses {
    logger.Info("Processing status", 
        "index", i,
        "change_id", changes[i].SourceChangeID,
        "status", status.Status,
        "table", changes[i].Table,
        "pk", changes[i].PK)
}
```

### Step 3: Verify Database State
Check _sync_pending table before and after upload:

```sql
-- Before upload
SELECT COUNT(*) FROM _sync_pending; -- Should be 180

-- After upload (if working correctly)  
SELECT COUNT(*) FROM _sync_pending; -- Should be 0

-- If still 180, check what's in the table
SELECT table_name, op, COUNT(*) FROM _sync_pending GROUP BY table_name, op;
```

### Step 4: Check Transaction Commit
Verify that the upload response processing transaction commits:

```go
// In oversqlite/sync.go uploadBatch method
defer func() {
    if err != nil {
        logger.Error("Upload transaction rolling back", "error", err)
        tx.Rollback()
    } else {
        logger.Info("Upload transaction committing")
        if commitErr := tx.Commit(); commitErr != nil {
            logger.Error("Upload transaction commit failed", "error", commitErr)
        } else {
            logger.Info("Upload transaction committed successfully")
        }
    }
}()
```

## Expected Fix Implementation

Based on the investigation, the most likely fix will be one of:

### Fix 1: Server Status Response Correction
Ensure server creates exactly one status per change:

```go
// Verify in server upload processing
if len(statuses) != len(req.Changes) {
    return nil, fmt.Errorf("status count mismatch: expected %d, got %d", 
        len(req.Changes), len(statuses))
}
```

### Fix 2: Client Response Validation
Add response validation in client:

```go
// In client upload processing
if len(response.Statuses) != len(changes) {
    return fmt.Errorf("response status count mismatch: sent %d changes, got %d statuses",
        len(changes), len(response.Statuses))
}
```

### Fix 3: Transaction Error Handling
Improve transaction error handling:

```go
// Ensure transaction commits properly
if err := tx.Commit(); err != nil {
    return fmt.Errorf("failed to commit upload response processing: %w", err)
}
```

## Success Verification

After implementing the fix, verify:

1. **✅ Server logs show**: "Successfully processed upload" with correct change count
2. **✅ Client logs show**: "Upload response processed" with matching status count  
3. **✅ Database state**: `SELECT COUNT(*) FROM _sync_pending` returns 0 after upload
4. **✅ Scenario completion**: Complex-multi-batch scenario completes within 30 seconds
5. **✅ No retries needed**: Single upload batch handles all 180 changes

## Prevention Measures

### Automated Testing
- Add unit tests for upload response processing
- Test status array length validation
- Test transaction commit/rollback scenarios
- Test large batch upload scenarios (100+ changes)

### Monitoring
- Log upload response processing metrics
- Monitor pending change count before/after uploads
- Alert on upload completion failures
- Track upload batch sizes and success rates

### Code Review Checklist
- [ ] Every uploaded change gets exactly one status response
- [ ] Client validates response structure before processing
- [ ] Transaction commit/rollback is properly handled
- [ ] Error cases are logged with sufficient detail
- [ ] Large batch scenarios are tested regularly

This issue demonstrates the importance of comprehensive stress testing with realistic data volumes to uncover subtle bugs that don't appear in simple test cases.
