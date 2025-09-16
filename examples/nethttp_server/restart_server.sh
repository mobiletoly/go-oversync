#!/bin/bash

# Script to properly restart the nethttp_server with logging
set -e

SERVER_DIR="."
LOG_FILE="/tmp/server.log"

echo "Restarting nethttp_server..."

# Step 1: Kill existing server process
echo "Killing existing server processes..."
lsof -ti:8080 | xargs kill -9 2>/dev/null || true
pkill -f "go run.*nethttp_server" 2>/dev/null || true
sleep 2

# Step 2: Clean up logs and database
echo "Cleaning up old logs and database..."
rm -f "$LOG_FILE"

# Clean up test data from database
echo "Cleaning up test data from database..."
psql "postgres://postgres:postgres@localhost:5432/clisync_example" -c "
  DELETE FROM sync.server_change_log WHERE user_id LIKE 'user-fresh-%' OR user_id LIKE 'user-%';
  DELETE FROM sync.sync_row_meta WHERE schema_name = 'business';
  DELETE FROM business.posts;
  DELETE FROM business.users;
" 2>/dev/null || echo "⚠️  Database cleanup failed (database might not exist yet)"

# Step 3: Build and check for errors
echo "Building server..."
cd "$SERVER_DIR"
if ! go build . 2>&1; then
    echo "❌ Build failed!"
    exit 1
fi
echo "✅ Build successful"

# Step 4: Start server in background
echo "Starting server..."
nohup go run . > "$LOG_FILE" 2>&1 &
SERVER_PID=$!
echo "🚀 Server started with PID: $SERVER_PID"

# Step 5: Wait for server to start and check logs
echo "Waiting for server to start..."
sleep 4

# Step 6: Verify server is running
if ! lsof -i :8080 >/dev/null 2>&1; then
    echo "❌ Server is not listening on port 8080!"
    echo "Server logs:"
    cat "$LOG_FILE" 2>/dev/null || echo "No logs found"
    exit 1
fi

# Step 7: Show initial logs
echo "✅ Server is running on port 8080"
echo "Initial server logs:"
head -20 "$LOG_FILE" 2>/dev/null || echo "No logs yet" && exit 1

echo ""
echo "Server restart complete!"
echo "Use 'tail -f /tmp/server.log' to monitor logs"
echo "Use 'lsof -i :8080' to check if server is running"
