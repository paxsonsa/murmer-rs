#!/bin/bash

# Simple script to test two nodes running in separate processes

echo "Testing multi-process node communication..."

# Start server in background
echo "Starting server on port 8001..."
cargo run -p murmer-core --bin node_ping server 8001 &
SERVER_PID=$!

# Give server time to start
sleep 2

echo "Starting client connecting to 127.0.0.1:8001..."
# Run client for a few seconds then kill it
timeout 5s cargo run -p murmer-core --bin node_ping client 127.0.0.1:8001 &
CLIENT_PID=$!

# Wait for client to finish
wait $CLIENT_PID

# Kill server
echo "Stopping server..."
kill $SERVER_PID
wait $SERVER_PID 2>/dev/null

echo "Multi-process test completed!"