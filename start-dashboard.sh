#!/usr/bin/env bash
set -e

echo "🚀 Starting CRDTdb Dashboard..."
echo ""

# Start backend
echo "▶ Starting Rust backend on :8888"
cargo run --bin dashboard-server &
BACKEND_PID=$!

# Wait for backend
sleep 2

echo ""
echo "✅ CRDTdb Dashboard running!"
echo "   Dashboard: http://localhost:3000"
echo "   API:       http://localhost:8888/api/peers"
echo "   WebSocket: ws://localhost:8888/ws"
echo ""
echo "Press Ctrl+C to stop both servers."

trap "kill $BACKEND_PID $FRONTEND_PID 2>/dev/null" EXIT
wait
