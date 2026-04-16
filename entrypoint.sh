#!/bin/bash

# Export environment variables so cron jobs can access them
# (cron runs in a clean environment without the container's env vars)
printenv | grep -E '^(DATABASE_URL|NODE_ENV|PORT)=' > /etc/environment

# Start cron daemon in the background
cron

echo "[entrypoint] Cron daemon started (ingestion runs daily at 2:00 AM UTC)"
echo "[entrypoint] Starting MCP server..."

# Start the MCP server in the foreground
exec node dist/index.js
