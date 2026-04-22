#!/bin/bash

# Write all environment variables to /etc/environment so cron jobs can source them
# (cron runs in a clean environment without the container's env vars)
printenv | sed 's/=\(.*\)/="\1"/' > /etc/environment

echo "[entrypoint] Environment written to /etc/environment"
echo "[entrypoint] DATABASE_URL is $([ -n "$DATABASE_URL" ] && echo 'SET' || echo 'NOT SET')"

# Start cron daemon in the background
service cron start

echo "[entrypoint] Cron daemon started (ingestion runs daily at 2:00 AM UTC)"
echo "[entrypoint] Cron log: /var/log/ingest.log"
echo "[entrypoint] Starting MCP server..."

# Start the MCP server in the foreground
exec node dist/index.js
