# ---- Build stage ----
FROM node:22-slim AS builder
WORKDIR /app
COPY package.json package-lock.json* ./
RUN npm install
COPY tsconfig.json ./
COPY src ./src
RUN npm run build

# ---- Runtime stage ----
FROM node:22-slim
WORKDIR /app

# Install cron
RUN apt-get update && apt-get install -y cron && rm -rf /var/lib/apt/lists/*

COPY package.json package-lock.json* ./
RUN npm install --omit=dev

COPY --from=builder /app/dist ./dist

# Cron job: run ingestion daily at 2:00 AM UTC
# Logs go to /var/log/ingest.log so you can inspect from the container
RUN echo "0 2 * * * cd /app && DATABASE_URL=\$DATABASE_URL node dist/ingest.js >> /var/log/ingest.log 2>&1" > /etc/cron.d/ingest \
    && chmod 0644 /etc/cron.d/ingest \
    && crontab /etc/cron.d/ingest \
    && touch /var/log/ingest.log

# Startup script: export env vars for cron, start cron, then start the MCP server
COPY entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

EXPOSE 3000
CMD ["/app/entrypoint.sh"]
