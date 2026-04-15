# IDFPR MCP Server

MCP server for querying Illinois Department of Financial and Professional Regulation (IDFPR) license data for **Professional Engineers** and **Structural Engineers** via the Socrata Open Data API.

## Tools

| Tool | Description |
|------|-------------|
| `search_by_name` | Search engineers by first and/or last name |
| `lookup_by_license_number` | Look up a specific license by number |
| `verify_license_status` | Verify if an engineer's license is active |
| `list_by_location` | List engineers by city, county, or state |

## Deploy to Railway

1. Create a [Railway account](https://railway.com)
2. Install the Railway CLI: `npm i -g @railway/cli`
3. Login: `railway login`
4. Initialize and deploy:

```bash
cd idfpr-mcp-server
git init && git add -A && git commit -m "Initial commit"
railway init
railway up
```

5. After deploy, get your public URL from the Railway dashboard
6. Your MCP SSE endpoint will be: `https://your-app.up.railway.app/sse`

## Connect to Claude Code

Add to your Claude Code MCP settings (`~/.claude/settings.json` or project `.claude/settings.json`):

```json
{
  "mcpServers": {
    "idfpr": {
      "type": "sse",
      "url": "https://your-app.up.railway.app/sse"
    }
  }
}
```

## Local Development

```bash
npm install
npm run dev
```

Server runs on `http://localhost:3000` with SSE at `/sse`.
