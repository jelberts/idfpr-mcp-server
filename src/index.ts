import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { SSEServerTransport } from "@modelcontextprotocol/sdk/server/sse.js";
import { z } from "zod";
import http from "node:http";

const SODA_BASE = "https://data.illinois.gov/resource/pzzh-kp68.json";
const ALLOWED_LICENSE_TYPES = ["PROF. ENGINEER", "STRUCTURAL ENGINEER"];
const DEFAULT_LIMIT = 50;
const MAX_LIMIT = 1000;

interface LicenseRecord {
  license_type: string;
  description: string;
  license_number: string;
  license_status: string;
  business: string;
  title: string;
  first_name: string;
  middle: string;
  last_name: string;
  prefix: string;
  suffix: string;
  business_name: string;
  businessdba: string;
  original_issue_date: string;
  effective_date: string;
  expiration_date: string;
  city: string;
  state: string;
  zip: string;
  county: string;
  ever_disciplined: string;
  lastmodifieddate: string;
}

async function querySODA(params: Record<string, string>): Promise<LicenseRecord[]> {
  const url = new URL(SODA_BASE);
  for (const [key, value] of Object.entries(params)) {
    url.searchParams.set(key, value);
  }

  const response = await fetch(url.toString());
  if (!response.ok) {
    throw new Error(`SODA API error: ${response.status} ${response.statusText}`);
  }
  return response.json() as Promise<LicenseRecord[]>;
}

function buildWhereClause(conditions: string[]): string {
  const typeFilter = `license_type in('${ALLOWED_LICENSE_TYPES.join("','")}')`;
  return [typeFilter, ...conditions].join(" AND ");
}

function formatRecord(r: LicenseRecord): string {
  const name = r.business === "Y"
    ? r.business_name || r.businessdba || "(Business)"
    : [r.prefix, r.first_name, r.middle, r.last_name, r.suffix].filter(Boolean).join(" ") || "(No name)";

  return [
    `Name: ${name}`,
    `License Type: ${r.license_type}`,
    `Description: ${r.description}`,
    `License #: ${r.license_number}`,
    `Status: ${r.license_status}`,
    `Business: ${r.business}`,
    r.business_name ? `Business Name: ${r.business_name}` : null,
    r.businessdba ? `DBA: ${r.businessdba}` : null,
    `Original Issue: ${r.original_issue_date}`,
    `Effective: ${r.effective_date}`,
    `Expiration: ${r.expiration_date}`,
    `Location: ${[r.city, r.state, r.zip].filter(Boolean).join(", ")}`,
    r.county ? `County: ${r.county}` : null,
    `Disciplined: ${r.ever_disciplined}`,
    `Last Modified: ${r.lastmodifieddate}`,
  ].filter(Boolean).join("\n");
}

const server = new McpServer({
  name: "idfpr-mcp-server",
  version: "1.0.0",
});

// Tool 1: Search by name
server.tool(
  "search_by_name",
  "Search for licensed engineers or structural engineers by first and/or last name",
  {
    first_name: z.string().optional().describe("First name to search (case-insensitive)"),
    last_name: z.string().optional().describe("Last name to search (case-insensitive)"),
    limit: z.number().min(1).max(MAX_LIMIT).optional().describe(`Max results to return (default ${DEFAULT_LIMIT})`),
  },
  async ({ first_name, last_name, limit }) => {
    if (!first_name && !last_name) {
      return { content: [{ type: "text", text: "Error: Provide at least a first_name or last_name." }] };
    }

    const conditions: string[] = [];
    if (first_name) conditions.push(`upper(first_name) like upper('%${first_name.replace(/'/g, "''")}%')`);
    if (last_name) conditions.push(`upper(last_name) like upper('%${last_name.replace(/'/g, "''")}%')`);

    const records = await querySODA({
      $where: buildWhereClause(conditions),
      $limit: String(limit || DEFAULT_LIMIT),
      $order: "last_name,first_name",
    });

    if (records.length === 0) {
      return { content: [{ type: "text", text: "No matching engineers found." }] };
    }

    const formatted = records.map((r, i) => `--- Result ${i + 1} ---\n${formatRecord(r)}`).join("\n\n");
    return { content: [{ type: "text", text: `Found ${records.length} result(s):\n\n${formatted}` }] };
  }
);

// Tool 2: Lookup by license number
server.tool(
  "lookup_by_license_number",
  "Look up a specific engineer license by its license number",
  {
    license_number: z.string().describe("The license number to look up"),
  },
  async ({ license_number }) => {
    const records = await querySODA({
      $where: buildWhereClause([`license_number='${license_number.replace(/'/g, "''")}'`]),
      $limit: "10",
    });

    if (records.length === 0) {
      return { content: [{ type: "text", text: `No engineer license found with number: ${license_number}` }] };
    }

    const formatted = records.map(r => formatRecord(r)).join("\n\n---\n\n");
    return { content: [{ type: "text", text: formatted }] };
  }
);

// Tool 3: Check license status / verify
server.tool(
  "verify_license_status",
  "Verify whether an engineer's license is active. Search by name and/or license number.",
  {
    license_number: z.string().optional().describe("License number to verify"),
    first_name: z.string().optional().describe("First name of the license holder"),
    last_name: z.string().optional().describe("Last name of the license holder"),
  },
  async ({ license_number, first_name, last_name }) => {
    if (!license_number && !first_name && !last_name) {
      return { content: [{ type: "text", text: "Error: Provide a license_number, name, or both." }] };
    }

    const conditions: string[] = [];
    if (license_number) conditions.push(`license_number='${license_number.replace(/'/g, "''")}'`);
    if (first_name) conditions.push(`upper(first_name) like upper('%${first_name.replace(/'/g, "''")}%')`);
    if (last_name) conditions.push(`upper(last_name) like upper('%${last_name.replace(/'/g, "''")}%')`);

    const records = await querySODA({
      $where: buildWhereClause(conditions),
      $limit: "20",
    });

    if (records.length === 0) {
      return { content: [{ type: "text", text: "No matching engineer license found. Cannot verify." }] };
    }

    const results = records.map(r => {
      const name = [r.first_name, r.middle, r.last_name].filter(Boolean).join(" ") || r.business_name || "(Unknown)";
      const isActive = r.license_status?.toUpperCase() === "ACTIVE";
      const icon = isActive ? "ACTIVE" : "NOT ACTIVE";
      return [
        `${name} — License #${r.license_number}`,
        `  Status: ${icon} (${r.license_status})`,
        `  Type: ${r.license_type} — ${r.description}`,
        `  Expiration: ${r.expiration_date}`,
        `  Ever Disciplined: ${r.ever_disciplined}`,
      ].join("\n");
    });

    return { content: [{ type: "text", text: `Verification results:\n\n${results.join("\n\n")}` }] };
  }
);

// Tool 4: List by city or county
server.tool(
  "list_by_location",
  "List licensed engineers and structural engineers in a given city, county, or state",
  {
    city: z.string().optional().describe("City name (case-insensitive)"),
    county: z.string().optional().describe("County name (case-insensitive)"),
    state: z.string().optional().describe("Two-letter state abbreviation (e.g. IL)"),
    license_status: z.string().optional().describe("Filter by status, e.g. ACTIVE, NOT RENEWED (default: all)"),
    limit: z.number().min(1).max(MAX_LIMIT).optional().describe(`Max results (default ${DEFAULT_LIMIT})`),
  },
  async ({ city, county, state, license_status, limit }) => {
    if (!city && !county && !state) {
      return { content: [{ type: "text", text: "Error: Provide at least a city, county, or state." }] };
    }

    const conditions: string[] = [];
    if (city) conditions.push(`upper(city)=upper('${city.replace(/'/g, "''")}')`);
    if (county) conditions.push(`upper(county)=upper('${county.replace(/'/g, "''")}')`);
    if (state) conditions.push(`upper(state)=upper('${state.replace(/'/g, "''")}')`);
    if (license_status) conditions.push(`upper(license_status)=upper('${license_status.replace(/'/g, "''")}')`);

    const records = await querySODA({
      $where: buildWhereClause(conditions),
      $limit: String(limit || DEFAULT_LIMIT),
      $order: "last_name,first_name",
    });

    if (records.length === 0) {
      return { content: [{ type: "text", text: "No engineers found matching the location criteria." }] };
    }

    const formatted = records.map((r, i) => `--- Result ${i + 1} ---\n${formatRecord(r)}`).join("\n\n");
    return { content: [{ type: "text", text: `Found ${records.length} result(s):\n\n${formatted}` }] };
  }
);

// --- HTTP + SSE Transport for Railway ---

const PORT = parseInt(process.env.PORT || "3000", 10);

const transports: Record<string, SSEServerTransport> = {};

const httpServer = http.createServer(async (req, res) => {
  const url = new URL(req.url || "/", `http://localhost:${PORT}`);

  // CORS headers
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");

  if (req.method === "OPTIONS") {
    res.writeHead(204);
    res.end();
    return;
  }

  // Health check
  if (url.pathname === "/" || url.pathname === "/health") {
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ status: "ok", server: "idfpr-mcp-server" }));
    return;
  }

  // SSE endpoint — client connects here
  if (url.pathname === "/sse" && req.method === "GET") {
    const transport = new SSEServerTransport("/messages", res);
    transports[transport.sessionId] = transport;
    res.on("close", () => {
      delete transports[transport.sessionId];
    });
    await server.connect(transport);
    return;
  }

  // Messages endpoint — client sends messages here
  if (url.pathname === "/messages" && req.method === "POST") {
    const sessionId = url.searchParams.get("sessionId");
    if (!sessionId || !transports[sessionId]) {
      res.writeHead(400, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ error: "Invalid or missing sessionId" }));
      return;
    }
    const body = await new Promise<string>((resolve) => {
      let data = "";
      req.on("data", (chunk) => (data += chunk));
      req.on("end", () => resolve(data));
    });
    req.body = body;
    await transports[sessionId].handlePostMessage(req, res);
    return;
  }

  res.writeHead(404, { "Content-Type": "application/json" });
  res.end(JSON.stringify({ error: "Not found" }));
});

httpServer.listen(PORT, () => {
  console.log(`IDFPR MCP Server running on http://localhost:${PORT}`);
  console.log(`SSE endpoint: http://localhost:${PORT}/sse`);
});
