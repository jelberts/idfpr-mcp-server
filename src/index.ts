import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";
import { isInitializeRequest } from "@modelcontextprotocol/sdk/types.js";
import { randomUUID } from "node:crypto";
import { z } from "zod";
import express, { Request, Response } from "express";
import cors from "cors";
import {
  initSchema,
  searchByName,
  lookupByLicenseNumber,
  verifyLicenseStatus,
  listByLocation,
  getRecordCount,
  LicenseRow,
  pool,
} from "./db.js";
import { runIngestion } from "./ingest.js";

// ---- Config ----

const DEFAULT_LIMIT = 50;
const MAX_LIMIT = 1000;

// ---- Formatting ----

function formatRecord(r: LicenseRow): string {
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

// ---- Register MCP tools on a server instance ----

function registerTools(server: McpServer) {
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
        return { content: [{ type: "text" as const, text: "Error: Provide at least a first_name or last_name." }] };
      }
      const records = await searchByName(first_name, last_name, limit || DEFAULT_LIMIT);
      if (records.length === 0) {
        return { content: [{ type: "text" as const, text: "No matching engineers found." }] };
      }
      const formatted = records.map((r, i) => `--- Result ${i + 1} ---\n${formatRecord(r)}`).join("\n\n");
      return { content: [{ type: "text" as const, text: `Found ${records.length} result(s):\n\n${formatted}` }] };
    }
  );

  server.tool(
    "lookup_by_license_number",
    "Look up a specific engineer license by its license number",
    {
      license_number: z.string().describe("The license number to look up"),
    },
    async ({ license_number }) => {
      const records = await lookupByLicenseNumber(license_number);
      if (records.length === 0) {
        return { content: [{ type: "text" as const, text: `No engineer license found with number: ${license_number}` }] };
      }
      const formatted = records.map(r => formatRecord(r)).join("\n\n---\n\n");
      return { content: [{ type: "text" as const, text: formatted }] };
    }
  );

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
        return { content: [{ type: "text" as const, text: "Error: Provide a license_number, name, or both." }] };
      }
      const records = await verifyLicenseStatus(license_number, first_name, last_name);
      if (records.length === 0) {
        return { content: [{ type: "text" as const, text: "No matching engineer license found. Cannot verify." }] };
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
      return { content: [{ type: "text" as const, text: `Verification results:\n\n${results.join("\n\n")}` }] };
    }
  );

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
        return { content: [{ type: "text" as const, text: "Error: Provide at least a city, county, or state." }] };
      }
      const records = await listByLocation(city, county, state, license_status, limit || DEFAULT_LIMIT);
      if (records.length === 0) {
        return { content: [{ type: "text" as const, text: "No engineers found matching the location criteria." }] };
      }
      const formatted = records.map((r, i) => `--- Result ${i + 1} ---\n${formatRecord(r)}`).join("\n\n");
      return { content: [{ type: "text" as const, text: `Found ${records.length} result(s):\n\n${formatted}` }] };
    }
  );

  // New tool: check ingestion status
  server.tool(
    "ingestion_status",
    "Check the current status of the data ingestion pipeline — how many records are loaded, what phase it's in, and when it last ran.",
    {},
    async () => {
      const state = await pool.query(`SELECT * FROM ingest_state WHERE id = 1`);
      const count = await getRecordCount();
      const s = state.rows[0];
      const lines = [
        `Phase: ${s.phase}`,
        `Initial Load Complete: ${s.initial_load_complete}`,
        `Current Offset: ${s.current_offset}`,
        `Total Records Ingested (cumulative): ${s.total_ingested}`,
        `Records in Database: ${count}`,
        `Batch Size: ${s.batch_size}`,
        `Last Run: ${s.last_run_at || "Never"}`,
        `Last Modified Watermark: ${s.last_modified_watermark || "Not set"}`,
      ];
      return { content: [{ type: "text" as const, text: lines.join("\n") }] };
    }
  );
}

// ---- Express Server + Streamable HTTP Transport ----

const PORT = parseInt(process.env.PORT || "3000", 10);
const app = express();

app.use(cors());
app.use(express.json());

// Session store
const transports: Record<string, StreamableHTTPServerTransport> = {};

// Health check
app.get("/", async (_req, res) => {
  let dbRecords = 0;
  try {
    dbRecords = await getRecordCount();
  } catch { /* db not ready yet */ }
  res.json({
    status: "ok",
    server: "idfpr-mcp-server",
    sessions: Object.keys(transports).length,
    records_in_db: dbRecords,
  });
});

// GET & POST /ingest — trigger ingestion on demand
const handleIngest = async (_req: Request, res: Response) => {
  try {
    console.log("[HTTP] Manual ingestion triggered");
    const output = await runIngestion();
    res.json({ status: "ok", output });
  } catch (err) {
    console.error("[HTTP] Ingestion error:", err);
    res.status(500).json({ status: "error", message: String(err) });
  }
};
app.get("/ingest", handleIngest);
app.post("/ingest", handleIngest);

// POST /mcp — initialize new sessions and handle JSON-RPC requests
app.post("/mcp", async (req: Request, res: Response) => {
  try {
    const sessionId = req.headers["mcp-session-id"] as string | undefined;

    if (sessionId && transports[sessionId]) {
      console.log(`[POST] Existing session: ${sessionId}`);
      await transports[sessionId].handleRequest(req, res, req.body);
      return;
    }

    if (isInitializeRequest(req.body)) {
      console.log("[POST] New initialize request received");

      const transport = new StreamableHTTPServerTransport({
        sessionIdGenerator: () => randomUUID(),
        onsessioninitialized: (sid: string) => {
          console.log(`[POST] Session initialized: ${sid}`);
          transports[sid] = transport;
        },
      });

      transport.onclose = () => {
        const sid = transport.sessionId;
        if (sid) {
          console.log(`[CLOSE] Session closed: ${sid}`);
          delete transports[sid];
        }
      };

      const mcpServer = new McpServer({ name: "idfpr-mcp-server", version: "2.0.0" });
      registerTools(mcpServer);
      await mcpServer.connect(transport);

      await transport.handleRequest(req, res, req.body);
      return;
    }

    console.log("[POST] Bad request: not initialize and no valid session");
    res.status(400).json({
      jsonrpc: "2.0",
      error: { code: -32000, message: "Bad request: no valid session" },
      id: null,
    });
  } catch (err) {
    console.error("[POST] Error:", err);
    if (!res.headersSent) {
      res.status(500).json({ error: "Internal server error" });
    }
  }
});

// GET /mcp — SSE stream for server-to-client notifications
app.get("/mcp", async (req: Request, res: Response) => {
  const sessionId = req.headers["mcp-session-id"] as string | undefined;
  if (!sessionId || !transports[sessionId]) {
    res.status(400).json({ error: "Invalid or missing session ID" });
    return;
  }
  console.log(`[GET] SSE stream for session: ${sessionId}`);
  await transports[sessionId].handleRequest(req, res);
});

// DELETE /mcp — terminate session
app.delete("/mcp", async (req: Request, res: Response) => {
  const sessionId = req.headers["mcp-session-id"] as string | undefined;
  if (!sessionId || !transports[sessionId]) {
    res.status(400).json({ error: "Invalid or missing session ID" });
    return;
  }
  console.log(`[DELETE] Closing session: ${sessionId}`);
  await transports[sessionId].handleRequest(req, res);
});

// ---- Start ----

async function start() {
  console.log("Initializing database schema...");
  await initSchema();
  console.log("Database schema ready.");

  const count = await getRecordCount();
  console.log(`Records currently in database: ${count}`);

  app.listen(PORT, () => {
    console.log(`IDFPR MCP Server running on http://localhost:${PORT}`);
    console.log(`Streamable HTTP endpoint: /mcp`);
  });
}

start().catch((err) => {
  console.error("Failed to start server:", err);
  process.exit(1);
});
