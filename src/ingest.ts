/**
 * Ingestion script — run daily via crontab.
 *
 * Phase 1 (initial_load):
 *   Fetches 2,000 records per run using $offset pagination.
 *   Uses bulk upsert (single query via unnest) for speed.
 *   When a batch returns fewer than 2,000 records, the initial load is complete.
 *
 * Phase 2 (delta_sync):
 *   Fetches records modified since the last watermark (lastmodifieddate).
 *   Upserts them into the local database.
 */

import { pool, initSchema } from "./db.js";

const SODA_BASE = "https://data.illinois.gov/resource/pzzh-kp68.json";
const BATCH_SIZE = 2000;
// Only load active, fully licensed engineers — no interns, no expired/not-renewed
const TYPE_FILTER = `license_status='ACTIVE' AND description in('LICENSED PROFESSIONAL ENGINEER','LICENSED STRUCTURAL ENGINEER') AND license_number IS NOT NULL AND license_number != ''`;

interface SodaRecord {
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

async function fetchFromSoda(params: Record<string, string>): Promise<SodaRecord[]> {
  const url = new URL(SODA_BASE);
  for (const [key, value] of Object.entries(params)) {
    url.searchParams.set(key, value);
  }
  const response = await fetch(url.toString());
  if (!response.ok) {
    throw new Error(`SODA API error: ${response.status} ${response.statusText}`);
  }
  return response.json() as Promise<SodaRecord[]>;
}

// Bulk upsert using unnest — single query regardless of batch size, very fast
async function bulkUpsert(records: SodaRecord[]): Promise<number> {
  if (records.length === 0) return 0;

  // Deduplicate by license_number — keep the last occurrence
  const seen = new Map<string, SodaRecord>();
  for (const r of records) seen.set(r.license_number, r);
  records = Array.from(seen.values());

  const s = (field: keyof SodaRecord) => records.map(r => r[field] ?? null);

  await pool.query(
    `INSERT INTO licenses (
      license_number, license_type, description, license_status,
      business, title, first_name, middle, last_name, prefix, suffix,
      business_name, businessdba, original_issue_date, effective_date,
      expiration_date, city, state, zip, county, ever_disciplined,
      lastmodifieddate, ingested_at
    )
    SELECT
      license_number, license_type, description, license_status,
      business, title, first_name, middle, last_name, prefix, suffix,
      business_name, businessdba, original_issue_date, effective_date,
      expiration_date, city, state, zip, county, ever_disciplined,
      lastmodifieddate, NOW()
    FROM unnest(
      $1::text[], $2::text[], $3::text[], $4::text[], $5::text[], $6::text[],
      $7::text[], $8::text[], $9::text[], $10::text[], $11::text[], $12::text[],
      $13::text[], $14::text[], $15::text[], $16::text[], $17::text[], $18::text[],
      $19::text[], $20::text[], $21::text[], $22::text[]
    ) AS t(
      license_number, license_type, description, license_status,
      business, title, first_name, middle, last_name, prefix, suffix,
      business_name, businessdba, original_issue_date, effective_date,
      expiration_date, city, state, zip, county, ever_disciplined,
      lastmodifieddate
    )
    ON CONFLICT (license_number) DO UPDATE SET
      license_type        = EXCLUDED.license_type,
      description         = EXCLUDED.description,
      license_status      = EXCLUDED.license_status,
      business            = EXCLUDED.business,
      title               = EXCLUDED.title,
      first_name          = EXCLUDED.first_name,
      middle              = EXCLUDED.middle,
      last_name           = EXCLUDED.last_name,
      prefix              = EXCLUDED.prefix,
      suffix              = EXCLUDED.suffix,
      business_name       = EXCLUDED.business_name,
      businessdba         = EXCLUDED.businessdba,
      original_issue_date = EXCLUDED.original_issue_date,
      effective_date      = EXCLUDED.effective_date,
      expiration_date     = EXCLUDED.expiration_date,
      city                = EXCLUDED.city,
      state               = EXCLUDED.state,
      zip                 = EXCLUDED.zip,
      county              = EXCLUDED.county,
      ever_disciplined    = EXCLUDED.ever_disciplined,
      lastmodifieddate    = EXCLUDED.lastmodifieddate,
      ingested_at         = NOW()`,
    [
      s("license_number"), s("license_type"), s("description"), s("license_status"),
      s("business"), s("title"), s("first_name"), s("middle"), s("last_name"),
      s("prefix"), s("suffix"), s("business_name"), s("businessdba"),
      s("original_issue_date"), s("effective_date"), s("expiration_date"),
      s("city"), s("state"), s("zip"), s("county"), s("ever_disciplined"),
      s("lastmodifieddate"),
    ]
  );

  return records.length;
}

function maxWatermark(records: SodaRecord[], current: string | null): string | null {
  const maxMod = records
    .map(r => r.lastmodifieddate)
    .filter(Boolean)
    .sort()
    .pop() ?? null;
  if (!maxMod) return current;
  if (!current || maxMod > current) return maxMod;
  return current;
}

async function getIngestState() {
  const result = await pool.query(`SELECT * FROM ingest_state WHERE id = 1`);
  return result.rows[0];
}

async function runDeltaSync(): Promise<string> {
  const state = await getIngestState();
  const watermark = state.last_modified_watermark as string;

  if (!watermark) {
    return "[ingest] No watermark — falling back to initial load";
  }

  const records = await fetchFromSoda({
    $where: `${TYPE_FILTER} AND lastmodifieddate > '${watermark}'`,
    $order: "lastmodifieddate",
    $limit: String(BATCH_SIZE),
  });

  if (records.length > 0) {
    await bulkUpsert(records);
    const newWatermark = maxWatermark(records, watermark);
    await pool.query(
      `UPDATE ingest_state SET
        total_ingested = total_ingested + $1,
        last_run_at = NOW(),
        last_modified_watermark = COALESCE($2, last_modified_watermark)
      WHERE id = 1`,
      [records.length, newWatermark]
    );
    return `[ingest] Delta sync: upserted ${records.length} new/updated records`;
  }

  await pool.query(`UPDATE ingest_state SET last_run_at = NOW() WHERE id = 1`);
  return "[ingest] Delta sync: no new or modified records found";
}

// ---- Exported run function (used by HTTP endpoint and CLI) ----

export async function runIngestion(): Promise<string> {
  const logs: string[] = [];
  const log = (msg: string) => { console.log(msg); logs.push(msg); };

  log(`[ingest] Starting at ${new Date().toISOString()} | batch_size=${BATCH_SIZE}`);

  try {
    const state = await getIngestState();
    log(`[ingest] State: phase=${state.phase}, offset=${state.current_offset}, complete=${state.initial_load_complete}`);

    if (state.phase === "initial_load" && !state.initial_load_complete) {
      const offset = state.current_offset as number;
      log(`[ingest] Fetching ${BATCH_SIZE} records at offset ${offset}...`);

      const records = await fetchFromSoda({
        $where: TYPE_FILTER,
        $order: "license_number",
        $limit: String(BATCH_SIZE),
        $offset: String(offset),
      });

      log(`[ingest] Fetched ${records.length} records (first: ${records[0]?.license_number ?? "none"})`);

      if (records.length > 0) {
        await bulkUpsert(records);
        log(`[ingest] Bulk upserted ${records.length} records`);
      }

      const newOffset = offset + records.length;
      const isComplete = records.length < BATCH_SIZE;
      const watermark = maxWatermark(records, state.last_modified_watermark);

      await pool.query(
        `UPDATE ingest_state SET
          current_offset = $1,
          total_ingested = total_ingested + $2,
          last_run_at = NOW(),
          last_modified_watermark = COALESCE($3, last_modified_watermark),
          initial_load_complete = $4,
          phase = $5
        WHERE id = 1`,
        [newOffset, records.length, watermark, isComplete, isComplete ? "delta_sync" : "initial_load"]
      );

      log(isComplete
        ? `[ingest] Initial load COMPLETE — total offset: ${newOffset}`
        : `[ingest] Next offset: ${newOffset}`);
    } else {
      const msg = await runDeltaSync();
      log(msg);
    }
  } catch (err) {
    log(`[ingest] ERROR: ${String(err)}`);
  }

  const countResult = await pool.query<{ count: string }>(`SELECT COUNT(*) as count FROM licenses`);
  log(`[ingest] Total records in database: ${countResult.rows[0].count}`);
  log(`[ingest] Done.`);

  return logs.join("\n");
}

// ---- CLI entrypoint (used by crontab) ----

const isMainModule = process.argv[1]?.endsWith("ingest.js") || process.argv[1]?.endsWith("ingest.ts");

if (isMainModule) {
  (async () => {
    await initSchema();
    await runIngestion();
    await pool.end();
    process.exit(0);
  })().catch((err) => {
    console.error("[ingest] Fatal error:", err);
    process.exit(1);
  });
}
