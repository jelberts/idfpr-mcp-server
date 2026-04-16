/**
 * Ingestion script — run daily via crontab.
 *
 * Phase 1 (initial_load):
 *   Fetches 500 records per run using $offset pagination.
 *   When a batch returns fewer than 500 records, the initial load is complete.
 *
 * Phase 2 (delta_sync):
 *   Fetches records modified since the last watermark (lastmodifieddate).
 *   Upserts them into the local database.
 */

import { pool, initSchema } from "./db.js";

const SODA_BASE = "https://data.illinois.gov/resource/pzzh-kp68.json";
const ALLOWED_LICENSE_TYPES = ["PROF. ENGINEER", "STRUCTURAL ENGINEER"];
const BATCH_SIZE = 500;

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
  console.log(`[ingest] Fetching: ${url.toString()}`);
  const response = await fetch(url.toString());
  if (!response.ok) {
    throw new Error(`SODA API error: ${response.status} ${response.statusText}`);
  }
  return response.json() as Promise<SodaRecord[]>;
}

async function upsertRecords(records: SodaRecord[]): Promise<number> {
  let upserted = 0;
  for (const r of records) {
    await pool.query(
      `INSERT INTO licenses (
        license_number, license_type, description, license_status,
        business, title, first_name, middle, last_name, prefix, suffix,
        business_name, businessdba, original_issue_date, effective_date,
        expiration_date, city, state, zip, county, ever_disciplined,
        lastmodifieddate, ingested_at
      ) VALUES (
        $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,NOW()
      )
      ON CONFLICT (license_number) DO UPDATE SET
        license_type = EXCLUDED.license_type,
        description = EXCLUDED.description,
        license_status = EXCLUDED.license_status,
        business = EXCLUDED.business,
        title = EXCLUDED.title,
        first_name = EXCLUDED.first_name,
        middle = EXCLUDED.middle,
        last_name = EXCLUDED.last_name,
        prefix = EXCLUDED.prefix,
        suffix = EXCLUDED.suffix,
        business_name = EXCLUDED.business_name,
        businessdba = EXCLUDED.businessdba,
        original_issue_date = EXCLUDED.original_issue_date,
        effective_date = EXCLUDED.effective_date,
        expiration_date = EXCLUDED.expiration_date,
        city = EXCLUDED.city,
        state = EXCLUDED.state,
        zip = EXCLUDED.zip,
        county = EXCLUDED.county,
        ever_disciplined = EXCLUDED.ever_disciplined,
        lastmodifieddate = EXCLUDED.lastmodifieddate,
        ingested_at = NOW()`,
      [
        r.license_number, r.license_type, r.description, r.license_status,
        r.business, r.title, r.first_name, r.middle, r.last_name,
        r.prefix, r.suffix, r.business_name, r.businessdba,
        r.original_issue_date, r.effective_date, r.expiration_date,
        r.city, r.state, r.zip, r.county, r.ever_disciplined,
        r.lastmodifieddate,
      ]
    );
    upserted++;
  }
  return upserted;
}

async function getIngestState() {
  const result = await pool.query(
    `SELECT * FROM ingest_state WHERE id = 1`
  );
  return result.rows[0];
}

async function runInitialLoad(): Promise<void> {
  const state = await getIngestState();
  const offset = state.current_offset as number;

  const typeFilter = `license_type in('${ALLOWED_LICENSE_TYPES.join("','")}')`;
  const records = await fetchFromSoda({
    $where: typeFilter,
    $order: "license_number",
    $limit: String(BATCH_SIZE),
    $offset: String(offset),
  });

  console.log(`[ingest] Phase: initial_load | Offset: ${offset} | Fetched: ${records.length}`);

  if (records.length > 0) {
    const upserted = await upsertRecords(records);
    console.log(`[ingest] Upserted ${upserted} records`);
  }

  const newOffset = offset + records.length;
  const isComplete = records.length < BATCH_SIZE;

  // Find the max lastmodifieddate for the watermark
  let watermark = state.last_modified_watermark;
  if (records.length > 0) {
    const maxMod = records
      .map((r) => r.lastmodifieddate)
      .filter(Boolean)
      .sort()
      .pop();
    if (maxMod && (!watermark || maxMod > watermark)) {
      watermark = maxMod;
    }
  }

  await pool.query(
    `UPDATE ingest_state SET
      current_offset = $1,
      total_ingested = total_ingested + $2,
      last_run_at = NOW(),
      last_modified_watermark = COALESCE($3, last_modified_watermark),
      initial_load_complete = $4,
      phase = $5
    WHERE id = 1`,
    [
      newOffset,
      records.length,
      watermark,
      isComplete,
      isComplete ? "delta_sync" : "initial_load",
    ]
  );

  if (isComplete) {
    console.log(`[ingest] Initial load COMPLETE. Total offset reached: ${newOffset}`);
  } else {
    console.log(`[ingest] Initial load in progress. Next offset: ${newOffset}`);
  }
}

async function runDeltaSync(): Promise<void> {
  const state = await getIngestState();
  const watermark = state.last_modified_watermark as string;

  if (!watermark) {
    console.log("[ingest] No watermark found, falling back to initial load");
    await pool.query(`UPDATE ingest_state SET phase = 'initial_load' WHERE id = 1`);
    return runInitialLoad();
  }

  const typeFilter = `license_type in('${ALLOWED_LICENSE_TYPES.join("','")}')`;
  const records = await fetchFromSoda({
    $where: `${typeFilter} AND lastmodifieddate > '${watermark}'`,
    $order: "lastmodifieddate",
    $limit: String(BATCH_SIZE),
  });

  console.log(`[ingest] Phase: delta_sync | Watermark: ${watermark} | Fetched: ${records.length}`);

  if (records.length > 0) {
    const upserted = await upsertRecords(records);
    console.log(`[ingest] Upserted ${upserted} records (new/updated)`);

    const maxMod = records
      .map((r) => r.lastmodifieddate)
      .filter(Boolean)
      .sort()
      .pop();

    await pool.query(
      `UPDATE ingest_state SET
        total_ingested = total_ingested + $1,
        last_run_at = NOW(),
        last_modified_watermark = COALESCE($2, last_modified_watermark)
      WHERE id = 1`,
      [records.length, maxMod]
    );
  } else {
    console.log("[ingest] No new or modified records found");
    await pool.query(
      `UPDATE ingest_state SET last_run_at = NOW() WHERE id = 1`
    );
  }
}

// ---- Exported run function (used by HTTP endpoint and CLI) ----

export async function runIngestion(): Promise<string> {
  const logs: string[] = [];
  const log = (msg: string) => { console.log(msg); logs.push(msg); };

  log(`[ingest] Starting ingestion run at ${new Date().toISOString()}`);

  const state = await getIngestState();
  log(`[ingest] Current state: phase=${state.phase}, offset=${state.current_offset}, complete=${state.initial_load_complete}`);

  if (state.phase === "initial_load" && !state.initial_load_complete) {
    await runInitialLoad();
  } else {
    await runDeltaSync();
  }

  const countResult = await pool.query<{ count: string }>(
    `SELECT COUNT(*) as count FROM licenses`
  );
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
