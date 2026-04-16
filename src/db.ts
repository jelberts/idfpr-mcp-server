import pg from "pg";

const { Pool } = pg;

// ---- Connection ----

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DATABASE_URL?.includes("localhost")
    ? false
    : { rejectUnauthorized: false },
});

export { pool };

// ---- Schema ----

export async function initSchema(): Promise<void> {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS licenses (
      license_number  TEXT PRIMARY KEY,
      license_type    TEXT,
      description     TEXT,
      license_status  TEXT,
      business        TEXT,
      title           TEXT,
      first_name      TEXT,
      middle          TEXT,
      last_name       TEXT,
      prefix          TEXT,
      suffix          TEXT,
      business_name   TEXT,
      businessdba     TEXT,
      original_issue_date TEXT,
      effective_date  TEXT,
      expiration_date TEXT,
      city            TEXT,
      state           TEXT,
      zip             TEXT,
      county          TEXT,
      ever_disciplined TEXT,
      lastmodifieddate TEXT,
      ingested_at     TIMESTAMPTZ DEFAULT NOW()
    );

    CREATE INDEX IF NOT EXISTS idx_licenses_last_name
      ON licenses (UPPER(last_name));
    CREATE INDEX IF NOT EXISTS idx_licenses_first_name
      ON licenses (UPPER(first_name));
    CREATE INDEX IF NOT EXISTS idx_licenses_city
      ON licenses (UPPER(city));
    CREATE INDEX IF NOT EXISTS idx_licenses_county
      ON licenses (UPPER(county));
    CREATE INDEX IF NOT EXISTS idx_licenses_state
      ON licenses (UPPER(state));
    CREATE INDEX IF NOT EXISTS idx_licenses_status
      ON licenses (UPPER(license_status));

    CREATE TABLE IF NOT EXISTS ingest_state (
      id              INTEGER PRIMARY KEY DEFAULT 1,
      phase           TEXT NOT NULL DEFAULT 'initial_load',
      current_offset  INTEGER NOT NULL DEFAULT 0,
      batch_size      INTEGER NOT NULL DEFAULT 500,
      total_ingested  INTEGER NOT NULL DEFAULT 0,
      last_run_at     TIMESTAMPTZ,
      last_modified_watermark TEXT,
      initial_load_complete BOOLEAN NOT NULL DEFAULT FALSE
    );

    INSERT INTO ingest_state (id, phase, current_offset)
    VALUES (1, 'initial_load', 0)
    ON CONFLICT (id) DO NOTHING;
  `);
}

// ---- Query helpers for MCP tools ----

export interface LicenseRow {
  license_number: string;
  license_type: string;
  description: string;
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

export async function searchByName(
  firstName?: string,
  lastName?: string,
  limit = 50
): Promise<LicenseRow[]> {
  const conditions: string[] = [];
  const params: string[] = [];
  let idx = 1;

  if (firstName) {
    conditions.push(`UPPER(first_name) LIKE UPPER($${idx})`);
    params.push(`%${firstName}%`);
    idx++;
  }
  if (lastName) {
    conditions.push(`UPPER(last_name) LIKE UPPER($${idx})`);
    params.push(`%${lastName}%`);
    idx++;
  }

  const where = conditions.length > 0 ? `WHERE ${conditions.join(" AND ")}` : "";
  const result = await pool.query<LicenseRow>(
    `SELECT * FROM licenses ${where} ORDER BY last_name, first_name LIMIT $${idx}`,
    [...params, limit]
  );
  return result.rows;
}

export async function lookupByLicenseNumber(licenseNumber: string): Promise<LicenseRow[]> {
  const result = await pool.query<LicenseRow>(
    `SELECT * FROM licenses WHERE license_number = $1`,
    [licenseNumber]
  );
  return result.rows;
}

export async function verifyLicenseStatus(
  licenseNumber?: string,
  firstName?: string,
  lastName?: string
): Promise<LicenseRow[]> {
  const conditions: string[] = [];
  const params: string[] = [];
  let idx = 1;

  if (licenseNumber) {
    conditions.push(`license_number = $${idx}`);
    params.push(licenseNumber);
    idx++;
  }
  if (firstName) {
    conditions.push(`UPPER(first_name) LIKE UPPER($${idx})`);
    params.push(`%${firstName}%`);
    idx++;
  }
  if (lastName) {
    conditions.push(`UPPER(last_name) LIKE UPPER($${idx})`);
    params.push(`%${lastName}%`);
    idx++;
  }

  const where = conditions.length > 0 ? `WHERE ${conditions.join(" AND ")}` : "";
  const result = await pool.query<LicenseRow>(
    `SELECT * FROM licenses ${where} ORDER BY last_name, first_name LIMIT 20`,
    params
  );
  return result.rows;
}

export async function listByLocation(
  city?: string,
  county?: string,
  state?: string,
  licenseStatus?: string,
  limit = 50
): Promise<LicenseRow[]> {
  const conditions: string[] = [];
  const params: string[] = [];
  let idx = 1;

  if (city) {
    conditions.push(`UPPER(city) = UPPER($${idx})`);
    params.push(city);
    idx++;
  }
  if (county) {
    conditions.push(`UPPER(county) = UPPER($${idx})`);
    params.push(county);
    idx++;
  }
  if (state) {
    conditions.push(`UPPER(state) = UPPER($${idx})`);
    params.push(state);
    idx++;
  }
  if (licenseStatus) {
    conditions.push(`UPPER(license_status) = UPPER($${idx})`);
    params.push(licenseStatus);
    idx++;
  }

  const where = conditions.length > 0 ? `WHERE ${conditions.join(" AND ")}` : "";
  const result = await pool.query<LicenseRow>(
    `SELECT * FROM licenses ${where} ORDER BY last_name, first_name LIMIT $${idx}`,
    [...params, limit]
  );
  return result.rows;
}

export async function getRecordCount(): Promise<number> {
  const result = await pool.query<{ count: string }>(`SELECT COUNT(*) as count FROM licenses`);
  return parseInt(result.rows[0].count, 10);
}
