import pg from "pg";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

let pool = null;
if (process.env.DATABASE_URL) {
  pool = new pg.Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: process.env.DATABASE_URL.includes("localhost") ? false : { rejectUnauthorized: false },
  });
}

// Fallback khi deploy container không có file db/*.sql (Railway/Docker)
const SCHEMA_SQL = `
CREATE TABLE IF NOT EXISTS regions (
  id SERIAL PRIMARY KEY,
  code VARCHAR(10) UNIQUE NOT NULL,
  name VARCHAR(100) NOT NULL
);
CREATE TABLE IF NOT EXISTS lottery_provinces (
  id SERIAL PRIMARY KEY,
  region_id INTEGER NOT NULL REFERENCES regions(id),
  code VARCHAR(20) NOT NULL,
  name VARCHAR(100) NOT NULL,
  api_game_code VARCHAR(20),
  UNIQUE(region_id, code)
);
CREATE TABLE IF NOT EXISTS lottery_draws (
  id SERIAL PRIMARY KEY,
  draw_date DATE NOT NULL,
  province_id INTEGER NOT NULL REFERENCES lottery_provinces(id),
  region_id INTEGER NOT NULL REFERENCES regions(id),
  created_at TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE(draw_date, province_id)
);
CREATE TABLE IF NOT EXISTS lottery_results (
  id SERIAL PRIMARY KEY,
  draw_id INTEGER NOT NULL REFERENCES lottery_draws(id) ON DELETE CASCADE,
  prize_code VARCHAR(10) NOT NULL,
  prize_order INTEGER NOT NULL DEFAULT 1,
  result_number VARCHAR(20) NOT NULL,
  UNIQUE(draw_id, prize_code, prize_order)
);
CREATE INDEX IF NOT EXISTS idx_lottery_draws_date ON lottery_draws(draw_date);
CREATE INDEX IF NOT EXISTS idx_lottery_draws_province ON lottery_draws(province_id);
CREATE INDEX IF NOT EXISTS idx_lottery_draws_region ON lottery_draws(region_id);
CREATE INDEX IF NOT EXISTS idx_lottery_results_draw ON lottery_results(draw_id);
INSERT INTO regions (id, code, name) VALUES (1, 'MB', 'Miền Bắc'), (2, 'MT', 'Miền Trung'), (3, 'MN', 'Miền Nam') ON CONFLICT (code) DO NOTHING;

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS auth_accept (
  id            bigserial PRIMARY KEY,
  client_id     text NOT NULL,
  api_key       text NOT NULL UNIQUE,
  ip_address    text,
  user_agent    text,
  scopes        text,
  created_at    timestamptz NOT NULL DEFAULT now(),
  last_used_at  timestamptz,
  is_active     boolean NOT NULL DEFAULT true
);

CREATE INDEX IF NOT EXISTS idx_auth_accept_active ON auth_accept(is_active);
CREATE INDEX IF NOT EXISTS idx_auth_accept_client ON auth_accept(client_id);

-- Bảng Xổ Số Trực Tiếp: lưu từng giải khi có kết quả (poll 5s trong giờ xổ)
CREATE TABLE IF NOT EXISTS kq_tructiep (
  id SERIAL PRIMARY KEY,
  draw_date DATE NOT NULL,
  region_code VARCHAR(10) NOT NULL,
  province_code VARCHAR(20) NOT NULL,
  province_name VARCHAR(100),
  prize_code VARCHAR(10) NOT NULL,
  prize_order INTEGER NOT NULL DEFAULT 1,
  result_number VARCHAR(20) NOT NULL,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE(draw_date, region_code, province_code, prize_code, prize_order)
);
CREATE INDEX IF NOT EXISTS idx_kq_tructiep_draw ON kq_tructiep(draw_date);
CREATE INDEX IF NOT EXISTS idx_kq_tructiep_region ON kq_tructiep(region_code);
CREATE INDEX IF NOT EXISTS idx_kq_tructiep_province ON kq_tructiep(province_code);
CREATE INDEX IF NOT EXISTS idx_kq_tructiep_created ON kq_tructiep(created_at DESC);

-- Đài đang xổ hôm nay (từ Minh Ngọc tinh) – chỉ hiển thị các đài này
CREATE TABLE IF NOT EXISTS live_active_provinces (
  draw_date DATE NOT NULL,
  region_code VARCHAR(10) NOT NULL,
  province_codes TEXT NOT NULL,
  updated_at TIMESTAMPTZ DEFAULT NOW(),
  PRIMARY KEY (draw_date, region_code)
);
`;

const PROVINCES_SEED_SQL = `
INSERT INTO lottery_provinces (region_id, code, name, api_game_code) VALUES
  (1, 'TB', 'Thái Bình', 'miba'), (1, 'HN', 'Hà Nội', 'miba'), (1, 'QN', 'Quảng Ninh', 'miba'), (1, 'BN', 'Bắc Ninh', 'miba'), (1, 'HP', 'Hải Phòng', 'miba'), (1, 'ND', 'Nam Định', 'miba')
ON CONFLICT (region_id, code) DO NOTHING;
INSERT INTO lottery_provinces (region_id, code, name, api_game_code) VALUES
  (2, 'DN', 'Đà Nẵng', 'dana'), (2, 'BDI', 'Bình Định', 'bidi'), (2, 'DLK', 'Đắk Lắk', 'dalak'), (2, 'DNO', 'Đắk Nông', 'dano'), (2, 'GLA', 'Gia Lai', 'gila'), (2, 'KHO', 'Khánh Hòa', 'khho'), (2, 'KTU', 'Kon Tum', 'kotu'), (2, 'NTH', 'Ninh Thuận', 'nith'), (2, 'PYE', 'Phú Yên', 'phye'), (2, 'QBI', 'Quảng Bình', 'qubi'), (2, 'QNM', 'Quảng Nam', 'quna'), (2, 'QNG', 'Quảng Ngãi', 'qung'), (2, 'QTR', 'Quảng Trị', 'qutr'), (2, 'THH', 'Thừa Thiên Huế', 'thth')
ON CONFLICT (region_id, code) DO NOTHING;
INSERT INTO lottery_provinces (region_id, code, name, api_game_code) VALUES
  (3, 'AGI', 'An Giang', 'angi'), (3, 'BLI', 'Bạc Liêu', 'bali'), (3, 'BTR', 'Bến Tre', 'btre'), (3, 'BDU', 'Bình Dương', 'bidu'), (3, 'BPH', 'Bình Phước', 'biph'), (3, 'BTH', 'Bình Thuận', 'bith'), (3, 'CMA', 'Cà Mau', 'cama'), (3, 'CTH', 'Cần Thơ', 'cath'), (3, 'DLT', 'Đà Lạt', 'dalat'), (3, 'DNA', 'Đồng Nai', 'dona'), (3, 'DTH', 'Đồng Tháp', 'doth'), (3, 'HGI', 'Hậu Giang', 'hagi'), (3, 'KGI', 'Kiên Giang', 'kigi'), (3, 'LAN', 'Long An', 'loan'), (3, 'STR', 'Sóc Trăng', 'sotr'), (3, 'TNI', 'Tây Ninh', 'tani'), (3, 'TGI', 'Tiền Giang', 'tigi'), (3, 'HCM', 'TP. Hồ Chí Minh', 'tphc'), (3, 'TVI', 'Trà Vinh', 'trvi'), (3, 'VLO', 'Vĩnh Long', 'vilo'), (3, 'VTA', 'Vũng Tàu', 'vuta')
ON CONFLICT (region_id, code) DO NOTHING;
`;

function readSqlFile(name, fallback) {
  const p = path.join(__dirname, name);
  try {
    if (fs.existsSync(p)) return fs.readFileSync(p, "utf8");
  } catch (_) {}
  return fallback;
}

export async function initDb() {
  if (!process.env.DATABASE_URL) {
    console.warn("⚠ DATABASE_URL not set – DB features disabled");
    return null;
  }
  try {
    const schema = readSqlFile("schema.sql", SCHEMA_SQL);
    await pool.query(schema);

    const provinces = readSqlFile("provinces-seed.sql", PROVINCES_SEED_SQL);
    await pool.query(provinces);

    console.log("✅ DB initialized");
    return pool;
  } catch (err) {
    console.error("❌ DB init error:", err.message);
    return null;
  }
}

/**
 * Lưu từng giải vào KQ_tructiep (Xổ Số Trực Tiếp).
 * @param {Array<{ draw_date, region_code, province_code, province_name?, prize_code, prize_order, result_number }>} items
 * @returns {Promise<{ saved: number, skipped: number }>}
 */
export async function importLiveResults(items) {
  if (!pool || !Array.isArray(items) || items.length === 0) {
    return { saved: 0, skipped: 0 };
  }
  let saved = 0;
  let skipped = 0;
  const client = await pool.connect();
  try {
    for (const it of items) {
      const { draw_date, region_code, province_code, province_name, prize_code, prize_order = 1, result_number } = it;
      if (!draw_date || !region_code || !province_code || !prize_code || result_number == null) {
        skipped++;
        continue;
      }
      try {
        await client.query(
          `INSERT INTO kq_tructiep (draw_date, region_code, province_code, province_name, prize_code, prize_order, result_number)
           VALUES ($1::date, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (draw_date, region_code, province_code, prize_code, prize_order) DO UPDATE SET
             result_number = EXCLUDED.result_number,
             province_name = COALESCE(EXCLUDED.province_name, kq_tructiep.province_name)`,
          [draw_date, region_code, province_code, province_name || null, prize_code, prize_order, String(result_number)]
        );
        saved++;
      } catch (e) {
        skipped++;
      }
    }
    return { saved, skipped };
  } finally {
    client.release();
  }
}

/**
 * Lấy kết quả trực tiếp theo ngày + miền (optional).
 */
export async function getLiveResults(drawDate, regionCode = null) {
  if (!pool) return [];
  let query = `SELECT draw_date, region_code, province_code, province_name, prize_code, prize_order, result_number, created_at
               FROM kq_tructiep WHERE draw_date = $1::date`;
  const params = [drawDate];
  if (regionCode) {
    query += " AND region_code = $2";
    params.push(regionCode);
  }
  query += " ORDER BY region_code, province_code, prize_code, prize_order";
  const { rows } = await pool.query(query, params);
  return rows;
}

/**
 * Lưu danh sách đài đang xổ (từ Minh Ngọc tinh).
 */
export async function saveActiveProvinces(drawDate, regionCode, provinceCodes) {
  if (!pool || !drawDate || !regionCode || !Array.isArray(provinceCodes)) return;
  try {
    await pool.query(
      `INSERT INTO live_active_provinces (draw_date, region_code, province_codes)
       VALUES ($1::date, $2, $3)
       ON CONFLICT (draw_date, region_code) DO UPDATE SET
         province_codes = EXCLUDED.province_codes,
         updated_at = NOW()`,
      [drawDate, regionCode, JSON.stringify(provinceCodes)]
    );
  } catch (e) {
    console.warn("[saveActiveProvinces]", e.message);
  }
}

/**
 * Lấy danh sách đài đang xổ theo ngày + miền.
 */
export async function getActiveProvinces(drawDate, regionCode) {
  if (!pool || !drawDate || !regionCode) return [];
  try {
    const { rows } = await pool.query(
      `SELECT province_codes FROM live_active_provinces
       WHERE draw_date = $1::date AND region_code = $2`,
      [drawDate, regionCode]
    );
    if (!rows.length) return [];
    const parsed = JSON.parse(rows[0].province_codes || "[]");
    return Array.isArray(parsed) ? parsed : [];
  } catch {
    return [];
  }
}

export async function importLotteryResults(payload) {
  const client = await pool.connect();
  try {
    const { draws } = payload;
    let imported = 0;
    let skipped = 0;

    const isPlaceholderResult = (v) => {
      if (v == null) return true;
      const s = String(v).trim();
      if (s === "") return true;
      return /^[*]+$/.test(s) || /^[+]+$/.test(s);
    };

    for (const d of draws) {
      const { draw_date, province_code, region_code, results } = d;
      if (!draw_date || !province_code || !region_code || !results?.length) continue;

      const regionRes = await client.query(
        "SELECT id FROM regions WHERE code = $1",
        [region_code]
      );
      const regionId = regionRes.rows[0]?.id;
      if (!regionId) {
        skipped++;
        continue;
      }

      const provRes = await client.query(
        "SELECT id FROM lottery_provinces WHERE code = $1 AND region_id = $2",
        [province_code, regionId]
      );
      const provinceId = provRes.rows[0]?.id;
      if (!provinceId) {
        skipped++;
        continue;
      }

      const { rows: insertDraw } = await client.query(
        `INSERT INTO lottery_draws (draw_date, province_id, region_id)
         VALUES ($1::date, $2, $3)
         ON CONFLICT (draw_date, province_id) DO UPDATE SET draw_date = EXCLUDED.draw_date
         RETURNING id`,
        [draw_date, provinceId, regionId]
      );
      const drawId = insertDraw[0]?.id;
      if (!drawId) continue;

      for (const r of results) {
        const { prize_code, prize_order = 1, result_number } = r;
        if (!prize_code || result_number == null) continue;
        // Không lưu placeholder vào lottery_results (tránh kẹt loading / dữ liệu rác)
        if (isPlaceholderResult(result_number)) continue;
        await client.query(
          `INSERT INTO lottery_results (draw_id, prize_code, prize_order, result_number)
           VALUES ($1, $2, $3, $4)
           ON CONFLICT (draw_id, prize_code, prize_order) DO UPDATE SET result_number = EXCLUDED.result_number`,
          [drawId, prize_code, prize_order, String(result_number)]
        );
      }
      imported++;
    }

    return { imported, skipped };
  } finally {
    client.release();
  }
}

export async function getDrawsByDate(drawDate, regionCode = null) {
  let query = `
    SELECT d.id, d.draw_date, p.code as province_code, p.name as province_name,
           r.code as region_code, d.created_at
    FROM lottery_draws d
    JOIN lottery_provinces p ON d.province_id = p.id
    JOIN regions r ON d.region_id = r.id
    WHERE d.draw_date = $1::date
  `;
  const params = [drawDate];
  if (regionCode) {
    query += " AND r.code = $2";
    params.push(regionCode);
  }
  query += " ORDER BY r.id, p.name";

  const { rows } = await pool.query(query, params);
  return rows;
}

export async function getResultsByDrawId(drawId) {
  const { rows } = await pool.query(
    `SELECT prize_code, prize_order, result_number
     FROM lottery_results WHERE draw_id = $1
     ORDER BY prize_code, prize_order`,
    [drawId]
  );
  return rows;
}

export async function getDrawWithResults(drawDate, provinceCode, regionCode) {
  const { rows } = await pool.query(
    `SELECT d.id FROM lottery_draws d
     JOIN lottery_provinces p ON d.province_id = p.id
     JOIN regions r ON d.region_id = r.id
     WHERE d.draw_date = $1::date AND p.code = $2 AND r.code = $3`,
    [drawDate, provinceCode, regionCode]
  );
  if (!rows.length) return null;
  const results = await getResultsByDrawId(rows[0].id);
  return { draw_id: rows[0].id, results };
}

/** Giờ mở thưởng theo miền (HH:MM) */
const REGION_OPEN_TIME = { MB: "18:15:00", MT: "17:15:00", MN: "16:15:00" };
/** sort theo miền (giống frontend) */
const REGION_SORT = { MB: 10, MT: 20, MN: 30 };

/**
 * Lấy dữ liệu cho API /api/front/open/lottery/history/list/game
 * @returns {Promise<{ name, code, sort, navCate, openTimeByRegion, draws: [{ draw_date, draw_id, results }] } | null>}
 */
export async function getLotteryHistoryListGame(gameCode, limitNum) {
  if (!pool) return null;
  const limit = Math.min(parseInt(limitNum, 10) || 200, 500);
  const { rows: metaRows } = await pool.query(
    `SELECT p.name, p.api_game_code, p.id as province_id, r.code as region_code
     FROM lottery_provinces p
     JOIN regions r ON p.region_id = r.id
     WHERE p.api_game_code = $1
     LIMIT 1`,
    [gameCode]
  );
  if (!metaRows.length) return null;

  const meta = metaRows[0];
  const { rows: drawRows } = await pool.query(
    `SELECT d.id as draw_id, d.draw_date
     FROM lottery_draws d
     JOIN lottery_provinces p ON d.province_id = p.id
     WHERE p.api_game_code = $1
     ORDER BY d.draw_date DESC
     LIMIT $2`,
    [gameCode, limit]
  );
  if (!drawRows.length) {
    return {
      name: meta.name,
      code: meta.api_game_code,
      sort: REGION_SORT[meta.region_code] || 0,
      navCate: meta.region_code.toLowerCase(),
      openTimeByRegion: REGION_OPEN_TIME[meta.region_code] || "17:15:00",
      draws: [],
    };
  }

  const drawIds = drawRows.map((r) => r.draw_id);
  const { rows: resultRows } = await pool.query(
    `SELECT draw_id, prize_code, prize_order, result_number
     FROM lottery_results
     WHERE draw_id = ANY($1::int[])
     ORDER BY draw_id, prize_code, prize_order`,
    [drawIds]
  );
  const byDrawId = {};
  for (const r of resultRows) {
    if (!byDrawId[r.draw_id]) byDrawId[r.draw_id] = [];
    byDrawId[r.draw_id].push(r);
  }

  const draws = drawRows.map((row) => ({
    draw_date: row.draw_date,
    draw_id: row.draw_id,
    results: byDrawId[row.draw_id] || [],
  }));

  return {
    name: meta.name,
    code: meta.api_game_code,
    sort: REGION_SORT[meta.region_code] || 0,
    navCate: meta.region_code.toLowerCase(),
    openTimeByRegion: REGION_OPEN_TIME[meta.region_code] || "17:15:00",
    draws,
  };
}

export { pool };

export { scheduleLotterySync } from "./lotterySync.js";
