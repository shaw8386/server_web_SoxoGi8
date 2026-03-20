/**
 * LOTTERY SYNC – Automation của server (không liên quan web gi8 / client).
 * Mục đích: Tự động lấy kết quả xổ số → lưu DB cho user/client.
 *
 * MINH_NGOC_BASE có 2 case:
 *
 * Case 1 – Kết quả cuối ngày (lottery_draws):
 *   Từ lúc START giờ xổ mỗi miền → call mỗi 5 PHÚT đến khi HẾT giờ xổ.
 *   Khi lấy đầy đủ kết quả → ngưng → lưu lottery_draws.
 *   Nếu lỗi/không lấy được khi kết thúc giờ xổ → fallback xoso188.
 *
 * Case 2 – Xổ Số Trực Tiếp (kq_tructiep):
 *   Trong giờ xổ mỗi miền → call mỗi 5s lấy Minh Ngọc.
 *   Cứ có giải nào → lưu ngay vào kq_tructiep (từng giải, ngày, đài, miền...).
 *
 * 3) 20h cuối ngày: kiểm tra lottery_draws, backfill từ xoso188 nếu thiếu.
 */

import fetch from "node-fetch";
import cron from "node-cron";
import { fetchMinhNgocKqxsData } from "../Get_DataXS_tructiep.js";
import { kqxsDataToDraws, getActiveProvinceCodesFromKqxsData } from "../utils/minhNgocToXoso188.js";

// ---------- API server tự cron gọi (không dùng cho web gi8 / client) ----------
const MINH_NGOC_BASE = "https://dc.minhngoc.net/O0O/0/xstt";
const XOSO188_API =
  "https://xoso188.net/api/front/open/lottery/history/list/game";
// Cloudflare Worker proxy: gọi Worker thay vì xoso188 trực tiếp (tránh bị chặn trên Railway). Có thể override bằng env XOSO188_WORKER_URL.
const XOSO188_WORKER_URL =
  process.env.XOSO188_WORKER_URL || "https://xoso188-proxy.xoso188-proxy.workers.dev";
const getXoso188BaseUrl = () => (XOSO188_WORKER_URL || XOSO188_API).replace(/\/$/, "");

// ---------- Lịch từng miền: Case 1 poll 5 phút từ START giờ xổ → HẾT giờ xổ ----------
const REGION_SCHEDULE = {
  mn: {
    label: "Miền Nam",
    cronAt: "15 16 * * *",       // 16:15 – đúng giờ xổ bắt đầu
    drawStart: "16:15",
    drawEnd: "16:40",
    pollIntervalMs: 5 * 60 * 1000,   // 5 phút
    maxPollDurationMs: 30 * 60 * 1000, // ~30 phút (đến 16:45)
  },
  mt: {
    label: "Miền Trung",
    cronAt: "15 17 * * *",
    drawStart: "17:15",
    drawEnd: "17:40",
    pollIntervalMs: 5 * 60 * 1000,
    maxPollDurationMs: 30 * 60 * 1000,
  },
  mb: {
    label: "Miền Bắc",
    cronAt: "15 18 * * *",
    drawStart: "18:15",
    drawEnd: "18:40",
    pollIntervalMs: 5 * 60 * 1000,
    maxPollDurationMs: 30 * 60 * 1000,
  },
};

// Case 2 – Xổ Số Trực Tiếp: poll 1s trong giờ xổ (khớp frontend gi8 poll 1s)
const LIVE_POLL_INTERVAL_MS = 1 * 1000;

// Header khớp tools/fetch_lottery_and_upload.py BROWSER_HEADERS (từ DevTools xoso188 - Edge)
const XOSO188_HEADERS = {
  "User-Agent":
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36 Edg/144.0.0.0",
  Accept:
    "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
  "Accept-Language": "en-US,en;q=0.9",
  "Accept-Encoding": "gzip, deflate, br, zstd",
  "Cache-Control": "max-age=0",
  "Sec-Ch-Ua":
    '"Not(A:Brand";v="8", "Chromium";v="144", "Microsoft Edge";v="144"',
  "Sec-Ch-Ua-Mobile": "?0",
  "Sec-Ch-Ua-Platform": '"Windows"',
  "Sec-Fetch-Dest": "document",
  "Sec-Fetch-Mode": "navigate",
  "Sec-Fetch-Site": "none",
  "Sec-Fetch-User": "?1",
  "Upgrade-Insecure-Requests": "1",
  Priority: "u=0, i",
};

const PRIZE_CODES = ["DB", "G1", "G2", "G3", "G4", "G5", "G6", "G7", "G8"];

// Expected counts per draw (per province) by region.
// MB không có G8; MN/MT có G8.
const EXPECTED_COUNTS_BY_REGION = {
  MB: { DB: 1, G1: 1, G2: 2, G3: 6, G4: 4, G5: 6, G6: 3, G7: 4 },
  MT: { DB: 1, G1: 1, G2: 2, G3: 2, G4: 7, G5: 1, G6: 3, G7: 1, G8: 1 },
  MN: { DB: 1, G1: 1, G2: 2, G3: 2, G4: 7, G5: 1, G6: 3, G7: 1, G8: 1 },
};

function isPlaceholderNumber(val) {
  if (val == null) return true;
  const s = String(val).trim();
  if (s === "") return true;
  return /^[*]+$/.test(s) || /^[+]+$/.test(s);
}

function isDrawCompleteObject(draw) {
  const expected = EXPECTED_COUNTS_BY_REGION[draw?.region_code];
  if (!expected) return false;
  const counts = {};
  for (const r of draw?.results || []) {
    if (!r?.prize_code) continue;
    if (isPlaceholderNumber(r.result_number)) continue;
    counts[r.prize_code] = (counts[r.prize_code] || 0) + 1;
  }
  // must have all expected prize codes with exact counts
  for (const [code, cnt] of Object.entries(expected)) {
    if ((counts[code] || 0) !== cnt) return false;
  }
  return true;
}

async function isDbDrawComplete(pool, drawDate, regionCode, provinceCode) {
  const expected = EXPECTED_COUNTS_BY_REGION[regionCode];
  if (!expected) return false;
  const { rows } = await pool.query(
    `SELECT d.id as draw_id
     FROM lottery_draws d
     JOIN lottery_provinces p ON d.province_id = p.id
     JOIN regions r ON d.region_id = r.id
     WHERE d.draw_date = $1::date AND r.code = $2 AND p.code = $3
     LIMIT 1`,
    [drawDate, regionCode, provinceCode]
  );
  if (!rows.length) return false;
  const drawId = rows[0].draw_id;
  const { rows: resRows } = await pool.query(
    `SELECT prize_code, result_number
     FROM lottery_results
     WHERE draw_id = $1`,
    [drawId]
  );
  const counts = {};
  for (const rr of resRows) {
    if (isPlaceholderNumber(rr.result_number)) continue;
    counts[rr.prize_code] = (counts[rr.prize_code] || 0) + 1;
  }
  for (const [code, cnt] of Object.entries(expected)) {
    if ((counts[code] || 0) !== cnt) return false;
  }
  return true;
}

// gameCode -> [region_code, province_code | null (MB tính theo ngày)]
const GAME_TO_REGION_PROVINCE = {
  miba: ["MB", null],
  dana: ["MT", "DN"],
  bidi: ["MT", "BDI"],
  dalak: ["MT", "DLK"],
  dano: ["MT", "DNO"],
  gila: ["MT", "GLA"],
  khho: ["MT", "KHO"],
  kotu: ["MT", "KTU"],
  nith: ["MT", "NTH"],
  phye: ["MT", "PYE"],
  qubi: ["MT", "QBI"],
  quna: ["MT", "QNM"],
  qung: ["MT", "QNG"],
  qutr: ["MT", "QTR"],
  thth: ["MT", "THH"],
  angi: ["MN", "AGI"],
  bali: ["MN", "BLI"],
  bidu: ["MN", "BDU"],
  biph: ["MN", "BPH"],
  bith: ["MN", "BTH"],
  cama: ["MN", "CMA"],
  cath: ["MN", "CTH"],
  dalat: ["MN", "DLT"],
  dona: ["MN", "DNA"],
  doth: ["MN", "DTH"],
  hagi: ["MN", "HGI"],
  kigi: ["MN", "KGI"],
  loan: ["MN", "LAN"],
  sotr: ["MN", "STR"],
  tani: ["MN", "TNI"],
  tigi: ["MN", "TGI"],
  tphc: ["MN", "HCM"],
  trvi: ["MN", "TVI"],
  vilo: ["MN", "VLO"],
  vuta: ["MN", "VTA"],
};

// Map station từ Minh Ngọc -> [region_code, province_code] trong DB
const MINH_NGOC_STATION_TO_REGION_PROVINCE = {
  // Miền Trung
  dana: ["MT", "DN"],
  bidi: ["MT", "BDI"],
  dalak: ["MT", "DLK"],
  dano: ["MT", "DNO"],
  gila: ["MT", "GLA"],
  khho: ["MT", "KHO"],
  kotu: ["MT", "KTU"],
  nith: ["MT", "NTH"],
  phye: ["MT", "PYE"],
  qubi: ["MT", "QBI"],
  quna: ["MT", "QNM"],
  qung: ["MT", "QNG"],
  qutr: ["MT", "QTR"],
  thth: ["MT", "THH"],

  // Miền Nam
  angi: ["MN", "AGI"],
  bali: ["MN", "BLI"],
  bidu: ["MN", "BDU"],
  biph: ["MN", "BPH"],
  bith: ["MN", "BTH"],
  cama: ["MN", "CMA"],
  cath: ["MN", "CTH"],
  dalat: ["MN", "DLT"],
  dona: ["MN", "DNA"],
  doth: ["MN", "DTH"],
  hagi: ["MN", "HGI"],
  kigi: ["MN", "KGI"],
  loan: ["MN", "LAN"],
  sotr: ["MN", "STR"],
  tani: ["MN", "TNI"],
  tigi: ["MN", "TGI"],
  tphc: ["MN", "HCM"],
  trvi: ["MN", "TVI"],
  vilo: ["MN", "VLO"],
  vuta: ["MN", "VTA"],

  // Miền Bắc (nếu Minh Ngọc dùng giống xoso188)
  miba: ["MB", null], // province_code sẽ tính theo ngày như logic hiện tại
};

const REGION_GAME_CODES = {
  mn: ["angi", "bali", "bidu", "biph", "bith", "cama", "cath", "dalat", "dona", "doth", "hagi", "kigi", "loan", "sotr", "tani", "tigi", "tphc", "trvi", "vilo", "vuta"],
  mt: ["dana", "bidi", "dalak", "dano", "gila", "khho", "kotu", "nith", "phye", "qubi", "quna", "qung", "qutr", "thth"],
  mb: ["miba"],
};

const MB_NAME_TO_CODE = {
  "Thái Bình": "TB",
  "Hà Nội": "HN",
  "Quảng Ninh": "QN",
  "Bắc Ninh": "BN",
  "Hải Phòng": "HP",
  "Nam Định": "ND",
};

function getStationNameMB(dateStr) {
  // dateStr: DD/MM/YYYY; JS getDay() 0=Chủ nhật
  try {
    const parts = String(dateStr).replace(/-/g, "/").split("/").map(Number);
    const [d, m, y] = parts;
    const dt = new Date(y, (m || 1) - 1, d || 1);
    const map = {
      0: "Thái Bình",
      1: "Hà Nội",
      2: "Quảng Ninh",
      3: "Bắc Ninh",
      4: "Hà Nội",
      5: "Hải Phòng",
      6: "Nam Định",
    };
    return map[dt.getDay()] ?? "Hà Nội";
  } catch {
    return "Hà Nội";
  }
}

function getTodayDrawDate() {
  const now = new Date();
  const y = now.getFullYear();
  const m = String(now.getMonth() + 1).padStart(2, "0");
  const d = String(now.getDate()).padStart(2, "0");
  return `${y}-${m}-${d}`;
}

function parseVNDateToYMD(dateStr) {
  const s = String(dateStr || "").trim();
  const parts = s.split(/[\/\-]/).map(Number);
  if (parts.length < 3) return null;
  let day, month, year;
  if (parts[0] > 31) {
    // format YYYY-MM-DD
    year = parts[0];
    month = (parts[1] || 1) - 1;
    day = parts[2] || 1;
  } else {
    // format DD/MM/YYYY
    day = parts[0] || 1;
    month = (parts[1] || 1) - 1;
    year = parts[2];
  }
  const d = new Date(year, month, day);
  if (Number.isNaN(d.getTime())) return null;
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, "0");
  const dd = String(d.getDate()).padStart(2, "0");
  return `${y}-${m}-${dd}`;
}

/** Trả về ngày (YYYY-MM-DD) lùi `daysBack` ngày so với hôm nay (theo giờ VN nếu TZ đã set). */
function getDrawDateOffset(daysBack) {
  const now = new Date();
  const d = new Date(now.getFullYear(), now.getMonth(), now.getDate() - daysBack);
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, "0");
  const day = String(d.getDate()).padStart(2, "0");
  return `${y}-${m}-${day}`;
}

function parseDetail(detailStr) {
  const results = [];
  let groups = [];
  try {
    groups = JSON.parse(detailStr || "[]");
  } catch {
    return results;
  }
  if (!Array.isArray(groups)) return results;
  for (let i = 0; i < PRIZE_CODES.length; i++) {
    if (i >= groups.length) break;
    const val = groups[i];
    if (val == null || val === "") continue;
    // Bỏ placeholder dạng "****", "++++", ... để không lưu rác vào DB
    const trimmed = String(val).trim();
    if (/^[*]+$/.test(trimmed) || /^[+]+$/.test(trimmed)) continue;
    const parts = String(val).split(",");
    parts.forEach((num, idx) => {
      const n = num.trim();
      if (n && !/^[*]+$/.test(n) && !/^[+]+$/.test(n))
        results.push({
          prize_code: PRIZE_CODES[i],
          prize_order: idx + 1,
          result_number: n,
        });
    });
  }
  return results;
}

function issuesToDraws(gameCode, issues, filterDrawDate) {
  const meta = GAME_TO_REGION_PROVINCE[gameCode];
  if (!meta) return [];
  const [regionCode, fixedProvince] = meta;
  const draws = [];
  for (const issue of issues) {
    const turnNum = issue.turnNum || "";
    if (!turnNum) continue;
    let d, m, y;
    try {
      const parts = String(turnNum).replace(/-/g, "/").split("/").map(Number);
      if (parts.length >= 3) {
        [d, m, y] = parts;
      } else continue;
    } catch {
      continue;
    }
    const drawDate = `${y}-${String(m).padStart(2, "0")}-${String(d).padStart(2, "0")}`;
    if (filterDrawDate && drawDate !== filterDrawDate) continue;

    const provinceCode = fixedProvince
      ? fixedProvince
      : MB_NAME_TO_CODE[getStationNameMB(turnNum)] || "HN";
    const results = parseDetail(issue.detail || "");
    if (!results.length) continue;
    draws.push({
      draw_date: drawDate,
      province_code: provinceCode,
      region_code: regionCode,
      results,
    });
  }
  return draws;
}

// ---------- API chính: Minh Ngọc – dùng logic từ Get_DataXS_tructiep.js (fetch + parse), convert → draws ----------
// Server cron gọi đúng giờ; nếu không có kết quả thì fallback xoso188.
async function fetchMinhNgoc(region) {
  try {
    const kqxs_data = await fetchMinhNgocKqxsData(region);
    if (!kqxs_data || typeof kqxs_data.kq !== "object") return { draws: [], kqxs_data: null };
    const draws = kqxsDataToDraws(kqxs_data, region) || [];
    return { draws, kqxs_data };
  } catch (err) {
    console.warn("[Minh Ngọc]", region, err.message);
    return { draws: [], kqxs_data: null };
  }
}

function fetchMinhNgocDrawsOnly(region) {
  return fetchMinhNgoc(region).then((r) => r?.draws || []);
}

// ---------- API phụ: xoso188 – gọi qua Cloudflare Worker (chỉ dùng khi Minh Ngọc không lấy được) ----------
const MAX_XOSO188_RETRIES = 5;

async function fetchXoso188Game(gameCode, limitNum = 10, retryCount = 0) {
  const baseUrl = getXoso188BaseUrl();
  const url = `${baseUrl}?limitNum=${limitNum}&gameCode=${gameCode}`;
  const options = {
    headers: { Accept: "application/json" },
    timeout: 20000,
  };
  try {
    const res = await fetch(url, options);
    const raw = await res.text();
    if (!res.ok) {
      console.warn("[xoso188]", gameCode, "status", res.status, "retry", retryCount + 1 + "/" + MAX_XOSO188_RETRIES);
      if (retryCount < MAX_XOSO188_RETRIES - 1) {
        return fetchXoso188Game(gameCode, limitNum, retryCount + 1);
      }
      return [];
    }
    let data;
    try {
      data = JSON.parse(raw);
    } catch (_) {
      console.warn("[xoso188]", gameCode, "response không phải JSON, retry", retryCount + 1 + "/" + MAX_XOSO188_RETRIES);
      if (retryCount < MAX_XOSO188_RETRIES - 1) {
        return fetchXoso188Game(gameCode, limitNum, retryCount + 1);
      }
      return [];
    }
    const list = data?.t?.issueList ?? [];
    const arr = Array.isArray(list) ? list : [];
    if (arr.length === 0 && (data?.code !== 0 || !data?.success)) {
      console.warn("[xoso188]", gameCode, "issueList rỗng, retry", retryCount + 1 + "/" + MAX_XOSO188_RETRIES);
      if (retryCount < MAX_XOSO188_RETRIES - 1) {
        return fetchXoso188Game(gameCode, limitNum, retryCount + 1);
      }
    }
    return arr;
  } catch (err) {
    console.warn("[xoso188]", gameCode, err.message, "retry", retryCount + 1 + "/" + MAX_XOSO188_RETRIES);
    if (retryCount < MAX_XOSO188_RETRIES - 1) {
      return fetchXoso188Game(gameCode, limitNum, retryCount + 1);
    }
    return [];
  }
}

/** Test từ server có gọi được xoso188 không. Trả về { ok, status, message, count }. */
export async function pingXoso188() {
  try {
    const issues = await fetchXoso188Game("miba", 2);
    return {
      ok: true,
      status: 200,
      message: "OK",
      count: issues.length,
      source: "xoso188",
    };
  } catch (err) {
    return {
      ok: false,
      status: 0,
      message: err?.message || String(err),
      count: 0,
      source: "xoso188",
    };
  }
}

export { XOSO188_HEADERS };

async function fetchXoso188ForRegion(region, filterDrawDate) {
  const gameCodes = REGION_GAME_CODES[region];
  if (!gameCodes) return [];
  const allDraws = [];
  for (const gameCode of gameCodes) {
    const issues = await fetchXoso188Game(gameCode, 15);
    const draws = issuesToDraws(gameCode, issues, filterDrawDate);
    allDraws.push(...draws);
    await new Promise((r) => setTimeout(r, 300)); // tránh gọi dồn
  }
  return allDraws;
}

let pollIntervals = { mn: null, mt: null, mb: null };
let livePollIntervals = { mn: null, mt: null, mb: null };

/** Kiểm tra có đang trong khung giờ xổ của region không */
function isInDrawWindow(region) {
  const schedule = REGION_SCHEDULE[region];
  if (!schedule) return false;
  const now = new Date();
  const h = now.getHours();
  const m = now.getMinutes();
  const nowStr = `${String(h).padStart(2, "0")}:${String(m).padStart(2, "0")}`;
  return nowStr >= schedule.drawStart && nowStr <= schedule.drawEnd;
}

/** Chuyển draws sang format items cho kq_tructiep */
function drawsToLiveItems(draws, provinceNames = {}) {
  const items = [];
  for (const d of draws || []) {
    for (const r of d.results || []) {
      items.push({
        draw_date: d.draw_date,
        region_code: d.region_code,
        province_code: d.province_code,
        province_name: provinceNames[d.province_code],
        prize_code: r.prize_code,
        prize_order: r.prize_order || 1,
        result_number: r.result_number,
      });
    }
  }
  return items;
}

/**
 * Case 2 – Xổ Số Trực Tiếp: poll 1s trong giờ xổ, lưu từng giải vào kq_tructiep.
 * Chỉ log khi có kết quả mới (saved > 0).
 */
async function pollLiveUntilEnd(region, pool, importLiveResults, saveActiveProvinces) {
  if (livePollIntervals[region]) return;
  const schedule = REGION_SCHEDULE[region];
  if (!schedule || !importLiveResults) return;

  const today = getTodayDrawDate();
  const { label } = schedule;

  const tick = async () => {
    if (!isInDrawWindow(region)) {
      clearInterval(livePollIntervals[region]);
      livePollIntervals[region] = null;
      console.log(`[XSTT] ${label}: hết giờ xổ, ngưng poll trực tiếp`);
      return;
    }

    const { draws, kqxs_data } = await fetchMinhNgoc(region);
    if (kqxs_data && saveActiveProvinces) {
      const active = getActiveProvinceCodesFromKqxsData(kqxs_data, region);
      if (active.length > 0) {
        await saveActiveProvinces(today, region.toUpperCase(), active);
      }
    }
    if (!draws || draws.length === 0) return;

    const items = drawsToLiveItems(draws);
    if (items.length > 0) {
      const { saved } = await importLiveResults(items);
      if (saved > 0) {
        console.log(`[XSTT] ${label}: đã lưu ${saved} giải trực tiếp`);
      }
    }
  };

  console.log(`[XSTT] ${label}: bắt đầu poll trực tiếp mỗi ${LIVE_POLL_INTERVAL_MS / 1000}s (${today})`);
  await tick();
  livePollIntervals[region] = setInterval(tick, LIVE_POLL_INTERVAL_MS);
}

/**
 * 20h cuối ngày (và khi startup): kiểm tra 5 ngày theo giờ VN.
 * - Trước 16:00 VN (chưa xổ ngày hôm nay) → bỏ qua hôm nay, check 5 ngày trước: D-1 .. D-5.
 * - Từ 16:00 trở đi → check hôm nay + 4 ngày trước: D .. D-4.
 * Với mỗi ngày chưa có trong lottery_draws → gọi xoso188 lấy MN, MT, MB và lưu DB.
 * @param {object} pool - pg.Pool
 * @param {Function} importLotteryResults - (payload) => Promise<{ imported, skipped }>
 */
export async function checkAndBackfillToday(pool, importLotteryResults) {
  if (!pool || !importLotteryResults) return;
  const now = new Date();
  const hourVN = now.getHours();
  const today = getTodayDrawDate();
  const includeToday = hourVN >= 16;
  const datesToCheck = includeToday
    ? [0, 1, 2, 3, 4].map((d) => getDrawDateOffset(d))
    : [1, 2, 3, 4, 5].map((d) => getDrawDateOffset(d));
  console.log(
    "[LotterySync] 20h check: giờ VN",
    hourVN + ":xx, includeToday=" + includeToday,
    "→ kiểm tra",
    datesToCheck.join(", ")
  );
  const REQUIRED_REGIONS = ["MN", "MT", "MB"];
  try {
    for (const drawDate of datesToCheck) {
      const { rows: regionRows } = await pool.query(
        `SELECT r.code FROM lottery_draws d
         JOIN regions r ON d.region_id = r.id
         WHERE d.draw_date = $1::date`,
        [drawDate]
      );
      const presentCodes = regionRows.map((r) => r.code);
      const hasAllRegions = REQUIRED_REGIONS.every((c) => presentCodes.includes(c));
      // Even when all regions present, still backfill if there are placeholders or missing prizes.
      let hasBadResults = false;
      try {
        const { rows: badRows } = await pool.query(
          `SELECT COUNT(*)::int AS cnt
           FROM lottery_results lr
           JOIN lottery_draws d ON lr.draw_id = d.id
           WHERE d.draw_date = $1::date AND (lr.result_number = '' OR lr.result_number ~ '^[*+]+$')`,
          [drawDate]
        );
        hasBadResults = (badRows[0]?.cnt || 0) > 0;
      } catch (_) {
        hasBadResults = false;
      }
      if (hasAllRegions && !hasBadResults) {
        console.log("[LotterySync] 20h check:", drawDate, "đã đủ MN, MT, MB (không placeholder) → bỏ qua");
        continue;
      }
      const missing = REQUIRED_REGIONS.filter((c) => !presentCodes.includes(c));
      if (missing.length) {
        console.log("[LotterySync] 20h check:", drawDate, "thiếu miền:", missing.join(", "));
      } else if (hasBadResults) {
        console.log("[LotterySync] 20h check:", drawDate, "phát hiện placeholder/thiếu giải → backfill");
      }
      console.log("[LotterySync] 20h backfill: bắt đầu gọi xoso188 cho MN, MT, MB (draw_date=" + drawDate + ")");
      const allDraws = [];
      for (const region of ["mn", "mt", "mb"]) {
        const draws = await fetchXoso188ForRegion(region, drawDate);
        allDraws.push(...draws);
        console.log("[LotterySync] 20h backfill:", drawDate, region.toUpperCase(), "lấy được", draws.length, "draws");
        await new Promise((r) => setTimeout(r, 300));
      }
      if (allDraws.length === 0) {
        console.warn("[LotterySync] 20h backfill: xoso188 không trả về kết quả cho", drawDate);
        continue;
      }
      console.log("[LotterySync] 20h backfill: tổng", allDraws.length, "draws cho", drawDate, ", đang import...");
      const result = await importLotteryResults({ draws: allDraws });
      console.log("[LotterySync] 20h backfill đã lưu", drawDate, ":", result);
    }
  } catch (err) {
    console.error("[LotterySync] 20h backfill lỗi:", err.message);
  }
}

/**
 * Backfill dữ liệu cho một ngày cụ thể (YYYY-MM-DD) từ xoso188 cho MN/MT/MB.
 * Dùng cho các ngày đã xổ nhưng còn thiếu giải hoặc còn placeholder.
 * @param {object} pool - pg.Pool
 * @param {Function} importLotteryResults - (payload) => Promise<{ imported, skipped }>
 * @param {string} drawDate - YYYY-MM-DD
 */
export async function backfillSpecificDate(pool, importLotteryResults, drawDate) {
  if (!pool || !importLotteryResults || !drawDate) return;
  const REQUIRED_REGIONS = ["MN", "MT", "MB"];
  try {
    const { rows: regionRows } = await pool.query(
      `SELECT r.code FROM lottery_draws d
       JOIN regions r ON d.region_id = r.id
       WHERE d.draw_date = $1::date`,
      [drawDate]
    );
    const presentCodes = regionRows.map((r) => r.code);
    const hasAllRegions = REQUIRED_REGIONS.every((c) => presentCodes.includes(c));
    let hasBadResults = false;
    try {
      const { rows: badRows } = await pool.query(
        `SELECT COUNT(*)::int AS cnt
         FROM lottery_results lr
         JOIN lottery_draws d ON lr.draw_id = d.id
         WHERE d.draw_date = $1::date AND (lr.result_number = '' OR lr.result_number ~ '^[*+]+$')`,
        [drawDate]
      );
      hasBadResults = (badRows[0]?.cnt || 0) > 0;
    } catch (_) {
      hasBadResults = false;
    }

    if (!hasBadResults && hasAllRegions) {
      console.log("[LotterySync] backfillSpecificDate:", drawDate, "đã đủ MN, MT, MB (không placeholder) → bỏ qua");
      return;
    }

    console.log("[LotterySync] backfillSpecificDate: bắt đầu gọi xoso188 cho MN, MT, MB (draw_date=" + drawDate + ")");
    const allDraws = [];
    for (const region of ["mn", "mt", "mb"]) {
      const draws = await fetchXoso188ForRegion(region, drawDate);
      allDraws.push(...draws);
      console.log("[LotterySync] backfillSpecificDate:", drawDate, region.toUpperCase(), "lấy được", draws.length, "draws");
      await new Promise((r) => setTimeout(r, 300));
    }
    if (allDraws.length === 0) {
      console.warn("[LotterySync] backfillSpecificDate: xoso188 không trả về kết quả cho", drawDate);
      return;
    }
    console.log("[LotterySync] backfillSpecificDate: tổng", allDraws.length, "draws cho", drawDate, ", đang import...");
    const result = await importLotteryResults({ draws: allDraws });
    console.log("[LotterySync] backfillSpecificDate đã lưu", drawDate, ":", result);
  } catch (err) {
    console.error("[LotterySync] backfillSpecificDate lỗi:", err.message);
  }
}

/**
 * Backfill N ngày gần nhất từ xoso188, cập nhật toàn bộ DB.
 * Không skip – luôn fetch và import (ghi đè dữ liệu cũ).
 * @param {object} pool - pg.Pool
 * @param {Function} importLotteryResults - (payload) => Promise<{ imported, skipped }>
 * @param {number} n - số ngày (mặc định 10)
 * @returns {Promise<{ dates: string[], totalImported: number, totalSkipped: number, byDate: Object }>}
 */
export async function backfillLastNDays(pool, importLotteryResults, n = 10) {
  if (!pool || !importLotteryResults) {
    return { dates: [], totalImported: 0, totalSkipped: 0, byDate: {} };
  }
  const dates = [];
  for (let i = 0; i < n; i++) {
    dates.push(getDrawDateOffset(i));
  }
  let totalImported = 0;
  let totalSkipped = 0;
  const byDate = {};

  console.log("[LotterySync] backfillLastNDays: bắt đầu cập nhật", n, "ngày từ xoso188:", dates.join(", "));

  for (const drawDate of dates) {
    try {
      const allDraws = [];
      for (const region of ["mn", "mt", "mb"]) {
        const draws = await fetchXoso188ForRegion(region, drawDate);
        allDraws.push(...draws);
        await new Promise((r) => setTimeout(r, 300));
      }
      if (allDraws.length === 0) {
        console.log("[LotterySync] backfillLastNDays:", drawDate, "– xoso188 không trả kết quả");
        byDate[drawDate] = { imported: 0, skipped: 0 };
        continue;
      }
      const result = await importLotteryResults({ draws: allDraws });
      totalImported += result.imported || 0;
      totalSkipped += result.skipped || 0;
      byDate[drawDate] = result;
      console.log("[LotterySync] backfillLastNDays:", drawDate, "–", result.imported, "imported,", result.skipped, "skipped");
    } catch (err) {
      console.error("[LotterySync] backfillLastNDays:", drawDate, "lỗi:", err.message);
      byDate[drawDate] = { error: err.message };
    }
  }

  console.log("[LotterySync] backfillLastNDays hoàn tất – total imported:", totalImported, ", skipped:", totalSkipped);
  return { dates, totalImported, totalSkipped, byDate };
}

/**
 * Poll liên tục từ giờ bắt đầu → đến khi có kết quả hoặc hết khung giờ xổ:
 * 1) Gọi API Minh Ngọc (MINH_NGOC_BASE) lấy kết quả trực tiếp ngày hôm đó.
 * 2) Nếu không lấy được → gọi XOSO188_API (fallback) với header chuẩn.
 * Khi có draws ngày hôm nay → import vào DB → ngưng poll.
 * @param {string} region - 'mn' | 'mt' | 'mb'
 * @param {object} pool - pg.Pool
 * @param {Function} importLotteryResults - (payload) => Promise<{ imported, skipped }>
 */
async function pollUntilResult(region, pool, importLotteryResults) {
  if (pollIntervals[region]) return;
  const schedule = REGION_SCHEDULE[region];
  if (!schedule) return;
  const today = getTodayDrawDate();
  const { label, pollIntervalMs, maxPollDurationMs } = schedule;
  console.log(`[LotterySync] ${label}: bắt đầu poll (${today}), giờ xổ ${schedule.drawStart}–${schedule.drawEnd}, mỗi ${pollIntervalMs / 1000}s`);

  const start = Date.now();
  let loggedFallback = false;
  let triedBackfill = false;

  const tick = async () => {
    if (Date.now() - start > maxPollDurationMs) {
      clearInterval(pollIntervals[region]);
      pollIntervals[region] = null;
      console.warn(`[LotterySync] ${label}: hết khung giờ xổ, ngưng poll → gọi xoso188 lần cuối`);
      try {
        const draws = await fetchXoso188ForRegion(region, today);
        const forToday = draws.filter((d) => d.draw_date === today);
        if (forToday.length > 0) {
          const result = await importLotteryResults({ draws: forToday });
          console.log(`[LotterySync] ${label}: fallback xoso188 sau khi hết giờ poll đã lưu:`, result);
        } else {
          console.warn(`[LotterySync] ${label}: xoso188 cũng không trả kết quả cho ${today}`);
        }
      } catch (err) {
        console.warn(`[LotterySync] ${label}: fallback xoso188 lỗi:`, err.message);
      }
      return;
    }

    // 1) Ưu tiên Minh Ngọc – lấy kết quả trực tiếp ngày hôm đó
    let draws = await fetchMinhNgocDrawsOnly(region);
    // 2) Không có thì fallback xoso188
    if (!draws || draws.length === 0) {
      if (!loggedFallback) {
        console.log(`[LotterySync] ${label}: Minh Ngọc chưa có kết quả → fallback xoso188`);
        loggedFallback = true;
      }
      draws = await fetchXoso188ForRegion(region, today);
    }

    if (draws.length > 0) {
      const forToday = draws.filter((d) => d.draw_date === today);
      if (forToday.length > 0) {
        try {
          const result = await importLotteryResults({ draws: forToday });
          console.log(`[LotterySync] ${label} đã lưu DB:`, result);

          // Callback/backfill: nếu dữ liệu chưa đủ (thiếu giải) -> gọi xoso188 để bù.
          const regionCode = region.toUpperCase();
          const incomplete = forToday.filter((d) => !isDrawCompleteObject(d));
          if (incomplete.length > 0) {
            console.warn(`[LotterySync] ${label}: phát hiện draw chưa đủ (${incomplete.length}) → callback xoso188 để backfill`);
            const backfillDraws = await fetchXoso188ForRegion(region, today);
            const backfillToday = backfillDraws.filter((d) => d.draw_date === today);
            if (backfillToday.length > 0) {
              const backfillRes = await importLotteryResults({ draws: backfillToday });
              console.log(`[LotterySync] ${label}: backfill xoso188 đã lưu:`, backfillRes);
              triedBackfill = true;
            }
          }

          // Stop polling only when DB is complete (or we've tried backfill and DB is complete).
          const provinceCodes = [...new Set(forToday.map((d) => d.province_code))];
          let allComplete = true;
          for (const pc of provinceCodes) {
            const ok = await isDbDrawComplete(pool, today, regionCode, pc);
            if (!ok) { allComplete = false; break; }
          }
          if (allComplete || triedBackfill) {
            clearInterval(pollIntervals[region]);
            pollIntervals[region] = null;
            if (!allComplete) {
              console.warn(`[LotterySync] ${label}: đã thử backfill nhưng DB vẫn chưa đủ → sẽ ngưng poll (kiểm tra cron 20h/backfill)`);
            } else {
              console.log(`[LotterySync] ${label}: DB đã đủ → ngưng poll`);
            }
          }
        } catch (err) {
          console.error("[LotterySync] Import lỗi:", err.message);
        }
      }
    }
  };

  await tick();
  pollIntervals[region] = setInterval(tick, pollIntervalMs);
}

/**
 * Test link phụ xoso188: gọi đúng fetchXoso188ForRegion (header chuẩn).
 * @param {string} region - 'mn' | 'mt' | 'mb'
 * @returns {Promise<{ ok: boolean, drawsCount?: number, region?: string, drawDate?: string, sample?: object, error?: string }>}
 */
export async function runSyncTest(region) {
  const valid = { mn: 1, mt: 1, mb: 1 };
  if (!valid[region]) {
    return { ok: false, error: "region phải là mn | mt | mb" };
  }
  try {
    const today = getTodayDrawDate();
    const draws = await fetchXoso188ForRegion(region, null);
    const forToday = draws.filter((d) => d.draw_date === today);
    const sample = draws[0] ? { draw_date: draws[0].draw_date, province_code: draws[0].province_code, resultsCount: draws[0].results?.length } : null;
    return {
      ok: true,
      drawsCount: draws.length,
      forTodayCount: forToday.length,
      region,
      drawDate: today,
      sample,
    };
  } catch (err) {
    return { ok: false, error: err.message || String(err) };
  }
}

/**
 * Gọi sync cho một miền ngay (từ HTTP trigger hoặc cron ngoài).
 * Trả về ngay; poll chạy nền. Dùng khi Railway sleep hoặc cron trong process không chạy đúng giờ.
 * @param {string} region - 'mn' | 'mt' | 'mb'
 * @param {object} pool - pg.Pool
 * @param {Function} importLotteryResults - (payload) => Promise<{ imported, skipped }>
 * @param {Function} [importLiveResults] - (items) => Promise<{ saved, skipped }>
 * @param {Function} [saveActiveProvinces] - (drawDate, regionCode, provinceCodes)
 */
export function triggerRegionSync(region, pool, importLotteryResults, importLiveResults, saveActiveProvinces) {
  if (!pool || !importLotteryResults) {
    console.warn("[LotterySync] triggerRegionSync: thiếu pool hoặc importLotteryResults");
    return;
  }
  const r = (region || "").toLowerCase();
  if (r !== "mn" && r !== "mt" && r !== "mb") {
    console.warn("[LotterySync] triggerRegionSync: region phải là mn | mt | mb");
    return;
  }
  console.log("[LotterySync] Trigger thủ công:", { mn: "Miền Nam", mt: "Miền Trung", mb: "Miền Bắc" }[r], new Date().toISOString());
  pollUntilResult(r, pool, importLotteryResults);
  if (importLiveResults && isInDrawWindow(r)) {
    pollLiveUntilEnd(r, pool, importLiveResults, saveActiveProvinces);
  }
}

/**
 * Đăng ký cron nội bộ.
 * Case 1: MN 16:15, MT 17:15, MB 18:15 – poll 5 phút Minh Ngọc → fallback xoso188 → lưu lottery_draws.
 * Case 2: Trong giờ xổ – poll 1s Minh Ngọc → lưu từng giải vào kq_tructiep (Xổ Số Trực Tiếp).
 * @param {object} pool - pg.Pool
 * @param {Function} importLotteryResults - (payload) => Promise<{ imported, skipped }>
 * @param {Function} [importLiveResults] - (items) => Promise<{ saved, skipped }> – cho Case 2
 * @param {Function} [saveActiveProvinces] - (drawDate, regionCode, provinceCodes) – lưu đài đang xổ từ tinh
 */
export function scheduleLotterySync(pool, importLotteryResults, importLiveResults, saveActiveProvinces) {
  if (!pool || !importLotteryResults) {
    console.warn("[LotterySync] Bỏ qua cron: thiếu pool hoặc importLotteryResults");
    return;
  }
  const tz = "Asia/Ho_Chi_Minh";
  // Case 1: poll 5 phút từ giờ xổ → lưu lottery_draws
  cron.schedule(REGION_SCHEDULE.mn.cronAt, () => {
    console.log("[LotterySync] Cron:", REGION_SCHEDULE.mn.label, "16:15 (poll 5 phút)", new Date().toISOString());
    pollUntilResult("mn", pool, importLotteryResults);
    if (importLiveResults) pollLiveUntilEnd("mn", pool, importLiveResults, saveActiveProvinces);
  }, { timezone: tz });
  cron.schedule(REGION_SCHEDULE.mt.cronAt, () => {
    console.log("[LotterySync] Cron:", REGION_SCHEDULE.mt.label, "17:15 (poll 5 phút)", new Date().toISOString());
    pollUntilResult("mt", pool, importLotteryResults);
    if (importLiveResults) pollLiveUntilEnd("mt", pool, importLiveResults, saveActiveProvinces);
  }, { timezone: tz });
  cron.schedule(REGION_SCHEDULE.mb.cronAt, () => {
    console.log("[LotterySync] Cron:", REGION_SCHEDULE.mb.label, "18:15 (poll 5 phút)", new Date().toISOString());
    pollUntilResult("mb", pool, importLotteryResults);
    if (importLiveResults) pollLiveUntilEnd("mb", pool, importLiveResults, saveActiveProvinces);
  }, { timezone: tz });
  // 20h cuối ngày: kiểm tra đã có data ngày hôm nay chưa → chưa thì backfill từ xoso188
  cron.schedule("0 20 * * *", () => {
    console.log("[LotterySync] Cron: 20h check & backfill", new Date().toISOString());
    checkAndBackfillToday(pool, importLotteryResults);
  }, { timezone: tz });
  // Chạy ngay khi deploy/startup (kiểm tra & backfill nếu thiếu data hôm nay)
  checkAndBackfillToday(pool, importLotteryResults);
  console.log("[LotterySync] Đã lên lịch: MN/MT/MB 16:15/17:15/18:15 (poll 5 phút); XSTT poll 1s; 20h backfill.");
}
