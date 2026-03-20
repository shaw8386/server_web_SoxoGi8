/**
 * Get_DataXS_tructiep.js
 * Logic gọi Minh Ngọc (fetch + parse kqxs_data). Server (db/lotterySync.js) gọi khi cron
 * tới giờ; không cần chạy node Get_DataXS_tructiep.js để tự động.
 *
 * - Chạy thủ công 1 miền: node Get_DataXS_tructiep.js mn|mt|mb  → fetch + POST /api/lottery/push-kqxs
 * - Không tham số: in hướng dẫn (cron đã gộp vào server).
 * Env: RAILWAY_URL (khi push thủ công)
 */

import fetch from "node-fetch";
import { parseMinhNgocJs } from "./utils/minhNgocToXoso188.js";

process.env.TZ = "Asia/Ho_Chi_Minh";

const MINH_NGOC_BASE = "https://dc.minhngoc.net/O0O/0/xstt";
const RAILWAY_URL = process.env.RAILWAY_URL || process.env.API_BASE || "http://localhost:3000";

// js_m1 = MN, js_m2 = MB, js_m3 = MT (theo minhNgocToXoso188.js)
const REGION_CONFIG = {
  mn: { url: "js_m1.js", label: "Miền Nam" },
  mt: { url: "js_m3.js", label: "Miền Trung" },
  mb: { url: "js_m2.js", label: "Miền Bắc" },
};

/**
 * Fetch raw body từ URL Minh Ngọc
 */
async function fetchMinhNgocRaw(region) {
  const cfg = REGION_CONFIG[region];
  if (!cfg) throw new Error("region phải là mn | mt | mb");

  const url = `${MINH_NGOC_BASE}/${cfg.url}?_=${Date.now()}`;
  const res = await fetch(url, {
    headers: { Accept: "*/*", "User-Agent": "Mozilla/5.0 (compatible; Get_DataXS_tructiep/1.0)" },
    timeout: 15000,
  });

  if (!res.ok) throw new Error(`Minh Ngọc HTTP ${res.status}`);
  return res.text();
}

/**
 * Fetch Minh Ngọc và trả về kqxs_data. Server (lotterySync) gọi khi cron tới giờ.
 * @param {string} region - 'mn' | 'mt' | 'mb'
 * @returns {Promise<object|null>} kqxs_data hoặc null
 */
export async function fetchMinhNgocKqxsData(region) {
  try {
    const raw = await fetchMinhNgocRaw(region);
    const kqxs_data = parseMinhNgocJs(raw, region);
    if (!kqxs_data || typeof kqxs_data.kq !== "object") return null;
    return kqxs_data;
  } catch (err) {
    console.warn("[Get_DataXS] fetchMinhNgocKqxsData", region, err.message);
    return null;
  }
}

/**
 * Lấy kqxs_data từ Minh Ngọc, push lên Railway (dùng khi chạy thủ công)
 */
async function fetchAndPush(region) {
  const cfg = REGION_CONFIG[region];
  const baseUrl = RAILWAY_URL.replace(/\/$/, "");
  const pushUrl = `${baseUrl}/api/lottery/push-kqxs`;

  try {
    const kqxs_data = await fetchMinhNgocKqxsData(region);

    if (!kqxs_data || typeof kqxs_data.kq !== "object") {
      console.log(`[Get_DataXS] ${cfg.label}: chưa có kq (đang chờ xổ)`);
      return { ok: false, reason: "no_kq" };
    }

    const res = await fetch(pushUrl, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Accept: "application/json",
        "ngrok-skip-browser-warning": "true",
      },
      body: JSON.stringify({ kqxs_data, region }),
      timeout: 15000,
    });

    const result = await res.json();

    if (!res.ok) {
      throw new Error(result.message || result.error || `HTTP ${res.status}`);
    }

    console.log(`[Get_DataXS] ${cfg.label}: OK`, result);
    return { ok: true, ...result };
  } catch (err) {
    console.warn(`[Get_DataXS] ${cfg.label} lỗi:`, err.message);
    return { ok: false, error: err.message };
  }
}

/**
 * Chạy 1 lần cho 1 region (dùng khi gọi thủ công: node Get_DataXS_tructiep.js mn)
 */
async function runOnce(region) {
  const r = (region || "").toLowerCase();
  if (r !== "mn" && r !== "mt" && r !== "mb") {
    console.error("Usage: node Get_DataXS_tructiep.js [mn|mt|mb]");
    console.error("  Không có arg: in hướng dẫn (cron đã gộp vào server)");
    process.exit(1);
  }
  const result = await fetchAndPush(r);
  process.exit(result.ok ? 0 : 1);
}

// ====================== MAIN ======================
const args = process.argv.slice(2);
const manualRegion = args[0];

if (manualRegion) {
  runOnce(manualRegion);
} else {
  console.log("[Get_DataXS] Cron đã gộp vào server (db/lotterySync.js). Server tự gọi đúng giờ MN 16:15, MT 17:15, MB 18:15 (VN).");
  console.log("[Get_DataXS] Push thủ công 1 miền: node Get_DataXS_tructiep.js mn|mt|mb (cần RAILWAY_URL)");
}
