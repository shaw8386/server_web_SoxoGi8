// ====================== IMPORTS ======================
import express from "express";
import fetch from "node-fetch";
import cors from "cors";
import path from "path";
import { fileURLToPath } from "url";
import cron from "node-cron";
import * as db from "./db/index.js";
import { XOSO188_HEADERS, pingXoso188, triggerRegionSync, backfillLastNDays } from "./db/lotterySync.js";

process.env.TZ = "Asia/Ho_Chi_Minh";

const app = express();
app.use(cors());
app.use(express.json({ limit: "10mb" }));

// ====================== SIMPLE IN-MEM CACHE ======================
const memCache = new Map(); // key -> { exp, value }
function cacheGet(k) {
  const v = memCache.get(k);
  if (!v) return null;
  if (Date.now() > v.exp) {
    memCache.delete(k);
    return null;
  }
  return v.value;
}
function cacheSet(k, value, ttlMs) {
  memCache.set(k, { exp: Date.now() + ttlMs, value });
}

// ====================== 🔐 AUTH_ACCEPT GUARD (DB) ======================
// - Verify bằng auth_accept.api_key
// - Update last_used_at (+ ip/user_agent nếu chưa có) trong 1 query
app.use(async (req, res, next) => {
  const pathNorm = req.path.replace(/\/$/, "") || "/";

  // ===== WHITELIST (public / không cần key) =====
  if (pathNorm === "/health") return next();
  if (pathNorm === "/" || pathNorm.startsWith("/HTML_XoSo")) return next();
  if (pathNorm.startsWith("/api/lottery/db/")) return next();
  if (pathNorm === "/api/lottery/sync-test") return next();
  if (pathNorm === "/api/lottery/ping-xoso188") return next();

  // Import (POST), push-kqxs (Genlogin), backfill-last-days – whitelist (không cần x-gi8-key)
  if (pathNorm === "/api/lottery/import" && req.method === "POST") return next();
  if (pathNorm === "/api/lottery/push-kqxs" && req.method === "POST") return next();
  if (pathNorm === "/api/lottery/backfill-last-days" && req.method === "GET") return next();

  // ===== REQUIRE DB =====
  if (!db.pool) {
    return res.status(503).json({
      error: "DB not ready",
      message: "DATABASE_URL not set or init failed",
    });
  }

  // ===== REQUIRE KEY =====
  const key = req.headers["x-gi8-key"];
  if (!key) {
    return res.status(403).json({ error: "Forbidden", message: "Missing x-gi8-key" });
  }

  try {
    const ip =
      req.headers["x-forwarded-for"]?.split(",")[0]?.trim() ||
      req.socket?.remoteAddress ||
      null;

    const ua = req.headers["user-agent"] || null;

    // 1 query: verify + touch last_used
    const { rows } = await db.pool.query(
      `UPDATE auth_accept
       SET last_used_at = now(),
           ip_address   = COALESCE($2, ip_address),
           user_agent   = COALESCE($3, user_agent)
       WHERE api_key = $1 AND is_active = true
       RETURNING id, client_id, scopes`,
      [key, ip, ua]
    );

    if (!rows.length) {
      return res
        .status(403)
        .json({ error: "Forbidden", message: "Invalid or inactive x-gi8-key" });
    }

    req.gi8 = {
      auth_id: rows[0].id,
      client_id: rows[0].client_id,
      scopes: rows[0].scopes,
    };

    return next();
  } catch (e) {
    return res.status(500).json({ error: "Auth error", message: e.message });
  }
});

// ====================== SERVE FRONTEND ======================
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
app.use(express.static(path.join(__dirname, "public")));
app.use("/HTML_XoSo", express.static(path.join(__dirname, "HTML_XoSo")));
app.get("/", (_, res) => res.redirect("/HTML_XoSo/index_tructiep_miennam.html"));

// ====================== PROXY: /api/* -> DB hoặc https://xoso188.net/api/* ======================
const TARGET_BASE = "https://xoso188.net";

function formatDrawDate(d) {
  const day = String(d.getDate()).padStart(2, "0");
  const month = String(d.getMonth() + 1).padStart(2, "0");
  const year = d.getFullYear();
  return { turnNum: `${day}/${month}/${year}`, ymd: `${year}-${month}-${day}` };
}

// ====================== LOTTERY: push-kqxs, import (TRƯỚC app.use("/api") để không bị proxy sang xoso188) ======================
app.post("/api/lottery/push-kqxs", async (req, res) => {
  if (!db.pool) {
    return res.status(503).json({ error: "DB not configured", message: "DATABASE_URL not set" });
  }
  try {
    const { kqxs_data, region } = req.body;
    if (!kqxs_data || typeof kqxs_data !== "object") {
      return res.status(400).json({
        error: "Invalid payload",
        message: "kqxs_data (object) required. VD: { run, tinh, ntime, kq: { 13: {...}, 14: {...} } }",
      });
    }

    const { kqxsDataToDraws } = await import("./utils/minhNgocToXoso188.js");
    const regionKey = (region || "mn").toLowerCase();
    if (regionKey !== "mn" && regionKey !== "mt" && regionKey !== "mb") {
      return res.status(400).json({ error: "Invalid region", message: "region phải là mn | mt | mb" });
    }

    const draws = kqxsDataToDraws(kqxs_data, regionKey);
    if (draws.length === 0) {
      return res.json({
        ok: true,
        imported: 0,
        skipped: 0,
        message: "Không có dữ liệu hợp lệ để ghi (kq rỗng hoặc chưa có số)",
      });
    }

    const result = await db.importLotteryResults({ draws });
    memCache.clear();

    return res.json({ ok: true, ...result });
  } catch (err) {
    console.error("push-kqxs error:", err);
    return res.status(500).json({ error: "Push failed", message: err.message });
  }
});

app.post("/api/lottery/import", async (req, res) => {
  if (!db.pool) {
    return res.status(503).json({ error: "DB not configured", message: "DATABASE_URL not set" });
  }
  try {
    const { draws } = req.body;
    if (!Array.isArray(draws) || draws.length === 0) {
      return res.status(400).json({ error: "Invalid payload", message: "draws array required" });
    }

    const result = await db.importLotteryResults(req.body);

    // DB đã đổi => clear cache để list/game phản ánh ngay
    memCache.clear();

    return res.json({ ok: true, ...result });
  } catch (err) {
    console.error("Import error:", err);
    return res.status(500).json({ error: "Import failed", message: err.message });
  }
});

// ====================== PROXY: /api/* -> DB hoặc https://xoso188.net/api/* ======================
app.use("/api", async (req, res) => {
  const pathNorm = req.path.replace(/\/$/, "") || "/";

  // ======================
  // DB: /api/lottery/db/live, /api/lottery/db/draws (TRƯỚC proxy)
  // ======================
  if (pathNorm === "/lottery/db/active-provinces" && req.method === "GET" && db.pool) {
    try {
      const dateStr = req.query.date;
      const region = req.query.region || null;
      if (!dateStr || !region) return res.status(400).json({ error: "Missing date (DD/MM/YYYY) và region (MN|MT|MB)" });
      const [d, m, y] = dateStr.split(/[\/\-]/).map(Number);
      const drawDate = `${y}-${String(m).padStart(2, "0")}-${String(d).padStart(2, "0")}`;
      const codes = await db.getActiveProvinces(drawDate, region.toUpperCase());
      return res.json({ province_codes: codes });
    } catch (err) {
      console.error("Get active-provinces error:", err);
      return res.status(500).json({ error: err.message });
    }
  }
  if (pathNorm === "/lottery/db/live" && req.method === "GET" && db.pool) {
    try {
      const dateStr = req.query.date;
      const region = req.query.region || null;
      if (!dateStr) return res.status(400).json({ error: "Missing date (DD/MM/YYYY)" });
      const [d, m, y] = dateStr.split(/[\/\-]/).map(Number);
      const drawDate = `${y}-${String(m).padStart(2, "0")}-${String(d).padStart(2, "0")}`;
      const rows = await db.getLiveResults(drawDate, region);
      return res.json({ live: rows });
    } catch (err) {
      console.error("Get live error:", err);
      return res.status(500).json({ error: err.message });
    }
  }
  if (pathNorm === "/lottery/db/draws" && req.method === "GET" && db.pool) {
    try {
      const dateStr = req.query.date;
      const region = req.query.region || null;
      if (!dateStr) return res.status(400).json({ error: "Missing date (DD/MM/YYYY)" });
      const [d, m, y] = dateStr.split(/[\/\-]/).map(Number);
      const drawDate = `${y}-${String(m).padStart(2, "0")}-${String(d).padStart(2, "0")}`;
      const draws = await db.getDrawsByDate(drawDate, region);
      const withResults = await Promise.all(
        draws.map(async (dr) => {
          const results = await db.getResultsByDrawId(dr.id);
          return { ...dr, results };
        })
      );
      return res.json({ draws: withResults });
    } catch (err) {
      console.error("Get draws error:", err);
      return res.status(500).json({ error: err.message });
    }
  }

  const match = req.path.match(/^\/front\/open\/lottery\/history\/list\/game/);

  // ======================
  // DB READ (HOT PATH): /api/front/open/lottery/history/list/game
  // ======================
  if (match && req.method === "GET" && req.query.gameCode && db.pool) {
    const gameCode = String(req.query.gameCode);
    const limitNum = String(req.query.limitNum || "200");

    // cache 30s–60s tuỳ bạn
    const cacheKey = `history:${gameCode}:${limitNum}`;
    const cached = cacheGet(cacheKey);
    if (cached) return res.json(cached);

    try {
      const data = await db.getLotteryHistoryListGame(gameCode, limitNum);
      if (!data) {
        return res.status(400).json({
          success: false,
          msg: "gameCode không tồn tại",
          code: 400,
        });
      }

      const now = new Date();
      const serverTime =
        `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, "0")}-${String(
          now.getDate()
        ).padStart(2, "0")} ` +
        `${String(now.getHours()).padStart(2, "0")}:${String(now.getMinutes()).padStart(
          2,
          "0"
        )}:${String(now.getSeconds()).padStart(2, "0")}`;

      const issueList = [];
      for (const draw of data.draws) {
        const groups = ["", "", "", "", "", "", "", "", ""];
        const prizeMap = {
          DB: 0,
          G1: 1,
          G2: 2,
          G3: 3,
          G4: 4,
          G5: 5,
          G6: 6,
          G7: 7,
          G8: 8,
        };

        for (const r of draw.results) {
          const idx = prizeMap[r.prize_code];
          if (idx !== undefined) {
            groups[idx] = groups[idx] ? groups[idx] + "," + r.result_number : r.result_number;
          }
        }

        const { turnNum, ymd } = formatDrawDate(draw.draw_date);
        const openTime = `${ymd} ${data.openTimeByRegion}`;
        const openTimeStamp = new Date(openTime).getTime();
        const openNum = groups[0] || ""; // giải đặc biệt

        issueList.push({
          turnNum,
          openNum,
          openTime,
          openTimeStamp,
          detail: JSON.stringify(groups),
          status: 2,
          replayUrl: null,
          n11: null,
          jackpot: 0,
        });
      }

      const latestTurn = data.draws.length
        ? formatDrawDate(data.draws[0].draw_date)
        : { turnNum: "", ymd: "" };

      const t = {
        turnNum: latestTurn.turnNum,
        openTime: data.draws.length ? `${latestTurn.ymd} ${data.openTimeByRegion}` : "",
        serverTime,
        name: data.name,
        code: data.code,
        sort: data.sort,
        navCate: data.navCate,
        issueList,
      };

      const payload = { success: true, msg: "ok", code: 0, t };

      cacheSet(cacheKey, payload, 60_000); // 60s
      return res.json(payload);
    } catch (e) {
      console.warn("DB history/list/game error:", e.message);
      return res.status(500).json({
        success: false,
        msg: e.message || "Lỗi server",
        code: 500,
      });
    }
  }

  // ======================
  // FALLBACK PROXY TO xoso188
  // ======================
  const targetUrl = TARGET_BASE + req.originalUrl;
  try {
    const response = await fetch(targetUrl, {
      method: req.method,
      headers: { ...XOSO188_HEADERS, Accept: req.headers.accept || "application/json" },
      timeout: 20000,
    });
    const body = await response.text();
    res.status(response.status);
    const ct = response.headers.get("content-type");
    if (ct) res.setHeader("content-type", ct);
    return res.send(body);
  } catch (err) {
    return res.status(500).json({ error: "Proxy failed", message: err.message });
  }
});

// ====================== HEALTH ======================
app.get("/health", (_, res) => res.send("✅ Railway Lottery Proxy Running"));

// ====================== LOTTERY FETCH (proxy xoso188) ======================
// GET /api/lottery/fetch?gameCode=xxx&limit=200
app.get("/api/lottery/fetch", async (req, res) => {
  const gameCode = req.query.gameCode;
  const limit = Math.min(parseInt(req.query.limit || "200", 10) || 200, 500);

  if (!gameCode) return res.status(400).json({ error: "Missing gameCode" });

  const targetUrl = `https://xoso188.net/api/front/open/lottery/history/list/game?limitNum=${limit}&gameCode=${gameCode}`;
  try {
    const response = await fetch(targetUrl, {
      headers: { ...XOSO188_HEADERS, Accept: "application/json" },
      timeout: 20000,
    });
    const body = await response.text();
    res.status(response.status);
    res.setHeader("content-type", response.headers.get("content-type") || "application/json");
    return res.send(body);
  } catch (err) {
    return res.status(500).json({ error: "Fetch failed", message: err.message });
  }
});

// GET /api/lottery/ping-xoso188 (public)
app.get("/api/lottery/ping-xoso188", async (req, res) => {
  try {
    const result = await pingXoso188();
    return res.json(result);
  } catch (err) {
    return res.status(500).json({
      ok: false,
      status: 0,
      message: err?.message || String(err),
      count: 0,
      source: "xoso188",
    });
  }
});

// ====================== LOTTERY DB ======================
// GET /api/lottery/sync-test?region=mn|mt|mb (public)
app.get("/api/lottery/sync-test", async (req, res) => {
  try {
    const { runSyncTest } = await import("./db/lotterySync.js");
    const region = (req.query.region || "").toLowerCase();
    const result = await runSyncTest(region);
    return res.json(result);
  } catch (err) {
    return res.status(500).json({ ok: false, error: err.message || String(err) });
  }
});

// GET /api/lottery/backfill-last-days?n=10 – Cập nhật N ngày gần nhất từ xoso188 (ghi đè DB)
app.get("/api/lottery/backfill-last-days", async (req, res) => {
  if (!db.pool) {
    return res.status(503).json({ error: "DB not configured" });
  }
  try {
    const n = Math.min(Math.max(parseInt(req.query.n || "10", 10) || 10, 1), 31);
    const result = await backfillLastNDays(db.pool, db.importLotteryResults, n);
    memCache.clear();
    return res.json({ ok: true, ...result });
  } catch (err) {
    console.error("backfill-last-days error:", err);
    return res.status(500).json({ ok: false, error: err.message });
  }
});

// GET /api/lottery/trigger-sync?region=mn|mt|mb (requires key by guard)
app.get("/api/lottery/trigger-sync", (req, res) => {
  if (!db.pool) {
    return res.status(503).json({ success: false, msg: "DB chưa sẵn sàng", code: 503 });
  }
  const region = (req.query.region || "").toLowerCase();
  if (region !== "mn" && region !== "mt" && region !== "mb") {
    return res.status(400).json({ success: false, msg: "region phải là mn | mt | mb", code: 400 });
  }
  triggerRegionSync(region, db.pool, db.importLotteryResults, db.importLiveResults, db.saveActiveProvinces);
  const label = { mn: "Miền Nam (16:15)", mt: "Miền Trung (17:15)", mb: "Miền Bắc (18:15)" }[region];
  return res.status(202).json({
    success: true,
    msg: `Đã kích hoạt sync ${label}. Poll 5 phút + XSTT 1s (nếu trong giờ xổ).`,
    code: 0,
    region,
  });
});

// GET /api/lottery/db/live?date=DD/MM/YYYY&region=MN|MT|MB – Xổ Số Trực Tiếp (kq_tructiep)
app.get("/api/lottery/db/live", async (req, res) => {
  if (!db.pool) return res.status(503).json({ error: "DB not configured" });
  try {
    const dateStr = req.query.date;
    const region = req.query.region || null;
    if (!dateStr) return res.status(400).json({ error: "Missing date (DD/MM/YYYY)" });

    const [d, m, y] = dateStr.split(/[\/\-]/).map(Number);
    const drawDate = `${y}-${String(m).padStart(2, "0")}-${String(d).padStart(2, "0")}`;

    const rows = await db.getLiveResults(drawDate, region);
    return res.json({ live: rows });
  } catch (err) {
    console.error("Get live error:", err);
    return res.status(500).json({ error: err.message });
  }
});

// GET /api/lottery/db/draws?date=DD/MM/YYYY&region=MB|MT|MN (public by whitelist)
app.get("/api/lottery/db/draws", async (req, res) => {
  if (!db.pool) return res.status(503).json({ error: "DB not configured" });
  try {
    const dateStr = req.query.date;
    const region = req.query.region || null;
    if (!dateStr) return res.status(400).json({ error: "Missing date (DD/MM/YYYY)" });

    const [d, m, y] = dateStr.split(/[\/\-]/).map(Number);
    const drawDate = `${y}-${String(m).padStart(2, "0")}-${String(d).padStart(2, "0")}`;

    const draws = await db.getDrawsByDate(drawDate, region);
    const withResults = await Promise.all(
      draws.map(async (dr) => {
        const results = await db.getResultsByDrawId(dr.id);
        return { ...dr, results };
      })
    );
    return res.json({ draws: withResults });
  } catch (err) {
    console.error("Get draws error:", err);
    return res.status(500).json({ error: err.message });
  }
});

// GET /api/lottery/db/history/:gameCode?limit=200 (public by whitelist)
app.get("/api/lottery/db/history/:gameCode", async (req, res) => {
  if (!db.pool) return res.status(503).json({ error: "DB not configured" });
  try {
    const gameCode = req.params.gameCode;
    const limit = Math.min(parseInt(req.query.limit || "200", 10) || 200, 500);

    const { rows } = await db.pool.query(
      `SELECT d.draw_date, d.id as draw_id, p.api_game_code, p.code as province_code, r.code as region_code
       FROM lottery_draws d
       JOIN lottery_provinces p ON d.province_id = p.id
       JOIN regions r ON d.region_id = r.id
       WHERE p.api_game_code = $1
       ORDER BY d.draw_date DESC
       LIMIT $2`,
      [gameCode, limit]
    );

    const issueList = [];
    for (const row of rows) {
      const resRows = await db.getResultsByDrawId(row.draw_id);
      const groups = ["", "", "", "", "", "", "", "", ""];
      const prizeMap = { DB: 0, G1: 1, G2: 2, G3: 3, G4: 4, G5: 5, G6: 6, G7: 7, G8: 8 };

      for (const r of resRows) {
        const idx = prizeMap[r.prize_code];
        if (idx !== undefined) {
          groups[idx] = groups[idx] ? groups[idx] + "," + r.result_number : r.result_number;
        }
      }

      const turnNum = row.draw_date.toISOString().slice(0, 10).split("-").reverse().join("/");
      issueList.push({ turnNum, detail: JSON.stringify(groups) });
    }

    return res.json({ t: { issueList } });
  } catch (err) {
    console.error("History error:", err);
    return res.status(500).json({ error: err.message });
  }
});

// ====================== START ======================
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log("🚀 Server chạy port", PORT);

  db
    .initDb()
    .then(async (pool) => {
      if (pool) {
        // db.scheduleLotterySync(...) – KHÔNG gọi (Minh Ngọc cron 16:15/17:15/18:15 đã tắt)
        // 20h hàng ngày + startup: tự call xoso188 cập nhật 10 ngày gần nhất vào DB
        const tz = "Asia/Ho_Chi_Minh";
        const BACKFILL_DAYS = 10;
        cron.schedule("0 20 * * *", () => {
          console.log("[index] Cron 20h: backfill", BACKFILL_DAYS, "ngày từ xoso188", new Date().toISOString());
          backfillLastNDays(pool, db.importLotteryResults, BACKFILL_DAYS).then((r) => {
            memCache.clear();
            console.log("[index] Cron 20h hoàn tất:", r.totalImported, "imported,", r.totalSkipped, "skipped");
          });
        }, { timezone: tz });
        backfillLastNDays(pool, db.importLotteryResults, BACKFILL_DAYS).then((r) => {
          memCache.clear();
          console.log("[Startup] Backfill 10 ngày xong:", r.totalImported, "imported,", r.totalSkipped, "skipped");
        });
        console.log("[Startup] 20h hàng ngày + startup: backfill", BACKFILL_DAYS, "ngày từ xoso188. Minh Ngọc cron đã tắt.");
      } else {
        console.warn(
          "[Startup] DB init trả null → không chạy scheduleLotterySync. Kiểm tra DATABASE_URL và log lỗi phía trên."
        );
      }

      const ping = await pingXoso188();
      console.log(
        "[Startup] xoso188:",
        ping.ok ? "OK (count=" + ping.count + ")" : "FAIL",
        ping.message || ""
      );
    })
    .catch((e) => console.warn("DB init:", e.message));
});
