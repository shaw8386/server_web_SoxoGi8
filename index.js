// ====================== IMPORTS ======================
import express from "express";
import fetch from "node-fetch";
import cors from "cors";

import path from "path";
import { fileURLToPath } from "url";

process.env.TZ = "Asia/Ho_Chi_Minh";

const app = express();
app.use(cors());
app.use(express.json());

// ====================== SERVE FRONTEND (/public) ======================
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
app.use(express.static(path.join(__dirname, "public")));

// ====================== PROXY: /api/* -> https://xoso188.net/api/* ======================
// Ví dụ gọi từ client:
//   /api/front/open/lottery/history/list/game?limitNum=200&gameCode=bali
// sẽ được proxy sang:
//   https://xoso188.net/api/front/open/lottery/history/list/game?limitNum=200&gameCode=bali
const TARGET_BASE = "https://xoso188.net";

app.use("/api", async (req, res) => {
  const targetUrl = TARGET_BASE + req.originalUrl; // giữ nguyên full path + query
  try {
    const response = await fetch(targetUrl, {
      method: req.method,
      headers: {
        // Giữ accept/user-agent cơ bản (không forward hết header client để tránh rủi ro)
        Accept: req.headers.accept || "application/json",
        "User-Agent": "gi8-proxy",
      },
    });

    const body = await response.text();

    // Trả status giống upstream
    res.status(response.status);

    // Nếu upstream trả JSON thì set content-type cho đúng (nếu có)
    const ct = response.headers.get("content-type");
    if (ct) res.setHeader("content-type", ct);

    return res.send(body);
  } catch (err) {
    return res.status(500).json({ error: "Proxy failed", message: err.message });
  }
});

// ====================== HEALTH ======================
app.get("/health", (_, res) => res.send("✅ Railway Lottery Proxy Running"));

// ====================== START ======================
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log("🚀 Server chạy port", PORT));
