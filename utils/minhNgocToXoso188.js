/**
 * minhNgocToXoso188.js
 * Convert dữ liệu từ Minh Ngọc (dc.minhngoc.net) sang định dạng giống xoso188 API
 *
 * Minh Ngọc format: kqxs.mn={run,tinh,ntime,delay,kq:{13:{8:"27",7:"496",...,0:"******"},...}}
 * xoso188 format: { success, msg, code, t: { turnNum, openTime, issueList: [{ turnNum, detail, status, ... }] } }
 *
 * Usage (ESM):
 *   import { fetchAndConvert, convertMinhNgocToXoso188 } from './utils/minhNgocToXoso188.js';
 *
 *   // Fetch từ URL và convert
 *   const { raw, converted } = await fetchAndConvert('https://dc.minhngoc.net/O0O/0/xstt/js_m1.js');
 *   // converted = { tani: {...}, angi: {...}, bith: {...} } - mỗi key là gameCode
 *
 *   // Convert data đã có sẵn
 *   const converted = convertMinhNgocToXoso188(parsedData, 'mn');
 *
 * URL: js_m1.js = Miền Nam, js_m2.js = Miền Bắc, js_m3.js = Miền Trung
 */

const MINH_NGOC_BASE = "https://dc.minhngoc.net/O0O/0/xstt";

// Mã tỉnh Minh Ngọc (số) -> gameCode xoso188
const PROVINCE_ID_TO_GAME = {
  // Miền Nam
  1: "tphc",    // TP.HCM
  2: "doth",    // Đồng Tháp
  3: "cama",    // Cà Mau
  7: "btre",    // Bến Tre
  8: "vuta",    // Vũng Tàu
  9: "bali",    // Bạc Liêu
  10: "dona",   // Đồng Nai
  11: "cath",   // Cần Thơ
  12: "sotr",   // Sóc Trăng
  13: "tani",   // Tây Ninh
  14: "angi",   // An Giang
  15: "bith",   // Bình Thuận
  16: "vilo",   // Vĩnh Long
  17: "bidu",   // Bình Dương
  18: "trvi",   // Trà Vinh
  19: "loan",   // Long An
  20: "hagi",   // Hậu Giang
  21: "biph",   // Bình Phước
  22: "tigi",   // Tiền Giang
  23: "kigi",   // Kiên Giang
  24: "dalat",  // Đà Lạt
  // Miền Trung
  26: "thth",   // Huế
  27: "phye",   // Phú Yên
  28: "quna",   // Quảng Nam
  29: "dalak",  // Đắk Lắk
  30: "dana",   // Đà Nẵng
  31: "khho",   // Khánh Hòa
  32: "bidi",   // Bình Định
  33: "qutr",   // Quảng Trị
  34: "qubi",   // Quảng Bình
  35: "gila",   // Gia Lai
  36: "nith",   // Ninh Thuận
  37: "qung",   // Quảng Ngãi
  38: "dano",   // Đắk Nông
  39: "kotu",   // Kon Tum
  // Miền Bắc (46-51 dùng chung miba, tỉnh theo ngày)
  46: "miba",
  47: "miba",
  48: "miba",
  49: "miba",
  50: "miba",
  51: "miba",
};

/** Mã tỉnh -> [region_code, province_code] cho DB (lottery_provinces.code) */
const PROVINCE_ID_TO_REGION_PROVINCE = {
  1: ["MN", "HCM"], 2: ["MN", "DTH"], 3: ["MN", "CMA"], 7: ["MN", "BTR"], 8: ["MN", "VTA"],
  9: ["MN", "BLI"], 10: ["MN", "DNA"], 11: ["MN", "CTH"], 12: ["MN", "STR"], 13: ["MN", "TNI"],
  14: ["MN", "AGI"], 15: ["MN", "BTH"], 16: ["MN", "VLO"], 17: ["MN", "BDU"], 18: ["MN", "TVI"],
  19: ["MN", "LAN"], 20: ["MN", "HGI"], 21: ["MN", "BPH"], 22: ["MN", "TGI"], 23: ["MN", "KGI"],
  24: ["MN", "DLT"],
  26: ["MT", "THH"], 27: ["MT", "PYE"], 28: ["MT", "QNM"], 29: ["MT", "DLK"], 30: ["MT", "DN"],
  31: ["MT", "KHO"], 32: ["MT", "BDI"], 33: ["MT", "QTR"], 34: ["MT", "QBI"], 35: ["MT", "GLA"],
  36: ["MT", "NTH"], 37: ["MT", "QNG"], 38: ["MT", "DNO"], 39: ["MT", "KTU"],
  46: ["MB", "HN"], 47: ["MB", "HP"], 48: ["MB", "QN"], 49: ["MB", "BN"], 50: ["MB", "TB"], 51: ["MB", "ND"],
};

const PRIZE_CODES = ["DB", "G1", "G2", "G3", "G4", "G5", "G6", "G7", "G8"];

// Giờ mở thưởng theo miền
const REGION_OPEN_TIME = { MB: "18:15:00", MT: "17:15:00", MN: "16:15:00" };
const REGION_SORT = { MB: 10, MT: 20, MN: 30 };

/**
 * Placeholder trong Minh Ngọc (và các proxy) có thể thay đổi độ dài:
 * "**", "***", "****", "*****", "++++", "+++++", ...
 * Quy ước: chuỗi rỗng hoặc chỉ toàn '*' hoặc chỉ toàn '+' => placeholder (không phải số).
 */
function isPlaceholder(val) {
  if (val == null) return true;
  const s = String(val).trim();
  if (s === "") return true;
  return /^[*]+$/.test(s) || /^[+]+$/.test(s);
}

/**
 * Lấy giá trị "sạch" từ kq - bỏ placeholder, join array
 */
function toDetailPart(val) {
  if (val == null) return "";
  if (Array.isArray(val)) {
    const parts = val.filter((v) => !isPlaceholder(v));
    return parts.join(",");
  }
  if (isPlaceholder(val)) return "";
  return String(val).trim();
}

/**
 * Chuyển 1 tỉnh từ kq[provinceId] sang mảng detail (DB, G1..G8)
 * Minh Ngọc: 0=ĐB, 1=G1, 2=G2, 3=G3, 4=G4, 5=G5, 6=G6, 7=G7, 8=G8
 */
function provinceKqToDetail(kq) {
  const groups = ["", "", "", "", "", "", "", "", ""];
  for (let i = 0; i <= 8; i++) {
    const v = kq[i];
    groups[i] = toDetailPart(v);
  }
  return groups;
}

/**
 * Kiểm tra tỉnh đã xổ xong chưa (ĐB có số thật)
 */
function isComplete(kq) {
  const db = kq[0];
  return db != null && !isPlaceholder(db);
}

/**
 * Kiểm tra đang xổ (có +++++)
 */
function isDrawing(kq) {
  const str = JSON.stringify(kq);
  return str.includes("+");
}

/**
 * Lấy status: 2=hoàn thành, 1=đang xổ, 0=chờ
 */
function getStatus(kq) {
  if (isComplete(kq)) return 2;
  if (isDrawing(kq)) return 1;
  return 0;
}

/**
 * ntime (Unix sec) -> DD/MM/YYYY
 */
function ntimeToTurnNum(ntime) {
  const d = new Date(ntime * 1000);
  const day = String(d.getDate()).padStart(2, "0");
  const month = String(d.getMonth() + 1).padStart(2, "0");
  const year = d.getFullYear();
  return `${day}/${month}/${year}`;
}

/**
 * ntime -> YYYY-MM-DD HH:mm:ss
 */
function ntimeToOpenTime(ntime, regionCode) {
  const d = new Date(ntime * 1000);
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, "0");
  const dd = String(d.getDate()).padStart(2, "0");
  const time = REGION_OPEN_TIME[regionCode] || "16:15:00";
  return `${y}-${m}-${dd} ${time}`;
}

/**
 * Parse text JS từ Minh Ngọc -> object
 */
function parseMinhNgocJs(text, regionKey = "mn") {
  const re = new RegExp(`kqxs\\.(${regionKey})\\s*=\\s*(\\{)`);
  const m = text.match(re);
  if (!m) return null;

  let start = m.index + m[0].length - 1; // vị trí dấu {
  let depth = 0;
  let end = -1;
  for (let i = start; i < text.length; i++) {
    if (text[i] === "{") depth++;
    else if (text[i] === "}") {
      depth--;
      if (depth === 0) {
        end = i;
        break;
      }
    }
  }
  if (end < 0) return null;

  let objStr = text.slice(start, end + 1);
  objStr = objStr.replace(/(\w+)\s*:/g, '"$1":');

  try {
    return JSON.parse(objStr);
  } catch {
    return null;
  }
}

/**
 * Convert dữ liệu Minh Ngọc (MN) sang format xoso188
 * @param {Object} data - Object đã parse từ kqxs.mn
 * @returns {Object} - { [gameCode]: { success, msg, code, t } }
 */
function convertMinhNgocToXoso188(data, region = "mn") {
  const regionCode = region.toUpperCase() === "MN" ? "MN" : region.toUpperCase() === "MT" ? "MT" : "MB";
  const regionLabel = { MN: "Miền Nam", MT: "Miền Trung", MB: "Miền Bắc" }[regionCode];
  const openTimeStr = REGION_OPEN_TIME[regionCode] || "16:15:00";

  const results = {};

  if (!data || typeof data.kq !== "object") {
    return results;
  }

  const provinceIds = Object.keys(data.kq).map(Number).filter((n) => !isNaN(n));
  const ntime = data.ntime || Math.floor(Date.now() / 1000);

  for (const provId of provinceIds) {
    const kq = data.kq[provId];
    if (!kq || typeof kq !== "object") continue;

    const gameCode = PROVINCE_ID_TO_GAME[provId];
    if (!gameCode) continue;

    const groups = provinceKqToDetail(kq);
    const turnNum = ntimeToTurnNum(ntime);
    const openTime = ntimeToOpenTime(ntime, regionCode);
    const openTimeStamp = new Date(openTime).getTime();
    const openNum = groups[0] ? groups[0].replace(/,/g, "").slice(-5) : "";
    const status = getStatus(kq);

    const issueList = [
      {
        turnNum,
        openNum: openNum || groups[0] || "",
        openTime,
        openTimeStamp,
        detail: JSON.stringify(groups),
        status,
        replayUrl: null,
        n11: null,
        jackpot: 0,
      },
    ];

    const now = new Date();
    const serverTime =
      `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, "0")}-${String(now.getDate()).padStart(2, "0")} ` +
      `${String(now.getHours()).padStart(2, "0")}:${String(now.getMinutes()).padStart(2, "0")}:${String(now.getSeconds()).padStart(2, "0")}`;

    const t = {
      turnNum,
      openTime,
      serverTime,
      name: regionLabel,
      code: gameCode,
      sort: REGION_SORT[regionCode] || 0,
      navCate: region.toLowerCase(),
      issueList,
    };

    results[gameCode] = {
      success: true,
      msg: "ok",
      code: 0,
      t,
    };
  }

  return results;
}

/**
 * Chuyển kqxs_data sang format draws cho importLotteryResults (ghi DB)
 * @param {Object} kqxsData - Object từ kqxs.mn (run, tinh, ntime, kq: {13:{...}, 14:{...}})
 * @param {string} region - "mn" | "mt" | "mb"
 * @returns {Array<{ draw_date, province_code, region_code, results }>}
 */
function kqxsDataToDraws(kqxsData, region = "mn") {
  const regionCode = region.toUpperCase() === "MN" ? "MN" : region.toUpperCase() === "MT" ? "MT" : "MB";
  const draws = [];

  if (!kqxsData || typeof kqxsData.kq !== "object") return draws;

  const ntime = kqxsData.ntime || Math.floor(Date.now() / 1000);
  const d = new Date(ntime * 1000);
  const draw_date =
    `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}-${String(d.getDate()).padStart(2, "0")}`;

  for (const provIdStr of Object.keys(kqxsData.kq)) {
    const provId = Number(provIdStr);
    if (isNaN(provId)) continue;

    const meta = PROVINCE_ID_TO_REGION_PROVINCE[provId];
    if (!meta) continue;

    const [regCode, provinceCode] = meta;
    if (regCode !== regionCode) continue;

    const kq = kqxsData.kq[provId];
    if (!kq || typeof kq !== "object") continue;

    const groups = provinceKqToDetail(kq);
    const results = [];

    for (let i = 0; i < PRIZE_CODES.length; i++) {
      const val = groups[i];
      if (!val) continue;
      const parts = String(val).split(",").map((s) => s.trim()).filter(Boolean);
      parts.forEach((num, idx) => {
        results.push({
          prize_code: PRIZE_CODES[i],
          prize_order: idx + 1,
          result_number: num,
        });
      });
    }

    if (results.length > 0) {
      draws.push({ draw_date, province_code: provinceCode, region_code: regCode, results });
    }
  }

  return draws;
}

/**
 * Fetch từ URL Minh Ngọc và convert sang xoso188 format
 * @param {string} url - VD: https://dc.minhngoc.net/O0O/0/xstt/js_m1.js
 * @param {Object} opts - { fetch: customFetch }
 * @returns {Promise<Object>} - { raw, converted: { [gameCode]: xoso188Response } }
 */
async function fetchAndConvert(url, opts = {}) {
  const fetchFn = opts.fetch || (typeof fetch !== "undefined" ? fetch : null);
  if (!fetchFn) {
    throw new Error("fetch không có sẵn. Truyền opts.fetch hoặc dùng môi trường có fetch.");
  }

  const fullUrl = url.startsWith("http") ? url : `${MINH_NGOC_BASE}/${url.replace(/^\//, "")}`;
  const finalUrl = fullUrl.includes("?") ? fullUrl : `${fullUrl}?_=${Date.now()}`;

  const res = await fetchFn(finalUrl, {
    headers: { Accept: "*/*", "User-Agent": "Mozilla/5.0 (compatible; MinhNgocConverter/1.0)" },
    ...(opts.timeout && { signal: AbortSignal.timeout ? AbortSignal.timeout(opts.timeout) : undefined }),
  });

  const text = await res.text();

  let regionKey = "mn";
  if (finalUrl.includes("js_m2")) regionKey = "mb";
  else if (finalUrl.includes("js_m3")) regionKey = "mt";

  const raw = parseMinhNgocJs(text, regionKey);
  if (!raw) {
    return { raw: null, converted: {}, error: "Parse thất bại" };
  }

  const converted = convertMinhNgocToXoso188(raw, regionKey);

  return { raw, converted };
}

/**
 * Lấy danh sách province_code đang xổ hôm nay từ kqxs_data.
 * Dùng tinh (VD: "29,28") nếu có, ngược lại lấy từ keys của kq.
 * @param {Object} kqxsData - Object từ parseMinhNgocJs (có tinh, kq)
 * @param {string} region - "mn" | "mt" | "mb"
 * @returns {string[]} - VD ["DLK","QNM"] cho MT
 */
function getActiveProvinceCodesFromKqxsData(kqxsData, region = "mn") {
  const regionCode = region.toUpperCase() === "MN" ? "MN" : region.toUpperCase() === "MT" ? "MT" : "MB";
  let ids = [];
  if (kqxsData?.tinh && typeof kqxsData.tinh === "string") {
    ids = kqxsData.tinh.split(",").map((x) => parseInt(x, 10)).filter((n) => !isNaN(n));
  }
  if (ids.length === 0 && kqxsData?.kq && typeof kqxsData.kq === "object") {
    ids = Object.keys(kqxsData.kq).map(Number).filter((n) => !isNaN(n));
  }
  const codes = [];
  for (const id of ids) {
    const meta = PROVINCE_ID_TO_REGION_PROVINCE[id];
    if (meta && meta[0] === regionCode) codes.push(meta[1]);
  }
  return [...new Set(codes)];
}

// -------- Export (ESM) --------
export {
  parseMinhNgocJs,
  convertMinhNgocToXoso188,
  kqxsDataToDraws,
  fetchAndConvert,
  getActiveProvinceCodesFromKqxsData,
  PROVINCE_ID_TO_GAME,
  PROVINCE_ID_TO_REGION_PROVINCE,
  MINH_NGOC_BASE,
};
