// Perp Spread Desk bot: runs the arb scanner on Hyperliquid + Lighter and posts a session summary to X.
//
//   node bot/run.mjs                  dry run: prints the posts, posts nothing
//   X_API_KEY=… X_API_SECRET=… X_ACCESS_TOKEN=… X_ACCESS_SECRET=… node bot/run.mjs --post
//
// Same rule as the site's Arb scanner (default settings):
//   a window counts when the FILLABLE gap (sell the rich venue at its bid, buy the cheap one at its ask,
//   after paying to cross both books for the trade size) is >= ENTRY bps for >= MIN_WINDOW minutes;
//   you act REACT minutes after it appears, close at <= EXIT bps or on a flip, the same day.
//   Results are market orders: taker fees + book impact (median of the saved order-book checks for that pair, bot/costs.json).
// No dependencies: Node 20+ (global fetch, crypto).

import crypto from "node:crypto";
import fs from "node:fs";

const CFG = {
  entry: 10, exit: 3, minWin: 60, react: 15, iv: "5m", days: 3, size: 10_000, minVol: 250_000,
  session: process.env.SESSION || "",            // "morning" | "evening" (auto from the clock if empty)
  tz: process.env.TZ || "Europe/Istanbul",
  site: "0xsatoshinakamoto.github.io/perp-spread-desk",
  historyFile: new URL("./history.json", import.meta.url),
  // order-book cost samples, recorded every ~30 min by .github/workflows/book-recorder.yml (kept on the "data" branch)
  costsFile: process.env.COSTS_FILE || new URL("./costs.json", import.meta.url),
};
const POST = process.argv.includes("--post");
const SLOTTED = process.argv.includes("--slot");   // scheduled runs: post once per slot, whenever the (late) runner gets here
// Post times in UTC. Testing: 06:00 and 18:00 UTC (09:00 / 21:00 Istanbul). Change here and in .github/workflows/x-posts.yml.
const SLOT_HOURS = (process.env.SLOT_HOURS_UTC || "6,18").split(",").map(Number).sort((a, b) => a - b);
const MAX_LATE_H = 4;                               // a slot not done within 4h is skipped rather than posted stale
const HL = "https://api.hyperliquid.xyz/info";
const LT = "https://mainnet.zklighter.elliot.ai/api/v1";
const IV_MS = { "5m": 300e3, "15m": 900e3 };

/* ------------------------------------------------------------------ http */
const sleep = ms => new Promise(r => setTimeout(r, ms));
async function getJSON(url, body, tries = 5) {
  for (let i = 0; i < tries; i++) {
    try {
      const r = await fetch(url, body ? { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) } : { headers: { "User-Agent": "perp-spread-desk-bot" } });
      if (r.status === 429) { await sleep(5000 * (i + 1)); continue; }
      if (!r.ok) throw new Error(`${r.status} ${r.statusText} ${url}`);
      return await r.json();
    } catch (e) { if (i === tries - 1) throw e; await sleep(2000 * (i + 1)); }
  }
}

/* ------------------------------------------------------------------ asset matching (same tables as the site) */
const ALIASES = [
  ["SP500", "US500", "USA500", "US500-SWAP", "SPX500"], ["NASDAQ100", "XYZ100", "US100", "US100-SWAP", "NDX", "NAS100"],
  ["DOW30", "US30", "US30-SWAP"], ["RUSSELL2000", "US2K-SWAP", "US2000"], ["GOLD", "XAU", "XAU-SWAP"], ["SILVER", "XAG", "XAG-SWAP"],
  ["WTI", "CL", "USOIL-SWAP", "OIL"], ["BRENT", "BRENTOIL", "UKOIL-SWAP", "BZ"], ["NATGAS", "USFNG-SWAP"], ["COPPER", "XCU", "COPP-SWAP"],
  ["PLATINUM", "XPT"], ["PALLADIUM", "XPD"], ["EURUSD", "EUR"], ["GBPUSD", "GBP"], ["USDJPY", "JPY"],
  ["ANTHROPIC", "ANTH", "ANT"], ["OPENAI", "OAI"], ["SPACEX", "SPCX"], ["SKHYNIX", "SKHX", "SKHY", "SKHYNIXUSD"],
  ["SAMSUNG", "SMSN", "SAMSUNGUSD"], ["HYUNDAI", "HYUNDAIUSD"], ["TENCENT", "TCNT"], ["US10Y", "10Y"],
  ["1000PEPE", "KPEPE"], ["1000BONK", "KBONK"], ["1000SHIB", "KSHIB"], ["1000FLOKI", "KFLOKI"],
];
const ALIAS_OF = Object.create(null);
for (const g of ALIASES) for (const n of g) ALIAS_OF[n] = g[0];
const canonOf = t => { const u = t.toUpperCase().replace(/-PERP$/, ""); return ALIAS_OF[u] || u; };
const NICE = { SKHYNIX: "SK Hynix", SAMSUNG: "Samsung", USDJPY: "USD/JPY", GBPUSD: "GBP/USD", EURUSD: "EUR/USD", SP500: "S&P 500", NASDAQ100: "Nasdaq 100" };
const nice = c => NICE[c] || c;
const INDEXES = new Set(["SP500", "NASDAQ100", "DOW30", "RUSSELL2000", "TUR30", "JP225", "KR200"]);
const COMMODITIES = new Set(["GOLD", "SILVER", "WTI", "BRENT", "NATGAS", "COPPER", "PLATINUM", "PALLADIUM"]);
const PRE_IPO = new Set(["ANTHROPIC", "OPENAI", "SPACEX", "XTEN", "OURA", "VARIATIONAL"]);
const TRADFI = new Set(["equity", "index", "commodity", "fx"]);
function classOf(ms, canon) {
  if (PRE_IPO.has(canon)) return "preipo";
  if (INDEXES.has(canon)) return "index";
  if (COMMODITIES.has(canon)) return "commodity";
  if (ms.some(m => m.ex === "hl")) return "crypto";
  const l = ms.find(m => m.lighterCat);
  if (l) {
    const { strategy, fpm } = l.lighterCat;
    if (+fpm === 1) return "preipo";
    if (+fpm === 100) return "crypto";
    return strategy === 3 ? "commodity" : strategy === 4 ? "fx" : strategy === 7 ? "index" : "equity";
  }
  return "equity";
}

/* ------------------------------------------------------------------ markets */
async function hlMarkets() {
  const dexList = (await getJSON(HL, { type: "perpDexs" })).filter(Boolean);
  const full = Object.fromEntries(dexList.map(d => [d.name, d.fullName || d.name]));
  const dexes = [""].concat(dexList.map(d => d.name));
  const out = [];
  for (const dex of dexes) {
    const [meta, ctx] = await getJSON(HL, dex ? { type: "metaAndAssetCtxs", dex } : { type: "metaAndAssetCtxs" });
    meta.universe.forEach((a, i) => {
      const c = ctx[i]; if (!c || a.isDelisted) return;
      const s = +a.deployerFeeScale || 0, scale = dex ? (s < 1 ? s + 1 : s * 2) * (a.growthMode === "enabled" ? 0.1 : 1) : 1;
      out.push({ venue: "hl", ex: dex ? "hl-" + dex : "hl", label: dex ? "HL " + dex : "HL main", name: dex ? `Hyperliquid ${full[dex]}` : "Hyperliquid", id: a.name, ticker: a.name.split(":").pop(),
        price: +(c.midPx ?? c.markPx), vol: +c.dayNtlVlm, fundHr: +c.funding, taker: 0.00045 * scale, maker: 0.00015 * scale });
    });
    await sleep(250);
  }
  return out;
}
async function lighterMarkets() {
  const d = await getJSON(`${LT}/orderBookDetails`);
  const fr = await getJSON(`${LT}/funding-rates`).catch(() => ({ funding_rates: [] }));   // "rate" is per 8h for exchange "lighter"
  const f8 = new Map(fr.funding_rates.filter(x => x.exchange === "lighter").map(x => [String(x.market_id), +x.rate]));
  return d.order_book_details.filter(m => m.status === "active" && m.market_type === "perp").map(m => ({
    venue: "lighter", ex: "lighter", label: "Lighter", name: "Lighter", id: String(m.market_id), ticker: m.symbol, price: +m.last_trade_price,
    vol: +m.daily_quote_token_volume, taker: 0, maker: 0, fundHr: f8.has(String(m.market_id)) ? f8.get(String(m.market_id)) / 8 : NaN, lighterCat: { strategy: m.strategy_index, fpm: m.funding_premium_multiplier } }));
}
function pairsFrom(markets) {
  const by = new Map();
  for (const m of markets) { if (!(m.price > 0)) continue; const c = canonOf(m.ticker); if (!by.has(c)) by.set(c, []); by.get(c).push(m); }
  const pairs = [];
  for (const [c, list] of by) {
    list.sort((a, b) => a.price - b.price);
    const clusters = [];
    for (const m of list) { const cl = clusters.at(-1); if (cl && m.price / cl[0].price < 1.6) cl.push(m); else clusters.push([m]); }
    for (const cl of clusters) {
      const per = new Map();
      for (const m of cl) { const o = per.get(m.ex); if (!o || m.vol > o.vol) per.set(m.ex, m); }
      const ms = [...per.values()].filter(m => m.vol >= CFG.minVol);
      const cls = classOf([...per.values()], c);
      if (!TRADFI.has(cls)) continue;                                   // stocks, indices, FX, commodities only
      for (let i = 0; i < ms.length; i++) for (let j = i + 1; j < ms.length; j++) {
        const [A, B] = ms[i].vol >= ms[j].vol ? [ms[i], ms[j]] : [ms[j], ms[i]];
        pairs.push({ canon: c, cls, A, B, minVol: Math.min(A.vol, B.vol) });
      }
    }
  }
  return pairs.sort((x, y) => y.minVol - x.minVol);
}

/* ------------------------------------------------------------------ candles + books */
const candleCache = new Map(), bookCache = new Map();
async function candles(m) {
  if (candleCache.has(m.ex + m.id)) return candleCache.get(m.ex + m.id);
  const end = Date.now(), start = end - CFG.days * 86400e3;
  let rows;
  if (m.venue === "hl") {
    const c = await getJSON(HL, { type: "candleSnapshot", req: { coin: m.id, interval: CFG.iv, startTime: start, endTime: end } });
    rows = c.map(x => [x.t, +x.c, +x.n > 0, +x.v * +x.c]);            // [t, close, traded, $ volume]
  } else {
    const step = IV_MS[CFG.iv] / 1000, got = new Map();
    let e = Math.floor(end / 1000); const s0 = Math.floor(start / 1000);
    for (let g = 0; g < 40 && e > s0; g++) {
      const page = (await getJSON(`${LT}/candles?market_id=${m.id}&resolution=${CFG.iv}&start_timestamp=${Math.max(s0, e - 500 * step)}&end_timestamp=${e}&count_back=500`)).c || [];
      if (!page.length) break;
      for (const x of page) got.set(x.t, [x.t, +x.c, +x.V > 0, +x.V]);
      const first = Math.floor(page[0].t / 1000); if (first >= e) break; e = first - 1;
      await sleep(150);
    }
    rows = [...got.values()].sort((a, b) => a[0] - b[0]);
  }
  candleCache.set(m.ex + m.id, rows);
  return rows;
}
async function book(m) {
  if (bookCache.has(m.ex + m.id)) return bookCache.get(m.ex + m.id);
  let b;
  if (m.venue === "hl") {
    const lv = (await getJSON(HL, { type: "l2Book", coin: m.id })).levels;
    b = { bids: lv[0].map(l => [+l.px, +l.sz]), asks: lv[1].map(l => [+l.px, +l.sz]) };
  } else {
    const r = await getJSON(`${LT}/orderBookOrders?market_id=${m.id}&limit=100`);
    const side = a => a.map(o => [+o.price, +o.remaining_base_amount]);
    b = { bids: side(r.bids || []).sort((a, c) => c[0] - a[0]), asks: side(r.asks || []).sort((a, c) => a[0] - c[0]) };
  }
  bookCache.set(m.ex + m.id, b);
  return b;
}
function walk(levels, qty) { let left = qty, cost = 0; for (const [px, sz] of levels) { const t = Math.min(left, sz); cost += t * px; left -= t; if (left <= 1e-12) break; } return left > 1e-12 ? null : cost / qty; }
function roundTripBps(b, qty) {
  if (!b?.bids.length || !b?.asks.length) return null;
  const mid = (b.bids[0][0] + b.asks[0][0]) / 2, buy = walk(b.asks, qty), sell = walk(b.bids, qty);
  return buy == null || sell == null ? null : ((buy - mid) + (mid - sell)) / mid * 1e4;
}

/* ------------------------------------------------------------------ the rule */
function sessionOf(t) {     // weekend = US equity markets shut (Fri 16:00 → Sun 18:00 New York)
  const p = Object.fromEntries(new Intl.DateTimeFormat("en-US", { timeZone: "America/New_York", weekday: "short", hour: "numeric", minute: "numeric", hour12: false }).formatToParts(new Date(t)).map(x => [x.type, x.value]));
  const min = (+p.hour % 24) * 60 + +p.minute;
  return p.weekday === "Sat" || (p.weekday === "Sun" && min < 1080) || (p.weekday === "Fri" && min >= 960) ? "weekend" : "week";
}
function localMidnightAfter(t) {
  const f = new Intl.DateTimeFormat("en-CA", { timeZone: CFG.tz, year: "numeric", month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit", hour12: false });
  const parts = Object.fromEntries(f.formatToParts(new Date(t)).map(x => [x.type, x.value]));
  const minsLeft = (23 - (+parts.hour % 24)) * 60 + (59 - +parts.minute);
  return t + minsLeft * 60e3;
}
function analyse(p, rows, costAt) {
  const mapB = new Map(rows.b.map(r => [r[0], r]));
  const d = { t: [], pa: [], pb: [], act: [], va: [], vb: [] };
  for (const r of rows.a) { const o = mapB.get(r[0]); if (!o) continue; d.t.push(r[0]); d.pa.push(r[1]); d.pb.push(o[1]); d.act.push(r[2] && o[2]); d.va.push(r[3] || 0); d.vb.push(o[3] || 0); }
  const n = d.t.length, step = IV_MS[CFG.iv], g = i => (d.pb[i] / d.pa[i] - 1) * 1e4;
  // cost to cross both books at each bar (avg of the two venues' $10k round trips), from the recorded samples near that time
  const cx = d.t.map(t => costAt(t)), cross = i => cx[i].bps, need = i => CFG.entry + cross(i);
  // a price only counts if real size traded there: both venues must have done >= the trade size in the 30 min to entry
  const tradedEnough = e => { let a = 0, b = 0; for (let k = Math.max(0, e - 5); k <= e; k++) { a += d.va[k]; b += d.vb[k]; } return a >= CFG.size && b >= CFG.size; };
  const trades = [];
  let thinPrints = 0;
  let i = 0, spreadOnly = 0;
  // windows on mid prices, to count the ones that were only the spreads
  const windows = lvl => { const w = []; let k = 0; while (k < n) { if (!(d.act[k] && Math.abs(g(k)) >= lvl(k))) { k++; continue; } const side = Math.sign(g(k)); let j = k, last = k, peak = Math.abs(g(k)); while (j + 1 < n && Math.sign(g(j + 1)) === side && Math.abs(g(j + 1)) >= lvl(j + 1) * 0.7) { j++; if (Math.abs(g(j)) >= lvl(j)) last = j; peak = Math.max(peak, Math.abs(g(j))); } w.push({ i0: k, i1: last, side, peak, start: d.t[k], minutes: (d.t[last] + step - d.t[k]) / 60e3 }); k = j + 1; } return w.filter(x => x.minutes >= CFG.minWin); };
  const real = windows(need), flat = () => CFG.entry;
  const held60 = w => { const k = Math.round(CFG.minWin * 60e3 / step); let best = 0;
    for (let i = w.i0; i + k - 1 <= w.i1; i++) { let m = Infinity; for (let j = i; j < i + k; j++) if (d.act[j]) m = Math.min(m, w.side * g(j)); if (m < Infinity) best = Math.max(best, m); }
    return best; };
  const loose = windows(flat).map(w => ({ ...w, held: held60(w), real: real.some(r => r.start <= d.t[w.i1] && d.t[r.i1] >= w.start), need: need(w.i0), cross: cross(w.i0) }));
  spreadOnly = Math.max(0, loose.length - real.length);
  for (const op of real) {
    let e = op.i0; while (e <= op.i1 && d.t[e] - op.start < CFG.react * 60e3) e++;
    while (e <= op.i1 && !(d.act[e] && Math.abs(g(e)) >= need(e))) e++;
    if (e > op.i1 || e >= n - 1) continue;
    if (!tradedEnough(e)) { thinPrints++; continue; }                 // the gap was a few small prints, not a fillable price
    const deadline = localMidnightAfter(d.t[e]);
    let x = -1, reason = "";
    for (let k = e + 1; k < n; k++) {
      if (d.t[k] >= deadline) { x = k; reason = "deadline"; break; }
      if (!d.act[k]) continue;
      if (Math.abs(g(k)) <= CFG.exit || Math.sign(g(k)) === -op.side) { x = k; reason = Math.sign(g(k)) === -op.side && Math.abs(g(k)) > CFG.exit ? "flipped" : "converged"; break; }
    }
    if (x < 0) { trades.push({ open: true, tIn: d.t[e], gIn: g(e), side: op.side, peak: op.peak }); continue; }
    const q = CFG.size / d.pa[e], notional = q * (d.pa[e] + d.pa[x]) / 2;
    const gross = q * op.side * ((d.pb[e] - d.pa[e]) - (d.pb[x] - d.pa[x]));
    const crossIn = cross(e), crossOut = cross(x);                       // crossing both books on the way in, and again on the way out
    const cost = notional * (2 * (p.A.taker + p.B.taker) + (crossIn + crossOut) / 1e4);
    trades.push({ tIn: d.t[e], tOut: d.t[x], gIn: g(e), gOut: g(x), fillIn: Math.abs(g(e)) - crossIn, costSrc: [cx[e].src, cx[x].src], side: op.side, peak: op.peak, net: gross - cost, hours: (d.t[x] - d.t[e]) / 3600e3, reason, weekend: sessionOf(d.t[e]) === "weekend",
      pxIn: [d.pa[e], d.pb[e]], pxOut: [d.pa[x], d.pb[x]], grossBps: gross / notional * 1e4, costBps: cost / notional * 1e4, grossUsd: gross, costUsd: cost,
      feeUsd: notional * 2 * (p.A.taker + p.B.taker), bookUsd: notional * (crossIn + crossOut) / 1e4,
      feeBps: 2 * (p.A.taker + p.B.taker) * 1e4, crossBps: (crossIn + crossOut) / 2 });
  }
  return { trades, spreadOnly, thinPrints, bars: n, d, g, loose };
}

/* ------------------------------------------------------------------ quality guard (same checks as the site) */
function spikesPerDay(p, other) {
  const r = p.map((v, i) => i ? Math.log(v / p[i - 1]) : 0), ro = other.map((v, i) => i ? Math.log(v / other[i - 1]) : 0);
  let n = 0;
  for (let i = 49; i < r.length - 3; i++) {
    let s2 = 0; for (let k = i - 48; k < i; k++) s2 += r[k] * r[k];
    const sd = Math.sqrt(s2 / 48) || 1e-9;
    if (Math.abs(r[i]) > 5 * sd && Math.abs(r[i]) > 0.002 && Math.abs(ro[i]) < 1.5 * sd && -(r[i + 1] + r[i + 2] + r[i + 3]) / r[i] > 0.6) n++;
  }
  return n / CFG.days;
}
function quality(p, rows) {
  const mapB = new Map(rows.b.map(r => [r[0], r])), pa = [], pb = [], act = [];
  for (const r of rows.a) { const o = mapB.get(r[0]); if (!o) continue; pa.push(r[1]); pb.push(o[1]); act.push(r[2] && o[2]); }
  const flags = [];
  let score = 100;
  if (p.minVol < 1e6) { score -= 15; flags.push("thin volume"); }
  const stale = 1 - act.filter(Boolean).length / Math.max(1, act.length);
  if (stale > 0.2) { score -= 15; flags.push(`${Math.round(stale * 100)}% stale bars`); }
  const sp = Math.max(spikesPerDay(pa, pb), spikesPerDay(pb, pa));
  if (sp >= 0.5) { score -= Math.min(30, sp * 12); flags.push(`${sp.toFixed(1)} one-venue spikes/day`); }
  return { score: Math.round(score), flags };
}

/* ------------------------------------------------------------------ order-book costs over time */
// COSTS.m[venueKey] = [[unixSec, roundTripBps for $10k], ...], recorded every ~30 min (--record).
let COSTS = { m: {} };
try { const j = JSON.parse(fs.readFileSync(CFG.costsFile, "utf8")); if (j && j.m) COSTS = j; } catch { }
const mkey = m => `${m.ex}|${m.id}`;
const KEEP_S = 14 * 86400, NEAR_S = 45 * 60;
const median = xs => { const v = xs.filter(Number.isFinite).sort((a, b) => a - b); if (!v.length) return NaN; const k = v.length >> 1; return v.length % 2 ? v[k] : (v[k - 1] + v[k]) / 2; };
// Cost for market m at time t: median of samples within 45 min ("measured"), else the same time of day on
// other days ("typical"), else all samples ("typical"), else the live book now ("live"). Posts say which.
function costFor(m, liveBps) {
  const xs = COSTS.m[mkey(m)] || [];
  const tod = s => ((s % 86400) + 86400) % 86400;
  return t => {
    const ts = t / 1000;
    let near = xs.filter(x => Math.abs(x[0] - ts) <= NEAR_S).map(x => x[1]);
    if (near.length) return { bps: median(near), src: "measured" };
    const dt = s => { const d = Math.abs(tod(s) - tod(ts)); return Math.min(d, 86400 - d); };
    near = xs.filter(x => dt(x[0]) <= 3600).map(x => x[1]);
    if (near.length >= 2) return { bps: median(near), src: "typical" };
    if (xs.length >= 4) return { bps: median(xs.map(x => x[1])), src: "typical" };
    return { bps: liveBps, src: "live" };
  };
}

// --record: snapshot the $10k round-trip cost of every market in a tradfi pair, append, save
async function record() {
  const [hl, lt] = await Promise.all([hlMarkets(), lighterMarkets()]);
  const pairs = pairsFrom([...hl, ...lt]), seen = new Map();
  for (const p of pairs) for (const m of [p.A, p.B]) seen.set(mkey(m), m);
  const now = Math.floor(Date.now() / 1000); let ok = 0;
  for (const m of seen.values()) {
    try {
      const b = await book(m); if (!b?.bids.length || !b?.asks.length) continue;
      const mid = (b.bids[0][0] + b.asks[0][0]) / 2, rt = roundTripBps(b, CFG.size / mid);
      if (rt == null) continue;
      (COSTS.m[mkey(m)] ||= []).push([now, +rt.toFixed(2)]); ok++;
    } catch (e) { console.error("book", mkey(m), e.message); }
    await sleep(m.venue === "hl" ? 120 : 250);
  }
  for (const k of Object.keys(COSTS.m)) { COSTS.m[k] = COSTS.m[k].filter(x => x[0] >= now - KEEP_S); if (!COSTS.m[k].length) delete COSTS.m[k]; }
  fs.writeFileSync(CFG.costsFile, JSON.stringify(COSTS));
  console.log(`Recorded ${ok}/${seen.size} books at ${new Date(now * 1000).toISOString()}; ${Object.keys(COSTS.m).length} markets on file.`);
}

/* ------------------------------------------------------------------ run the scan */
async function scan() {
  const [hl, lt] = await Promise.all([hlMarkets(), lighterMarkets().catch(e => { console.error("Lighter unavailable:", e.message); return []; })]);
  const pairs = pairsFrom([...hl, ...lt]);
  console.error(`${hl.length} HL + ${lt.length} Lighter markets, ${pairs.length} tradfi pairs`);
  const results = [];
  let skipped = 0;
  for (const p of pairs) {
    try {
      const [a, b] = [await candles(p.A), await candles(p.B)];
      const q = CFG.size / p.A.price;
      const [ba, bb] = [await book(p.A), await book(p.B)];
      const ca = roundTripBps(ba, q), cb = roundTripBps(bb, q);
      if (ca == null || cb == null) { skipped++; continue; }                 // book too thin for the size
      const fa = costFor(p.A, ca), fb = costFor(p.B, cb);
      const costAt = t => { const x = fa(t), y = fb(t), src = [x.src, y.src];
        return { bps: (x.bps + y.bps) / 2, src: src.includes("live") ? "live" : src.includes("typical") ? "typical" : "measured" }; };
      const r = analyse(p, { a, b }, costAt);
      if (r.bars < 50) { skipped++; continue; }
      const q2 = quality(p, { a, b });
      results.push({ p, crossBps: (ca + cb) / 2, rtA: ca, rtB: cb, quality: q2, ...r });
      if (process.env.DEBUG && r.trades.length) console.error(`${p.canon} ${p.A.label}/${p.B.label}: q${q2.score} [${q2.flags.join(", ")}], trades ${r.trades.map(t => t.open ? "open" : `${t.net.toFixed(0)}(${t.costSrc})`).join(" ")}`);
    } catch (e) { skipped++; console.error("skip", p.canon, p.A.label, p.B.label, e.message); }
  }
  return { results, pairs: pairs.length, skipped };
}

/* ------------------------------------------------------------------ realized funding over the window */
// Average hourly funding actually paid in [from, to), as a fraction. Positive = longs pay shorts.
// (The live "current" rate is only a prediction for this hour and swings hard, so it isn't used.)
const fundCache = new Map();
async function fundingAvg(m, from, to) {
  const k = m.ex + m.id; if (fundCache.has(k)) return fundCache.get(k);
  let xs = [];
  if (m.venue === "hl") {
    const h = await getJSON(HL, { type: "fundingHistory", coin: m.id, startTime: from, endTime: to });
    xs = h.map(x => +x.fundingRate);
  } else {
    const r = await getJSON(`${LT}/fundings?market_id=${m.id}&resolution=1h&start_timestamp=${Math.floor(from / 1000)}&end_timestamp=${Math.floor(to / 1000)}&count_back=24`);
    xs = (r.fundings || []).map(x => (x.direction === "short" ? -1 : 1) * +x.rate / 100);   // Lighter: % per hour + direction
  }
  const v = xs.length >= 6 ? xs.reduce((a, b) => a + b, 0) / xs.length : NaN;
  fundCache.set(k, v); return v;
}
async function addFunding(scan, from, to) {
  for (const r of scan.results) {
    try { r.fA = await fundingAvg(r.p.A, from, to); r.fB = await fundingAvg(r.p.B, from, to); } catch { r.fA = r.fB = NaN; }
    await sleep(60);
  }
}

/* ------------------------------------------------------------------ writing the posts */
const fmt$ = v => (v < 0 ? "-$" : "+$") + Math.abs(v).toFixed(Math.abs(v) < 100 ? 0 : 0);
const hhmm = t => new Intl.DateTimeFormat("en-GB", { timeZone: "UTC", hour: "2-digit", minute: "2-digit" }).format(new Date(t));      // posts use UTC
const dayName = t => new Intl.DateTimeFormat("en-GB", { timeZone: "UTC", weekday: "short", day: "numeric", month: "short" }).format(new Date(t));
// X's own weighting (twitter-text v3): these code points count 1, everything else (→ • ↓ … emoji, CJK) counts 2
const xLen = s => [...s].reduce((n, ch) => { const c = ch.codePointAt(0); return n + ((c <= 4351 || (c >= 8192 && c <= 8205) || (c >= 8208 && c <= 8223) || (c >= 8242 && c <= 8247)) ? 1 : 2); }, 0);
function fit(s, max = 275) { if (xLen(s) <= max) return s; let out = s; while (xLen(out + "…") > max) out = out.slice(0, -1); return out.trimEnd() + "…"; }

/* The slot a run belongs to: the latest slot hour (UTC) at or before `t`.
   Posts are labelled and windowed by the slot, not by when GitHub happened to start the run. */
function slotOf(t = Date.now()) {
  const day = Math.floor(t / 864e5) * 864e5, h = new Date(t).getUTCHours();
  const s = [...SLOT_HOURS].reverse().find(x => h >= x);
  return s === undefined ? day - 864e5 + SLOT_HOURS.at(-1) * 3600e3 : day + s * 3600e3;
}

const pct = bps => (Math.abs(bps) / 100).toFixed(2) + "%";
function px(v) { const d = v >= 1000 ? 1 : v >= 100 ? 2 : v >= 1 ? 3 : 5; return "$" + v.toLocaleString("en-US", { minimumFractionDigits: d, maximumFractionDigits: d }); }
const dur = h => h < 1 ? Math.round(h * 60) + " min" : (Math.round(h * 10) / 10) + "h";

const sgnPct = bps => (bps >= 0 ? "+" : "−") + pct(bps);
const usd = v => (v < 0 ? "−$" : "+$") + Math.abs(Math.round(v));
const hm = m => m >= 60 ? `${Math.floor(m / 60)}h${String(Math.round(m % 60)).padStart(2, "0")}` : `${Math.round(m)}m`;
function sessionName(t) {
  if (sessionOf(t) === "weekend") return "weekend";
  const h = new Date(t).getUTCHours() + new Date(t).getUTCMinutes() / 60;
  return h < 7 ? "Asia" : h < 13.5 ? "Europe" : h < 20 ? "US" : "US after-hours";
}
// pack paragraphs into as few posts as fit
function pack(paras, max = 280) {
  const out = []; let cur = "";
  for (const para of paras.filter(Boolean)) { const next = cur ? cur + "\n\n" + para : para; if (xLen(next) <= max) cur = next; else { if (cur) out.push(cur); cur = para; } }
  if (cur) out.push(cur);
  return out;
}

// one post: title, then as many lines as fit (best first), footer always kept
function section(title, lines, footer = "") {
  let cur = title;
  for (const l of lines) { const next = cur + "\n" + l; if (xLen(next + (footer ? "\n\n" + footer : "")) <= 280) cur = next; else break; }
  return [footer ? cur + "\n\n" + footer : cur];
}

const sesProse = (t, name = sessionName(t)) => ({ Asia: "Asian hours", Europe: "European hours", US: "US market hours", "US after-hours": "US after-hours", weekend: "the weekend" })[name] || name;

function compose(scan, slot = slotOf()) {
  const now = slot, from = slot - 12 * 3600e3;                            // each report covers the 12 hours ending at its slot
  const inRange = t => t >= from && t < now;
  const clean = scan.results.filter(r => r.quality.score >= 65);
  const flagged = scan.results.filter(r => r.quality.score < 65);
  const flaggedWins = flagged.filter(r => r.trades.some(t => !t.open && inRange(t.tIn)));
  const all = clean.flatMap(r => r.trades.map(t => ({ ...t, r })));
  const inWin = all.filter(t => inRange(t.tIn) && !t.open).sort((a, b) => b.net - a.net);
  const last3 = all.filter(t => !t.open && t.tIn < now);
  const total = inWin.reduce((s, t) => s + t.net, 0);
  const spreadOnly = scan.results.reduce((s, r) => s + r.spreadOnly, 0);
  const thinPrints = scan.results.reduce((s, r) => s + r.thinPrints, 0);
  const nPairs = scan.results.length;
  const isNight = new Date(now).getUTCHours() < 12;
  const period = isNight ? "Overnight" : "Daytime";
  const span = `${hhmm(from)}–${hhmm(now)} UTC`;
  // prose venue names: "Paragon on Hyperliquid", "Lighter"
  const vn = m => m.venue === "hl" ? (m.ex === "hl" ? "Hyperliquid" : `${m.name.replace("Hyperliquid ", "")} on Hyperliquid`) : m.name;
  const vs = m => m.name.replace("Hyperliquid ", "");                     // short: "Paragon", "XYZ", "Lighter"
  const rich = t => t.side > 0 ? { m: t.r.p.B, i: 1 } : { m: t.r.p.A, i: 0 };
  const cheap = t => t.side > 0 ? { m: t.r.p.A, i: 0 } : { m: t.r.p.B, i: 1 };
  const posts = [];

  // ---- 1. what this is, then the headline
  const intro = `Same stock, two exchanges, two prices. I track ${nPairs} stock, index, FX and commodity perps on Hyperliquid and Lighter to see if those gaps can actually be traded after fees and spreads.`;
  let headline;
  if (inWin.length) {
    const names = [...new Set(inWin.map(t => nice(t.r.p.canon)))];
    headline = `${period} (${span}): ${inWin.length === 1 ? `one trade cleared the bar, in ${names[0]}` : `${inWin.length} trades cleared the bar (${names.slice(0, 3).join(", ")}), ${usd(total)} combined on $10k a side`}.`;
  } else {
    headline = `${period} (${span}): nothing cleared the bar. The gaps that showed up were too small to survive the cost of crossing both books.`;
  }
  posts.push(`${intro}\n\n${headline}`);

  // ---- 2. the best trade, told as a story
  if (inWin.length) {
    const t = inWin[0], R = rich(t), C = cheap(t);
    const exitTxt = t.reason === "flipped" ? `closed and overshot by ${pct(Math.abs(t.gOut))}`
      : t.reason === "deadline" ? `was still ${pct(Math.abs(t.gOut))} wide at the end of the day, so it was closed there`
      : `closed to ${pct(Math.abs(t.gOut))}`;
    const costs = Math.round(t.feeUsd) + Math.round(t.bookUsd);
    const both = R.m.venue === "hl" && C.m.venue === "hl" && R.m.ex !== "hl" && C.m.ex !== "hl";
    const lst = m => m.venue === "hl" && m.ex !== "hl" ? `Hyperliquid's ${vs(m)} listing` : vn(m);
    const who = both ? `Hyperliquid's ${vs(R.m)} listing traded ${pct(Math.abs(t.gIn))} above ${vs(C.m)}'s` : `${lst(R.m)} traded ${pct(Math.abs(t.gIn))} above ${lst(C.m)}`;
    posts.push(`${nice(t.r.p.canon)}: ${who} for 1h+ during ${sesProse(t.tIn)}` +
      (t.peak > Math.abs(t.gIn) * 1.2 ? `, peaking at ${pct(t.peak)}` : "") + `.\n\n` +
      `Short ${vs(R.m)} at ${px(t.pxIn[R.i])}, long ${vs(C.m)} at ${px(t.pxIn[C.i])}. The gap ${exitTxt} within ${dur(t.hours)}.\n\n` +
      `On $10k a side: ${usd(t.grossUsd)} gross, −$${costs} fees and spread${t.costSrc.every(x => x === "measured") ? "" : " (est.)"}, ${usd(Math.round(t.grossUsd) - costs)} net.`);
  }

  // ---- 3. what it means: the most useful observations, ranked
  const notes = [];
  // a) near misses, when there was no trade (or one stands out)
  const nearBy = new Map();
  for (const r of clean) for (const w of r.loose) {
    if (w.real || !inRange(w.start) || !(w.held >= CFG.entry * 0.5) || w.held >= w.need) continue;
    if (inWin.some(t => t.r === r)) continue;
    const k = r.p.canon + r.p.A.ex + r.p.B.ex, o = nearBy.get(k);
    if (!o || w.held > o.w.held) nearBy.set(k, { r, w });
  }
  const near = [...nearBy.values()].sort((a, b) => (a.w.need - a.w.held) - (b.w.need - b.w.held));
  if (near.length) {
    const { r, w } = near[0];
    notes.push({ score: inWin.length ? 2 : 5, text: `Closest miss: ${nice(r.p.canon)}, ${vs(r.p.A)} vs ${vs(r.p.B)}, held a ${pct(w.held)} gap for an hour but needed ${pct(w.need)} to pay for crossing both books.` });
  }
  // b) a venue that is persistently rich, and whether its books let you use it
  const prem = new Map(), bookCost = new Map(), seen = new Set();
  for (const r of clean) {
    const ix = r.d.t.map((t, i) => i).filter(i => inRange(r.d.t[i]) && r.d.act[i]);
    if (ix.length < 24) continue;
    const m = median(ix.map(i => r.g(i)));
    for (const [v, x] of [[r.p.B, m], [r.p.A, -m]]) { const k = vs(v); if (!prem.has(k)) prem.set(k, { m: v, xs: [] }); prem.get(k).xs.push(x); }
    for (const [v, rt] of [[r.p.A, r.rtA], [r.p.B, r.rtB]]) { const k = v.ex + v.id; if (seen.has(k)) continue; seen.add(k); if (!bookCost.has(vs(v))) bookCost.set(vs(v), []); bookCost.get(vs(v)).push(rt); }
  }
  const venues = [...prem.values()].filter(x => x.xs.length >= 3).map(x => ({ m: x.m, med: median(x.xs), n: x.xs.length })).sort((a, b) => b.med - a.med);
  const cheapest = [...bookCost].map(([v, xs]) => ({ v, c: median(xs) })).filter(x => Number.isFinite(x.c)).sort((a, b) => a.c - b.c);
  if (venues.length >= 2 && Math.abs(venues[0].med) >= 8) {
    const top = venues[0], c = median(bookCost.get(vs(top.m)) || []);
    const lo = cheapest[0];
    notes.push({ score: 4, text: `${top.m.venue === "hl" && top.m.ex !== "hl" ? `Hyperliquid's ${vs(top.m)} market` : vn(top.m)} is the rich venue: its listings sat a median ${pct(top.med)} above the same assets elsewhere.` +
      (Number.isFinite(c) && lo ? ` But its books cost ${pct(c)} to get in and out of $10k, vs ${pct(lo.c)} on ${lo.v}, ${c >= top.med ? "which wipes out the whole premium" : c >= top.med * 0.5 ? "which eats most of it" : "so there is still room to work with"}.` : "") });
  }
  // c) funding: realized carry vs how much the price gap itself moved
  const apr = f => f * 24 * 365 * 100;
  const carry = scan.results.filter(r => Number.isFinite(r.fA) && Number.isFinite(r.fB))
    .map(r => ({ r, diff: r.fB - r.fA })).sort((a, b) => Math.abs(b.diff) - Math.abs(a.diff));
  if (carry.length && Math.abs(apr(carry[0].diff)) >= 20) {
    const { r, diff } = carry[0];
    const [hi, lo, fh, fl] = diff > 0 ? [r.p.B, r.p.A, r.fB, r.fA] : [r.p.A, r.p.B, r.fA, r.fB];
    const ix = r.d.t.map((t, i) => i).filter(i => inRange(r.d.t[i]) && r.d.act[i]);
    const gs = ix.map(i => r.g(i)), swing = gs.length ? Math.max(...gs) - Math.min(...gs) : NaN;
    const perDay = Math.abs(diff) * 24 * 1e4;
    const crowd = fh < 0 && fl < 0 ? ` Shorts pay on both venues: a crowded short.` : fh > 0 && fl > 0 ? ` Longs pay on both venues: a crowded long.` : "";
    notes.push({ score: 3, text: `Funding: ${nice(r.p.canon)} paid ${apr(fh).toFixed(0)}% a year on ${vs(hi)} vs ${apr(fl).toFixed(0)}% on ${vs(lo)}.${crowd} The carry is worth ≈$${perDay.toFixed(0)}/day per $10k` +
      (Number.isFinite(swing) && swing > 0 ? `, but the price gap between those two swung ${pct(swing)} (≈$${Math.round(swing)}) in the same 12h.` : ".") });
  }
  // d) where trades come from
  if (last3.length >= 4) {
    const bySes = {}; for (const t of last3) bySes[sessionName(t.tIn)] = (bySes[sessionName(t.tIn)] || 0) + 1;
    const [topS, topN] = Object.entries(bySes).sort((a, b) => b[1] - a[1])[0];
    if (topN / last3.length >= 0.5) notes.push({ score: 2.5, text: `Pattern: ${topN} of the last ${last3.length} trades started during ${sesProse(null, topS)}.` });
  }
  // e) the filter is the point
  if (spreadOnly + thinPrints >= 10) notes.push({ score: 1, text: `For scale: ${spreadOnly + thinPrints} "gaps" in the last 3 days looked real on a price chart and weren't. ${spreadOnly} were just the bid/ask spread, ${thinPrints} were a few trades under $10k.` });
  if (flaggedWins.length) notes.push({ score: 1.5, text: `Ignored ${nice(flaggedWins[0].p.canon)} on ${vs(flaggedWins[0].p.A)}/${vs(flaggedWins[0].p.B)}: ${flaggedWins[0].quality.flags[0] || "data looked unreliable"}, more likely a bad print than a real gap.` });
  notes.sort((a, b) => b.score - a.score);
  // at most two posts of observations; a note that doesn't fit is dropped rather than padded
  const notePosts = [""];
  for (const n of notes) {
    const i = notePosts.length - 1, next = notePosts[i] ? notePosts[i] + "\n\n" + n.text : n.text;
    if (xLen(next) <= 280) notePosts[i] = next; else if (notePosts.length < 2 && n.score >= 2.5) notePosts.push(n.text);
  }
  posts.push(...notePosts.filter(Boolean));

  // ---- 4. scorecard + method, so the numbers can be judged
  const wins3 = last3.filter(t => t.net > 0).length, net3 = last3.reduce((s, t) => s + t.net, 0);
  posts.push(`Scorecard, last 3 days: ${last3.length} trade${last3.length === 1 ? "" : "s"}, ${wins3} profitable, ${usd(net3)} net on $10k a side.\n\n` +
    `The rule: at least 0.10% left after crossing both order books, held for an hour, real volume on both sides, closed the same day. Market orders, fees included.\n\nSimulated, not advice.`);

  for (const p of posts) if (xLen(p) > 280) console.error(`WARNING: post is ${xLen(p)} chars, will be cut:\n${p}`);
  return { session: span, posts: posts.map(p => fit(p, 280)), stats: { from, to: now, opportunities: inWin.length, total: +total.toFixed(2), spreadOnly, thinPrints, pairs: nPairs,
    best: inWin.slice(0, 3).map(t => ({ asset: t.r.p.canon, rich: rich(t).m.name, cheap: cheap(t).m.name, net: +t.net.toFixed(2), grossUsd: +t.grossUsd.toFixed(2), feeUsd: +t.feeUsd.toFixed(2), bookUsd: +t.bookUsd.toFixed(2), crossBps: +t.crossBps.toFixed(1), reason: t.reason, tIn: t.tIn, tOut: t.tOut, costSrc: t.costSrc })) } };
}

/* ------------------------------------------------------------------ X API (OAuth 1.0a user context) */
function oauthHeader(method, url, keys) {
  const enc = s => encodeURIComponent(s).replace(/[!'()*]/g, c => "%" + c.charCodeAt(0).toString(16).toUpperCase());
  const o = { oauth_consumer_key: keys.apiKey, oauth_nonce: crypto.randomBytes(16).toString("hex"), oauth_signature_method: "HMAC-SHA1",
    oauth_timestamp: String(Math.floor(Date.now() / 1000)), oauth_token: keys.accessToken, oauth_version: "1.0" };
  const params = Object.keys(o).sort().map(k => `${enc(k)}=${enc(o[k])}`).join("&");      // JSON body is not signed
  const base = [method.toUpperCase(), enc(url), enc(params)].join("&");
  o.oauth_signature = crypto.createHmac("sha1", `${enc(keys.apiSecret)}&${enc(keys.accessSecret)}`).update(base).digest("base64");
  return "OAuth " + Object.keys(o).sort().map(k => `${enc(k)}="${enc(o[k])}"`).join(", ");
}
async function tweet(text, replyTo, keys) {
  const url = "https://api.x.com/2/tweets";
  const body = replyTo ? { text, reply: { in_reply_to_tweet_id: replyTo } } : { text };
  const r = await fetch(url, { method: "POST", headers: { Authorization: oauthHeader("POST", url, keys), "Content-Type": "application/json" }, body: JSON.stringify(body) });
  const j = await r.json().catch(() => ({}));
  if (!r.ok) throw new Error(`X API ${r.status}: ${JSON.stringify(j).slice(0, 300)}`);
  return j.data.id;
}

/* ------------------------------------------------------------------ main */
if (process.argv.includes("--record")) { await record(); process.exit(0); }
let hist = [];
try { hist = JSON.parse(fs.readFileSync(CFG.historyFile, "utf8")); } catch { }
const slot = slotOf(process.env.SLOT_AT ? Date.parse(process.env.SLOT_AT) : Date.now());   // SLOT_AT: dry-run tests of a past slot
const slotISO = new Date(slot).toISOString();
const lateMin = Math.round((Date.now() - slot) / 60e3);
console.log(`Slot ${hhmm(slot)} UTC ${dayName(slot)}, ${slotISO}; this run started ${lateMin} min after it.`);
if (SLOTTED) {
  const done = hist.find(h => h.slot === slotISO && (h.posted || h.trigger === "schedule"));
  if (done) { console.log(`Slot already handled at ${done.at}: nothing to do.`); process.exit(0); }
  if (lateMin > MAX_LATE_H * 60) { console.log(`More than ${MAX_LATE_H}h after the slot: skipping it rather than posting late.`); process.exit(0); }
}
const scanned = await scan();
if (scanned.results.length < 5) { console.error(`Only ${scanned.results.length} pairs scanned: data looks wrong, not posting.`); process.exit(1); }
await addFunding(scanned, slot - 12 * 3600e3, slot);
const out = compose(scanned, slot);
console.log(`--- ${out.session} thread (${out.posts.length} posts) ---`);
out.posts.forEach((p, i) => console.log(`\n[${i + 1}] ${xLen(p)} chars\n${p}`));

if (POST) {
  const keys = { apiKey: process.env.X_API_KEY, apiSecret: process.env.X_API_SECRET, accessToken: process.env.X_ACCESS_TOKEN, accessSecret: process.env.X_ACCESS_SECRET };
  if (Object.values(keys).some(v => !v)) { console.log("\nNo X keys set: dry run only, nothing posted."); }
  else {
    let id = null; const ids = [];
    try { for (const p of out.posts) { id = await tweet(p, id, keys); ids.push(id); await sleep(1500); } }
    catch (e) { if (!ids.length) throw e; console.error(`Reply failed after the first post (kept, not retried): ${e.message}`); }
    console.log(`\nPosted: https://x.com/i/status/${ids[0]}`);
    out.stats.tweetIds = ids;
  }
}
// keep a short history (also keeps the repo active so GitHub keeps running the schedule)
hist.unshift({ at: new Date().toISOString(), slot: slotISO, trigger: SLOTTED ? "schedule" : "manual", lateMin,
  posted: POST && !!out.stats.tweetIds, ...out.stats, posts: out.posts });
fs.writeFileSync(CFG.historyFile, JSON.stringify(hist.slice(0, 120), null, 1));
