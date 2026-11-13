// Shared logic for the always-on flip watcher: load quotes from the three venues, group the same
// asset across venues (same rules as the website), compute the fill gap, and run the flip state machine.
// Pure functions + fetch only, so it runs in a Cloudflare Worker and in Node for tests.

export const HL_REST = "https://api.hyperliquid.xyz/info";
export const LT_REST = "https://mainnet.zklighter.elliot.ai/api/v1";
export const LT_STREAM = "https://mainnet.zklighter.elliot.ai/stream?readonly=true";   // Workers open sockets with https:// + Upgrade
export const VR_REST = "https://omni-client-api.prod.ap-northeast-1.variational.io/metadata/v2/stats";

/* ---------- asset matching: copied from app.js so the watcher and the site agree ---------- */
const ALIASES = [
  ["SP500", "US500", "USA500", "US500-SWAP", "SPX500"],
  ["NASDAQ100", "XYZ100", "US100", "US100-SWAP", "NDX", "NAS100"],
  ["DOW30", "US30", "US30-SWAP"], ["RUSSELL2000", "US2K-SWAP", "US2000"],
  ["GOLD", "XAU", "XAU-SWAP"], ["SILVER", "XAG", "XAG-SWAP"],
  ["WTI", "CL", "USOIL-SWAP", "OIL"], ["BRENT", "BRENTOIL", "UKOIL-SWAP", "BZ"],
  ["NATGAS", "USFNG-SWAP"], ["COPPER", "XCU", "COPP-SWAP"], ["PLATINUM", "XPT"], ["PALLADIUM", "XPD"],
  ["EURUSD", "EUR"], ["GBPUSD", "GBP"], ["USDJPY", "JPY"],
  ["ANTHROPIC", "ANTH", "ANT"], ["OPENAI", "OAI"], ["SPACEX", "SPCX"],
  ["SKHYNIX", "SKHX", "SKHY", "SKHYNIXUSD"], ["SAMSUNG", "SMSN", "SAMSUNGUSD"], ["HYUNDAI", "HYUNDAIUSD"],
  ["TENCENT", "TCNT"], ["US10Y", "10Y"],
  ["1000PEPE", "KPEPE"], ["1000BONK", "KBONK"], ["1000SHIB", "KSHIB"], ["1000FLOKI", "KFLOKI"],
];
const NOT_SAME = new Set(["hl|SPX"]);   // SPX on HL main = SPX6900 memecoin
const ALIAS_OF = {};
for (const g of ALIASES) for (const n of g) ALIAS_OF[n] = g[0];
export const canonOf = t => { const u = t.toUpperCase().replace(/-PERP$/, ""); return ALIAS_OF[u] || u; };
const INDEXES = new Set(["SP500", "NASDAQ100", "DOW30", "RUSSELL2000", "US400-SWAP", "CNA50-SWAP", "TUR30", "JP225", "KR200", "VXI-SWAP", "DXI-SWAP"]);
const COMMODITIES = new Set(["GOLD", "SILVER", "WTI", "BRENT", "NATGAS", "COPPER", "PLATINUM", "PALLADIUM", "COCOA-SWAP", "HOIL-SWAP", "GASOL-SWAP", "H100"]);
const PRE_IPO = new Set(["ANTHROPIC", "OPENAI", "SPACEX", "XTEN", "OURA", "VARIATIONAL"]);
function classOfMarket(m, canon) {
  if (PRE_IPO.has(canon)) return "preipo";
  if (INDEXES.has(canon)) return "index";
  if (COMMODITIES.has(canon)) return "commodity";
  if (m.lighterCat) {
    const { strategy, fpm } = m.lighterCat;
    if (+fpm === 1) return "preipo";
    if (+fpm === 100) return "crypto";
    if (strategy === 3) return "commodity";
    if (strategy === 4) return "fx";
    if (strategy === 7) return "index";
    return "equity";
  }
  if (m.varType === "swap") return "index";
  if (m.varType && /\b(Inc|Corp|Corporation|ETF|Ltd|Holdings|Trust|Group|plc|Company|Co\.|Fund|SA|AG|NV)\b/i.test(m.varName)) return "equity";
  if (m.venue === "hl" && m.exchange === "hl") return "crypto";
  if (m.venue === "hl") return "equity";
  return "crypto";
}
function classOfAsset(markets, canon) {
  const rank = m => m.lighterCat ? 0 : m.varType ? 1 : m.exchange === "hl" ? 2 : 3;
  const best = [...markets].sort((a, b) => rank(a) - rank(b))[0];
  if (markets.some(m => m.exchange === "hl") && !PRE_IPO.has(canon) && !INDEXES.has(canon) && !COMMODITIES.has(canon)) return "crypto";
  return best ? classOfMarket(best, canon) : "crypto";
}
export const TYPE_SETS = { tradfi: ["equity", "index", "commodity", "fx"], "tradfi+pre": ["equity", "index", "commodity", "fx", "preipo"], crypto: ["crypto"], all: null };
const median = xs => { const v = [...xs].sort((a, b) => a - b); return v[v.length >> 1]; };

/* ---------- venue loaders ---------- */
async function getJSON(url, body) {
  const r = await fetch(url, body ? { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) } : {});
  if (!r.ok) throw new Error(`${url.split("?")[0]} → ${r.status}`);
  return r.json();
}

// Hyperliquid: every dex's contexts; bid/ask = impact prices (what a small market order fills at), same as the site
export async function loadHL() {
  const dexes = (await getJSON(HL_REST, { type: "perpDexs" })).filter(Boolean);
  const names = Object.fromEntries(dexes.map(d => [d.name, d.fullName || d.name]));
  const list = [""].concat(dexes.map(d => d.name));
  const res = await Promise.allSettled(list.map(dex => getJSON(HL_REST, dex ? { type: "metaAndAssetCtxs", dex } : { type: "metaAndAssetCtxs" }).then(r => [dex, r])));
  const out = [];
  for (const r of res) {
    if (r.status !== "fulfilled") continue;
    const [dex, [meta, ctxs]] = r.value;
    meta.universe.forEach((a, i) => {
      const c = ctxs[i]; if (a.isDelisted || !c) return;
      const imp = c.impactPxs || [], mid = c.midPx != null ? +c.midPx : +c.markPx;
      const ticker = a.name.split(":").pop(), exchange = dex ? "hl-" + dex : "hl";
      if (NOT_SAME.has(`${exchange}|${ticker}`)) return;
      out.push({ key: `${exchange}|${a.name}`, venue: "hl", exchange, label: dex ? `HL ${dex}` : "HL main", longLabel: dex ? `Hyperliquid ${names[dex]}` : "Hyperliquid",
        ticker, price: mid, bid: imp[0] != null ? +imp[0] : null, ask: imp[1] != null ? +imp[1] : null, vol: +c.dayNtlVlm || 0 });
    });
  }
  return out;
}

// Lighter: market list (symbols, categories, volume) by REST; best bid/ask come from the stream (see collectLighter)
export async function loadLighterMeta() {
  const d = await getJSON(`${LT_REST}/orderBookDetails`);
  const meta = new Map();
  for (const m of d.order_book_details) {
    if (m.status !== "active" || m.market_type !== "perp") continue;
    meta.set(String(m.market_id), { symbol: m.symbol, lighterCat: { strategy: m.strategy_index, fpm: m.funding_premium_multiplier }, vol: +m.daily_quote_token_volume || 0 });
  }
  return meta;
}
export function lighterMarkets(meta, stats) {
  const out = [];
  for (const [id, s] of stats) {
    const m = meta.get(id); if (!m) continue;
    const bid = +s.best_bid_price || null, ask = +s.best_ask_price || null;
    const price = +s.mid_price || +s.last_trade_price || (bid && ask ? (bid + ask) / 2 : 0);
    out.push({ key: `lighter|${id}`, venue: "lighter", exchange: "lighter", label: "Lighter", longLabel: "Lighter", ticker: m.symbol,
      price, bid, ask, vol: s.daily_quote_token_volume != null ? +s.daily_quote_token_volume : m.vol, lighterCat: m.lighterCat, at: s._at });
  }
  return out;
}

// Variational: one snapshot with RFQ quotes for $1k. Closed swaps and quotes older than maxAgeSec are dropped.
export async function loadVariational(maxAgeSec = 60, now = Date.now()) {
  const d = await getJSON(VR_REST);
  const out = [];
  for (const m of d.listings || []) {
    const q = m.quotes || {}, k = q.size_1k || q.base || {};
    const bid = k.bid != null ? +k.bid : null, ask = k.ask != null ? +k.ask : null;
    const closed = m.contract_type === "swap" && !(m.funding && m.funding.next);
    const at = q.updated_at ? Date.parse(q.updated_at) : null;
    if (closed || !bid || !ask || (at && (now - at) / 1000 > maxAgeSec)) continue;
    out.push({ key: `variational|${m.ticker}`, venue: "variational", exchange: "variational", label: "Variational", longLabel: "Variational", ticker: m.ticker.replace(/-PERP$/, ""),
      price: (bid + ask) / 2, bid, ask, vol: +m.volume_24h || 0, varType: m.contract_type, varName: m.name || "", at });
  }
  return out;
}

/* ---------- group markets into assets (same rules as the site's rebuildAssets) ---------- */
export function groupAssets(markets, types = "tradfi+pre") {
  const byCanon = new Map();
  for (const m of markets) {
    if (!m.ticker || !(m.price > 0)) continue;
    const c = canonOf(m.ticker);
    if (!byCanon.has(c)) byCanon.set(c, []);
    byCanon.get(c).push(m);
  }
  const allow = TYPE_SETS[types] || null, assets = [];
  for (const [c, list] of byCanon) {
    list.sort((a, b) => a.price - b.price);
    const clusters = [];
    for (const m of list) { const cl = clusters[clusters.length - 1]; if (cl && m.price / cl[0].price < 1.6) cl.push(m); else clusters.push([m]); }
    const vol = cl => cl.reduce((s, m) => s + (m.vol || 0), 0), venues = cl => new Set(cl.map(m => m.exchange)).size;
    const main = [...clusters].sort((x, y) => venues(y) - venues(x) || vol(y) - vol(x))[0];
    for (const cl of clusters) {
      const per = new Map();
      for (const m of cl) { const o = per.get(m.exchange); if (!o || (m.vol || 0) > (o.vol || 0)) per.set(m.exchange, m); }
      const ms = [...per.values()];
      if (ms.length < 2) continue;
      const cls = classOfAsset(ms, c);
      if (allow && !allow.includes(cls)) continue;
      const key = cl === main ? c : `${c}~${Math.floor(Math.log10(cl[0].price))}`;
      const label = cl === main ? c : `${c} (≈${median(ms.map(x => x.price)).toPrecision(3)} size)`;
      assets.push({ key, label, cls, ms });
    }
  }
  return assets;
}

// best fill gap: sell at one venue's bid, buy at another's ask (bps), plus the thinner venue's daily volume
export function fillGap(a) {
  let exec = null, pair = null;
  for (const b of a.ms) for (const s of a.ms) {
    if (b === s || !(b.ask > 0) || !(s.bid > 0)) continue;
    const v = (s.bid / b.ask - 1) * 1e4;
    if (exec == null || v > exec) { exec = v; pair = { buy: b, sell: s }; }
  }
  return { exec, pair, minVol: Math.min(...a.ms.map(m => m.vol || 0)) };
}

/* ---------- flip state machine ----------
   state[assetKey] = { runDir, runStart, seen: {dir: {t, gap, sellLab}}, fired: {dir: t} }
   A flip fires when direction D (sell X / buy Y) has held ≥ thr for hold ms, and the reverse direction
   was ≥ thr earlier, at most win ms before D's run started. Fires once per reversal.            */
export function stepFlips(state, assets, now, cfg) {
  const events = [];
  for (const a of assets) {
    const { exec, pair, minVol } = fillGap(a);
    let k = state[a.key];
    const strong = exec != null && pair && exec >= cfg.thr && minVol >= cfg.minVol;
    if (!strong) { if (k) k.runDir = null; continue; }
    if (!k) k = state[a.key] = { runDir: null, runStart: 0, seen: {}, fired: {} };
    const dir = `${pair.sell.key}>${pair.buy.key}`, rev = `${pair.buy.key}>${pair.sell.key}`;
    if (k.runDir !== dir) { k.runDir = dir; k.runStart = now; }
    const prev = k.seen[rev];
    if (prev && prev.t < k.runStart && k.runStart - prev.t <= cfg.win && now - k.runStart >= cfg.hold && !(k.fired[dir] > prev.t)) {
      k.fired[dir] = now;
      events.push({ type: "flip", asset: a, exec, pair, minVol, prev, flippedMs: k.runStart - prev.t, heldMs: now - k.runStart });
    }
    const s = k.seen[dir];
    k.seen[dir] = { t: now, gap: Math.max(exec, s && now - s.t < 60e3 ? s.gap : 0), sellLab: pair.sell.label };
  }
  // forget directions not seen within the window, so state stays small
  for (const [key, k] of Object.entries(state)) {
    for (const d of Object.keys(k.seen)) if (now - k.seen[d].t > cfg.win) delete k.seen[d];
    for (const d of Object.keys(k.fired)) if (now - k.fired[d] > cfg.win) delete k.fired[d];
    if (!Object.keys(k.seen).length && !k.runDir) delete state[key];
  }
  return events;
}

/* ---------- message text ---------- */
const pct = bps => (bps / 100).toFixed(2) + "%";
const px = v => v >= 1000 ? v.toFixed(1) : v >= 100 ? v.toFixed(2) : v >= 1 ? v.toFixed(3) : v.toPrecision(4);
const usdShort = v => v >= 1e9 ? "$" + (v / 1e9).toFixed(2) + "B" : v >= 1e6 ? "$" + (v / 1e6).toFixed(1) + "M" : "$" + Math.round(v / 1e3) + "k";
const dur = ms => { const m = Math.round(ms / 60e3); return m < 1 ? Math.round(ms / 1000) + " s" : m < 60 ? m + " min" : (m / 60).toFixed(1) + " h"; };
const utc = t => new Date(t).toISOString().slice(11, 16) + " UTC";
function warnings(pair, now) {
  const w = [];
  if ([pair.buy, pair.sell].some(m => m.venue === "variational")) w.push("Variational quotes can lag up to ~1 min: confirm on its app first.");
  if ([pair.buy, pair.sell].some(m => m.venue === "hl")) w.push("HL prices are impact prices (a small market order), not the full $10k.");
  const d = new Date(now).getUTCDay();
  if (d === 0 || d === 6) w.push("Weekend: the underlying market is shut.");
  return w.length ? "\n\n⚠️ " + w.join("\n⚠️ ") : "";
}
export function flipText(e, now) {
  const { asset, exec, pair, minVol, prev } = e;
  return `🔁 FLIP · ${asset.label}\n\n` +
    `Before (${utc(prev.t)}): ${prev.sellLab} was rich, fill gap ${pct(prev.gap)}\n` +
    `Now: ${pair.sell.label} is rich, fill gap ${pct(exec)}\n` +
    `Sell ${pair.sell.label} ${px(pair.sell.bid)} (bid) / buy ${pair.buy.label} ${px(pair.buy.ask)} (ask)\n\n` +
    `Flipped in ${dur(e.flippedMs)}, new side held ${Math.round(e.heldMs / 1000)}s. Thinner venue trades ${usdShort(minVol)}/day.` + warnings(pair, now);
}
