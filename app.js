"use strict";
/* =============================================================================================
   EXCHANGE ADAPTERS
   ---------------------------------------------------------------------------------------------
   A venue is an object in VENUES with:
     id, label, color
     start(feed)      begins streaming. Call feed.upsert(market) as often as data arrives,
                      feed.status("ok" | "fallback" | "err" | "load", "note") when the link changes,
                      feed.message() once per received message (for the msgs/s counter).
     candles(m, interval, startMs, endMs)  optional -> Promise<[tMs, close][]>
   A market passed to feed.upsert needs { exchange, exchangeLabel, id, ticker } the first time,
   and any of: price, bid, ask, fundingApr (annual fraction, + = longs pay), volume24h, oiUsd,
   quoteAt (ms timestamp the venue says the quote is from; omit for live feeds),
   hist (whatever candles() needs). Later calls may send only the changed fields.

   pollingVenue({...}) wraps a plain REST endpoint into a venue; the TEMPLATE below uses it.
   ============================================================================================= */

const HL_REST = "https://api.hyperliquid.xyz/info";
const HL_WS = "wss://api.hyperliquid.xyz/ws";
const LT_REST = "https://mainnet.zklighter.elliot.ai/api/v1";
const LT_WS = "wss://mainnet.zklighter.elliot.ai/stream?readonly=true";
const VR_REST = "https://omni-client-api.prod.ap-northeast-1.variational.io/metadata/v2/stats";
const IV_MS = { "1m": 60e3, "5m": 300e3, "15m": 900e3, "1h": 3600e3, "4h": 14400e3 };

async function getJSON(url, body) {
  const opt = body ? { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) } : {};
  const r = await fetch(url, opt);
  if (r.status === 429) {
    let wait = 15;
    try { const j = await r.json(); if (j.retry_after_seconds) wait = +j.retry_after_seconds; } catch { }
    const e = new Error(`rate-limited, retrying in ${wait}s`); e.retryAfterMs = wait * 1000; throw e;
  }
  if (!r.ok) throw new Error(`${r.status} ${r.statusText}`);
  return r.json();
}

/* A WebSocket that reconnects with backoff and reports its state. */
function liveSocket({ url, onOpen, onMessage, onState, pingMs, pingMsg }) {
  let ws, timer, ping, tries = 0, closedByUs = false;
  const connect = () => {
    onState("load");
    try { ws = new WebSocket(url); } catch (e) { onState("err", e.message); return retry(); }
    ws.onopen = () => {
      tries = 0; onState("ok"); onOpen(ws);
      if (pingMs) ping = setInterval(() => { try { ws.send(JSON.stringify(pingMsg)); } catch { } }, pingMs);
    };
    ws.onmessage = ev => onMessage(ev.data);
    ws.onerror = () => { };
    ws.onclose = () => { clearInterval(ping); if (!closedByUs) { onState("err", "connection closed, reconnecting"); retry(); } };
  };
  const retry = () => { clearTimeout(timer); timer = setTimeout(connect, Math.min(30000, 1000 * 2 ** tries++)); };
  connect();
  return { isOpen: () => ws && ws.readyState === 1, close() { closedByUs = true; clearInterval(ping); ws && ws.close(); } };
}

async function inflateBase64Json(b64) {   // HL fastAssetCtxs: base64 of raw-DEFLATE JSON
  const bin = atob(b64), bytes = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) bytes[i] = bin.charCodeAt(i);
  const stream = new Blob([bytes]).stream().pipeThrough(new DecompressionStream("deflate-raw"));
  return JSON.parse(await new Response(stream).text());
}

/* ---------------------------------- Hyperliquid ---------------------------------- */
const hyperliquid = {
  id: "hl", label: "Hyperliquid", color: "#5fe0bd",
  // keyed by dex names that dex deployers choose: no prototype, so a name like "__proto__" is just a key
  uni: Object.create(null),     // dex -> [{name, delisted}] in API order (WS arrays follow this order)
  names: Object.create(null),   // dex -> full name
  async bootstrap(feed) {
    const dexes = await getJSON(HL_REST, { type: "perpDexs" });
    const list = [""].concat(dexes.filter(Boolean).map(d => { this.names[d.name] = d.fullName || d.name; return d.name; }));
    const res = await Promise.allSettled(list.map(dex =>
      getJSON(HL_REST, dex ? { type: "metaAndAssetCtxs", dex } : { type: "metaAndAssetCtxs" }).then(r => [dex, r])));
    let n = 0;
    for (const r of res) {
      if (r.status !== "fulfilled") continue;
      const [dex, [meta, ctxs]] = r.value;
      this.uni[dex] = meta.universe.map(a => ({ name: a.name, delisted: !!a.isDelisted }));
      meta.universe.forEach((a, i) => { if (!a.isDelisted && ctxs[i]) { this.applyCtx(feed, dex, a.name, ctxs[i], true, this.feeOf(dex, a)); n++; } });
    }
    if (!n) throw new Error("no markets returned");
  },
  exOf(dex) { return dex ? "hl-" + dex : "hl"; },
  // taker fee per fill, from the Hyperliquid fee docs: builder dexes scale the base fee, growth mode cuts it 90 %
  feeOf(dex, a) {
    if (!dex) return 0.00045;
    const s = +a.deployerFeeScale || 0;
    return 0.00045 * (s < 1 ? s + 1 : s * 2) * (a.growthMode === "enabled" ? 0.1 : 1);
  },
  applyCtx(feed, dex, name, c, first, fee) {
    const imp = c.impactPxs || [];
    const mark = +c.markPx, mid = c.midPx != null ? +c.midPx : mark;
    const m = {
      exchange: this.exOf(dex), id: name, price: mid || mark,
      bid: imp[0] != null ? +imp[0] : null, ask: imp[1] != null ? +imp[1] : null,
      fundingApr: +c.funding * 8760, volume24h: +c.dayNtlVlm, oiUsd: +c.openInterest * mark, quoteAgeSec: 0,
    };
    if (first) Object.assign(m, {
      exchangeLabel: dex ? "HL " + dex : "HL main",
      exchangeTitle: dex ? `Hyperliquid builder dex "${dex}" (${this.names[dex]})` : "Hyperliquid validator-operated perps",
      ticker: name.split(":").pop(), hist: { coin: name }, takerFee: fee ?? 0.00045, hlDex: dex,
    });
    feed.upsert(m);
  },
  start(feed) {
    const boot = () => this.bootstrap(feed).catch(e => feed.status("err", "market list: " + e.message));
    boot(); setInterval(boot, 10 * 60e3);                         // pick up new listings
    let queue = Promise.resolve();
    this.sock = liveSocket({
      url: HL_WS,
      onState: (s, note) => feed.status(s === "ok" ? "ok" : s, note || (s === "ok" ? "WebSocket" : "")),
      onOpen: ws => {
        ws.send(JSON.stringify({ method: "subscribe", subscription: { type: "fastAssetCtxs" } }));
        ws.send(JSON.stringify({ method: "subscribe", subscription: { type: "allDexsAssetCtxs" } }));
      },
      pingMs: 50000, pingMsg: { method: "ping" },
      onMessage: raw => {
        let j; try { j = JSON.parse(raw); } catch { return; }
        if (j.channel === "fastAssetCtxs" && typeof j.data === "string") {
          feed.message();
          queue = queue.then(() => inflateBase64Json(j.data)).then(obj => {
            for (const [coin, v] of Object.entries(obj)) {
              if (coin[0] === "#" || coin[0] === "@") continue;      // outcome and spot markets
              const dex = coin.includes(":") ? coin.split(":")[0] : "";
              const px = v.midPx != null ? +v.midPx : v.markPx != null ? +v.markPx : null;
              if (px) feed.upsert({ exchange: this.exOf(dex), id: coin, price: px }, true);
            }
          }).catch(() => { });
        } else if (j.channel === "allDexsAssetCtxs") {
          feed.message();
          for (const [dex, arr] of j.data.ctxs || []) {
            const u = this.uni[dex]; if (!u) continue;
            arr.forEach((c, i) => { const a = u[i]; if (a && !a.delisted && c) this.applyCtx(feed, dex, a.name, c, false); });
          }
        }
      },
    });
    // fallback: if the socket is down, poll REST every 15 s
    setInterval(() => { if (!this.sock.isOpen()) { boot(); feed.status("fallback", "WebSocket down, polling REST"); } }, 15000);
  },
  async candles(m, iv, startMs, endMs) {
    const c = await getJSON(HL_REST, { type: "candleSnapshot", req: { coin: m.hist.coin, interval: iv, startTime: startMs, endTime: endMs } });
    return c.map(x => [x.t, +x.c, +x.h, +x.l, +x.n > 0]);
  },
};

/* ---------------------------------- Lighter ---------------------------------- */
const lighter = {
  id: "lighter", label: "Lighter", color: "#b69cff",
  perps: new Map(),   // market_id -> symbol (active perps only)
  async bootstrap(feed) {
    const d = await getJSON(`${LT_REST}/orderBookDetails`);
    this.perps.clear();
    for (const m of d.order_book_details) {
      if (m.status !== "active" || m.market_type !== "perp") continue;
      this.perps.set(String(m.market_id), m.symbol);
      feed.upsert({
        exchange: "lighter", exchangeLabel: "Lighter", exchangeTitle: "Lighter (zk order book)",
        id: String(m.market_id), ticker: m.symbol, hist: { marketId: m.market_id }, takerFee: 0,   // Standard account: 0 fees
        lighterCat: { strategy: m.strategy_index, fpm: m.funding_premium_multiplier },
        price: +m.last_trade_price || null, volume24h: +m.daily_quote_token_volume, oiUsd: +m.open_interest_usd || null,
      });
    }
  },
  apply(feed, id, s) {
    const mid = +s.mid_price, last = +s.last_trade_price;
    const m = { exchange: "lighter", id: String(id) };
    if (mid || last) m.price = mid || last;
    if (s.best_bid_price != null) m.bid = +s.best_bid_price || null;
    if (s.best_ask_price != null) m.ask = +s.best_ask_price || null;
    if (s.current_funding_rate != null) m.fundingApr = +s.current_funding_rate / 100 * 8760;   // % per hour
    if (s.daily_quote_token_volume != null) m.volume24h = +s.daily_quote_token_volume;
    if (s.open_interest_usd != null) m.oiUsd = +s.open_interest_usd;
    m.quoteAgeSec = 0;
    feed.upsert(m, true);
  },
  start(feed) {
    const boot = () => this.bootstrap(feed).catch(e => feed.status("err", "market list: " + e.message));
    boot(); setInterval(boot, 10 * 60e3);
    this.sock = liveSocket({
      url: LT_WS,
      onState: (s, note) => feed.status(s, note || (s === "ok" ? "WebSocket" : "")),
      onOpen: ws => ws.send(JSON.stringify({ type: "subscribe", channel: "market_stats/all" })),
      pingMs: 50000, pingMsg: { type: "ping" },                       // Lighter drops idle sockets after ~2 min
      onMessage: raw => {
        let j; try { j = JSON.parse(raw); } catch { return; }
        if (!j.market_stats) return;
        feed.message();
        for (const [id, s] of Object.entries(j.market_stats)) if (this.perps.has(String(id))) this.apply(feed, id, s);
      },
    });
    setInterval(async () => {                                         // fallback when the socket is down
      if (this.sock.isOpen()) return;
      try {
        const s = await getJSON(`${LT_REST}/exchangeStats`);
        for (const x of s.order_book_stats) {
          const id = [...this.perps].find(([, sym]) => sym === x.symbol)?.[0];
          if (id) feed.upsert({ exchange: "lighter", id, price: +x.last_trade_price, volume24h: +x.daily_quote_token_volume }, true);
        }
        feed.status("fallback", "WebSocket down, polling REST");
      } catch (e) { feed.status("err", e.message); }
    }, 15000);
  },
  async candles(m, iv, startMs, endMs) {
    const step = IV_MS[iv] / 1000;
    let end = Math.floor(endMs / 1000); const start = Math.floor(startMs / 1000);
    const rows = new Map();
    for (let guard = 0; guard < 40 && end > start; guard++) {
      const from = Math.max(start, end - 500 * step);
      const r = await getJSON(`${LT_REST}/candles?market_id=${m.hist.marketId}&resolution=${iv}&start_timestamp=${from}&end_timestamp=${end}&count_back=500`);
      const page = r.c || [];
      if (!page.length) break;
      for (const x of page) rows.set(x.t, [x.t, +x.c, +x.h, +x.l, +x.V > 0]);
      const first = Math.floor(page[0].t / 1000);
      if (first >= end) break;
      end = first - 1;
    }
    return [...rows.values()].sort((a, b) => a[0] - b[0]);
  },
};

/* ------------------------- generic REST venue (also used for Variational) ------------------------- */
function pollingVenue({ id, label, color, intervalMs, load, candles }) {
  return {
    id, label, color, candles,
    start(feed) {
      const run = async () => {
        let next = intervalMs;
        try {
          const list = await load();
          feed.message();
          feed.replace(id, list);
          feed.status("ok", `REST every ${intervalMs / 1000}s`);
        } catch (e) {
          feed.status("err", e.message);
          if (e.retryAfterMs) next = Math.max(intervalMs, e.retryAfterMs + 1000);
        }
        setTimeout(run, next);
      };
      run();
    },
  };
}

const variational = pollingVenue({
  id: "variational", label: "Variational", color: "#e6b35a", intervalMs: 5000,   // API limit: 10 req / 10 s per IP
  async load() {
    const d = await getJSON(VR_REST);
    const now = Date.now();
    return d.listings.map(m => {
      const q = m.quotes || {}, k = q.size_1k || q.base || {};
      const bid = k.bid != null ? +k.bid : null, ask = k.ask != null ? +k.ask : null;
      // perps carry one signed rate; -SWAP contracts charge separate long/short financing (left out)
      const apr = /-SWAP$/.test(m.ticker) ? null : (m.funding?.next?.annualized_rate ?? m.funding?.last?.annualized_rate);
      return {
        exchange: "variational", exchangeLabel: "Variational", exchangeTitle: "Variational Omni (RFQ quotes, $1k size)",
        id: m.ticker, ticker: m.ticker.replace(/-PERP$/, ""),
        price: bid && ask ? (bid + ask) / 2 : +m.mark_price, bid, ask,
        fundingApr: apr != null ? +apr : null, volume24h: +m.volume_24h || 0,
        oiUsd: m.open_interest ? (+m.open_interest.long_open_interest || 0) + (+m.open_interest.short_open_interest || 0) : null,
        quoteAt: q.updated_at ? Date.parse(q.updated_at) : null, hist: null, takerFee: 0,
        varType: m.contract_type, varName: m.name || "",
        // swaps stop quoting when their market is shut (weekends): no next funding time = closed
        closed: m.contract_type === "swap" && !(m.funding && m.funding.next),
        spreadBps: m.base_spread_bps != null ? +m.base_spread_bps : null,
      };
    }).filter(m => m.price > 0);
  },
  // no candles: Variational publishes no price history. The page records its own while open.
});

/* TEMPLATE for a new exchange (REST). Copy, fill in, add to VENUES below.
   ALSO add the exchange's https:// (and wss://) address to connect-src in the Content-Security-Policy
   line at the top of index.html, or the browser will block the requests.
const myExchange = pollingVenue({
  id: "myex", label: "MyExchange", color: "#7cc47c", intervalMs: 5000,
  async load() {
    const d = await getJSON("https://api.myexchange.com/markets");        // must allow browser (CORS) requests
    return d.markets.map(m => ({ exchange: "myex", exchangeLabel: "MyExchange", id: m.id, ticker: m.symbol,
      price: +m.mid, bid: +m.bid, ask: +m.ask, fundingApr: +m.funding_hourly * 8760, volume24h: +m.vol_usd,
      hist: { symbol: m.symbol } }));
  },
  async candles(m, iv, startMs, endMs) { return []; },                  // optional: [[tMs, close], ...]
});
*/

const VENUES = [hyperliquid, lighter, variational];

/* =============================================================================================
   ASSET MATCHING. Each line is one asset; the first name is what the page shows.
   Same name but a very different price (io:ANTH ~1500 vs xyz:ANT ~95) stays apart automatically.
   ============================================================================================= */
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
const NOT_SAME = new Set(["hl|SPX"]);   // SPX on HL main = SPX6900 memecoin, not the S&P 500
const ALIAS_OF = {};
for (const g of ALIASES) for (const n of g) ALIAS_OF[n] = g[0];
const canonOf = t => { const u = t.toUpperCase().replace(/-PERP$/, ""); return ALIAS_OF[u] || u; };

/* =============================================================================================
   ASSET CLASSES. Lighter publishes a category per market (strategy_index) and a funding multiplier
   (100 = crypto, 50 = stocks/indices/FX/commodities, 1 = pre-IPO). That is the most reliable source,
   then Variational's contract type and company name, then which Hyperliquid dex lists it.
   ============================================================================================= */
const INDEXES = new Set(["SP500", "NASDAQ100", "DOW30", "RUSSELL2000", "US400-SWAP", "CNA50-SWAP", "TUR30", "JP225", "KR200", "VXI-SWAP", "DXI-SWAP"]);
const COMMODITIES = new Set(["GOLD", "SILVER", "WTI", "BRENT", "NATGAS", "COPPER", "PLATINUM", "PALLADIUM", "COCOA-SWAP", "HOIL-SWAP", "GASOL-SWAP", "H100"]);
const PRE_IPO = new Set(["ANTHROPIC", "OPENAI", "SPACEX", "XTEN", "OURA", "VARIATIONAL"]);
const CLASS_LABEL = { equity: "Stock", index: "Index", commodity: "Commodity", fx: "FX", preipo: "Pre-IPO", crypto: "Crypto" };
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
    if (strategy === 7) return "index";            // TUR30, US10Y and other one-off real-world markets
    return "equity";
  }
  if (m.varType === "swap") return "index";
  if (m.varType && /\b(Inc|Corp|Corporation|ETF|Ltd|Holdings|Trust|Group|plc|Company|Co\.|Fund|SA|AG|NV)\b/i.test(m.varName)) return "equity";
  if (m.venue === "hl" && m.exchange === "hl") return "crypto";
  if (m.venue === "hl") return "equity";            // builder dexes list mostly stocks; refined by other venues
  return "crypto";
}
function classOfAsset(markets, canon) {
  const rank = m => m.lighterCat ? 0 : m.varType ? 1 : m.exchange === "hl" ? 2 : 3;
  const best = [...markets].sort((a, b) => rank(a) - rank(b))[0];
  // a token that also trades on Hyperliquid's crypto dex is crypto, whatever else lists it
  if (markets.some(m => m.exchange === "hl") && !PRE_IPO.has(canon) && !INDEXES.has(canon) && !COMMODITIES.has(canon)) return "crypto";
  return best ? classOfMarket(best, canon) : "crypto";
}
const TYPE_SETS = { tradfi: ["equity", "index", "commodity", "fx"], "tradfi+pre": ["equity", "index", "commodity", "fx", "preipo"], crypto: ["crypto"], all: null };
let TYPE_FILTER = "tradfi";
const typeOk = v => { const set = TYPE_SETS[TYPE_FILTER]; return !set || set.includes(v.a.cls); };
const jumpTag = v => v.jump ? ` <span class="tag stale" title="${esc(v.jump.m.exchangeLabel)} moved ${fmtPct(v.jump.r)} in about a minute while the other venues barely moved. Treat the gap as suspect.">jump ${esc(v.jump.m.exchangeLabel)}</span>` : "";
const classTag = cls => `<span class="tag${cls === "crypto" || cls === "preipo" ? " stale" : ""}" title="${cls === "crypto" ? "Crypto token: easier to push around on one venue" : cls === "preipo" ? "Pre-IPO: no outside price, each venue prices off its own book" : "Asset class"}">${CLASS_LABEL[cls] || cls}</span>`;

/* US equity markets: closed from Friday 16:00 to Sunday 18:00 New York time (when index futures reopen).
   Stock and index perps keep trading then, but nothing outside anchors their price. */
const NY = new Intl.DateTimeFormat("en-US", { timeZone: "America/New_York", weekday: "short", hour: "numeric", minute: "numeric", hour12: false });
const nyCache = new Map();
function nyParts(tMs) {
  const k = Math.floor(tMs / 60000);
  let v = nyCache.get(k);
  if (!v) {
    const parts = Object.fromEntries(NY.formatToParts(new Date(k * 60000)).map(p => [p.type, p.value]));
    v = { day: parts.weekday, min: (+parts.hour % 24) * 60 + +parts.minute };
    if (nyCache.size > 200000) nyCache.clear();
    nyCache.set(k, v);
  }
  return v;
}
function sessionOf(tMs) {
  const { day, min } = nyParts(tMs);
  const weekend = day === "Sat" || (day === "Sun" && min < 18 * 60) || (day === "Fri" && min >= 16 * 60);
  if (weekend) return "weekend";
  const weekday = !["Sat", "Sun"].includes(day);
  return weekday && min >= 9 * 60 + 30 && min < 16 * 60 ? "us" : "off";
}

/* =============================================================================================
   STATE
   ============================================================================================= */
const store = (() => {
  const ok = (() => { try { localStorage.setItem("__t", "1"); localStorage.removeItem("__t"); return true; } catch { return false; } })();
  return {
    get(k, d) { if (!ok) return d; try { const v = localStorage.getItem("psd:" + k); return v == null ? d : JSON.parse(v); } catch { return d; } },
    set(k, v) { if (!ok) return; try { localStorage.setItem("psd:" + k, JSON.stringify(v)); } catch { } },
  };
})();

const state = {
  markets: new Map(), exchanges: new Map(), venueStatus: {}, msgTimes: {},
  assets: new Map(), structureDirty: true, views: new Map(),
  ribbon: store.get("ribbon", ["SP500", "GOLD", "ANTHROPIC", "OPENAI", "NVDA", "BTC", "PRL", "SILVER"]),
  shownEx: store.get("shownEx", null),
  refEx: store.get("refEx", ""),
  sort: { cmp: null, opp: { col: "gap", dir: -1 }, fund: { col: "spread", dir: -1 } },
  liveHist: store.get("liveHist", {}),           // Variational markets: [[t, price]] recorded by this page
  gapHist: new Map(),                            // asset key -> {t:[], max:[], per:{exchange:[]}} 1 s, 1 h
  prevPx: new Map(),                             // for the price flash
  events: store.get("events2", []).filter(e => e && typeof e.text === "string" && ["open", "close", "venue"].includes(e.kind) && isFinite(e.t)),
  alertActive: new Map(),                        // asset key -> {t, peak, hi, lo}
  alertsArmed: false,
  startedAt: Date.now(),
};

/* ---------- the feed object venues write into ---------- */
function makeFeed(v) {
  return {
    upsert(m, partial) {
      const key = `${m.exchange}|${m.id}`;
      let cur = state.markets.get(key);
      if (!cur) {
        if (partial || !m.ticker) return;                       // first sight needs a ticker
        if (NOT_SAME.has(`${m.exchange}|${m.ticker}`)) return;
        cur = { key, venue: v.id, updatedAt: 0 };
        state.markets.set(key, cur);
        state.structureDirty = true;
      }
      for (const k in m) if (m[k] !== undefined) cur[k] = m[k];
      cur.updatedAt = Date.now();
      if (!state.exchanges.has(m.exchange) && cur.exchangeLabel) {
        state.exchanges.set(m.exchange, { id: m.exchange, label: cur.exchangeLabel, title: cur.exchangeTitle || cur.exchangeLabel, venue: v.id });
        state.structureDirty = true;
      }
    },
    replace(venueId, list) {
      const seen = new Set();
      for (const m of list) { this.upsert(m); seen.add(`${m.exchange}|${m.id}`); }
      for (const [k, m] of state.markets) if (m.venue === venueId && !seen.has(k)) { state.markets.delete(k); state.structureDirty = true; }
    },
    status(s, note) {
      const prev = state.venueStatus[v.id]?.state;
      state.venueStatus[v.id] = { ...(state.venueStatus[v.id] || {}), state: s, note: note || "", at: Date.now() };
      if (prev && prev !== s && (s === "err" || (prev === "err" && s === "ok"))) {
        // note can carry text from a server error: escape it before it reaches the HTML event feed
        pushEvent("venue", s === "ok" ? `${esc(v.label)} feed back online` : `${esc(v.label)} feed problem: ${esc(note || "error")}`);
      }
    },
    message() {
      const arr = state.msgTimes[v.id] || (state.msgTimes[v.id] = []);
      const now = Date.now(); arr.push(now);
      while (arr.length && now - arr[0] > 10000) arr.shift();
      state.venueStatus[v.id] = { ...(state.venueStatus[v.id] || { state: "ok" }), lastMsg: now };
    },
  };
}

function venueOfExchange(exId) { return exId.startsWith("hl") ? "hl" : exId; }
function exColor(exId) {
  if (exId === "hl") return "#5fe0bd";
  if (exId.startsWith("hl-")) {
    const pal = ["#4fb3d9", "#7aa7ff", "#3fc1a4", "#93d7f0", "#5b8de0", "#6fd3c8", "#a3c4ff", "#2fa3c7", "#8fe0b0", "#7fb7d6"];
    let h = 0; for (const ch of exId) h = (h * 31 + ch.charCodeAt(0)) >>> 0; return pal[h % pal.length];
  }
  return VENUES.find(v => v.id === exId)?.color || "#999";
}

/* ---------- formatting ---------- */
const fmtPx = p => {
  if (p == null || !isFinite(p)) return "–";
  const a = Math.abs(p), d = a >= 1000 ? 1 : a >= 100 ? 2 : a >= 1 ? 3 : a >= 0.01 ? 5 : 7;
  return p.toLocaleString("en-US", { minimumFractionDigits: d, maximumFractionDigits: d });
};
const fmtBps = b => b == null || !isFinite(b) ? "–" : (b > 0 ? "+" : "") + b.toFixed(Math.abs(b) >= 100 ? 0 : 1);
const fmtPct = (f, d = 1) => f == null || !isFinite(f) ? "–" : (f > 0 ? "+" : "") + (f * 100).toFixed(d) + "%";
const fmtUsd = v => v == null || !isFinite(v) ? "–" : v >= 1e9 ? "$" + (v / 1e9).toFixed(2) + "B" : v >= 1e6 ? "$" + (v / 1e6).toFixed(1) + "M" : v >= 1e3 ? "$" + (v / 1e3).toFixed(0) + "k" : "$" + v.toFixed(0);
const fmtDur = ms => { const s = Math.round(ms / 1000); if (s < 90) return s + " s"; const m = Math.round(s / 60); if (m < 60) return m + " min"; const h = m / 60; if (h < 48) return h.toFixed(h < 10 ? 1 : 0) + " h"; return (h / 24).toFixed(1) + " d"; };
const fmtTime = t => new Date(t).toLocaleString(undefined, { weekday: "short", month: "short", day: "numeric", hour: "2-digit", minute: "2-digit" });
const fmtClock = t => new Date(t).toLocaleTimeString(undefined, { hour: "2-digit", minute: "2-digit", second: "2-digit" });
const esc = s => String(s).replace(/[&<>"']/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
const cls = v => v > 0 ? "above" : v < 0 ? "below" : "muted";
const median = a => { const s = [...a].sort((x, y) => x - y); const n = s.length; return n ? (n % 2 ? s[(n - 1) / 2] : (s[n / 2 - 1] + s[n / 2]) / 2) : NaN; };

/* =============================================================================================
   ASSETS + VIEWS (recomputed every second)
   ============================================================================================= */
function rebuildAssets() {
  const byCanon = new Map();
  for (const m of state.markets.values()) {
    if (!m.ticker || !(m.price > 0)) continue;
    const c = canonOf(m.ticker);
    if (!byCanon.has(c)) byCanon.set(c, []);
    byCanon.get(c).push(m);
  }
  const assets = new Map();
  for (const [c, list] of byCanon) {
    list.sort((a, b) => a.price - b.price);
    const clusters = [];
    for (const m of list) { const cl = clusters[clusters.length - 1]; if (cl && m.price / cl[0].price < 1.6) cl.push(m); else clusters.push([m]); }
    // the contract size with the most venues (then volume) keeps the plain name; other sizes get a price hint
    const vol = cl => cl.reduce((s, m) => s + (m.volume24h || 0), 0);
    const venues = cl => new Set(cl.map(m => m.exchange)).size;
    const main = [...clusters].sort((x, y) => venues(y) - venues(x) || vol(y) - vol(x))[0];
    for (const cl of clusters) {
      const per = new Map();
      for (const m of cl) { const o = per.get(m.exchange); if (!o || (m.volume24h || 0) > (o.volume24h || 0)) per.set(m.exchange, m); }
      const ms = [...per.values()];
      const plain = cl === main;
      const key = plain ? c : `${c}~${Math.floor(Math.log10(cl[0].price))}`;
      const label = plain ? c : `${c} (≈${fmtPx(median(ms.map(x => x.price)))} size)`;
      assets.set(key, { key, label, canon: c, keys: ms.map(m => m.key), cls: classOfAsset(ms, c) });
    }
  }
  state.assets = assets;
  state.structureDirty = false;
}

const isShown = exId => !state.shownEx || state.shownEx.includes(exId);
function resolveAssetKey(input) {
  const t = String(input || "").trim(); if (!t) return null;
  if (state.assets.has(t)) return t;
  const c = canonOf(t);
  if (state.assets.has(c)) return c;
  const cands = [...state.assets.keys()].filter(k => k.startsWith(c + "~"));
  const vol = k => state.assets.get(k).keys.reduce((s, mk) => s + (state.markets.get(mk)?.volume24h || 0), 0);
  return cands.length ? cands.sort((a, b) => vol(b) - vol(a))[0] : null;
}

/* quote age: live feeds are fresh by definition; venues that say how old a quote is get checked */
const ageOf = m => m.quoteAt ? Math.max(0, (Date.now() - m.quoteAt) / 1000) : 0;
let MAX_AGE = 60;
const freshEnough = m => !m.closed && (MAX_AGE == null || ageOf(m) <= MAX_AGE);
const ageTag = m => { if (m.closed) return ` <span class="tag stale" title="This Variational swap is closed (no next funding time; swaps shut for the weekend). Its quote is not live and is left out of gaps.">closed</span>`; const s = ageOf(m); return s > 15 ? ` <span class="tag ${s > (MAX_AGE ?? 1e9) ? "stale" : ""}" title="The venue says this quote is ${Math.round(s)} s old">${s >= 90 ? Math.round(s / 60) + "m" : Math.round(s) + "s"} old</span>` : ""; };

function assetView(a) {
  const all = a.keys.map(k => state.markets.get(k)).filter(m => m && m.price > 0 && isShown(m.exchange));
  const ms = all.filter(freshEnough);                     // used for every gap number
  let ref = ms.find(m => m.exchange === state.refEx);
  if (!ref) ref = [...ms].sort((x, y) => (y.volume24h || 0) - (x.volume24h || 0))[0];
  const gaps = {};
  for (const m of all) gaps[m.exchange] = ref ? (m.price / ref.price - 1) * 1e4 : null;
  let hi = null, lo = null;
  for (const m of ms) { if (!hi || m.price > hi.price) hi = m; if (!lo || m.price < lo.price) lo = m; }
  const maxGap = hi && lo && hi !== lo ? (hi.price / lo.price - 1) * 1e4 : null;
  let exec = null, execPair = null;
  for (const b of ms) for (const s of ms) {
    if (b === s || !(b.ask > 0) || !(s.bid > 0)) continue;
    const v = (s.bid / b.ask - 1) * 1e4;
    if (exec == null || v > exec) { exec = v; execPair = [b, s]; }
  }
  let fHi = null, fLo = null;
  for (const m of all) { if (m.fundingApr == null || !isFinite(m.fundingApr)) continue; if (!fHi || m.fundingApr > fHi.fundingApr) fHi = m; if (!fLo || m.fundingApr < fLo.fundingApr) fLo = m; }
  const minVol = ms.length ? Math.min(...ms.map(m => m.volume24h || 0)) : 0;
  const fundMinVol = all.length ? Math.min(...all.map(m => m.volume24h || 0)) : 0;
  // a venue that moved > 1 % in the last minute while its twins moved < 0.3 %: possible push on one book
  let jump = null;
  const moves = ms.filter(m => m._px && m._px.length > 30).map(m => [m, m.price / m._px[0] - 1]);
  for (const [m, r] of moves) if (Math.abs(r) > 0.01 && moves.every(([o, ro]) => o === m || Math.abs(ro) < 0.003)) jump = { m, r };
  return { a, ms, all, ref, gaps, hi, lo, maxGap, exec, execPair, jump, fHi, fLo, fundSpread: fHi && fLo && fHi !== fLo ? fHi.fundingApr - fLo.fundingApr : null, minVol, fundMinVol };
}

function sortedExchanges() {
  const order = { hl: 0, lighter: 1, variational: 2 };
  return [...state.exchanges.values()].sort((a, b) =>
    (order[a.venue] ?? 9) - (order[b.venue] ?? 9) || (a.id === "hl" ? -1 : b.id === "hl" ? 1 : a.label.localeCompare(b.label)));
}

/* =============================================================================================
   ONE-SECOND ENGINE: recompute, record history, fire alerts, render the open view
   ============================================================================================= */
const HIST_MAX = 3600;
function trackedKeys() {
  const s = new Set();
  for (const k of state.ribbon) { const r = resolveAssetKey(k); if (r) s.add(r); }
  const mon = resolveAssetKey(document.getElementById("monAsset").value); if (mon) s.add(mon);
  if (dv.asset) s.add(dv.asset);
  return s;
}

function tick() {
  if (state.structureDirty) rebuildAssets();
  const now = Date.now();
  state.views = new Map();
  for (const a of state.assets.values()) if (a.keys.length >= 2) state.views.set(a.key, assetView(a));
  // one-venue jump check: keep ~60 s of prices for every market that has a twin
  for (const v of state.views.values()) for (const m of v.all) {
    const b = m._px || (m._px = []);
    b.push(m.price); if (b.length > 61) b.shift();
  }

  // gap history (1 s) for tracked assets
  for (const key of trackedKeys()) {
    const v = state.views.get(key); if (!v) continue;
    let h = state.gapHist.get(key);
    if (!h) { h = { t: [], max: [], per: {} }; state.gapHist.set(key, h); }
    const n = h.t.length;
    h.t.push(now / 1000); h.max.push(v.maxGap);
    for (const ex of new Set([...Object.keys(h.per), ...Object.keys(v.gaps)])) {
      const arr = h.per[ex] || (h.per[ex] = new Array(n).fill(null));
      arr.push(v.gaps[ex] ?? null);
    }
    if (h.t.length > HIST_MAX) { const cut = h.t.length - HIST_MAX; h.t.splice(0, cut); h.max.splice(0, cut); for (const ex in h.per) h.per[ex].splice(0, cut); }
  }

  // live record for venues without candles (Variational), for the divergence view
  const watch = new Set();
  for (const key of trackedKeys()) state.assets.get(key)?.keys.forEach(k => watch.add(k));
  for (const k of watch) {
    const m = state.markets.get(k);
    if (!m || m.hist || !(m.price > 0)) continue;
    const arr = state.liveHist[k] || (state.liveHist[k] = []);
    if (!arr.length || now - arr[arr.length - 1][0] >= 5000) arr.push([now, m.price]);
    if (arr.length > 20000) arr.splice(0, arr.length - 20000);
  }
  if (now % 60000 < 1000) store.set("liveHist", state.liveHist);

  runAlerts(now);
  renderChrome(now);
  renderCurrent();
}

/* ---------- alerts ---------- */
function pushEvent(kind, text) {
  state.events.unshift({ t: Date.now(), kind, text });
  if (state.events.length > 300) state.events.length = 300;
  store.set("events2", state.events);
  state.newEvents = (state.newEvents || 0) + 1;
}
function runAlerts(now) {
  const thrBps = (+document.getElementById("alertThr").value || 1) * 100;
  const minVol = +document.getElementById("alertVol").value || 0;
  const ready = VENUES.every(v => state.venueStatus[v.id]?.lastMsg || state.venueStatus[v.id]?.state === "err");
  if (!state.alertsArmed) {
    if (!ready || now - state.startedAt < 6000) return;
    let n = 0;
    for (const v of state.views.values()) if (typeOk(v) && v.ms.length >= 2 && v.minVol >= minVol && v.maxGap >= thrBps) { state.alertActive.set(v.a.key, { t: now, peak: v.maxGap, start: true }); n++; }
    state.alertsArmed = true;
    pushEvent("venue", `Watching ${state.views.size} assets across ${state.exchanges.size} venue columns. ${n} already past ±${thrBps / 100}% at start.`);
    return;
  }
  for (const v of state.views.values()) {
    if (!typeOk(v) || v.ms.length < 2 || v.minVol < minVol || v.maxGap == null) continue;
    const act = state.alertActive.get(v.a.key);
    if (!act && v.maxGap >= thrBps) {
      state.alertActive.set(v.a.key, { t: now, peak: v.maxGap });
      const warn = v.jump ? ` <span class="tag stale">${esc(v.jump.m.exchangeLabel)} jumped ${fmtPct(v.jump.r)} in a minute: possible push on one book</span>`
        : sessionOf(now) === "weekend" && ["equity", "index"].includes(v.a.cls) ? ` <span class="tag">weekend: US market shut, no outside price</span>` : "";
      pushEvent("open", `<b>${esc(v.a.label)}</b> ${classTag(v.a.cls)} gap opened: ${esc(v.hi.exchangeLabel)} ${fmtBps(v.maxGap)} bps above ${esc(v.lo.exchangeLabel)}${warn}`);
    } else if (act) {
      act.peak = Math.max(act.peak, v.maxGap);
      if (v.maxGap < thrBps * 0.8) {
        state.alertActive.delete(v.a.key);
        if (!act.start) pushEvent("close", `<b>${esc(v.a.label)}</b> gap closed to ${fmtBps(v.maxGap)} bps after ${fmtDur(now - act.t)} (peak ${fmtBps(act.peak)} bps)`);
        else pushEvent("close", `<b>${esc(v.a.label)}</b> gap closed to ${fmtBps(v.maxGap)} bps (it was already open when the page started; peak since then ${fmtBps(act.peak)} bps)`);
      }
    }
  }
}
document.getElementById("clearFeed").addEventListener("click", () => { state.events = []; store.set("events2", []); renderFeed(true); });
["alertThr", "alertVol"].forEach(id => {
  const el = document.getElementById(id);
  el.value = store.get(id, el.value);
  el.addEventListener("change", () => { store.set(id, el.value); state.alertActive.clear(); state.alertsArmed = false; state.startedAt = Date.now() - 6000; });
});

/* =============================================================================================
   RENDER — chrome (band, side, chips) every second
   ============================================================================================= */
function msgRate(id) { const a = state.msgTimes[id] || []; const now = Date.now(); return a.filter(t => now - t <= 10000).length / 10; }
function renderChrome(now) {
  const anyLive = VENUES.some(v => state.venueStatus[v.id]?.lastMsg && now - state.venueStatus[v.id].lastMsg < 30000);
  document.getElementById("livePulse").classList.toggle("off", !anyLive);
  document.getElementById("liveClock").textContent = (anyLive ? "LIVE " : "OFFLINE ") + fmtClock(now);
  document.getElementById("liveRate").textContent = VENUES.reduce((s, v) => s + msgRate(v.id), 0).toFixed(1) + " msg/s";
  document.getElementById("sideVenues").innerHTML = VENUES.map(v => {
    const s = state.venueStatus[v.id] || { state: "load" };
    const age = s.lastMsg ? Math.round((now - s.lastMsg) / 1000) + "s" : "–";
    return `<div class="vstat" title="${esc(s.note || "")}"><span class="dot ${s.state}"></span>${esc(v.label)} <span class="num" style="margin-left:auto">${age}</span></div>`;
  }).join("");
  renderExChips(); renderDatalist();
}

function renderExChips() {
  const exs = sortedExchanges();
  const el = document.getElementById("exChips");
  const sig = exs.map(e => e.id + (isShown(e.id) ? 1 : 0)).join(",");
  if (el.dataset.sig === sig) return;
  el.dataset.sig = sig;
  el.innerHTML = exs.map(e => `<button type="button" class="chip" aria-pressed="${isShown(e.id)}" data-ex="${esc(e.id)}" title="${esc(e.title)}"><span class="sw" style="background:${exColor(e.id)}"></span>${esc(e.label)}</button>`).join("");
  const ref = document.getElementById("refSel");
  ref.innerHTML = `<option value="">Most volume</option>` + exs.map(e => `<option value="${esc(e.id)}">${esc(e.label)}</option>`).join("");
  ref.value = state.refEx;
}
document.getElementById("exChips").addEventListener("click", e => {
  const b = e.target.closest(".chip"); if (!b) return;
  const all = sortedExchanges().map(x => x.id);
  let cur = state.shownEx ? [...state.shownEx] : [...all];
  cur = cur.includes(b.dataset.ex) ? cur.filter(x => x !== b.dataset.ex) : cur.concat(b.dataset.ex);
  state.shownEx = cur.length === all.length ? null : cur;
  store.set("shownEx", state.shownEx);
  document.getElementById("exChips").dataset.sig = "";
  state.gapHist.clear();
  tick();
});
document.getElementById("refSel").addEventListener("change", e => { state.refEx = e.target.value; store.set("refEx", state.refEx); state.gapHist.clear(); tick(); });

function renderDatalist() {
  const el = document.getElementById("assetList");
  const keys = [...state.views.keys()].sort();
  const sig = keys.length + ":" + keys[0] + keys[keys.length - 1];
  if (el.dataset.sig === sig) return;
  el.dataset.sig = sig;
  el.innerHTML = keys.map(k => `<option value="${esc(k)}">${esc(state.assets.get(k).label)} · ${state.assets.get(k).keys.length} venues</option>`).join("");
}

function addToRibbon(key) { if (!key || state.ribbon.includes(key)) return; state.ribbon.push(key); store.set("ribbon", state.ribbon); tick(); }
function addFromInput(inp) {
  const key = resolveAssetKey(inp.value);
  if (!key) { inp.setCustomValidity("No venue lists that name. Try the ticker as one exchange writes it."); inp.reportValidity(); return; }
  inp.setCustomValidity(""); addToRibbon(key); inp.value = "";
}
document.getElementById("addAsset").addEventListener("click", () => addFromInput(document.getElementById("assetInput")));
for (const id of ["assetInput", "quickAdd"]) {
  const el = document.getElementById(id);
  el.addEventListener("keydown", e => { if (e.key === "Enter") addFromInput(el); });
  el.addEventListener("input", () => el.setCustomValidity(""));
}
document.getElementById("quickAdd").addEventListener("change", e => { if (resolveAssetKey(e.target.value)) addFromInput(e.target); });

/* ---------- shared table helpers ---------- */
function sortHeader(tableKey, col, label, title = "", extra = "") {
  const s = state.sort[tableKey];
  const aria = s && s.col === col ? ` aria-sort="${s.dir < 0 ? "descending" : "ascending"}"` : "";
  return `<th data-sort="${esc(col)}" data-table="${tableKey}"${aria} title="${esc(title)}" ${extra}>${label}</th>`;
}
document.addEventListener("click", e => {
  const th = e.target.closest("th[data-sort]"); if (!th) return;
  const t = th.dataset.table, col = th.dataset.sort, s = state.sort[t];
  state.sort[t] = s && s.col === col ? { col, dir: -s.dir } : { col, dir: -1 };
  if (t === "sc") { renderScanResults(); return; }
  renderCurrent();
});
function applySort(rows, key, getters) {
  const s = state.sort[key]; if (!s || !getters[s.col]) return rows;
  const g = getters[s.col];
  return rows.sort((x, y) => { const a = g(x), b = g(y); if (a == null && b == null) return 0; if (a == null) return 1; if (b == null) return -1; return (typeof a === "string" ? a.localeCompare(b) : a - b) * s.dir; });
}
function flash(cellKey, px) {
  const prev = state.prevPx.get(cellKey);
  state.prevPx.set(cellKey, px);
  if (prev == null || px == null || prev === px) return "";
  return px > prev ? "up" : "down";
}
function ribbonViews() {
  return state.ribbon.map(k => ({ k, key: resolveAssetKey(k) })).map(o => ({ ...o, v: o.key ? (state.views.get(o.key) || (state.assets.get(o.key) ? assetView(state.assets.get(o.key)) : null)) : null }));
}
function spark(key, w = 140, h = 28) {
  const hs = state.gapHist.get(key); if (!hs || hs.max.length < 2) return `<svg class="spark" viewBox="0 0 ${w} ${h}" aria-hidden="true"></svg>`;
  const vals = hs.max.slice(-600).map(v => v ?? NaN);
  const fin = vals.filter(isFinite); if (fin.length < 2) return `<svg class="spark" viewBox="0 0 ${w} ${h}" aria-hidden="true"></svg>`;
  let lo = Math.min(...fin), hi = Math.max(...fin); if (hi - lo < 1) { hi += .5; lo -= .5; }
  const pts = vals.map((v, i) => isFinite(v) ? `${(i / (vals.length - 1) * w).toFixed(1)},${(h - 3 - (v - lo) / (hi - lo) * (h - 6)).toFixed(1)}` : null).filter(Boolean);
  const last = pts[pts.length - 1].split(",");
  return `<svg class="spark" viewBox="0 0 ${w} ${h}" preserveAspectRatio="none" aria-hidden="true">
    <polyline points="${pts.join(" ")}" fill="none" stroke="var(--accent)" stroke-width="1.4" vector-effect="non-scaling-stroke"/>
    <circle cx="${last[0]}" cy="${last[1]}" r="2.2" fill="var(--accent)"/></svg>`;
}

/* =============================================================================================
   VIEWS
   ============================================================================================= */
let currentTab = "dashboard";
function renderCurrent() {
  if (currentTab === "dashboard") renderDashboard();
  else if (currentTab === "compare") renderCompare();
  else if (currentTab === "funding") renderFunding();
  else if (currentTab === "divergence") { renderDvSelectors(); dvLiveTick(); }
  else if (currentTab === "venues") renderVenues();
  else if (currentTab === "scanner") { if (!sc.agents.finder) { renderAgents(); renderScanResults(); scRuleHint(); } }
}

/* ---------- dashboard ---------- */
function topGapRows(n, minVol) {
  return [...state.views.values()].filter(v => typeOk(v) && v.ms.length >= 2 && v.maxGap != null && v.minVol >= minVol).sort((a, b) => b.maxGap - a.maxGap).slice(0, n);
}
function renderDashboard() {
  const now = Date.now();
  const alertVol = +document.getElementById("alertVol").value || 0;
  const thr = (+document.getElementById("alertThr").value || 1) * 100;
  const multi = [...state.views.values()].filter(v => v.all.length >= 2 && typeOk(v));
  const gaps = topGapRows(1, alertVol)[0];
  const fund = multi.filter(v => v.fundSpread != null && v.fundMinVol >= alertVol).sort((a, b) => b.fundSpread - a.fundSpread)[0];
  const online = VENUES.filter(v => state.venueStatus[v.id]?.lastMsg && now - state.venueStatus[v.id].lastMsg < 30000).length;
  const over = multi.filter(v => v.ms.length >= 2 && v.minVol >= alertVol && v.maxGap >= thr).length;
  const k = (key, val, sub, c = "") => `<div class="kpi"><span class="k">${key}</span><span class="v ${c}">${val}</span><span class="s">${sub}</span></div>`;
  document.getElementById("kpis").innerHTML =
    k("Feeds online", `${online} / ${VENUES.length}`, `${VENUES.reduce((s, v) => s + msgRate(v.id), 0).toFixed(1)} messages / s`) +
    k("Markets tracked", state.markets.size.toLocaleString(), `${state.exchanges.size} venue columns`) +
    k("Assets on 2+ venues", multi.length.toLocaleString(), sessionOf(now) === "weekend" ? "US market shut: stock gaps unanchored" : sessionOf(now) === "us" ? "US market open" : "US market closed (overnight)", sessionOf(now) === "weekend" ? "muted" : "") +
    k(`Gaps past ±${thr / 100}%`, over, `thinner venue ≥ ${fmtUsd(alertVol)} / day`, over ? "above" : "") +
    k("Widest gap", gaps ? fmtBps(gaps.maxGap) + " bps" : "–", gaps ? `${esc(gaps.a.label)}: ${esc(gaps.hi.exchangeLabel)} vs ${esc(gaps.lo.exchangeLabel)}` : "waiting for data", gaps ? "above" : "") +
    k("Top funding spread", fund ? fmtPct(fund.fundSpread) : "–", fund ? `${esc(fund.a.label)}: short ${esc(fund.fHi.exchangeLabel)}` : "waiting for data");

  // watchlist
  const rv = ribbonViews();
  document.getElementById("wlTable").innerHTML = `<thead><tr><th>Asset</th><th class="l">Richest venue</th><th>Price</th><th class="l">Cheapest venue</th><th>Price</th><th>Max gap bps</th><th>Executable bps</th><th style="min-width:150px">Max gap, live</th><th>Funding spread</th><th>Min 24h vol</th></tr></thead><tbody>` +
    (rv.map(({ k, key, v }) => {
      if (!v || v.ms.length < 2) return `<tr><td><div class="asset-cell"><b>${esc(v ? v.a.label : k)}</b></div></td><td class="l muted" colspan="9">${!v ? "not listed on the shown venues yet" : v.all.length >= 2 ? "other venues' quotes are older than the max quote age" : "listed on one shown venue only"}</td></tr>`;
      const fh = flash("wl|hi|" + key, v.hi.price), fl = flash("wl|lo|" + key, v.lo.price);
      return `<tr><td><div class="asset-cell"><b>${esc(v.a.label)}</b>${classTag(v.a.cls)}${jumpTag(v)}<button class="linkish" data-chart="${esc(key)}">chart</button></div></td>
        <td class="l">${esc(v.hi.exchangeLabel)}${ageTag(v.hi)}</td><td class="num ${fh}">${fmtPx(v.hi.price)}</td>
        <td class="l">${esc(v.lo.exchangeLabel)}${ageTag(v.lo)}</td><td class="num ${fl}">${fmtPx(v.lo.price)}</td>
        <td class="num ${v.maxGap >= thr ? "above" : ""}">${fmtBps(v.maxGap)}</td>
        <td class="num ${v.exec > 0 ? "above" : "muted"}" title="${v.execPair ? `buy ${esc(v.execPair[0].exchangeLabel)} ask, sell ${esc(v.execPair[1].exchangeLabel)} bid` : "needs bid/ask on two venues"}">${fmtBps(v.exec)}</td>
        <td style="padding-block:4px">${spark(key)}</td>
        <td class="num">${v.fundSpread == null ? "–" : fmtPct(v.fundSpread)}</td><td class="num">${fmtUsd(v.minVol)}</td></tr>`;
    }).join("") || `<tr><td class="empty" colspan="10">Your watchlist is empty. Type an asset in the search box at the top.</td></tr>`) + "</tbody>";

  // top gaps
  const tg = topGapRows(10, alertVol);
  const maxG = tg[0]?.maxGap || 1;
  document.getElementById("topGaps").innerHTML = tg.length ? `<thead><tr><th>Asset</th><th class="l">Above</th><th class="l">Below</th><th>Gap bps</th></tr></thead><tbody>` +
    tg.map(v => `<tr><td><div class="asset-cell"><b>${esc(v.a.label)}</b>${classTag(v.a.cls)}${jumpTag(v)}<button class="linkish" data-add="${esc(v.a.key)}">+ watch</button></div></td>
      <td class="l">${esc(v.hi.exchangeLabel)}${ageTag(v.hi)}</td><td class="l">${esc(v.lo.exchangeLabel)}${ageTag(v.lo)}</td>
      <td class="num above">${fmtBps(v.maxGap)}<span class="bar" style="width:${Math.max(4, v.maxGap / maxG * 60).toFixed(0)}px"></span></td></tr>`).join("") + "</tbody>"
    : `<tbody><tr><td class="empty">Waiting for two venues to report…</td></tr></tbody>`;

  // top funding
  const tf = multi.filter(v => v.fundSpread != null && v.fundMinVol >= alertVol).sort((a, b) => b.fundSpread - a.fundSpread).slice(0, 10);
  document.getElementById("topFunding").innerHTML = tf.length ? `<thead><tr><th>Asset</th><th class="l">Short</th><th class="l">Long</th><th>Spread / yr</th><th>$/day per $10k</th></tr></thead><tbody>` +
    tf.map(v => `<tr><td><div class="asset-cell"><b>${esc(v.a.label)}</b></div></td>
      <td class="l">${esc(v.fHi.exchangeLabel)} <span class="num ${cls(v.fHi.fundingApr)}">${fmtPct(v.fHi.fundingApr)}</span></td>
      <td class="l">${esc(v.fLo.exchangeLabel)} <span class="num ${cls(v.fLo.fundingApr)}">${fmtPct(v.fLo.fundingApr)}</span></td>
      <td class="num">${fmtPct(v.fundSpread)}</td><td class="num">$${(v.fundSpread * 10000 / 365).toFixed(2)}</td></tr>`).join("") + "</tbody>"
    : `<tbody><tr><td class="empty">Waiting for funding from two venues…</td></tr></tbody>`;

  renderFeed();
  renderFeedTable("feedTable", false);
  renderMonitor();
}

function renderFeed(force) {
  const el = document.getElementById("feed");
  const fresh = state.newEvents || 0; state.newEvents = 0;
  if (!force && !fresh && el.dataset.n === String(state.events.length)) return;
  el.dataset.n = state.events.length;
  el.innerHTML = state.events.slice(0, 120).map((e, i) => `<li class="${e.kind}${i < fresh ? " new" : ""}"><span class="t">${fmtClock(e.t)}</span><span>${e.text}</span></li>`).join("")
    || `<li><span class="t"></span><span class="muted">No events yet. A line appears when a gap crosses the threshold.</span></li>`;
}

function renderFeedTable(id, detailed) {
  const now = Date.now();
  const rows = VENUES.map(v => {
    const s = state.venueStatus[v.id] || { state: "load" };
    const cols = [...state.exchanges.values()].filter(e => e.venue === v.id).length;
    const mk = [...state.markets.values()].filter(m => m.venue === v.id).length;
    const stateTxt = { ok: "live", fallback: "fallback", err: "problem", load: "connecting" }[s.state] || s.state;
    return `<tr><td><div class="asset-cell"><span class="dot ${s.state}"></span><b>${esc(v.label)}</b></div></td>
      <td class="l">${esc(stateTxt)}${s.note ? ` · <span class="muted">${esc(s.note)}</span>` : ""}</td>
      <td class="num">${msgRate(v.id).toFixed(1)}</td><td class="num">${s.lastMsg ? Math.round((now - s.lastMsg) / 1000) + " s ago" : "–"}</td>
      ${detailed ? `<td class="num">${cols}</td><td class="num">${mk.toLocaleString()}</td><td class="l">${v.candles ? "yes" : "no (recorded live)"}</td>` : `<td class="num">${mk.toLocaleString()}</td>`}</tr>`;
  }).join("");
  document.getElementById(id).innerHTML = `<thead><tr><th>Venue</th><th class="l">State</th><th>Msg/s</th><th>Last message</th>${detailed ? "<th>Columns</th><th>Markets</th><th class=\"l\">Price history</th>" : "<th>Markets</th>"}</tr></thead><tbody>${rows}</tbody>`;
}

/* ---------- live gap monitor chart (1 s) ---------- */
let mon = { key: null, u: null, series: null, theme: null };
function renderMonitor() {
  const sel = document.getElementById("monAsset");
  const rv = ribbonViews().filter(o => o.key);
  const opts = rv.map(o => o.key);
  const sig = opts.join(",");
  if (sel.dataset.sig !== sig) {
    sel.dataset.sig = sig;
    const keep = sel.value || store.get("monAsset", "");
    sel.innerHTML = opts.map(k => `<option value="${esc(k)}">${esc(state.assets.get(k)?.label || k)}</option>`).join("");
    sel.value = opts.includes(keep) ? keep : (opts[0] || "");
  }
  const key = sel.value; if (!key) return;
  const h = state.gapHist.get(key); if (!h || h.t.length < 2) return;
  const win = +document.getElementById("monWin").value;
  const cutT = Date.now() / 1000 - win;
  let i0 = h.t.findIndex(t => t >= cutT); if (i0 < 0) i0 = 0;
  const exs = Object.keys(h.per).filter(ex => isShown(ex) && h.per[ex].slice(i0).some(x => x != null));
  const theme = document.documentElement.getAttribute("data-theme") + matchMedia("(prefers-color-scheme: dark)").matches;
  const box = document.getElementById("monChart");
  const sigSeries = key + "|" + exs.join(",") + "|" + theme;
  const data = [h.t.slice(i0), ...exs.map(ex => h.per[ex].slice(i0))];
  if (!mon.u || mon.series !== sigSeries) {
    mon.u?.destroy();
    const muted = cssVar("--muted"), line = cssVar("--line");
    const axis = { stroke: muted, grid: { stroke: line, width: 1 }, ticks: { stroke: line, width: 1 }, font: "11px " + cssVar("--font-data") };
    mon.u = new uPlot({
      width: Math.max(300, box.clientWidth - 12), height: 230,
      scales: { x: { time: true } },
      axes: [axis, { ...axis, size: 66, values: (u, v) => { const span = Math.abs(v[v.length - 1] - v[0]); return v.map(x => (x > 0 ? "+" : "") + x.toFixed(span < 10 ? 1 : 0)); } }],
      series: [{}, ...exs.map(ex => ({ label: state.exchanges.get(ex)?.label || ex, stroke: exColor(ex), width: 1.6, spanGaps: true, points: { show: false }, value: (u, v) => v == null ? "–" : fmtBps(v) + " bps" }))],
    }, data, box);
    mon.series = sigSeries;
  } else {
    mon.u.setData(data);
  }
}
document.getElementById("monAsset").addEventListener("change", e => { store.set("monAsset", e.target.value); mon.series = null; });
document.getElementById("monWin").addEventListener("change", () => { mon.series = null; });

/* ---------- compare ---------- */
function renderCompare() {
  const exs = sortedExchanges().filter(e => isShown(e.id));
  const rv = ribbonViews();
  const thr = (+document.getElementById("alertThr").value || 1) * 100;
  document.getElementById("ribbon").innerHTML = rv.map(({ k, key, v }) => {
    if (!v) return `<div class="rib"><button class="x" data-rm="${esc(k)}" aria-label="Remove ${esc(k)}">×</button><span class="name">${esc(k)}</span><span class="sub">not listed on the shown venues yet</span></div>`;
    return `<div class="rib"><button class="x" data-rm="${esc(k)}" aria-label="Remove ${esc(v.a.label)}">×</button>
      <span class="name">${esc(v.a.label)}</span>
      <span class="num ${v.maxGap >= thr ? "above" : ""}">${v.maxGap == null ? "one venue" : "max gap " + fmtBps(v.maxGap) + " bps"}</span>
      ${spark(key)}
      <span class="sub">${v.ms.length} venues${v.hi && v.lo && v.hi !== v.lo ? " · " + esc(v.hi.exchangeLabel) + " richest" : ""}</span>
      <button class="linkish" data-chart="${esc(key)}" style="text-align:left">Chart divergence →</button></div>`;
  }).join("") || `<p class="hint">The watchlist is empty. Add an asset above.</p>`;

  const views = rv.map(o => o.v).filter(Boolean);
  const getters = { asset: r => r.a.label, maxGap: r => r.maxGap, exec: r => r.exec, fund: r => r.fundSpread, vol: r => r.minVol };
  for (const e of exs) getters["ex:" + e.id] = r => r.gaps[e.id];
  const rows = applySort([...views], "cmp", getters);
  const head = `<thead><tr>${sortHeader("cmp", "asset", "Asset")}${exs.map(e => sortHeader("cmp", "ex:" + e.id, esc(e.label), e.title)).join("")}
    ${sortHeader("cmp", "maxGap", "Max gap bps", "Richest vs cheapest venue, mid prices")}
    ${sortHeader("cmp", "exec", "Executable bps", "Sell the best bid, buy the best ask across venues. Positive = locks in a gap before fees")}
    ${sortHeader("cmp", "fund", "Funding spread", "Highest minus lowest annualised funding across shown venues")}
    ${sortHeader("cmp", "vol", "Min 24h vol", "24h volume on the thinnest shown venue")}</tr></thead>`;
  const body = rows.map(r => `<tr>
      <td><div class="asset-cell"><b>${esc(r.a.label)}</b><button class="linkish" data-chart="${esc(r.a.key)}">chart</button></div></td>
      ${exs.map(e => {
        const m = r.all.find(x => x.exchange === e.id);
        if (!m) return `<td class="muted">·</td>`;
        const isRef = r.ref && m === r.ref, fresh = freshEnough(m);
        return `<td class="num ${flash("cmp|" + m.key, m.price)}" title="${esc(m.id)}${m.bid != null ? ` · bid ${fmtPx(m.bid)} / ask ${fmtPx(m.ask)}` : ""}${fresh ? "" : " · quote too old, left out of gaps"}"><span class="p">${ageTag(m)} ${fmtPx(m.price)}</span>
          <span class="g ${isRef ? "ref-mark" : fresh ? cls(r.gaps[e.id]) : "muted"}">${isRef ? "REF" : fmtBps(r.gaps[e.id])}</span></td>`;
      }).join("")}
      <td class="num ${cls(r.maxGap)}">${fmtBps(r.maxGap)}</td>
      <td class="num ${r.exec > 0 ? "above" : "muted"}" title="${r.execPair ? `buy ${esc(r.execPair[0].exchangeLabel)} ask, sell ${esc(r.execPair[1].exchangeLabel)} bid` : "needs bid/ask on two venues"}">${fmtBps(r.exec)}</td>
      <td class="num">${r.fundSpread == null ? "–" : fmtPct(r.fundSpread)}</td>
      <td class="num">${fmtUsd(r.minVol)}</td></tr>`).join("");
  document.getElementById("cmpTable").innerHTML = head + `<tbody>${body}</tbody>`;

  const minVol = +document.getElementById("minVol").value || 0;
  let opp = [...state.views.values()].filter(v => typeOk(v) && v.ms.length >= 2 && v.maxGap != null && v.minVol >= minVol);
  opp = applySort(opp, "opp", { asset: r => r.a.label, gap: r => r.maxGap, exec: r => r.exec, vol: r => r.minVol, n: r => r.ms.length, fund: r => r.fundSpread }).slice(0, 50);
  const oh = `<thead><tr>${sortHeader("opp", "asset", "Asset")}<th class="l">Richest</th><th class="l">Cheapest</th>${sortHeader("opp", "gap", "Gap bps")}${sortHeader("opp", "exec", "Executable bps")}${sortHeader("opp", "fund", "Funding spread")}${sortHeader("opp", "n", "Venues")}${sortHeader("opp", "vol", "Min 24h vol")}<th></th></tr></thead>`;
  const ob = opp.map(r => `<tr><td><div class="asset-cell"><b>${esc(r.a.label)}</b>${classTag(r.a.cls)}${jumpTag(r)}</div></td>
      <td class="l">${esc(r.hi.exchangeLabel)}${ageTag(r.hi)} <span class="num muted ${flash("opp|hi|" + r.a.key, r.hi.price)}">${fmtPx(r.hi.price)}</span></td>
      <td class="l">${esc(r.lo.exchangeLabel)}${ageTag(r.lo)} <span class="num muted ${flash("opp|lo|" + r.a.key, r.lo.price)}">${fmtPx(r.lo.price)}</span></td>
      <td class="num above">${fmtBps(r.maxGap)}</td>
      <td class="num ${r.exec > 0 ? "above" : "muted"}">${fmtBps(r.exec)}</td>
      <td class="num">${r.fundSpread == null ? "–" : fmtPct(r.fundSpread)}</td>
      <td class="num">${r.ms.length}</td><td class="num">${fmtUsd(r.minVol)}</td>
      <td><button class="linkish" data-add="${esc(r.a.key)}">+ watch</button> · <button class="linkish" data-chart="${esc(r.a.key)}">chart</button></td></tr>`).join("");
  document.getElementById("oppTable").innerHTML = opp.length ? oh + `<tbody>${ob}</tbody>` : `<tbody><tr><td class="empty">Waiting for market data from at least two venues…</td></tr></tbody>`;
}
document.getElementById("minVol").addEventListener("input", renderCompare);
document.addEventListener("click", e => {
  const rm = e.target.closest("[data-rm]"); if (rm) { state.ribbon = state.ribbon.filter(k => k !== rm.dataset.rm); store.set("ribbon", state.ribbon); tick(); return; }
  const add = e.target.closest("[data-add]"); if (add) { addToRibbon(add.dataset.add); return; }
  const ch = e.target.closest("[data-chart]"); if (ch) { openDivergence(ch.dataset.chart); return; }
  const go = e.target.closest("[data-go]"); if (go) location.hash = go.dataset.go;
});

/* ---------- funding ---------- */
function renderFunding() {
  const exs = sortedExchanges().filter(e => isShown(e.id));
  const q = document.getElementById("fundFilter").value.trim().toUpperCase();
  const minVol = +document.getElementById("fundMinVol").value || 0;
  const notional = +document.getElementById("fundNotional").value || 0;
  let rows = [...state.views.values()].filter(v => typeOk(v) && v.all.filter(m => m.fundingApr != null && isFinite(m.fundingApr)).length >= 2 && v.fundMinVol >= minVol && (!q || v.a.label.toUpperCase().includes(q)));
  const getters = { asset: r => r.a.label, spread: r => r.fundSpread, vol: r => r.fundMinVol };
  for (const e of exs) getters["ex:" + e.id] = r => r.all.find(m => m.exchange === e.id)?.fundingApr ?? null;
  rows = applySort(rows, "fund", getters);
  const head = `<thead><tr>${sortHeader("fund", "asset", "Asset")}${exs.map(e => sortHeader("fund", "ex:" + e.id, esc(e.label), e.title)).join("")}
    ${sortHeader("fund", "spread", "Spread / yr")}<th class="l" title="Short the highest-funding venue, long the lowest">Carry trade</th><th>$ / day</th>${sortHeader("fund", "vol", "Min 24h vol")}</tr></thead>`;
  const body = rows.slice(0, 200).map(r => `<tr><td><div class="asset-cell"><b>${esc(r.a.label)}</b><button class="linkish" data-chart="${esc(r.a.key)}">chart</button></div></td>
      ${exs.map(e => { const m = r.all.find(x => x.exchange === e.id); return m && m.fundingApr != null && isFinite(m.fundingApr) ? `<td class="num ${cls(m.fundingApr)}">${fmtPct(m.fundingApr)}</td>` : `<td class="muted">·</td>`; }).join("")}
      <td class="num">${fmtPct(r.fundSpread)}</td>
      <td class="l">${r.fHi && r.fLo && r.fHi !== r.fLo ? `short <b>${esc(r.fHi.exchangeLabel)}</b> · long <b>${esc(r.fLo.exchangeLabel)}</b>` : "–"}</td>
      <td class="num">${r.fundSpread == null ? "–" : "$" + (r.fundSpread * notional / 365).toFixed(2)}</td>
      <td class="num">${fmtUsd(r.fundMinVol)}</td></tr>`).join("");
  document.getElementById("fundTable").innerHTML = rows.length ? head + `<tbody>${body}</tbody>` : `<tbody><tr><td class="empty">No asset has funding on two of the shown venues with that volume yet.</td></tr></tbody>`;
}
["fundFilter", "fundMinVol", "fundNotional"].forEach(id => document.getElementById(id).addEventListener("input", renderFunding));

/* ---------- venues ---------- */
function renderVenues() {
  renderFeedTable("venueTable", true);
  const rows = sortedExchanges().map(e => {
    const ms = [...state.markets.values()].filter(m => m.exchange === e.id);
    const vol = ms.reduce((s, m) => s + (m.volume24h || 0), 0);
    const fresh = ms.length ? Math.round((Date.now() - Math.max(...ms.map(m => m.updatedAt || 0))) / 1000) : null;
    return `<tr><td><div class="asset-cell"><span class="dot ok" style="background:${exColor(e.id)}"></span><b>${esc(e.label)}</b></div></td><td class="l muted">${esc(e.title)}</td>
      <td class="num">${ms.length}</td><td class="num">${fmtUsd(vol)}</td><td class="num">${fresh == null ? "–" : fresh + " s ago"}</td></tr>`;
  }).join("");
  document.getElementById("colTable").innerHTML = `<thead><tr><th>Column</th><th class="l">What it is</th><th>Markets</th><th>24h volume</th><th>Last update</th></tr></thead><tbody>${rows}</tbody>`;
}

/* =============================================================================================
   DIVERGENCE (history from candles, newest bar live)
   ============================================================================================= */
const dv = { asset: null, a: null, b: null, iv: "15m", data: null, episodes: [], uPrice: null, uGap: null, loading: false, lastDraw: 0 };

function renderDvSelectors() {
  const sel = document.getElementById("dvAsset");
  const keys = [...state.views.keys()].sort();
  const sig = keys.length + ":" + keys[0] + keys[keys.length - 1];
  if (sel.dataset.sig !== sig) {
    sel.dataset.sig = sig;
    sel.innerHTML = keys.map(k => `<option value="${esc(k)}">${esc(state.assets.get(k).label)} (${state.assets.get(k).keys.length})</option>`).join("");
    if (!dv.asset || !keys.includes(dv.asset)) dv.asset = resolveAssetKey(store.get("dvAsset", null) || "ANTHROPIC") || keys[0];
    sel.value = dv.asset;
    fillDvVenues();
  }
}
function fillDvVenues() {
  const a = state.assets.get(dv.asset); if (!a) return;
  const ms = a.keys.map(k => state.markets.get(k)).filter(Boolean).sort((x, y) => (y.volume24h || 0) - (x.volume24h || 0));
  const opts = ms.map(m => `<option value="${esc(m.exchange)}">${esc(m.exchangeLabel)} · ${esc(m.ticker)}${m.hist ? "" : " (recorded live)"}</option>`).join("");
  const A = document.getElementById("dvA"), B = document.getElementById("dvB");
  A.innerHTML = opts; B.innerHTML = opts;
  const ids = ms.map(m => m.exchange), withHist = ms.filter(m => m.hist).map(m => m.exchange);
  A.value = ids.includes(dv.a) ? dv.a : (withHist[0] || ids[0] || "");
  B.value = ids.includes(dv.b) && dv.b !== A.value ? dv.b : (withHist.find(x => x !== A.value) || ids.find(x => x !== A.value) || "");
}
document.getElementById("dvAsset").addEventListener("change", e => { dv.asset = e.target.value; store.set("dvAsset", dv.asset); dv.a = dv.b = null; fillDvVenues(); });

function openDivergence(key) {
  dv.asset = key; store.set("dvAsset", key); dv.a = dv.b = null;
  location.hash = "divergence";
  setTimeout(() => { document.getElementById("dvAsset").dataset.sig = ""; renderDvSelectors(); document.getElementById("dvAsset").value = key; fillDvVenues(); loadDivergence(); }, 0);
}

async function seriesFor(m, iv, startMs, endMs) {
  const v = VENUES.find(x => x.id === m.venue);
  if (m.hist && v?.candles) return v.candles(m, iv, startMs, endMs);
  const step = IV_MS[iv], buckets = new Map();
  for (const [t, p] of state.liveHist[m.key] || []) if (t >= startMs) buckets.set(Math.floor(t / step) * step, p);
  return [...buckets.entries()].sort((a, b) => a[0] - b[0]);
}

async function loadDivergence() {
  const a = state.assets.get(dv.asset);
  const exA = document.getElementById("dvA").value, exB = document.getElementById("dvB").value;
  const status = document.getElementById("dvStatus");
  if (!a || !exA || !exB || exA === exB) { status.textContent = "Choose two different venues."; status.className = "status-line err"; return; }
  const mA = a.keys.map(k => state.markets.get(k)).find(m => m?.exchange === exA);
  const mB = a.keys.map(k => state.markets.get(k)).find(m => m?.exchange === exB);
  dv.a = exA; dv.b = exB; dv.iv = document.getElementById("dvIv").value;
  const days = +document.getElementById("dvDays").value;
  const endMs = Date.now(), startMs = endMs - days * 86400e3;
  status.textContent = `Loading ${a.label}: ${mA.exchangeLabel} vs ${mB.exchangeLabel}…`; status.className = "status-line";
  dv.loading = true;
  try {
    const [sa, sb] = await Promise.all([seriesFor(mA, dv.iv, startMs, endMs), seriesFor(mB, dv.iv, startMs, endMs)]);
    const mapB = new Map(sb);
    const ts = [], pa = [], pb = [];
    for (const [t, p] of sa) if (mapB.has(t)) { ts.push(t); pa.push(p); pb.push(mapB.get(t)); }
    dv.data = { ts, pa, pb, keyA: mA.key, keyB: mB.key, labA: mA.exchangeLabel, labB: mB.exchangeLabel, exA, exB };
    const notes = [];
    if (!mA.hist || !mB.hist) notes.push(`${(!mA.hist ? mA : mB).exchangeLabel} has no history API; its line starts when this page started recording it.`);
    if (sa.length && sb.length && ts.length < Math.min(sa.length, sb.length) * 0.5) notes.push("Many bars traded on only one venue; only bars present on both are compared.");
    document.getElementById("dvPriceNote").textContent = notes.join(" ");
    status.textContent = ts.length ? `${ts.length} bars from ${fmtTime(ts[0])} to ${fmtTime(ts[ts.length - 1])}. The newest bar updates every second.` : "No overlapping bars yet. A live-recorded venue fills in while this page stays open.";
    drawDivergence(true);
  } catch (e) {
    status.textContent = "Could not load history: " + (e.message || e); status.className = "status-line err";
  } finally { dv.loading = false; }
}
document.getElementById("dvLoad").addEventListener("click", loadDivergence);
document.getElementById("dvThr").addEventListener("input", () => dv.data && drawDivergence(true));

function computeEpisodes(ts, gap, thrPct, stepMs) {
  const eps = []; let cur = null;
  for (let i = 0; i < ts.length; i++) {
    const g = gap[i], side = g >= thrPct ? 1 : g <= -thrPct ? -1 : 0;
    if (side && cur && cur.side === side && ts[i] - cur.lastT <= stepMs * 1.5) {
      cur.lastT = ts[i]; cur.peak = side > 0 ? Math.max(cur.peak, g) : Math.min(cur.peak, g); cur.sum += g; cur.n++;
    } else {
      if (cur) eps.push(cur);
      cur = side ? { side, startT: ts[i], lastT: ts[i], peak: g, sum: g, n: 1 } : null;
    }
  }
  if (cur) { cur.open = true; eps.push(cur); }
  for (const e of eps) { e.endT = e.lastT + stepMs; e.avg = e.sum / e.n; }
  return eps;
}
const cssVar = n => getComputedStyle(document.documentElement).getPropertyValue(n).trim();

function drawDivergence(rebuild) {
  const d = dv.data; if (!d) return;
  const thr = +document.getElementById("dvThr").value || 0;
  const step = IV_MS[dv.iv];
  const gap = d.pa.map((p, i) => (d.pb[i] / p - 1) * 100);
  dv.episodes = computeEpisodes(d.ts, gap, thr, step);
  const n = gap.length, beyond = gap.filter(g => Math.abs(g) >= thr).length;
  const mean = n ? gap.reduce((s, x) => s + x, 0) / n : NaN;
  const longest = dv.episodes.reduce((m, e) => Math.max(m, e.endT - e.startT), 0);
  const aboveN = dv.episodes.filter(e => e.side > 0).length, belowN = dv.episodes.length - aboveN;
  const last = n ? gap[n - 1] : NaN;
  const tile = (k, v, c = "") => `<div class="kpi"><span class="k">${k}</span><span class="v ${c}">${v}</span></div>`;
  document.getElementById("dvTiles").innerHTML =
    tile("Gap now", isFinite(last) ? (last > 0 ? "+" : "") + last.toFixed(2) + "%" : "–", cls(last)) +
    tile("Average gap", isFinite(mean) ? (mean > 0 ? "+" : "") + mean.toFixed(2) + "%" : "–", cls(mean)) +
    tile(`Time beyond ±${thr}%`, n ? (beyond / n * 100).toFixed(0) + "%" : "–") +
    tile("Episodes (B above / below)", `${aboveN} / ${belowN}`) +
    tile("Longest episode", longest ? fmtDur(longest) : "–") +
    tile("Range", n ? `${Math.min(...gap).toFixed(1)} to ${Math.max(...gap).toFixed(1)}%` : "–");
  const rows = [...dv.episodes].reverse().slice(0, 300).map(e => `<tr>
      <td class="num">${fmtTime(e.startT)}</td><td class="num">${e.open ? "<b>still open</b>" : fmtTime(e.endT)}</td>
      <td class="num">${fmtDur(e.endT - e.startT)}</td>
      <td class="l ${e.side > 0 ? "above" : "below"}">${esc(d.labB)} ${e.side > 0 ? "above" : "below"} ${esc(d.labA)}</td>
      <td class="num ${cls(e.peak)}">${(e.peak > 0 ? "+" : "") + e.peak.toFixed(2)}%</td>
      <td class="num ${cls(e.avg)}">${(e.avg > 0 ? "+" : "") + e.avg.toFixed(2)}%</td></tr>`).join("");
  document.getElementById("dvEpisodes").innerHTML = dv.episodes.length
    ? `<thead><tr><th>Start</th><th>End</th><th>Lasted</th><th class="l">Direction</th><th>Peak gap</th><th>Avg gap</th></tr></thead><tbody>${rows}</tbody>`
    : `<tbody><tr><td class="empty">No stretch beyond ±${thr}% in this window. Lower the threshold to see smaller divergences.</td></tr></tbody>`;

  const xs = d.ts.map(t => t / 1000);
  const series = [[xs, d.pa, d.pb], [xs, gap, xs.map(() => thr), xs.map(() => -thr), xs.map(() => 0)]];
  if (!rebuild && dv.uPrice && dv.uGap) { dv.uPrice.setData(series[0]); dv.uGap.setData(series[1]); return; }
  const fg = cssVar("--fg"), muted = cssVar("--muted"), line = cssVar("--line"), above = cssVar("--above"), below = cssVar("--below");
  const shade = u => {
    const ctx = u.ctx, top = u.bbox.top, h = u.bbox.height;
    ctx.save();
    for (const e of dv.episodes) {
      const x0 = u.valToPos(e.startT / 1000, "x", true), x1 = u.valToPos(e.endT / 1000, "x", true);
      ctx.fillStyle = e.side > 0 ? above : below; ctx.globalAlpha = 0.14;
      ctx.fillRect(x0, top, Math.max(1, x1 - x0), h);
    }
    ctx.restore();
  };
  const axis = { stroke: muted, grid: { stroke: line, width: 1 }, ticks: { stroke: line, width: 1 }, font: "11px " + cssVar("--font-data") };
  const width = Math.max(300, document.getElementById("dvPriceChart").clientWidth - 12);
  const sync = uPlot.sync("dv");
  const base = h => ({ width, height: h, cursor: { sync: { key: sync.key } }, hooks: { drawClear: [shade] } });
  dv.uPrice?.destroy(); dv.uGap?.destroy();
  dv.uPrice = new uPlot({ ...base(260), scales: { x: { time: true } },
    axes: [axis, { ...axis, size: 72, values: (u, v) => v.map(fmtPx) }],
    series: [{}, { label: `${d.labA} (A)`, stroke: exColor(d.exA), width: 1.4, value: (u, v) => fmtPx(v) },
                 { label: `${d.labB} (B)`, stroke: exColor(d.exB), width: 1.4, value: (u, v) => fmtPx(v) }] }, series[0], document.getElementById("dvPriceChart"));
  dv.uGap = new uPlot({ ...base(200), scales: { x: { time: true } },
    axes: [axis, { ...axis, size: 72, values: (u, v) => v.map(x => x.toFixed(2) + "%") }],
    series: [{}, { label: "B vs A", stroke: fg, width: 1.2, value: (u, v) => v == null ? "–" : v.toFixed(2) + "%" },
                 { label: `+${thr}%`, stroke: above, width: 1, dash: [4, 4], points: { show: false } },
                 { label: `−${thr}%`, stroke: below, width: 1, dash: [4, 4], points: { show: false } },
                 { label: "0", stroke: muted, width: 0.6, points: { show: false } }] }, series[1], document.getElementById("dvGapChart"));
}

function dvLiveTick() {
  const d = dv.data; if (!d || dv.loading) return;
  const mA = state.markets.get(d.keyA), mB = state.markets.get(d.keyB);
  if (!mA || !mB) return;
  const step = IV_MS[dv.iv], t = Math.floor(Date.now() / step) * step, n = d.ts.length;
  if (n && d.ts[n - 1] === t) { d.pa[n - 1] = mA.price; d.pb[n - 1] = mB.price; }
  else if (!n || t > d.ts[n - 1]) { d.ts.push(t); d.pa.push(mA.price); d.pb.push(mB.price); }
  drawDivergence(false);
}
let rsTimer; addEventListener("resize", () => { clearTimeout(rsTimer); rsTimer = setTimeout(() => { mon.series = null; dv.data && drawDivergence(true); }, 200); });

/* =============================================================================================
   ARB SCANNER — a pipeline of six agents. They run in this browser on the venues' public data.
   They are rule-based analysts (no chat model): each does one job and hands its result on.
     1 Pair finder      which same-asset pairs on two venues are worth testing
     2 History          downloads candles for both legs
     3 Session tagger   marks every bar: US market hours, overnight, or weekend
     4 Quality guard    looks for one-venue spikes, stale books, permanent premiums, risky asset types
     5 Backtester       replays your entry/exit rule bar by bar, with fees and slippage
     6 Judge            turns all of that into Tradeable / Watch / Avoid, with reasons
   ============================================================================================= */
const AGENTS = [
  { id: "finder", name: "Pair finder" }, { id: "history", name: "History" }, { id: "session", name: "Session tagger" },
  { id: "quality", name: "Quality guard" }, { id: "backtest", name: "Backtester" }, { id: "judge", name: "Judge" },
];
const sc = { running: false, stop: false, agents: {}, log: [], results: [], open: new Set(), cache: new Map(), sort: { col: "net", dir: -1 } };
state.sort.sc = sc.sort;

function agentSay(id, msg, st = "working") {
  sc.agents[id] = { msg, st };
  sc.log.unshift({ t: Date.now(), who: AGENTS.find(a => a.id === id).name, msg });
  if (sc.log.length > 400) sc.log.length = 400;
  renderAgents();
}
function renderAgents() {
  document.getElementById("scAgents").innerHTML = AGENTS.map(a => {
    const s = sc.agents[a.id] || { msg: "waiting", st: "" };
    return `<div class="kpi agent ${s.st}"><span class="k">${esc(a.name)}</span><span class="v">${esc(s.msg)}</span></div>`;
  }).join("");
  document.getElementById("scLog").innerHTML = sc.log.slice(0, 120).map(e =>
    `<li class="venue"><span class="t">${fmtClock(e.t)}</span><span><b>${esc(e.who)}</b>: ${esc(e.msg)}</span></li>`).join("")
    || `<li><span class="t"></span><span class="muted">Set your rule and press Run scan.</span></li>`;
}

function scSettings() {
  const g = id => document.getElementById(id);
  return {
    types: g("scTypes").value, scope: g("scScope").value, iv: g("scIv").value, days: +g("scDays").value,
    unit: g("scUnit").value, entry: +g("scEntry").value, exit: +g("scExit").value,
    sizeMode: g("scSizeMode").value, size: +g("scSize").value, slip: +g("scCost").value,
    holdH: +g("scHold").value, session: g("scSession").value, minVol: +g("scMinVol").value,
  };
}
function scRuleHint() {
  const s = scSettings(), sp = [...state.markets.values()].find(m => canonOf(m.ticker || "") === "SP500" && m.price > 0);
  const el = document.getElementById("scRuleHint");
  if (s.unit === "bps") {
    el.textContent = sp ? `With SP500 near ${fmtPx(sp.price)}, ${s.entry} bps is about ${(sp.price * s.entry / 1e4).toFixed(1)} points and ${s.exit} bps about ${(sp.price * s.exit / 1e4).toFixed(1)} points. Your old trade (enter at 7–10 points, exit at 2–3) is roughly 10 bps in, 3 bps out.` : "";
  } else {
    el.textContent = "Price points are the raw price difference, the way you traded SP500 (enter at 7–10, exit at 2–3). Points only make sense when you scan one asset at a time, so use Watchlist only with a single asset, or switch to bps.";
  }
}
["scUnit", "scEntry", "scExit"].forEach(id => document.getElementById(id).addEventListener("input", scRuleHint));
document.getElementById("scSizeMode").addEventListener("change", e => { document.getElementById("scSize").value = e.target.value === "usd" ? 10000 : 10; });

/* ---------- 1. pair finder ---------- */
function findPairs(s) {
  const set = TYPE_SETS[s.types];
  const watch = new Set(state.ribbon.map(resolveAssetKey).filter(Boolean));
  const pairs = [];
  for (const a of state.assets.values()) {
    if (a.keys.length < 2 || (set && !set.includes(a.cls))) continue;
    if (s.scope === "watch" && !watch.has(a.key)) continue;
    const ms = a.keys.map(k => state.markets.get(k)).filter(m => m && m.hist && isShown(m.exchange) && (m.volume24h || 0) >= s.minVol);
    for (let i = 0; i < ms.length; i++) for (let j = i + 1; j < ms.length; j++) {
      const [A, B] = (ms[i].volume24h || 0) >= (ms[j].volume24h || 0) ? [ms[i], ms[j]] : [ms[j], ms[i]];
      pairs.push({ a, A, B, minVol: Math.min(A.volume24h || 0, B.volume24h || 0) });
    }
  }
  return pairs.sort((x, y) => y.minVol - x.minVol);
}

/* ---------- 2. history ---------- */
async function getCandles(m, iv, days) {
  const key = `${m.key}|${iv}|${days}`;
  if (sc.cache.has(key)) return sc.cache.get(key);
  const v = VENUES.find(x => x.id === m.venue);
  const end = Date.now(), start = end - days * 86400e3;
  const rows = await v.candles(m, iv, start, end);
  sc.cache.set(key, rows);
  return rows;
}

/* ---------- 4. quality guard ---------- */
function halfLifeHours(ts, gap) {
  const hourly = new Map();
  ts.forEach((t, i) => hourly.set(Math.floor(t / 3600e3), gap[i]));
  const g = [...hourly.values()];
  if (g.length < 48) return null;
  const x = g.slice(0, -1), y = g.slice(1).map((v, i) => v - x[i]);
  const mx = x.reduce((s, v) => s + v, 0) / x.length;
  let num = 0, den = 0;
  for (let i = 0; i < x.length; i++) { num += (x[i] - mx) * y[i]; den += (x[i] - mx) ** 2; }
  const beta = den ? num / den : 0;
  return beta < 0 && beta > -1 ? -Math.log(2) / Math.log(1 + beta) : Infinity;
}
function spikesPerDay(p, other, days) {
  // a bar where one venue jumps > 5x its usual move while the other barely moves, and it reverts within 3 bars
  const r = p.map((v, i) => i ? Math.log(v / p[i - 1]) : 0), ro = other.map((v, i) => i ? Math.log(v / other[i - 1]) : 0);
  let n = 0;
  for (let i = 49; i < r.length - 3; i++) {
    let s2 = 0; for (let k = i - 48; k < i; k++) s2 += r[k] * r[k];
    const sd = Math.sqrt(s2 / 48) || 1e-9;
    if (Math.abs(r[i]) > 5 * sd && Math.abs(r[i]) > 0.002 && Math.abs(ro[i]) < 1.5 * sd) {
      const back = r[i + 1] + r[i + 2] + r[i + 3];
      if (-back / r[i] > 0.6) n++;
    }
  }
  return n / Math.max(days, 1);
}

/* ---------- 5. backtester ---------- */
function backtest(d, s, allowFn, feeA, feeB) {
  const { t, pa, pb, act } = d;
  const meas = i => s.unit === "bps" ? (pb[i] / pa[i] - 1) * 1e4 : pb[i] - pa[i];
  const trades = [];
  let pos = 0, i0 = -1, worst = 0, k = 1;
  const holdMs = s.holdH * 3600e3;
  while (k < t.length - 1) {
    if (!pos) {
      const g = meas(k), gp = meas(k - 1);
      if (allowFn(t[k]) && act[k] && act[k - 1] && Math.abs(g) >= s.entry && Math.abs(gp) >= s.entry && g * gp > 0) {
        pos = g > 0 ? 1 : -1; i0 = k + 1; worst = 0; k += 2; continue;      // filled on the next bar
      }
    } else {
      const g = meas(k);
      const q = s.sizeMode === "usd" ? s.size / pa[i0] : s.size;
      worst = Math.min(worst, q * pos * ((pb[i0] - pa[i0]) - (pb[k] - pa[k])));
      if ((act[k] && (Math.abs(g) <= s.exit || g * pos < 0)) || t[k] - t[i0] >= holdMs) {
        const x = k + 1;
        const ptsIn = pb[i0] - pa[i0], ptsOut = pb[x] - pa[x];
        const gross = q * pos * (ptsIn - ptsOut);
        const cost = q * (pa[i0] + pa[x]) * (feeA + feeB + 2 * s.slip / 1e4);   // 4 fills
        trades.push({ tIn: t[i0], tOut: t[x], dir: pos, gapIn: meas(i0), gapOut: meas(x), ptsIn, ptsOut,
          gross, cost, net: gross - cost, worst, hours: (t[x] - t[i0]) / 3600e3, sess: sessionOf(t[i0]), forced: t[k] - t[i0] >= holdMs });
        pos = 0; k = x + 1; continue;
      }
    }
    k++;
  }
  return trades;
}
const sumNet = tr => tr.reduce((s, x) => s + x.net, 0);

/* ---------- the run ---------- */
async function runScan() {
  if (sc.running) return;
  const s = scSettings();
  sc.running = true; sc.stop = false; sc.results = []; sc.open.clear(); sc.agents = {}; sc.log = [];
  document.getElementById("scRun").disabled = true; document.getElementById("scStop").hidden = false;
  renderScanResults();
  try {
    const pairs = findPairs(s);
    const noHist = [...state.assets.values()].filter(a => a.keys.length >= 2).flatMap(a => a.keys.map(k => state.markets.get(k))).filter(m => m && !m.hist && isShown(m.exchange)).length;
    agentSay("finder", `${pairs.length} pairs with candle history on both venues and ≥ ${fmtUsd(s.minVol)} daily volume on the thinner one.${noHist ? ` ${noHist} Variational markets left out: Variational publishes no price history (watch them live on the Dashboard).` : ""}`, "done");
    if (!pairs.length) { agentSay("judge", "Nothing to test with these settings. Lower the volume floor or widen the asset types.", "done"); return; }
    const allow = { all: () => true, weekday: t => sessionOf(t) !== "weekend", us: t => sessionOf(t) === "us" };
    let done = 0;
    const queue = [...pairs];
    const worker = async () => {
      while (queue.length && !sc.stop) {
        const p = queue.shift();
        const label = `${p.a.label}: ${p.A.exchangeLabel} vs ${p.B.exchangeLabel}`;
        try {
          agentSay("history", `Downloading ${label} (${done + 1} of ${pairs.length})`);
          const [ca, cb] = await Promise.all([getCandles(p.A, s.iv, s.days), getCandles(p.B, s.iv, s.days)]);
          const mapB = new Map(cb.map(r => [r[0], r]));
          const d = { t: [], pa: [], pb: [], act: [] };
          for (const r of ca) { const o = mapB.get(r[0]); if (!o) continue; d.t.push(r[0]); d.pa.push(r[1]); d.pb.push(o[1]); d.act.push(!!(r[4] && o[4])); }
          if (d.t.length < 100) { sc.results.push({ p, label, skip: `only ${d.t.length} bars on both venues` }); continue; }

          agentSay("session", `Tagging ${d.t.length} bars of ${p.a.label} by market session`);
          const sess = d.t.map(sessionOf);
          const share = k => sess.filter(x => x === k).length / sess.length;

          agentSay("quality", `Checking ${label} for spikes, stale books and permanent premiums`);
          const gapB = d.pa.map((v, i) => (d.pb[i] / v - 1) * 1e4);
          const absG = gapB.map(Math.abs).sort((x, y) => x - y);
          const med = absG[absG.length >> 1], p90 = absG[Math.floor(absG.length * .9)];
          const meanGap = gapB.reduce((x, y) => x + y, 0) / gapB.length;
          const hl = halfLifeHours(d.t, gapB);
          const stale = 1 - d.act.filter(Boolean).length / d.act.length;
          const spA = spikesPerDay(d.pa, d.pb, s.days), spB = spikesPerDay(d.pb, d.pa, s.days);
          const wkG = gapB.filter((g, i) => sess[i] === "weekend").map(Math.abs), wdG = gapB.filter((g, i) => sess[i] !== "weekend").map(Math.abs);
          const avg = a => a.length ? a.reduce((x, y) => x + y, 0) / a.length : null;
          const flags = [];
          let score = 100;
          if (p.a.cls === "crypto") { score -= 35; flags.push(["Crypto token: one venue can be pushed around (big players have done this to drain market makers). Treat every gap as suspect.", "bad"]); }
          if (p.a.cls === "preipo") { score -= 25; flags.push(["Pre-IPO: no outside price, so the gap may never close.", "bad"]); }
          if (p.minVol < 1e6) { score -= 15; flags.push([`Thin: the quieter venue trades ${fmtUsd(p.minVol)} a day, so fills will move the price.`, "warn"]); }
          if (stale > 0.2) { score -= 15; flags.push([`Stale: ${(stale * 100).toFixed(0)}% of bars had no trade on one venue, so some gaps are just old prices.`, "warn"]); }
          const sp = Math.max(spA, spB);
          if (sp >= 0.5) { score -= Math.min(30, sp * 12); flags.push([`${(spA >= spB ? p.A : p.B).exchangeLabel} had ${sp.toFixed(1)} one-venue spikes a day that snapped back: possible manipulation or thin-book wicks.`, sp >= 1.5 ? "bad" : "warn"]); }
          const persistent = Math.abs(meanGap) >= s.entry * (s.unit === "bps" ? 1 : 1e4 / d.pa[0]) && (hl == null || hl > 48);
          if (persistent) { score -= 15; flags.push([`Standing premium: ${p.B.exchangeLabel} sits ${fmtBps(meanGap)} bps from ${p.A.exchangeLabel} on average and rarely returns to zero, so entries at your level may never close.`, "warn"]); }
          if (wkG.length && wdG.length && avg(wkG) > 2 * avg(wdG)) flags.push([`Weekend gaps are ${(avg(wkG) / avg(wdG)).toFixed(1)}× weekday gaps: the US market is shut then, so stock perps trade with no outside price${p.A.venue === "variational" || p.B.venue === "variational" ? " (and Variational's swaps stop quoting)" : ""}.`, "info"]);
          score = Math.max(0, Math.round(score));

          agentSay("backtest", `Replaying your rule on ${label}`);
          const fa = p.A.takerFee ?? 0.00045, fb = p.B.takerFee ?? 0.00045;
          const tr = { all: backtest(d, s, allow.all, fa, fb), weekday: backtest(d, s, allow.weekday, fa, fb), us: backtest(d, s, allow.us, fa, fb) };
          const main = tr[s.session];
          const wins = main.filter(x => x.net > 0).length;

          agentSay("judge", `Grading ${label}`);
          const net = sumNet(main), why = [];
          let verdict;
          if (main.length < 3) { verdict = "na"; why.push(`Only ${main.length} trade${main.length === 1 ? "" : "s"} at your entry level in ${s.days} days: not enough to judge.`); }
          else {
            // worth bothering with: at least 0.25 % of one leg's notional over the period ($25 per $10k)
            const notional = s.sizeMode === "usd" ? s.size : s.size * d.pa[d.pa.length - 1];
            const minNet = 0.0025 * notional, winRate = wins / main.length;
            if (net >= minNet && winRate >= 0.6 && score >= 65 && !["crypto", "preipo"].includes(p.a.cls)) { verdict = "go"; why.push(`${main.length} trades, ${wins} winners, ${fmtUsd(net)} net after fees and slippage.`); }
            else if (net > 0) {
              const reason = net < minNet ? `that is under ${fmtUsd(minNet)} (0.25% of your size), too small to be worth the risk`
                : score < 65 ? "the quality guard is worried (see flags)" : winRate < 0.6 ? `only ${Math.round(winRate * 100)}% of trades won, so a few big winners carried it` : "the asset type is risky";
              verdict = "watch"; why.push(`Made ${fmtUsd2(net)} over ${main.length} trades, but ${reason}.`);
            }
          }
          if (verdict === undefined) { verdict = "avoid"; why.push(`Lost ${fmtUsd2(-net)} over ${main.length} trades: the gap rarely closed enough to pay for fees and slippage.`); }
          const forced = main.filter(x => x.forced).length;
          if (forced) why.push(`${forced} trade${forced > 1 ? "s" : ""} never reached your exit and were closed after ${s.holdH / 24} day${s.holdH > 24 ? "s" : ""}.`);
          if (s.session !== "all") { const d2 = sumNet(tr.all) - net; if (Math.abs(d2) > 1) why.push(`Trading any time instead would have ${d2 > 0 ? "added" : "cost"} ${fmtUsd(Math.abs(d2))}${tr.all.filter(x => x.sess === "weekend").length ? `, mostly from ${tr.all.filter(x => x.sess === "weekend").length} weekend trades` : ""}.`); }
          sc.results.push({ p, label, d, flags, score, verdict, why, tr, main, wins, net, med, p90, meanGap, hl, stale, wkShare: share("weekend"),
            avgCap: main.length ? main.reduce((x, y) => x + (y.dir * (y.ptsIn - y.ptsOut)), 0) / main.length : null,
            avgHold: main.length ? main.reduce((x, y) => x + y.hours, 0) / main.length : null, worst: main.reduce((x, y) => Math.min(x, y.worst), 0) });
        } catch (e) {
          sc.results.push({ p, label, skip: "could not load history: " + (e.message || e) });
        } finally {
          done++;
          if (done % 3 === 0 || !queue.length) renderScanResults();
        }
      }
    };
    await Promise.all([worker(), worker()]);           // two at a time, gentle on the APIs
    const go = sc.results.filter(r => r.verdict === "go").length, w = sc.results.filter(r => r.verdict === "watch").length;
    for (const a of ["history", "session", "quality", "backtest"]) sc.agents[a] = { msg: `Finished ${done} pairs`, st: "done" };
    agentSay("judge", sc.stop ? `Stopped after ${done} pairs.` : `Done: ${go} tradeable, ${w} to watch, out of ${done} pairs.`, "done");
  } finally {
    sc.running = false;
    document.getElementById("scRun").disabled = false; document.getElementById("scStop").hidden = true;
    renderScanResults();
  }
}
document.getElementById("scRun").addEventListener("click", runScan);
document.getElementById("scStop").addEventListener("click", () => { sc.stop = true; agentSay("judge", "Stopping after the pairs in progress…"); });

/* ---------- results ---------- */
const VERDICT = { go: "Tradeable", watch: "Watch", avoid: "Avoid", na: "Too few trades" };
function renderScanResults() {
  const s = scSettings();
  const rs = sc.results.filter(r => !r.skip);
  const go = rs.filter(r => r.verdict === "go");
  const best = [...rs].sort((a, b) => b.net - a.net)[0];
  const k = (key, val, sub, c = "") => `<div class="kpi"><span class="k">${key}</span><span class="v ${c}">${val}</span><span class="s">${sub}</span></div>`;
  document.getElementById("scSummary").innerHTML = rs.length ?
    k("Pairs graded", rs.length, `${sc.results.length - rs.length} skipped (too little history)`) +
    k("Tradeable", go.length, "made money, passed the quality guard", go.length ? "above" : "") +
    k("Best pair", best ? fmtUsd(best.net) : "–", best ? esc(best.label) : "", best && best.net > 0 ? "above" : "") +
    k("Tradeable pairs together", fmtUsd(go.reduce((x, r) => x + r.net, 0)), `${s.days} days, ${s.sizeMode === "usd" ? fmtUsd(s.size) : s.size + " units"} per leg`) : "";
  const getters = { pair: r => r.label, cls: r => r.p.a.cls, score: r => r.score, trades: r => r.main.length, win: r => r.main.length ? r.wins / r.main.length : null,
    net: r => r.net, all: r => sumNet(r.tr.all), cap: r => r.avgCap, hold: r => r.avgHold, worst: r => r.worst, med: r => r.med, verdict: r => ({ go: 3, watch: 2, na: 1, avoid: 0 }[r.verdict]) };
  const rows = applySort([...rs], "sc", getters);
  const unitLbl = s.unit === "bps" ? "bps" : "pts";
  const head = `<thead><tr>${sortHeader("sc", "pair", "Pair")}${sortHeader("sc", "cls", "Type")}${sortHeader("sc", "verdict", "Verdict")}${sortHeader("sc", "score", "Quality")}
    ${sortHeader("sc", "med", "Typical gap bps", "Median absolute gap over the history")}${sortHeader("sc", "trades", "Trades")}${sortHeader("sc", "win", "Win %")}
    ${sortHeader("sc", "cap", "Avg capture / unit", "Average price points captured per unit of the asset, before costs")}${sortHeader("sc", "net", "Net $", "Your session setting, after fees and slippage")}
    ${sortHeader("sc", "all", "Net $, any time", "Same rule with weekend and overnight trades allowed")}${sortHeader("sc", "hold", "Avg hold")}${sortHeader("sc", "worst", "Worst drawdown $")}</tr></thead>`;
  const body = rows.map(r => {
    const id = r.p.A.key + "~" + r.p.B.key;
    const open = sc.open.has(id);
    const flagTxt = r.flags.filter(f => f[1] !== "info").length;
    let html = `<tr class="sc-row" data-sc="${esc(id)}" aria-expanded="${open}">
      <td><div class="asset-cell"><b>${esc(r.p.a.label)}</b><span class="muted">${esc(r.p.A.exchangeLabel)} vs ${esc(r.p.B.exchangeLabel)}</span></div></td>
      <td class="l">${classTag(r.p.a.cls)}</td>
      <td class="l"><span class="pill ${r.verdict}">${VERDICT[r.verdict]}</span></td>
      <td class="num ${r.score >= 65 ? "" : "above"}">${r.score}${flagTxt ? ` <span class="tag stale">${flagTxt} flag${flagTxt > 1 ? "s" : ""}</span>` : ""}</td>
      <td class="num">${r.med.toFixed(1)}</td><td class="num">${r.main.length}</td>
      <td class="num">${r.main.length ? Math.round(r.wins / r.main.length * 100) + "%" : "–"}</td>
      <td class="num">${r.avgCap == null ? "–" : fmtPx(r.avgCap)}</td>
      <td class="num ${r.net > 0 ? "above" : r.net < 0 ? "below" : ""}">${fmtUsd2(r.net)}</td>
      <td class="num muted">${fmtUsd2(sumNet(r.tr.all))}</td>
      <td class="num">${r.avgHold == null ? "–" : fmtDur(r.avgHold * 3600e3)}</td>
      <td class="num">${fmtUsd2(r.worst)}</td></tr>`;
    if (open) {
      const trs = r.main.slice(-40).reverse();
      html += `<tr class="sc-detail"><td colspan="12"><div style="display:grid;gap:4px;max-width:1000px">
        <b>Judge</b><ul>${r.why.map(w => `<li>${esc(w)}</li>`).join("")}</ul>
        <b>Quality guard (score ${r.score} of 100)</b><ul>${r.flags.length ? r.flags.map(f => `<li class="${f[1] === "bad" ? "above" : ""}">${esc(f[0])}</li>`).join("") : "<li>No warning signs: both books traded steadily and the gap kept returning to normal.</li>"}</ul>
        <span class="muted">Gap ${fmtBps(r.meanGap)} bps on average · 90% of the time within ${r.p90.toFixed(1)} bps · ${r.hl == null ? "half-life n/a" : r.hl === Infinity ? "gap does not revert" : "half-life " + fmtDur(r.hl * 3600e3)} · ${(r.wkShare * 100).toFixed(0)}% of bars on weekends · fees ${((r.p.A.takerFee ?? 0) * 1e4).toFixed(1)} + ${((r.p.B.takerFee ?? 0) * 1e4).toFixed(1)} bps per fill</span>
        <div><button class="linkish" data-dv="${esc(id)}">Open this pair in Divergence →</button></div>
        ${trs.length ? `<div class="table-wrap"><table><thead><tr><th>Opened</th><th>Closed</th><th class="l">Side</th><th title="Gap when the entry filled, one bar after the signal">Gap at entry fill (${unitLbl})</th><th title="Gap when the exit filled">Gap at exit fill (${unitLbl})</th><th>Held</th><th>Gross $</th><th>Costs $</th><th>Net $</th><th class="l">Session</th></tr></thead><tbody>
          ${trs.map(x => `<tr><td class="num">${fmtTime(x.tIn)}</td><td class="num">${fmtTime(x.tOut)}${x.forced ? " (gave up)" : ""}</td>
            <td class="l">sell ${esc(x.dir > 0 ? r.p.B.exchangeLabel : r.p.A.exchangeLabel)} / buy ${esc(x.dir > 0 ? r.p.A.exchangeLabel : r.p.B.exchangeLabel)}</td>
            <td class="num">${x.gapIn.toFixed(s.unit === "bps" ? 1 : 2)}</td><td class="num">${x.gapOut.toFixed(s.unit === "bps" ? 1 : 2)}</td>
            <td class="num">${fmtDur(x.hours * 3600e3)}</td><td class="num">${fmtUsd2(x.gross)}</td><td class="num">${fmtUsd2(x.cost)}</td>
            <td class="num ${x.net > 0 ? "above" : "below"}">${fmtUsd2(x.net)}</td><td class="l">${{ us: "US hours", off: "overnight", weekend: "weekend" }[x.sess]}</td></tr>`).join("")}
          </tbody></table></div>` : ""}
      </div></td></tr>`;
    }
    return html;
  }).join("");
  const skipped = sc.results.filter(r => r.skip);
  document.getElementById("scTable").innerHTML = rs.length || skipped.length
    ? head + `<tbody>${body}${skipped.length ? `<tr><td class="empty" colspan="12">Skipped: ${skipped.slice(0, 12).map(r => esc(r.label) + " (" + esc(r.skip) + ")").join("; ")}${skipped.length > 12 ? ` and ${skipped.length - 12} more` : ""}</td></tr>` : ""}</tbody>`
    : `<tbody><tr><td class="empty">${sc.running ? "Agents are working…" : "No scan yet. Set your rule above and press Run scan."}</td></tr></tbody>`;
}
const fmtUsd2 = v => v == null || !isFinite(v) ? "–" : (v < 0 ? "−$" : "$") + Math.abs(v).toLocaleString("en-US", { minimumFractionDigits: Math.abs(v) < 100 ? 2 : 0, maximumFractionDigits: Math.abs(v) < 100 ? 2 : 0 });
document.getElementById("scTable").addEventListener("click", e => {
  const dvb = e.target.closest("[data-dv]");
  if (dvb) {
    const r = sc.results.find(x => x.p && x.p.A.key + "~" + x.p.B.key === dvb.dataset.dv);
    if (r) { dv.asset = r.p.a.key; dv.a = r.p.A.exchange; dv.b = r.p.B.exchange; document.getElementById("dvIv").value = document.getElementById("scIv").value; location.hash = "divergence";
      setTimeout(() => { document.getElementById("dvAsset").dataset.sig = ""; renderDvSelectors(); document.getElementById("dvAsset").value = r.p.a.key; fillDvVenues(); loadDivergence(); }, 0); }
    return;
  }
  if (e.target.closest("th")) return;
  const tr = e.target.closest("tr.sc-row"); if (!tr) return;
  const id = tr.dataset.sc;
  if (sc.open.has(id)) sc.open.delete(id); else sc.open.add(id);
  renderScanResults();
});

/* =============================================================================================
   ROUTING, THEME, START
   ============================================================================================= */
const TABS = ["dashboard", "compare", "funding", "divergence", "scanner", "venues"];
function route() {
  const t = (location.hash || "#dashboard").slice(1);
  currentTab = TABS.includes(t) ? t : "dashboard";
  for (const s of TABS) document.getElementById("view-" + s).hidden = s !== currentTab;
  document.querySelectorAll(".side a").forEach(a => { if (a.dataset.tab === currentTab) a.setAttribute("aria-current", "page"); else a.removeAttribute("aria-current"); });
  mon.series = null;
  renderCurrent();
  if (currentTab === "divergence" && !dv.data && state.views.size) setTimeout(() => { renderDvSelectors(); if (!dv.data) loadDivergence(); }, 0);
}
addEventListener("hashchange", route);

const themeSel = document.getElementById("themeSel");
themeSel.value = store.get("theme", "dark");          // dark by default, like the explorer it is modelled on

const typeSel = document.getElementById("assetTypes");
typeSel.value = store.get("assetTypes", "tradfi");
const applyTypes = () => { TYPE_FILTER = typeSel.value; store.set("assetTypes", TYPE_FILTER); state.alertActive.clear(); state.alertsArmed = false; };
typeSel.addEventListener("change", () => { applyTypes(); tick(); }); applyTypes();

const maxAgeSel = document.getElementById("maxAge");
maxAgeSel.value = store.get("maxAge", "60");
const applyMaxAge = () => { MAX_AGE = maxAgeSel.value === "" ? null : +maxAgeSel.value; store.set("maxAge", maxAgeSel.value); state.gapHist.clear(); state.alertActive.clear(); state.alertsArmed = false; };
maxAgeSel.addEventListener("change", () => { applyMaxAge(); tick(); }); applyMaxAge();
const applyTheme = () => {
  const v = themeSel.value;
  if (v) document.documentElement.setAttribute("data-theme", v); else document.documentElement.removeAttribute("data-theme");
  store.set("theme", v); mon.series = null; dv.data && drawDivergence(true);
};
themeSel.addEventListener("change", applyTheme); applyTheme();

route();
for (const v of VENUES) { state.venueStatus[v.id] = { state: "load" }; v.start(makeFeed(v)); }
setInterval(tick, 1000);
const firstDv = setInterval(() => {
  if (currentTab === "divergence" && !dv.data && state.views.size) { renderDvSelectors(); loadDivergence(); }
  if (dv.data || currentTab !== "divergence") clearInterval(firstDv);
}, 1500);
