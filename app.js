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
      meta.universe.forEach((a, i) => { if (!a.isDelisted && ctxs[i]) { this.applyCtx(feed, dex, a.name, ctxs[i], true, this.feeOf(dex, a), this.feeOf(dex, a, 0.00015)); n++; } });
    }
    if (!n) throw new Error("no markets returned");
  },
  exOf(dex) { return dex ? "hl-" + dex : "hl"; },
  // taker fee per fill, from the Hyperliquid fee docs: builder dexes scale the base fee, growth mode cuts it 90 %
  feeOf(dex, a, base = 0.00045) {   // base 0.00045 = taker, 0.00015 = maker (tier 0)
    if (!dex) return base;
    const s = +a.deployerFeeScale || 0;
    return base * (s < 1 ? s + 1 : s * 2) * (a.growthMode === "enabled" ? 0.1 : 1);
  },
  applyCtx(feed, dex, name, c, first, fee, makerFee) {
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
      ticker: name.split(":").pop(), hist: { coin: name }, takerFee: fee ?? 0.00045, makerFee: makerFee ?? 0.00015, hlDex: dex,
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
  async book(m) {                    // full L2 book: [[px, size]...] bids high->low, asks low->high
    const lv = (await getJSON(HL_REST, { type: "l2Book", coin: m.id })).levels;
    return { bids: lv[0].map(l => [+l.px, +l.sz]), asks: lv[1].map(l => [+l.px, +l.sz]) };
  },
  async candles(m, iv, startMs, endMs) {
    const c = await getJSON(HL_REST, { type: "candleSnapshot", req: { coin: m.hist.coin, interval: iv, startTime: startMs, endTime: endMs } });
    return c.map(x => [x.t, +x.c, +x.h, +x.l, +x.n > 0, +x.v * +x.c]);   // last = $ volume in the bar
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
        id: String(m.market_id), ticker: m.symbol, hist: { marketId: m.market_id }, takerFee: 0, makerFee: 0,   // Standard account: 0 fees
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
  async book(m) {
    const r = await getJSON(`${LT_REST}/orderBookOrders?market_id=${m.id}&limit=100`);
    const side = arr => arr.map(o => [+o.price, +o.remaining_base_amount]);
    return { bids: side(r.bids || []).sort((a, b) => b[0] - a[0]), asks: side(r.asks || []).sort((a, b) => a[0] - b[0]) };
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
      for (const x of page) rows.set(x.t, [x.t, +x.c, +x.h, +x.l, +x.V > 0, +x.V]);
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
      const q = m.quotes || {}, k = q.size_1k || q.base || {}, k100 = q.size_100k || {};
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
        bid100k: k100.bid != null ? +k100.bid : null, ask100k: k100.ask != null ? +k100.ask : null,
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
  sort: { cmp: null, opp: { col: "exec", dir: -1 }, fund: { col: "spread", dir: -1 } },
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
    h.t.push(now / 1000); h.max.push(v.exec);
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
  try { runTelegram(now); } catch (e) { console.error("telegram", e); }
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
    for (const v of state.views.values()) if (typeOk(v) && v.ms.length >= 2 && v.minVol >= minVol && v.exec >= thrBps) { state.alertActive.set(v.a.key, { t: now, peak: v.exec, start: true }); n++; }
    state.alertsArmed = true;
    pushEvent("venue", `Watching ${state.views.size} assets across ${state.exchanges.size} venue columns. Alerts use the fill gap (sell at the bid, buy at the ask). ${n} already ≥ ${thrBps / 100}% at start.`);
    return;
  }
  for (const v of state.views.values()) {
    if (!typeOk(v) || v.ms.length < 2 || v.minVol < minVol || v.exec == null) continue;
    const act = state.alertActive.get(v.a.key);
    if (!act && v.exec >= thrBps) {
      state.alertActive.set(v.a.key, { t: now, peak: v.exec });
      const warn = v.jump ? ` <span class="tag stale">${esc(v.jump.m.exchangeLabel)} jumped ${fmtPct(v.jump.r)} in a minute: possible push on one book</span>`
        : sessionOf(now) === "weekend" && ["equity", "index"].includes(v.a.cls) ? ` <span class="tag">weekend: US market shut, no outside price</span>` : "";
      pushEvent("open", `<b>${esc(v.a.label)}</b> ${classTag(v.a.cls)} fillable gap ${fmtBps(v.exec)} bps: ${sideTxt(v)}${warn}`);
      if (v.execPair) tgOnGapOpen(v);
    } else if (act) {
      act.peak = Math.max(act.peak, v.exec);
      if (v.exec < thrBps * 0.8) {
        state.alertActive.delete(v.a.key);
        if (!act.start) pushEvent("close", `<b>${esc(v.a.label)}</b> fillable gap down to ${fmtBps(v.exec)} bps after ${fmtDur(now - act.t)} (peak ${fmtBps(act.peak)} bps; mid gap now ${fmtBps(v.maxGap)})`);
        else pushEvent("close", `<b>${esc(v.a.label)}</b> fillable gap down to ${fmtBps(v.exec)} bps (it was already open when the page started; peak since then ${fmtBps(act.peak)} bps)`);
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

/* ---------- Telegram alerts: gap flips (and optionally new gaps), sent from this tab ---------- */
// A flip: the tradeable (fill) gap on a venue pair was ≥ X one way (sell A at its bid, buy B at its ask),
// then within N minutes the reverse (sell B, buy A) is ≥ X and holds for H seconds. Only while this tab is open.
const tg = { keys: new Map(), queue: [], sending: false };
const tgEl = id => document.getElementById(id);
const TG_FIELDS = ["tgToken", "tgChat", "tgFlipThr", "tgWindow", "tgHold", "tgMinVol"];
const TG_CHECKS = ["tgGaps", "tgOn"];
const tgCfg = () => ({ token: tgEl("tgToken").value.trim(), chat: tgEl("tgChat").value.trim(), on: tgEl("tgOn").checked, gaps: tgEl("tgGaps").checked,
  thr: (+tgEl("tgFlipThr").value || 0.3) * 100, win: (+tgEl("tgWindow").value || 60) * 60e3, hold: (+tgEl("tgHold").value || 0) * 1e3, minVol: +tgEl("tgMinVol").value || 0 });
const tgTokenOk = t => /^\d{5,}:[A-Za-z0-9_-]{30,}$/.test(t);
function tgLog(text, kind = "") {
  const li = document.createElement("li"); li.className = kind;
  li.textContent = `${new Date().toLocaleTimeString()} · ${text}`;
  const ul = tgEl("tgLog"); ul.prepend(li); while (ul.children.length > 30) ul.lastChild.remove();
}
function tgStatus(text, err) { const s = tgEl("tgStatus"); s.textContent = text; s.className = "status-line" + (err ? " err" : ""); }
async function tgApi(method, body) {
  const { token } = tgCfg();
  if (!tgTokenOk(token)) throw new Error("bot token looks wrong (it is like 123456789:AA… from @BotFather)");
  const r = await fetch(`https://api.telegram.org/bot${token}/${method}`, body ? { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) } : {});
  const j = await r.json().catch(() => ({}));
  if (!j.ok) throw new Error(j.description || `${r.status} ${r.statusText}`);
  return j.result;
}
function tgSend(text) {
  tg.queue.push(text);
  if (tg.sending) return;
  tg.sending = true;
  (async () => {
    while (tg.queue.length) {
      const t = tg.queue.shift();
      try { await tgApi("sendMessage", { chat_id: tgCfg().chat, text: t, disable_web_page_preview: true }); tgLog(t.split("\n")[0], "open"); }
      catch (e) { tgLog("not sent: " + e.message, "close"); tgStatus("Telegram error: " + e.message, true); }
      await new Promise(r => setTimeout(r, 1100));                         // stay well under Telegram's 1 msg/s per chat
    }
    tg.sending = false;
  })();
}
const pctTxt = bps => (bps / 100).toFixed(2) + "%";
const tgWarn = v => {
  const w = [];
  if (v.execPair.some(m => m.exchange === "variational")) w.push("Variational quotes can lag up to ~1 min: confirm on its app first.");
  if (v.jump) w.push(`${v.jump.m.exchangeLabel} jumped ${fmtPct(v.jump.r)} in a minute: could be a push on one book.`);
  if (sessionOf(Date.now()) === "weekend" && ["equity", "index"].includes(v.a.cls)) w.push("Weekend: the underlying market is shut.");
  return w.length ? "\n⚠️ " + w.join("\n⚠️ ") : "";
};
function runTelegram(now) {
  const c = tgCfg();
  if (!c.on || !c.chat || !tgTokenOk(c.token) || !state.alertsArmed) return;
  for (const v of state.views.values()) {
    if (!typeOk(v) || v.ms.length < 2) continue;
    let k = tg.keys.get(v.a.key);
    if (!k) tg.keys.set(v.a.key, k = { runDir: null, runStart: 0, seen: {}, fired: {} });
    const strong = v.exec != null && v.execPair && v.exec >= c.thr && v.minVol >= c.minVol;
    if (!strong) { k.runDir = null; continue; }
    const [buy, sell] = v.execPair, dir = `${sell.key}>${buy.key}`, rev = `${buy.key}>${sell.key}`;
    if (k.runDir !== dir) { k.runDir = dir; k.runStart = now; }
    const prev = k.seen[rev];
    if (prev && prev.t < k.runStart && k.runStart - prev.t <= c.win && now - k.runStart >= c.hold && !(k.fired[dir] > prev.t)) {
      k.fired[dir] = now;
      tgSend(`🔁 FLIP · ${v.a.label}\n\n` +
        `Before (${new Date(prev.t).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" })}): ${prev.sellLab} was rich, fill gap ${pctTxt(prev.gap)}\n` +
        `Now: ${sell.exchangeLabel} is rich, fill gap ${pctTxt(v.exec)}\n` +
        `Sell ${sell.exchangeLabel} ${fmtPx(sell.bid)} (bid) / buy ${buy.exchangeLabel} ${fmtPx(buy.ask)} (ask)\n\n` +
        `Flipped in ${fmtDur(k.runStart - prev.t)}, new side held ${Math.round((now - k.runStart) / 1000)}s. Thinner venue trades ${fmtUsd(v.minVol)}/day.` + tgWarn(v));
    }
    const s = k.seen[dir];
    k.seen[dir] = { t: now, gap: Math.max(v.exec, s && now - s.t < 60e3 ? s.gap : 0), sellLab: sell.exchangeLabel };
  }
}
function tgOnGapOpen(v) {
  const c = tgCfg();
  if (!c.on || !c.gaps || !c.chat || !tgTokenOk(c.token) || v.minVol < c.minVol) return;
  const [buy, sell] = v.execPair;
  tgSend(`📈 GAP OPEN · ${v.a.label}\n\nFill gap ${pctTxt(v.exec)}: sell ${sell.exchangeLabel} ${fmtPx(sell.bid)} (bid) / buy ${buy.exchangeLabel} ${fmtPx(buy.ask)} (ask)\nThinner venue trades ${fmtUsd(v.minVol)}/day.` + tgWarn(v));
}
for (const id of TG_FIELDS) { const el = tgEl(id); el.value = store.get(id, el.value); el.addEventListener("change", () => store.set(id, el.value.trim())); }
for (const id of TG_CHECKS) { const el = tgEl(id); el.checked = !!store.get(id, false); el.addEventListener("change", () => { store.set(id, el.checked);
  if (id === "tgOn") tgStatus(el.checked ? "Watching for flips while this tab is open." : "Alerts off."); }); }
tgEl("tgFind").addEventListener("click", async () => {
  try {
    const ups = await tgApi("getUpdates");
    const m = [...ups].reverse().map(u => u.message || u.channel_post || u.my_chat_member).find(x => x?.chat);
    if (!m) return tgStatus("No messages yet: open your bot in Telegram, press Start or send it any message, then click Find again.", true);
    tgEl("tgChat").value = String(m.chat.id); store.set("tgChat", String(m.chat.id));
    tgStatus(`Chat found: ${m.chat.title || [m.chat.first_name, m.chat.last_name].filter(Boolean).join(" ") || m.chat.username || m.chat.id}. Now click Send test.`);
  } catch (e) { tgStatus("Could not reach your bot: " + e.message, true); }
});
tgEl("tgTest").addEventListener("click", async () => {
  try {
    await tgApi("sendMessage", { chat_id: tgCfg().chat, text: "✅ Perp Spread Desk is connected. You'll get a message here when a gap flips." });
    tgStatus("Test sent. Tick 'Alerts on' to start watching.");
  } catch (e) { tgStatus("Test failed: " + e.message, true); }
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
  if (t === "sc" || t === "opp2") { renderScanResults(); return; }
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
  else if (currentTab === "map") window.mapRender?.();
  else if (currentTab === "scanner") { if (!sc.agents.finder) { renderAgents(); renderScanResults(); scRuleHint(); } liveCheck(false); }
}

/* ---------- dashboard ---------- */
/* The tradeable gap: SELL the rich venue at its BID, BUY the cheap venue at its ASK.
   A gap between mid prices can be nothing but two wide spreads; only this one can be captured. */
function topGapRows(n, minVol) {
  return [...state.views.values()].filter(v => typeOk(v) && v.ms.length >= 2 && v.exec != null && v.minVol >= minVol).sort((a, b) => b.exec - a.exec).slice(0, n);
}
const sideTxt = v => v.execPair ? `sell ${esc(v.execPair[1].exchangeLabel)} at ${fmtPx(v.execPair[1].bid)} (bid), buy ${esc(v.execPair[0].exchangeLabel)} at ${fmtPx(v.execPair[0].ask)} (ask)` : "needs bid and ask on two venues";
const spreadOf = m => m.bid > 0 && m.ask > 0 ? (m.ask - m.bid) / ((m.ask + m.bid) / 2) * 1e4 : null;
const insideTag = v => v.maxGap != null && v.exec != null && v.exec <= 0 && v.maxGap >= 5 ? ` <span class="tag stale" title="The prices look ${fmtBps(v.maxGap)} bps apart, but after selling at the bid and buying at the ask you would lose ${fmtBps(-v.exec)} bps. The gap is just the spreads.">inside the spread</span>` : "";
function renderDashboard() {
  const now = Date.now();
  const alertVol = +document.getElementById("alertVol").value || 0;
  const thr = (+document.getElementById("alertThr").value || 1) * 100;
  const multi = [...state.views.values()].filter(v => v.all.length >= 2 && typeOk(v));
  const gaps = topGapRows(1, alertVol)[0];
  const fund = multi.filter(v => v.fundSpread != null && v.fundMinVol >= alertVol).sort((a, b) => b.fundSpread - a.fundSpread)[0];
  const online = VENUES.filter(v => state.venueStatus[v.id]?.lastMsg && now - state.venueStatus[v.id].lastMsg < 30000).length;
  const over = multi.filter(v => v.ms.length >= 2 && v.minVol >= alertVol && v.exec >= thr).length;
  const k = (key, val, sub, c = "") => `<div class="kpi"><span class="k">${key}</span><span class="v ${c}">${val}</span><span class="s">${sub}</span></div>`;
  document.getElementById("kpis").innerHTML =
    k("Feeds online", `${online} / ${VENUES.length}`, `${VENUES.reduce((s, v) => s + msgRate(v.id), 0).toFixed(1)} messages / s`) +
    k("Markets tracked", state.markets.size.toLocaleString(), `${state.exchanges.size} venue columns`) +
    k("Assets on 2+ venues", multi.length.toLocaleString(), sessionOf(now) === "weekend" ? "US market shut: stock gaps unanchored" : sessionOf(now) === "us" ? "US market open" : "US market closed (overnight)", sessionOf(now) === "weekend" ? "muted" : "") +
    k(`Fillable gaps ≥ ${thr / 100}%`, over, `bid vs ask, thinner venue ≥ ${fmtUsd(alertVol)} / day`, over ? "above" : "") +
    k("Best fillable gap", gaps ? fmtBps(gaps.exec) + " bps" : "–", gaps ? `${esc(gaps.a.label)}: sell ${esc(gaps.execPair[1].exchangeLabel)}, buy ${esc(gaps.execPair[0].exchangeLabel)}` : "waiting for data", gaps && gaps.exec > 0 ? "above" : "below") +
    k("Top funding spread", fund ? fmtPct(fund.fundSpread) : "–", fund ? `${esc(fund.a.label)}: short ${esc(fund.fHi.exchangeLabel)}` : "waiting for data");

  // watchlist
  const rv = ribbonViews();
  document.getElementById("wlTable").innerHTML = `<thead><tr><th>Asset</th><th class="l">Richest venue</th><th>Price</th><th class="l">Cheapest venue</th><th>Price</th><th title="Gap between mid prices: can be just two wide spreads">Mid gap bps</th><th title="Sell the rich venue at its bid, buy the cheap one at its ask. This is the gap you can actually take.">Fill gap bps</th><th style="min-width:150px" title="Fill gap over time while this page is open">Fill gap, live</th><th>Funding spread</th><th>Min 24h vol</th></tr></thead><tbody>` +
    (rv.map(({ k, key, v }) => {
      if (!v || v.ms.length < 2) return `<tr><td><div class="asset-cell"><b>${esc(v ? v.a.label : k)}</b></div></td><td class="l muted" colspan="9">${!v ? "not listed on the shown venues yet" : v.all.length >= 2 ? "other venues' quotes are older than the max quote age" : "listed on one shown venue only"}</td></tr>`;
      const fh = flash("wl|hi|" + key, v.hi.price), fl = flash("wl|lo|" + key, v.lo.price);
      return `<tr><td><div class="asset-cell"><b>${esc(v.a.label)}</b>${classTag(v.a.cls)}${jumpTag(v)}<button class="linkish" data-chart="${esc(key)}">chart</button></div></td>
        <td class="l">${esc(v.hi.exchangeLabel)}${ageTag(v.hi)}</td><td class="num ${fh}">${fmtPx(v.hi.price)}</td>
        <td class="l">${esc(v.lo.exchangeLabel)}${ageTag(v.lo)}</td><td class="num ${fl}">${fmtPx(v.lo.price)}</td>
        <td class="num muted">${fmtBps(v.maxGap)}</td>
        <td class="num ${v.exec >= thr ? "above" : v.exec > 0 ? "" : "below"}" title="${sideTxt(v)}">${fmtBps(v.exec)}${insideTag(v)}</td>
        <td style="padding-block:4px">${spark(key)}</td>
        <td class="num">${v.fundSpread == null ? "–" : fmtPct(v.fundSpread)}</td><td class="num">${fmtUsd(v.minVol)}</td></tr>`;
    }).join("") || `<tr><td class="empty" colspan="10">Your watchlist is empty. Type an asset in the search box at the top.</td></tr>`) + "</tbody>";

  // top gaps
  const tg = topGapRows(10, alertVol);
  const maxG = Math.max(1, tg[0]?.exec || 1);
  document.getElementById("topGaps").innerHTML = tg.length ? `<thead><tr><th>Asset</th><th class="l">Sell at bid</th><th class="l">Buy at ask</th><th title="What you lock in by crossing both books now">Fill gap bps</th><th title="Gap between mid prices">Mid gap</th></tr></thead><tbody>` +
    tg.map(v => `<tr><td><div class="asset-cell"><b>${esc(v.a.label)}</b>${classTag(v.a.cls)}${jumpTag(v)}<button class="linkish" data-add="${esc(v.a.key)}">+ watch</button></div></td>
      <td class="l">${esc(v.execPair[1].exchangeLabel)}${ageTag(v.execPair[1])} <span class="num muted">${fmtPx(v.execPair[1].bid)}</span></td>
      <td class="l">${esc(v.execPair[0].exchangeLabel)}${ageTag(v.execPair[0])} <span class="num muted">${fmtPx(v.execPair[0].ask)}</span></td>
      <td class="num ${v.exec > 0 ? "above" : "below"}">${fmtBps(v.exec)}${v.exec > 0 ? `<span class="bar" style="width:${Math.max(4, v.exec / maxG * 60).toFixed(0)}px"></span>` : ""}</td>
      <td class="num muted">${fmtBps(v.maxGap)}</td></tr>`).join("") + "</tbody>"
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
      <span class="num ${v.exec >= thr ? "above" : v.exec != null && v.exec <= 0 ? "below" : ""}" title="${sideTxt(v)}">${v.exec == null ? (v.maxGap == null ? "one venue" : "mid gap " + fmtBps(v.maxGap) + " bps") : "fill gap " + fmtBps(v.exec) + " bps"}</span>
      ${spark(key)}
      <span class="sub">${v.ms.length} venues${v.hi && v.lo && v.hi !== v.lo ? " · " + esc(v.hi.exchangeLabel) + " richest" : ""}</span>
      <button class="linkish" data-chart="${esc(key)}" style="text-align:left">Chart divergence →</button></div>`;
  }).join("") || `<p class="hint">The watchlist is empty. Add an asset above.</p>`;

  const views = rv.map(o => o.v).filter(Boolean);
  const getters = { asset: r => r.a.label, maxGap: r => r.maxGap, exec: r => r.exec, fund: r => r.fundSpread, vol: r => r.minVol };
  for (const e of exs) getters["ex:" + e.id] = r => r.gaps[e.id];
  const rows = applySort([...views], "cmp", getters);
  const head = `<thead><tr>${sortHeader("cmp", "asset", "Asset")}${exs.map(e => sortHeader("cmp", "ex:" + e.id, esc(e.label), e.title)).join("")}
    ${sortHeader("cmp", "exec", "Fill gap bps", "Sell the rich venue at its bid, buy the cheap one at its ask. Positive = a gap you can lock in before fees")}
    ${sortHeader("cmp", "maxGap", "Mid gap bps", "Richest vs cheapest venue on mid prices; can be just the spreads")}
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
      <td class="num ${r.exec > 0 ? "above" : r.exec != null ? "below" : "muted"}" title="${sideTxt(r)}">${fmtBps(r.exec)}${insideTag(r)}</td>
      <td class="num muted">${fmtBps(r.maxGap)}</td>
      <td class="num">${r.fundSpread == null ? "–" : fmtPct(r.fundSpread)}</td>
      <td class="num">${fmtUsd(r.minVol)}</td></tr>`).join("");
  document.getElementById("cmpTable").innerHTML = head + `<tbody>${body}</tbody>`;

  const minVol = +document.getElementById("minVol").value || 0;
  let opp = [...state.views.values()].filter(v => typeOk(v) && v.ms.length >= 2 && v.exec != null && v.minVol >= minVol);
  opp = applySort(opp, "opp", { asset: r => r.a.label, gap: r => r.maxGap, exec: r => r.exec, vol: r => r.minVol, n: r => r.ms.length, fund: r => r.fundSpread }).slice(0, 50);
  const oh = `<thead><tr>${sortHeader("opp", "asset", "Asset")}<th class="l">Sell at bid</th><th class="l">Buy at ask</th>${sortHeader("opp", "exec", "Fill gap bps", "Sell the rich venue at its bid, buy the cheap one at its ask")}${sortHeader("opp", "gap", "Mid gap bps", "Gap between mid prices; can be just the spreads")}${sortHeader("opp", "fund", "Funding spread")}${sortHeader("opp", "n", "Venues")}${sortHeader("opp", "vol", "Min 24h vol")}<th></th></tr></thead>`;
  const ob = opp.map(r => `<tr><td><div class="asset-cell"><b>${esc(r.a.label)}</b>${classTag(r.a.cls)}${jumpTag(r)}</div></td>
      <td class="l">${esc(r.execPair[1].exchangeLabel)}${ageTag(r.execPair[1])} <span class="num muted ${flash("opp|hi|" + r.a.key, r.execPair[1].bid)}">${fmtPx(r.execPair[1].bid)}</span></td>
      <td class="l">${esc(r.execPair[0].exchangeLabel)}${ageTag(r.execPair[0])} <span class="num muted ${flash("opp|lo|" + r.a.key, r.execPair[0].ask)}">${fmtPx(r.execPair[0].ask)}</span></td>
      <td class="num ${r.exec > 0 ? "above" : "below"}">${fmtBps(r.exec)}${insideTag(r)}</td>
      <td class="num muted">${fmtBps(r.maxGap)}</td>
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
    const load = async m => {
      for (let i = 0; ; i++) {
        try { return await seriesFor(m, dv.iv, startMs, endMs); }
        catch (e) { if (i >= 1) throw new Error(`${m.exchangeLabel} history failed (${e.message || e})`); await new Promise(r => setTimeout(r, e.retryAfterMs || 1500)); }
      }
    };
    const [sa, sb] = await Promise.all([load(mA), load(mB)]);
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
   ARB SCANNER — follows the trader's rule:
     1. the gap between two venues reaches the entry level (about 10)
     2. it STAYS there long enough to act on (default 1 hour), and you need a little time to react
     3. you sell the rich venue, buy the cheap one, and close the moment the gap shrinks to the exit
        level (or flips) — ideally the same day
     4. costs come from the real order books: market orders pay taker fees and walk the book,
        limit orders pay maker fees but may not fill on a thin book
   Eight rule-based agents (no chat model) run in this browser on the venues' public data.
   ============================================================================================= */
const AGENTS = [
  { id: "finder", name: "Pair finder" }, { id: "history", name: "History" }, { id: "session", name: "Session tagger" },
  { id: "books", name: "Order-book checker" }, { id: "spotter", name: "Opportunity spotter" }, { id: "quality", name: "Quality guard" },
  { id: "sim", name: "Trade simulator" }, { id: "analyst", name: "Analyst" },
];
const sc = { running: false, stop: false, agents: {}, log: [], results: [], opps: [], open: new Set(), cache: new Map(), books: new Map(),
  sort: { col: "market", dir: -1 }, osort: { col: "start", dir: -1 }, live: [], liveAt: 0, liveBusy: false };
state.sort.sc = sc.sort; state.sort.opp2 = sc.osort;

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
    minWin: +g("scWindow").value, react: +g("scReact").value, closeBy: g("scCloseBy").value, flip: g("scFlip").value, trail: +g("scTrail").value,
    sizeMode: g("scSizeMode").value, size: +g("scSize").value, minVol: +g("scMinVol").value, session: g("scSession").value,
  };
}
function scRuleHint() {
  const s = scSettings(), sp = [...state.markets.values()].find(m => canonOf(m.ticker || "") === "SP500" && m.price > 0);
  const el = document.getElementById("scRuleHint");
  const base = `Open when the gap reaches ${s.entry} ${s.unit === "bps" ? "bps" : "points"} and has stayed there at least ${fmtDur(s.minWin * 60e3)} (you act ${s.react} min after it first appears). Close when it shrinks to ${s.exit} or flips${s.flip === "ride" ? `, or ride a flip until it gives back ${s.trail}` : ""}, ${{ day: "before the day ends (your local midnight)", "12": "within 12 hours", "24": "within 24 hours" }[s.closeBy]}. `;
  el.textContent = base + (s.unit === "bps" && sp ? `With SP500 near ${fmtPx(sp.price)}, ${s.entry} bps ≈ ${(sp.price * s.entry / 1e4).toFixed(1)} points and ${s.exit} bps ≈ ${(sp.price * s.exit / 1e4).toFixed(1)} points.`
    : s.unit === "pts" ? "Price points are raw price differences; use them with one asset at a time (Watchlist only)." : "");
}
["scUnit", "scEntry", "scExit", "scWindow", "scReact", "scCloseBy", "scFlip", "scTrail"].forEach(id => document.getElementById(id).addEventListener("input", scRuleHint));
document.getElementById("scSizeMode").addEventListener("change", e => { document.getElementById("scSize").value = e.target.value === "usd" ? 10000 : 10; });
document.getElementById("scFlip").addEventListener("change", e => { document.getElementById("scTrailWrap").hidden = e.target.value !== "ride"; });

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

/* ---------- order books: what it really costs to trade `qty` units now ---------- */
async function getBook(m, maxAgeMs = 30000) {
  const c = sc.books.get(m.key);
  if (c && Date.now() - c.at < maxAgeMs) return c.book;
  const v = VENUES.find(x => x.id === m.venue);
  let book = null;
  if (v?.book) book = await v.book(m);
  else if (m.bid > 0 && m.ask > 0) {         // Variational RFQ: $1k quote for the first $1k, its $100k quote beyond that
    const u1 = 1000 / ((m.bid + m.ask) / 2);
    book = { bids: [[m.bid, u1], [m.bid100k || m.bid, Infinity]], asks: [[m.ask, u1], [m.ask100k || m.ask, Infinity]], quoteOnly: true };
  }
  sc.books.set(m.key, { at: Date.now(), book });
  return book;
}
function walk(levels, qty) {            // average fill price for qty units, or null if the book is too thin
  let left = qty, cost = 0;
  for (const [px, sz] of levels) { const take = Math.min(left, sz); cost += take * px; left -= take; if (left <= 1e-12) break; }
  return left > 1e-12 ? null : cost / qty;
}
/* market round trip on one leg, in bps of mid: buy then sell (or the reverse) qty units at today's book */
function marketCostBps(book, qty) {
  if (!book || !book.bids.length || !book.asks.length) return null;
  const mid = (book.bids[0][0] + book.asks[0][0]) / 2;
  const buy = walk(book.asks, qty), sell = walk(book.bids, qty);
  if (buy == null || sell == null) return { thin: true, spreadBps: (book.asks[0][0] - book.bids[0][0]) / mid * 1e4, depthUsd: depthUsd(book, mid) };
  return { bps: ((buy - mid) + (mid - sell)) / mid * 1e4, spreadBps: (book.asks[0][0] - book.bids[0][0]) / mid * 1e4, depthUsd: depthUsd(book, mid), quoteOnly: !!book.quoteOnly };
}
function depthUsd(book, mid) {         // $ resting within 0.25 % of mid on the thinner side
  const side = lv => lv.filter(([p]) => Math.abs(p / mid - 1) <= 0.0025).reduce((s, [p, z]) => s + p * (isFinite(z) ? z : 0), 0);
  return Math.min(side(book.bids), side(book.asks));
}

/* ---------- quality guard (history) ---------- */
function spikesPerDay(p, other, days) {
  const r = p.map((v, i) => i ? Math.log(v / p[i - 1]) : 0), ro = other.map((v, i) => i ? Math.log(v / other[i - 1]) : 0);
  let n = 0;
  for (let i = 49; i < r.length - 3; i++) {
    let s2 = 0; for (let k = i - 48; k < i; k++) s2 += r[k] * r[k];
    const sd = Math.sqrt(s2 / 48) || 1e-9;
    if (Math.abs(r[i]) > 5 * sd && Math.abs(r[i]) > 0.002 && Math.abs(ro[i]) < 1.5 * sd && -(r[i + 1] + r[i + 2] + r[i + 3]) / r[i] > 0.6) n++;
  }
  return n / Math.max(days, 1);
}
function qualityOf(p, d, s, sess) {
  const flags = []; let score = 100;
  if (p.a.cls === "crypto") { score -= 35; flags.push(["Crypto token: a big player can push one venue's price to drain market makers. Treat every gap as suspect.", "bad"]); }
  if (p.a.cls === "preipo") { score -= 25; flags.push(["Pre-IPO: no outside price, so a gap may never close.", "bad"]); }
  if (p.minVol < 1e6) { score -= 15; flags.push([`Thin: the quieter venue trades ${fmtUsd(p.minVol)} a day.`, "warn"]); }
  const stale = 1 - d.act.filter(Boolean).length / d.act.length;
  if (stale > 0.2) { score -= 15; flags.push([`Stale: ${(stale * 100).toFixed(0)}% of bars had no trade on one venue, so some gaps are just old prices.`, "warn"]); }
  const spA = spikesPerDay(d.pa, d.pb, s.days), spB = spikesPerDay(d.pb, d.pa, s.days), sp = Math.max(spA, spB);
  if (sp >= 0.5) { score -= Math.min(30, sp * 12); flags.push([`${(spA >= spB ? p.A : p.B).exchangeLabel} had ${sp.toFixed(1)} one-venue spikes a day that snapped back: possible manipulation or wicks on a thin book.`, sp >= 1.5 ? "bad" : "warn"]); }
  return { flags, score: Math.max(0, Math.round(score)), stale };
}

/* ---------- 4. opportunity spotter ---------- */
function findOpportunities(d, s, sess, gapOf, need) {
  const eps = [], n = d.t.length, step = IV_MS[s.iv];
  let i = 0;
  while (i < n) {
    const g = gapOf(i);
    if (!(d.act[i] && Math.abs(g) >= need(i))) { i++; continue; }
    const side = Math.sign(g); let j = i, last = i, peak = Math.abs(g);
    // the window lasts while the gap stays on the same side and near the level (short dips allowed)
    while (j + 1 < n && Math.sign(gapOf(j + 1)) === side && Math.abs(gapOf(j + 1)) >= need(j + 1) * 0.7) {
      j++; if (Math.abs(gapOf(j)) >= need(j)) last = j; peak = Math.max(peak, Math.abs(gapOf(j)));
    }
    eps.push({ i0: i, i1: last, side, peak, start: d.t[i], end: d.t[last] + step, minutes: (d.t[last] + step - d.t[i]) / 60e3, sess: sess[i] });
    i = j + 1;
  }
  return eps;
}

/* ---------- 7. trade simulator ---------- */
function closeDeadline(tMs, s) {
  if (s.closeBy === "day") { const d = new Date(tMs); d.setHours(23, 59, 0, 0); return d.getTime(); }
  return tMs + +s.closeBy * 3600e3;
}
function simulate(op, d, s, gapOf, costs, need) {
  const step = IV_MS[s.iv];
  // you notice it and act `react` minutes after it first appears, if it is still fillable at the entry level
  let e = op.i0; while (e <= op.i1 && d.t[e] - op.start < s.react * 60e3) e++;
  while (e <= op.i1 && !(d.act[e] && Math.abs(gapOf(e)) >= need(e))) e++;
  if (e > op.i1 || e >= d.t.length - 1) return { missed: true };
  // a price only counts if real size traded there: both venues did >= the trade size in the 30 min to entry
  const usd = s.sizeMode === "usd" ? s.size : s.size * d.pa[e];
  let va = 0, vb = 0; for (let k = Math.max(0, e - 5); k <= e; k++) { va += d.va[k] || 0; vb += d.vb[k] || 0; }
  if (va < usd || vb < usd) return { thin: true };
  const side = op.side, deadline = closeDeadline(d.t[e], s);
  let x = -1, reason = "", best = 0, flipped = false;
  for (let k = e + 1; k < d.t.length; k++) {
    const g = gapOf(k);
    if (d.t[k] >= deadline) { x = k; reason = "deadline"; break; }
    if (!d.act[k]) continue;
    if (Math.sign(g) === -side && Math.abs(g) > s.exit) flipped = true;
    if (s.flip === "ride" && flipped) {
      best = Math.max(best, -side * g);                      // how far it has swung the other way
      if (best - (-side * g) >= s.trail) { x = k; reason = "rode the flip"; break; }
      continue;
    }
    if (Math.abs(g) <= s.exit || Math.sign(g) === -side) { x = k; reason = flipped ? "flipped" : "converged"; break; }
  }
  if (x < 0) return { open: true, e };
  const q = s.sizeMode === "usd" ? s.size / d.pa[e] : s.size;
  const ptsIn = d.pb[e] - d.pa[e], ptsOut = d.pb[x] - d.pa[x];
  const gross = q * side * (ptsIn - ptsOut);
  const notional = q * (d.pa[e] + d.pa[x]) / 2;
  // market: taker fee on 4 fills + today's book impact on both legs; limit: maker fee on 4 fills
  const mkt = costs.mktA == null || costs.mktB == null ? null : notional * (2 * (costs.takerA + costs.takerB) + (costs.mktA + costs.mktB) / 1e4);
  const lim = notional * 2 * (costs.makerA + costs.makerB);
  return { e, x, tIn: d.t[e], tOut: d.t[x], gIn: gapOf(e), gOut: gapOf(x), fillIn: Math.abs(gapOf(e)) - (need(e) - s.entry), q, gross, netMkt: mkt == null ? null : gross - mkt, netLim: gross - lim, costMkt: mkt, costLim: lim,
    hours: (d.t[x] - d.t[e]) / 3600e3, reason, sameDay: new Date(d.t[e]).toDateString() === new Date(d.t[x]).toDateString(), sess: sessionOf(d.t[e]) };
}

/* ---------- the run ---------- */
async function runScan() {
  if (sc.running) return;
  const s = scSettings();
  sc.running = true; sc.stop = false; sc.results = []; sc.opps = []; sc.open.clear(); sc.agents = {}; sc.log = [];
  document.getElementById("scRun").disabled = true; document.getElementById("scStop").hidden = false;
  renderScanResults();
  const allow = { all: () => true, weekday: t => sessionOf(t) !== "weekend", us: t => sessionOf(t) === "us" }[s.session];
  try {
    const pairs = findPairs(s);
    const noHist = [...state.assets.values()].filter(a => a.keys.length >= 2).flatMap(a => a.keys.map(k => state.markets.get(k))).filter(m => m && !m.hist && isShown(m.exchange)).length;
    agentSay("finder", `${pairs.length} pairs to test (history on both venues, ≥ ${fmtUsd(s.minVol)} a day on the thinner one).${noHist ? ` ${noHist} Variational markets left out: Variational publishes no price history.` : ""}`, "done");
    if (!pairs.length) { agentSay("analyst", "Nothing to test with these settings. Lower the volume floor or widen the asset types.", "done"); return; }
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
          const d = { t: [], pa: [], pb: [], act: [], va: [], vb: [] };
          for (const r of ca) { const o = mapB.get(r[0]); if (!o) continue; d.t.push(r[0]); d.pa.push(r[1]); d.pb.push(o[1]); d.act.push(!!(r[4] && o[4])); d.va.push(r[5] || 0); d.vb.push(o[5] || 0); }
          if (d.t.length < 50) { sc.results.push({ p, label, skip: `only ${d.t.length} bars on both venues` }); continue; }
          const gapOf = s.unit === "bps" ? i => (d.pb[i] / d.pa[i] - 1) * 1e4 : i => d.pb[i] - d.pa[i];

          agentSay("session", `Tagging ${d.t.length} bars of ${p.a.label}`);
          const sess = d.t.map(sessionOf);

          // order books first: a gap only counts if it survives selling at the bid and buying at the ask
          agentSay("books", `Reading both order books for ${label}`);
          let costs = { takerA: p.A.takerFee ?? 0.00045, takerB: p.B.takerFee ?? 0.00045, makerA: p.A.makerFee ?? 0.00015, makerB: p.B.makerFee ?? 0.00015, mktA: null, mktB: null };
          const q0 = s.sizeMode === "usd" ? s.size / d.pa[d.pa.length - 1] : s.size;
          const [ba, bb] = await Promise.all([getBook(p.A).catch(() => null), getBook(p.B).catch(() => null)]);
          const ca2 = marketCostBps(ba, q0), cb2 = marketCostBps(bb, q0);
          costs.mktA = ca2 && !ca2.thin ? ca2.bps : null; costs.mktB = cb2 && !cb2.thin ? cb2.bps : null;
          const bookNote = { A: ca2, B: cb2, q: q0 };
          // crossing both books costs half of each round trip on the way in (in bps of price)
          const crossBps = costs.mktA != null && costs.mktB != null ? (costs.mktA + costs.mktB) / 2 : null;
          if (crossBps == null) {
            const thin = [[p.A, ca2], [p.B, cb2]].filter(([, c]) => !c || c.thin).map(([m]) => m.exchangeLabel).join(" and ");
            sc.results.push({ p, label, skip: `${thin} book too thin to fill ${fmtNum(q0)} units` }); continue;
          }
          const need = s.unit === "bps" ? () => s.entry + crossBps : i => s.entry + crossBps / 1e4 * d.pa[i];

          agentSay("spotter", `Looking for fillable gaps ≥ ${s.entry} that lasted ${fmtDur(s.minWin * 60e3)} on ${label}`);
          const eps = findOpportunities(d, s, sess, gapOf, need);
          const real = eps.filter(o => o.minutes >= s.minWin && allow(o.start));
          const brief = eps.filter(o => o.minutes < s.minWin).length;
          // what a mid-price scan would have shown: windows that were only the spreads
          const midEps = findOpportunities(d, s, sess, gapOf, () => s.entry).filter(o => o.minutes >= s.minWin && allow(o.start));
          const spreadOnly = Math.max(0, midEps.length - real.length);

          agentSay("quality", `Checking ${label}`);
          const qual = qualityOf(p, d, s, sess);
          const absMid = d.t.map((_, i) => Math.abs((d.pb[i] / d.pa[i] - 1) * 1e4)).sort((x, y) => x - y);
          const medGap = absMid[absMid.length >> 1];
          if (crossBps > medGap) { qual.score = Math.max(0, qual.score - 15); qual.flags.unshift([`Crossing both books costs ${crossBps.toFixed(1)} bps, more than the typical gap (${medGap.toFixed(1)} bps): most gaps you see here are just the spreads.`, "bad"]); }

          agentSay("sim", `Simulating ${real.length} opportunit${real.length === 1 ? "y" : "ies"} on ${label}`);
          const trades = [];
          let missed = 0, stillOpen = 0, thinPrints = 0;
          for (const op of real) {
            const r = simulate(op, d, s, gapOf, costs, need);
            if (r.missed) { missed++; continue; }
            if (r.thin) { thinPrints++; continue; }
            if (r.open) { stillOpen++; sc.opps.push({ p, label, op, open: true }); continue; }
            trades.push(r); sc.opps.push({ p, label, op, tr: r });
          }
          const conv = trades.filter(t => t.reason !== "deadline");
          const res = { p, label, d, eps, real, brief, trades, conv, missed, stillOpen, thinPrints, qual, bookNote, costs, crossBps, spreadOnly, medGap,
            lim: trades.reduce((x, t) => x + t.netLim, 0), mkt: trades.some(t => t.netMkt == null) ? null : trades.reduce((x, t) => x + t.netMkt, 0) };
          judge(res, s);
          sc.results.push(res);
        } catch (e) {
          sc.results.push({ p, label, skip: "could not load: " + (e.message || e) });
        } finally {
          done++;
          if (done % 3 === 0 || !queue.length) renderScanResults();
        }
      }
    };
    await Promise.all([worker(), worker()]);
    for (const a of ["history", "session", "spotter", "quality", "books", "sim"]) sc.agents[a] = { msg: `Finished ${done} pairs`, st: "done" };
    const n = sc.opps.filter(o => o.tr).length;
    agentSay("analyst", sc.stop ? `Stopped after ${done} pairs.` : `Done: ${n} opportunities you could have taken across ${done} pairs. Summary below.`, "done");
  } finally {
    sc.running = false;
    document.getElementById("scRun").disabled = false; document.getElementById("scStop").hidden = true;
    renderScanResults();
  }
}
document.getElementById("scRun").addEventListener("click", runScan);
document.getElementById("scStop").addEventListener("click", () => { sc.stop = true; agentSay("analyst", "Stopping after the pairs in progress…"); });

/* ---------- analyst: verdict per pair ---------- */
function judge(r, s) {
  const why = [], t = r.trades, wins = t.filter(x => x.netLim > 0).length;
  const notional = s.sizeMode === "usd" ? s.size : s.size * r.d.pa[r.d.pa.length - 1];
  const minNet = 0.0025 * notional;
  if (!r.real.length) {
    r.verdict = "na";
    why.push(r.eps.length ? `After paying ${r.crossBps.toFixed(1)} bps to cross both books, the gap reached ${s.entry} ${s.unit} ${r.eps.length} time${r.eps.length > 1 ? "s" : ""} but never stayed ${fmtDur(s.minWin * 60e3)}.` : `After paying ${r.crossBps.toFixed(1)} bps to cross both books, the gap never reached ${s.entry} ${s.unit} in ${s.days} days.`);
    if (r.spreadOnly) why.push(`${r.spreadOnly} window${r.spreadOnly > 1 ? "s" : ""} looked like ${s.entry}+ on mid prices but disappear once you sell at the bid and buy at the ask.`);
  } else {
    const conv = r.conv.length;
    why.push(`${r.real.length} window${r.real.length > 1 ? "s" : ""} stayed fillable at ${s.entry} ${s.unit} (after paying ${r.crossBps.toFixed(1)} bps to cross both books) for over ${fmtDur(s.minWin * 60e3)}.${r.spreadOnly ? ` ${r.spreadOnly} more looked like gaps on mid prices but were only the spreads.` : ""} ${t.length} could be taken after your ${s.react}-minute reaction${r.missed ? `, ${r.missed} closed before you could act` : ""}${r.thinPrints ? `, ${r.thinPrints} left out because one venue traded less than your size around entry (a few small prints, not a fillable price)` : ""}${r.stillOpen ? `, ${r.stillOpen} still open now` : ""}.`);
    if (t.length) why.push(`${conv} of ${t.length} converged before your deadline${t.length - conv ? `; ${t.length - conv} had to be closed unconverged` : ""}. Median time to close: ${fmtDur(median(t.map(x => x.hours)) * 3600e3)}.`);
    if (t.length) why.push(`With limit orders: ${fmtUsd2(r.lim)} (${wins} of ${t.length} won). With market orders: ${r.mkt == null ? "the book is too thin to fill your size" : fmtUsd2(r.mkt)}${r.mkt != null && r.lim > 0 ? `, so taker fees and crossing the book eat ${Math.round(Math.min(999, (r.lim - r.mkt) / r.lim * 100))}% of the profit` : ""}.`);
    const mwins = t.filter(x => x.netMkt > 0).length;
    const clean = t.length >= 3 && wins / Math.max(1, t.length) >= 0.6 && r.qual.score >= 65 && !["crypto", "preipo"].includes(r.p.a.cls);
    const tight = r.crossBps <= s.entry / 2;           // limits only plausible when the books are tight
    r.verdict = !t.length ? "na" : clean && r.mkt >= minNet && mwins / t.length >= 0.6 ? "go" : clean && tight && r.lim >= minNet ? "limit" : r.lim > 0 || r.mkt > 0 ? "watch" : "avoid";
    if (r.verdict === "limit") why.push(`Only with limit orders: market orders lose the edge. The books are tight (${r.crossBps.toFixed(1)} bps to cross), so resting limits have a fair chance to fill.`);
    if (r.verdict === "watch") why.push(t.length < 3 ? "Fewer than 3 trades: too few to trust." : r.lim < minNet ? `Under ${fmtUsd(minNet)} (0.25% of your size) over ${s.days} days: too small for the risk.` : r.qual.score < 65 ? "The quality guard is worried (see flags)." : wins / t.length < 0.6 ? "Fewer than 60% of trades won." : "The asset type is risky.");
  }
  if (r.bookNote) {
    const b = (m, c) => !c ? `${m.exchangeLabel}: no book` : c.thin ? `${m.exchangeLabel}: too thin for ${fmtNum(r.bookNote.q)} units (only ${fmtUsd(c.depthUsd)} within 0.25%)` : `${m.exchangeLabel}: spread ${c.spreadBps.toFixed(1)} bps, market round trip ${c.bps.toFixed(1)} bps, ${fmtUsd(c.depthUsd)} within 0.25%`;
    why.push(`Order books now (${new Date().toLocaleString(undefined, { weekday: "short", hour: "2-digit", minute: "2-digit" })}). ${b(r.p.A, r.bookNote.A)}. ${b(r.p.B, r.bookNote.B)}.${sessionOf(Date.now()) === "weekend" ? " Weekend books are usually wider than weekday ones, so weekday results here are on the cautious side." : ""}`);
  }
  r.why = why;
}
const fmtNum = v => v >= 100 ? v.toFixed(0) : v >= 1 ? v.toFixed(2) : v.toPrecision(3);

/* ---------- analyst: daily summary ---------- */
function dailySummary(s) {
  const taken = sc.opps.filter(o => o.tr);
  if (!sc.results.length) return "";
  const dayKey = t => { const d = new Date(t); return d.getFullYear() + "-" + (d.getMonth() + 1) + "-" + d.getDate(); };
  const days = [];
  for (let k = 0; k < 3; k++) { const d = new Date(); d.setHours(12, 0, 0, 0); d.setDate(d.getDate() - k); days.push(d); }
  const unit = s.unit === "bps" ? "bps" : "points";
  const lines = days.map((d, k) => {
    const key = dayKey(d.getTime());
    const list = taken.filter(o => dayKey(o.tr.tIn) === key).sort((a, b) => (b.tr.netMkt ?? -1e9) - (a.tr.netMkt ?? -1e9));
    const openNow = sc.opps.filter(o => o.open && dayKey(o.op.start) === key);
    const name = k === 0 ? "Today" : k === 1 ? "Yesterday" : d.toLocaleDateString(undefined, { weekday: "long" });
    const date = d.toLocaleDateString(undefined, { weekday: "short", month: "short", day: "numeric" });
    const wk = sessionOf(d.getTime()) === "weekend" || [0, 6].includes(d.getDay());
    if (!list.length) return `<p><b>${name} (${date}).</b> No gap stayed fillable at ${s.entry} ${unit} (after crossing both books) for ${fmtDur(s.minWin * 60e3)} on the pairs scanned${openNow.length ? `; ${openNow.length} window${openNow.length > 1 ? "s are" : " is"} open right now (${openNow.map(o => esc(o.p.a.label)).join(", ")})` : ""}.${wk ? " Stock, index, FX and commodity markets are shut at the weekend, so their perps drift without an outside price; gaps then are less reliable." : ""}</p>`;
    const sum = list.reduce((x, o) => x + o.tr.netLim, 0), sumM = list.every(o => o.tr.netMkt != null) ? list.reduce((x, o) => x + o.tr.netMkt, 0) : null;
    const conv = list.filter(o => o.tr.reason !== "deadline").length;
    const b = list[0], t = b.tr;
    const rich = t.gIn > 0 ? b.p.B.exchangeLabel : b.p.A.exchangeLabel, cheap = t.gIn > 0 ? b.p.A.exchangeLabel : b.p.B.exchangeLabel;
    const others = list.slice(1, 3).map(o => `${esc(o.p.a.label)} (${o.tr.netMkt == null ? "book too thin" : fmtUsd2(o.tr.netMkt)})`).join(", ");
    return `<p><b>${name} (${date}).</b> ${list.length} opportunit${list.length > 1 ? "ies" : "y"} that stayed fillable after crossing both books, ${conv} closed at your exit level the same day. ` +
      `Best: <b>${esc(b.p.a.label)}</b>, sell ${esc(rich)} / buy ${esc(cheap)} at ${fmtClock(t.tIn).slice(0, 5)}: ${Math.abs(t.gIn).toFixed(1)} ${unit} apart, ${t.fillIn.toFixed(1)} after crossing the books (peak ${b.op.peak.toFixed(1)}), ` +
      `${t.reason === "deadline" ? `still ${Math.abs(t.gOut).toFixed(1)} apart at your deadline` : t.reason === "rode the flip" ? `flipped and you rode it to ${(-Math.sign(t.gIn) * t.gOut).toFixed(1)} the other way` : t.reason === "flipped" ? `flipped to ${Math.abs(t.gOut).toFixed(1)} the other way after ${fmtDur(t.hours * 3600e3)} (you gain the full swing)` : `back to ${Math.abs(t.gOut).toFixed(1)} after ${fmtDur(t.hours * 3600e3)}`}: ` +
      `${t.netMkt != null ? `${fmtUsd2(t.netMkt)} with market orders` : "book too thin for market orders"} (${fmtUsd2(t.netLim)} if limit orders had filled).` +
      `${others ? ` Also ${others}.` : ""} All together ${sumM != null ? `${fmtUsd2(sumM)} with market orders` : "n/a with market orders"} (${fmtUsd2(sum)} with limits, if filled).${wk ? " Weekend: the underlying markets were shut, so these perps had no outside price. Treat them with care." : ""}</p>`;
  });
  const all = taken.reduce((x, o) => x + o.tr.netLim, 0);
  const allM = taken.every(o => o.tr.netMkt != null) ? taken.reduce((x, o) => x + o.tr.netMkt, 0) : null;
  const fake = sc.results.filter(r => !r.skip).reduce((x, r) => x + (r.spreadOnly || 0), 0);
  const pairsGo = sc.results.filter(r => r.verdict === "go" || r.verdict === "limit");
  const lead = `<p class="muted">${taken.length} opportunities over the last ${s.days} days across ${sc.results.filter(r => !r.skip).length} pairs that stayed fillable after selling at the bid and buying at the ask: ${allM == null ? "n/a" : fmtUsd2(allM)} with market orders, ${fmtUsd2(all)} if limit orders had filled, at ${s.sizeMode === "usd" ? fmtUsd(s.size) : s.size + " units"} per leg.${fake ? ` ${fake} more windows looked like gaps on mid prices but were only the spreads, and were thrown out.` : ""} ${pairsGo.length ? `Pairs that passed every check: ${pairsGo.sort((a, b) => b.lim - a.lim).slice(0, 6).map(r => esc(r.p.a.label) + " (" + esc(r.p.A.exchangeLabel) + " / " + esc(r.p.B.exchangeLabel) + (r.verdict === "limit" ? ", limit orders only" : "") + ")").join(", ")}.` : "No pair passed every check."} Prices are last trades on 5-minute bars, so check the live books before you act.</p>`;
  return lead + lines.join("");
}

/* ---------- results ---------- */
const VERDICT = { go: "Tradeable", limit: "Limit orders only", watch: "Watch", avoid: "Avoid", na: "No real window" };
function renderScanResults() {
  const s = scSettings();
  const rs = sc.results.filter(r => !r.skip);
  document.getElementById("scDaily").innerHTML = rs.length ? dailySummary(s) : `<p class="muted">${sc.running ? "The analyst writes this when the agents finish." : "Run a scan to get a written summary of the last three days."}</p>`;
  const taken = sc.opps.filter(o => o.tr);
  const k = (key, val, sub, c = "") => `<div class="kpi"><span class="k">${key}</span><span class="v ${c}">${val}</span><span class="s">${sub}</span></div>`;
  const lim = taken.reduce((x, o) => x + o.tr.netLim, 0), mk = taken.every(o => o.tr.netMkt != null) ? taken.reduce((x, o) => x + o.tr.netMkt, 0) : null;
  document.getElementById("scSummary").innerHTML = rs.length ?
    k("Pairs scanned", rs.length, `${sc.results.length - rs.length} skipped`) +
    k("Windows ≥ " + fmtDur(s.minWin * 60e3), rs.reduce((x, r) => x + r.real.length, 0), `${rs.reduce((x, r) => x + r.brief, 0)} more were too brief`) +
    k("Opportunities taken", taken.length, `${taken.filter(o => o.tr.reason !== "deadline").length} converged in time`) +
    k("With market orders", mk == null ? "–" : fmtUsd2(mk), "sell at the bid, buy at the ask, taker fees", mk > 0 ? "above" : mk < 0 ? "below" : "") +
    k("If limit orders filled", fmtUsd2(lim), "maker fees only; not guaranteed on thin books", "muted") +
    k("Spread-only windows thrown out", rs.reduce((x, r) => x + (r.spreadOnly || 0), 0), "looked like gaps on mid prices") : "";

  // per-pair table
  const getters = { pair: r => r.label, cls: r => r.p.a.cls, score: r => r.qual.score, win: r => r.real.length, taken: r => r.trades.length,
    conv: r => r.trades.length ? r.conv.length / r.trades.length : null, limit: r => r.lim, market: r => r.mkt, verdict: r => ({ go: 4, limit: 3, watch: 2, na: 1, avoid: 0 }[r.verdict]) };
  const rows = applySort([...rs], "sc", getters);
  const head = `<thead><tr>${sortHeader("sc", "pair", "Pair")}${sortHeader("sc", "cls", "Type")}${sortHeader("sc", "verdict", "Verdict")}${sortHeader("sc", "score", "Quality")}
    ${sortHeader("sc", "win", "Windows", "Gaps at your entry level that lasted at least your minimum time")}${sortHeader("sc", "taken", "Taken", "Windows still open after your reaction time")}
    ${sortHeader("sc", "conv", "Converged", "Share that reached your exit level before the deadline")}
    ${sortHeader("sc", "market", "Net, market $", "Sell at the bid, buy at the ask, taker fees: what you can count on")}${sortHeader("sc", "limit", "Limit $, if filled", "Maker fees only; assumes your resting orders fill")}<th class="l" title="Cost to buy and sell your size on each book right now">Books now</th></tr></thead>`;
  const bk = r => !r.bookNote ? `<span class="muted">–</span>` : [r.bookNote.A, r.bookNote.B].map(c => !c ? "no book" : c.thin ? `<span class="above">too thin</span>` : `${c.bps.toFixed(1)}`).join(" / ") + " bps";
  const body = rows.map(r => {
    const id = r.p.A.key + "~" + r.p.B.key, open = sc.open.has(id);
    const nf = r.qual.flags.filter(f => f[1] !== "info").length;
    let html = `<tr class="sc-row" data-sc="${esc(id)}" aria-expanded="${open}">
      <td><div class="asset-cell"><b>${esc(r.p.a.label)}</b><span class="muted">${esc(r.p.A.exchangeLabel)} vs ${esc(r.p.B.exchangeLabel)}</span></div></td>
      <td class="l">${classTag(r.p.a.cls)}</td><td class="l"><span class="pill ${r.verdict === "limit" ? "watch" : r.verdict}">${VERDICT[r.verdict]}</span></td>
      <td class="num ${r.qual.score >= 65 ? "" : "above"}">${r.qual.score}${nf ? ` <span class="tag stale">${nf} flag${nf > 1 ? "s" : ""}</span>` : ""}</td>
      <td class="num">${r.real.length}</td><td class="num">${r.trades.length}</td>
      <td class="num">${r.trades.length ? Math.round(r.conv.length / r.trades.length * 100) + "%" : "–"}</td>
      <td class="num ${r.mkt > 0 ? "above" : r.mkt < 0 ? "below" : ""}">${r.trades.length ? (r.mkt == null ? "book too thin" : fmtUsd2(r.mkt)) : "–"}</td>
      <td class="num muted">${r.trades.length ? fmtUsd2(r.lim) : "–"}</td>
      <td class="l num">${bk(r)}</td></tr>`;
    if (open) {
      html += `<tr class="sc-detail"><td colspan="10"><div style="display:grid;gap:4px;max-width:1000px">
        <b>Analyst</b><ul>${r.why.map(w => `<li>${esc(w)}</li>`).join("")}</ul>
        <b>Quality guard (score ${r.qual.score} of 100)</b><ul>${r.qual.flags.length ? r.qual.flags.map(f => `<li class="${f[1] === "bad" ? "above" : ""}">${esc(f[0])}</li>`).join("") : "<li>No warning signs: both books traded steadily.</li>"}</ul>
        <span class="muted">Fees per fill: ${esc(r.p.A.exchangeLabel)} taker ${(r.costs.takerA * 1e4).toFixed(2)} / maker ${(r.costs.makerA * 1e4).toFixed(2)} bps · ${esc(r.p.B.exchangeLabel)} taker ${(r.costs.takerB * 1e4).toFixed(2)} / maker ${(r.costs.makerB * 1e4).toFixed(2)} bps</span>
        <div><button class="linkish" data-dv="${esc(id)}">Open this pair in Divergence →</button></div>
        ${oppTable(sc.opps.filter(o => o.p === r.p), s, false)}
      </div></td></tr>`;
    }
    return html;
  }).join("");
  const skipped = sc.results.filter(r => r.skip);
  document.getElementById("scTable").innerHTML = rs.length || skipped.length
    ? head + `<tbody>${body}${skipped.length ? `<tr><td class="empty" colspan="10">Skipped: ${skipped.slice(0, 10).map(r => esc(r.label) + " (" + esc(r.skip) + ")").join("; ")}${skipped.length > 10 ? ` and ${skipped.length - 10} more` : ""}</td></tr>` : ""}</tbody>`
    : `<tbody><tr><td class="empty">${sc.running ? "Agents are working…" : "No scan yet. Set your rule above and press Run scan."}</td></tr></tbody>`;
  document.getElementById("scOpps").innerHTML = oppTable(sc.opps, s, true);
}
function oppTable(list, s, withPair) {
  const unit = s.unit === "bps" ? "bps" : "pts";
  const rows = applySort([...list], "opp2", { start: o => o.op.start, pair: o => o.label, peak: o => o.op.peak, lasted: o => o.op.minutes,
    limit: o => o.tr ? o.tr.netLim : null, market: o => o.tr ? o.tr.netMkt : null, hold: o => o.tr ? o.tr.hours : null }).slice(0, withPair ? 300 : 60);
  if (!rows.length) return withPair ? `<table><tbody><tr><td class="empty">${sc.running ? "Agents are working…" : "No opportunities yet."}</td></tr></tbody></table>` : "";
  const sh = (c, l, t) => sortHeader("opp2", c, l, t);
  return `<div class="table-wrap"><table><thead><tr>${sh("start", "Gap opened")}${withPair ? sh("pair", "Pair") : ""}<th class="l">Rich venue</th>${sh("peak", "Peak " + unit)}${sh("lasted", "Lasted")}
    <th title="Mid gap at entry → at exit; the fillable gap at entry is in the trade's reasons">In / out ${unit}</th>${sh("hold", "Held")}<th class="l">Outcome</th>${sh("market", "Market $")}${sh("limit", "Limit $, if filled")}<th class="l">Session</th></tr></thead><tbody>` +
    rows.map(o => {
      const t = o.tr, rich = o.op.side > 0 ? o.p.B.exchangeLabel : o.p.A.exchangeLabel;
      return `<tr><td class="num">${fmtTime(o.op.start)}</td>${withPair ? `<td><div class="asset-cell"><b>${esc(o.p.a.label)}</b><span class="muted">${esc(o.p.A.exchangeLabel)} / ${esc(o.p.B.exchangeLabel)}</span></div></td>` : ""}
        <td class="l">${esc(rich)}</td><td class="num">${o.op.peak.toFixed(1)}</td><td class="num">${fmtDur(o.op.minutes * 60e3)}</td>
        <td class="num">${t ? `${Math.abs(t.gIn).toFixed(1)} → ${(t.gOut * Math.sign(t.gIn)).toFixed(1)}` : "–"}</td>
        <td class="num">${t ? fmtDur(t.hours * 3600e3) : "–"}</td>
        <td class="l">${o.open ? "<b>still open</b>" : { converged: "closed at exit", flipped: "flipped, closed", "rode the flip": "rode the flip", deadline: "<span class=\"above\">deadline, not converged</span>" }[t.reason]}${t && !t.sameDay ? " · next day" : ""}</td>
        <td class="num ${t && t.netMkt > 0 ? "above" : t && t.netMkt < 0 ? "below" : ""}">${t ? (t.netMkt == null ? "too thin" : fmtUsd2(t.netMkt)) : "–"}</td>
        <td class="num muted">${t ? fmtUsd2(t.netLim) : "–"}</td>
        <td class="l">${{ us: "US hours", off: "overnight", weekend: "weekend" }[o.op.sess]}</td></tr>`;
    }).join("") + "</tbody></table></div>";
}
const fmtUsd2 = v => v == null || !isFinite(v) ? "–" : (v < 0 ? "−$" : "$") + Math.abs(v).toLocaleString("en-US", { minimumFractionDigits: Math.abs(v) < 100 ? 2 : 0, maximumFractionDigits: Math.abs(v) < 100 ? 2 : 0 });
document.getElementById("view-scanner").addEventListener("click", e => {
  const dvb = e.target.closest("[data-dv]");
  if (dvb) {
    const r = sc.results.find(x => x.p && x.p.A.key + "~" + x.p.B.key === dvb.dataset.dv);
    if (r) { dv.asset = r.p.a.key; dv.a = r.p.A.exchange; dv.b = r.p.B.exchange; document.getElementById("dvIv").value = document.getElementById("scIv").value;
      document.getElementById("dvThr").value = scSettings().unit === "bps" ? (scSettings().entry / 100).toFixed(2) : document.getElementById("dvThr").value; location.hash = "divergence";
      setTimeout(() => { document.getElementById("dvAsset").dataset.sig = ""; renderDvSelectors(); document.getElementById("dvAsset").value = r.p.a.key; fillDvVenues(); loadDivergence(); }, 0); }
    return;
  }
  if (e.target.closest("th")) return;
  const tr = e.target.closest("tr.sc-row"); if (!tr) return;
  const id = tr.dataset.sc;
  if (sc.open.has(id)) sc.open.delete(id); else sc.open.add(id);
  renderScanResults();
});

/* ---------- live check: gaps open right now, priced off the books for your size ----------
   The gap that matters is SELL the rich venue at its bid and BUY the cheap venue at its ask,
   walking each book for your size. Mid-price gaps that vanish inside the spreads are shown last. */
async function liveCheck(force) {
  if (sc.liveBusy || (!force && Date.now() - sc.liveAt < 30000)) return;
  sc.liveBusy = true;
  const s = scSettings();
  try {
    const set = TYPE_SETS[s.types];
    const cands = [...state.views.values()].filter(v => (!set || set.includes(v.a.cls)) && v.ms.length >= 2 && v.minVol >= s.minVol && v.hi && v.lo
      && (s.unit === "bps" ? v.maxGap : v.hi.price - v.lo.price) >= s.entry).sort((a, b) => (b.exec ?? -1e9) - (a.exec ?? -1e9)).slice(0, 10);
    const out = [];
    for (const v of cands) {
      // trade direction from the mid gap: sell the venue priced higher, buy the one priced lower
      const rich = v.hi, cheap = v.lo;
      const q = s.sizeMode === "usd" ? s.size / cheap.price : s.size;
      const [bh, bl] = await Promise.all([getBook(rich, 15000).catch(() => null), getBook(cheap, 15000).catch(() => null)]);
      const ch = marketCostBps(bh, q), cl = marketCostBps(bl, q);
      const sellAt = bh ? walk(bh.bids, q) : null, buyAt = bl ? walk(bl.asks, q) : null;
      const fillBps = sellAt && buyAt ? (sellAt / buyAt - 1) * 1e4 : null;
      const fees = 2 * ((rich.takerFee ?? 0.00045) + (cheap.takerFee ?? 0.00045)) * 1e4;   // 4 taker fills
      const exitCross = ch && cl && !ch.thin && !cl.thin ? (ch.bps + cl.bps) / 2 : null;      // closing crosses again
      const edge = fillBps != null && exitCross != null ? fillBps - s.exit - exitCross - fees : null;
      const notional = q * cheap.price;
      const verdict = fillBps == null ? ["thin", "Book too thin for your size"]
        : fillBps <= 0 ? ["no", "Not a trade: the gap is inside the spreads"]
        : fillBps < s.entry ? ["no", `Fills at only ${fillBps.toFixed(1)} bps, below your ${s.entry}`]
        : edge <= 0 ? ["no", "Costs eat it: closing later costs more than is left"]
        : Math.max(ageOf(rich), ageOf(cheap)) > 10 ? ["thin", `Check first: ${(ageOf(rich) > ageOf(cheap) ? rich : cheap).exchangeLabel} quote is ${Math.round(Math.max(ageOf(rich), ageOf(cheap)))} s old`]
        : ["yes", "Fillable now"];
      out.push({ v, rich, cheap, q, ch, cl, sellAt, buyAt, fillBps, fees, exitCross, edge, notional, verdict });
    }
    out.sort((a, b) => (b.edge ?? -1e9) - (a.edge ?? -1e9));
    sc.live = out; sc.liveAt = Date.now();
  } finally { sc.liveBusy = false; renderLive(); }
}
function renderLive() {
  const s = scSettings();
  const el = document.getElementById("scLive");
  if (!sc.liveAt) { el.innerHTML = `<tbody><tr><td class="empty">Checking the books…</td></tr></tbody>`; return; }
  document.getElementById("scLiveAt").textContent = "Checked " + fmtClock(sc.liveAt) + ". Refreshes every 30 s while this page is open.";
  if (!sc.live.length) { el.innerHTML = `<tbody><tr><td class="empty">No gap of ${s.entry} ${s.unit} on mid prices right now on the selected asset types.</td></tr></tbody>`; return; }
  const bk = c => !c ? "no book" : c.thin ? `<span class="above">too thin (${fmtUsd(c.depthUsd)} near mid)</span>` : `${c.spreadBps.toFixed(1)} spread · ${c.bps.toFixed(1)} round trip${c.quoteOnly ? " (quote)" : ""}`;
  el.innerHTML = `<thead><tr><th>Asset</th><th class="l">Verdict</th><th class="l">Sell (rich) at bid</th><th class="l">Buy (cheap) at ask</th><th title="Gap between mid prices">Mid gap</th><th title="Selling your size into the rich bid and buying it from the cheap ask">Fill gap</th><th class="l">Rich book</th><th class="l">Cheap book</th><th title="Fill gap minus your exit level, the cost of crossing back, and 4 taker fees">Edge after costs</th></tr></thead><tbody>` +
    sc.live.map(x => {
      const usd = b => b == null ? "–" : `${b > 0 ? "+" : ""}${b.toFixed(1)} bps <span class="muted">(${fmtUsd2(b / 1e4 * x.notional)})</span>`;
      return `<tr><td><div class="asset-cell"><b>${esc(x.v.a.label)}</b>${classTag(x.v.a.cls)}${jumpTag(x.v)}</div></td>
      <td class="l"><span class="pill ${x.verdict[0] === "yes" ? "go" : x.verdict[0] === "thin" ? "watch" : "avoid"}">${esc(x.verdict[1])}</span></td>
      <td class="l">${esc(x.rich.exchangeLabel)}${ageTag(x.rich)} <span class="num muted">${x.sellAt ? fmtPx(x.sellAt) : "–"}</span></td>
      <td class="l">${esc(x.cheap.exchangeLabel)}${ageTag(x.cheap)} <span class="num muted">${x.buyAt ? fmtPx(x.buyAt) : "–"}</span></td>
      <td class="num muted">${fmtBps(x.v.maxGap)}</td>
      <td class="num ${x.fillBps > 0 ? "above" : "below"}">${x.fillBps == null ? "–" : fmtBps(x.fillBps)}</td>
      <td class="l num">${bk(x.ch)}</td><td class="l num">${bk(x.cl)}</td>
      <td class="num ${x.edge > 0 ? "above" : "below"}">${usd(x.edge)}</td></tr>`;
    }).join("") + "</tbody>";
}
document.getElementById("scLiveBtn").addEventListener("click", () => liveCheck(true));

/* =============================================================================================
   ROUTING, THEME, START
   ============================================================================================= */
const TABS = ["dashboard", "map", "compare", "funding", "divergence", "scanner", "venues"];
function route() {
  const t = (location.hash || "#dashboard").slice(1);
  currentTab = TABS.includes(t) ? t : "dashboard";
  for (const s of TABS) document.getElementById("view-" + s).hidden = s !== currentTab;
  document.querySelectorAll(".side a").forEach(a => { if (a.dataset.tab === currentTab) a.setAttribute("aria-current", "page"); else a.removeAttribute("aria-current"); });
  mon.series = null;
  window.mapRoute?.(currentTab);
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
