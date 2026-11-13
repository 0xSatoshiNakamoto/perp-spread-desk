// Perp Spread Desk, always-on watcher (Cloudflare Worker, runs every minute).
//  1. Watches every asset listed on 2+ venues (Hyperliquid, Lighter, Variational) and sends a Telegram
//     message when the tradeable gap flips. It samples three times a minute; flip state is kept in a
//     Durable Object so it survives between runs.
//  2. Starts the GitHub workflows on time (GitHub's own timer runs hours late): the X post at each
//     slot, and the order-book recorder every 30 minutes.
// Secrets (set by .github/workflows/deploy-worker.yml): TG_TOKEN, TG_CHAT, GH_DISPATCH_TOKEN.
import { DurableObject } from "cloudflare:workers";
import { LT_STREAM, loadHL, loadLighterMeta, lighterMarkets, loadVariational, groupAssets, stepFlips, flipText } from "./core.js";

const cfgOf = env => ({
  thr: (+env.FLIP_PCT || 0.3) * 100,               // fill gap each way, bps
  win: (+env.FLIP_WINDOW_MIN || 60) * 60e3,
  hold: (+env.FLIP_HOLD_SEC || 30) * 1e3,
  minVol: +env.MIN_VOL_USD || 100000,               // thinner venue's daily volume
  types: env.ASSET_TYPES || "tradfi+pre",
});

async function telegram(env, text) {
  if (!env.TG_TOKEN || !env.TG_CHAT) return false;
  const r = await fetch(`https://api.telegram.org/bot${env.TG_TOKEN}/sendMessage`, {
    method: "POST", headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ chat_id: env.TG_CHAT, text, disable_web_page_preview: true }),
  });
  if (!r.ok) console.error("telegram", r.status, await r.text());
  return r.ok;
}

async function dispatch(env, workflow, inputs) {
  if (!env.GH_DISPATCH_TOKEN) return console.log("no GH_DISPATCH_TOKEN, not starting", workflow);
  const r = await fetch(`https://api.github.com/repos/${env.GH_REPO}/actions/workflows/${workflow}/dispatches`, {
    method: "POST",
    headers: { Authorization: `Bearer ${env.GH_DISPATCH_TOKEN}`, Accept: "application/vnd.github+json", "X-GitHub-Api-Version": "2022-11-28", "User-Agent": "perp-spread-watch", "Content-Type": "application/json" },
    body: JSON.stringify({ ref: "main", ...(inputs ? { inputs } : {}) }),
  });
  console.log("dispatch", workflow, r.status);
  if (r.status !== 204) await telegram(env, `⚠️ Watcher could not start ${workflow} on GitHub (HTTP ${r.status}). Check the GH_DISPATCH_TOKEN secret.`);
}

// Lighter: hold one stream open for the whole tick and keep the newest stats per market
async function openLighter(stats) {
  const resp = await fetch(LT_STREAM, { headers: { Upgrade: "websocket" } });
  const ws = resp.webSocket;
  if (!ws) throw new Error(`Lighter stream refused (HTTP ${resp.status})`);
  ws.accept();
  ws.addEventListener("message", ev => {
    let j; try { j = JSON.parse(typeof ev.data === "string" ? ev.data : new TextDecoder().decode(ev.data)); } catch { return; }
    if (!j.market_stats) return;
    const at = Date.now();
    for (const [id, s] of Object.entries(j.market_stats)) stats.set(String(id), { ...(stats.get(String(id)) || {}), ...s, _at: at });
  });
  ws.send(JSON.stringify({ type: "subscribe", channel: "market_stats/all" }));
  return ws;
}
const sleep = ms => new Promise(r => setTimeout(r, ms));

export class Watcher extends DurableObject {
  async fetch(req) {
    if (new URL(req.url).pathname !== "/tick") return Response.json((await this.ctx.storage.get("last")) || { note: "no tick yet" });
    if (this.busy) return new Response("busy");
    this.busy = true;
    try { await this.tick(); } catch (e) { console.error("tick", e.stack || e); } finally { this.busy = false; }
    return new Response("ok");
  }

  async tick() {
    const env = this.env, cfg = cfgOf(env), st = this.ctx.storage;
    if (!(await st.get("hello"))) {
      if (await telegram(env, `✅ Perp Spread Desk watcher is live. It checks Hyperliquid, Lighter and Variational 3 times a minute and messages you when a tradeable gap of ≥${(cfg.thr / 100).toFixed(2)}% flips sides within ${cfg.win / 60e3} min.`)) await st.put("hello", Date.now());
    }
    const flips = (await st.get("flips")) || {};
    const health = (await st.get("health")) || {};
    const stats = new Map();
    let ws = null, meta = new Map(), errs = [];
    try { [meta, ws] = await Promise.all([loadLighterMeta(), openLighter(stats)]); } catch (e) { errs.push("Lighter: " + e.message); }
    let summary = null;
    for (const at of [6e3, 24e3, 42e3]) {                                  // three samples inside the minute
      await sleep(at - (summary ? summary.offset : 0));
      const t0 = Date.now();
      const [hl, vr] = await Promise.allSettled([loadHL(), loadVariational(60, t0)]);
      if (hl.status === "rejected") errs.push("Hyperliquid: " + hl.reason.message);
      if (vr.status === "rejected") errs.push("Variational: " + vr.reason.message);
      const lt = lighterMarkets(meta, stats).filter(m => t0 - m.at < 120e3);
      const markets = [...(hl.value || []), ...lt, ...(vr.value || [])];
      const assets = groupAssets(markets, cfg.types);
      for (const e of stepFlips(flips, assets, t0, cfg)) await telegram(env, flipText(e, t0));
      summary = { offset: at, at: new Date(t0).toISOString(), assets: assets.length, markets: { hl: (hl.value || []).length, lighter: lt.length, variational: (vr.value || []).length }, watching: Object.keys(flips).length };
      await st.put("flips", flips);
    }
    try { ws?.close(); } catch { }
    // tell the user once if a venue has been failing for 30 straight minutes, and once when it is back
    for (const v of ["Hyperliquid", "Lighter", "Variational"]) {
      const bad = errs.some(e => e.startsWith(v)) || (v === "Lighter" && summary?.markets.lighter === 0);
      const h = health[v] || { fails: 0, told: false };
      h.fails = bad ? h.fails + 1 : 0;
      if (h.fails >= 30 && !h.told) { await telegram(env, `⚠️ Watcher: ${v} data has failed for 30 min (${errs.find(e => e.startsWith(v)) || "no quotes"}). Flips involving ${v} are not being watched.`); h.told = true; }
      if (!bad && h.told) { await telegram(env, `✅ Watcher: ${v} data is back.`); h.told = false; }
      health[v] = h;
    }
    await st.put("health", health);
    await st.put("last", { ...summary, errors: [...new Set(errs)].slice(0, 5) });
  }
}

export default {
  async scheduled(event, env, ctx) {
    const t = new Date(event.scheduledTime), h = t.getUTCHours(), m = t.getUTCMinutes();
    const slots = String(env.SLOT_HOURS_UTC || "6,18").split(",").map(Number);
    // X post: try at :02 and :12 after each slot; the bot posts once per slot and ignores the second start
    if (slots.includes(h) && (m === 2 || m === 12)) ctx.waitUntil(dispatch(env, "x-posts.yml", { slot: "true" }));
    if (m === 11 || m === 41) ctx.waitUntil(dispatch(env, "book-recorder.yml"));
    const stub = env.WATCHER.get(env.WATCHER.idFromName("main"));
    ctx.waitUntil(stub.fetch("https://watcher/tick"));
  },
  // GET / shows the last tick (counts and errors only, no secrets)
  async fetch(req, env) {
    const stub = env.WATCHER.get(env.WATCHER.idFromName("main"));
    return stub.fetch("https://watcher/status");
  },
};
