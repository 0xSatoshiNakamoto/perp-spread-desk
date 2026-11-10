/* =============================================================================================
   OPPORTUNITY MAP — every asset listed on 2+ venues is a planet.
     size   = opportunities the scanner found for it (fillable gap >= entry that lasted >= the window)
     colour = its fill gap right now (sell the rich venue at its bid, buy the cheap one at its ask)
     moons  = the venues that list it
     orbit  = asset type (stocks inside, then indices, FX, commodities, pre-IPO, crypto)
   Drawn on a 2D canvas with a small perspective projection: no 3D library needed.
   Runs after app.js and uses its state (state, sc, assetView helpers).
   ============================================================================================= */
(() => {
  const RINGS = [
    { cls: "equity", label: "Stocks", r: 250 }, { cls: "index", label: "Indices", r: 360 }, { cls: "fx", label: "FX", r: 450 },
    { cls: "commodity", label: "Commodities", r: 540 }, { cls: "preipo", label: "Pre-IPO", r: 630 }, { cls: "crypto", label: "Crypto", r: 720 },
  ];
  const TILT = 0.10;                                   // each ring is tipped a little out of the plane
  const reduceMotion = matchMedia("(prefers-reduced-motion: reduce)").matches;

  const wrap = document.getElementById("mapWrap"), cv = document.getElementById("mapCanvas"), ctx = cv.getContext("2d");
  const tip = document.getElementById("mapTip"), panel = document.getElementById("mapPanel");
  const cam = { yaw: 0.6, pitch: 0.5, dist: 1080, autoSpin: !reduceMotion };
  const M = { running: false, planets: new Map(), selected: null, hover: null, proj: [], t0: performance.now(), lastData: 0, w: 0, h: 0, dpr: 1, stars: [] };

  // stars on a big sphere, drawn with camera rotation only (gives depth when you turn)
  let seed = 7; const rnd = () => (seed = (seed * 16807) % 2147483647) / 2147483647;
  for (let i = 0; i < 420; i++) {
    const u = rnd() * 2 - 1, th = rnd() * Math.PI * 2, rr = Math.sqrt(1 - u * u);
    M.stars.push({ x: rr * Math.cos(th), y: u, z: rr * Math.sin(th), b: 0.25 + rnd() * 0.75, s: rnd() < 0.08 ? 1.6 : 0.9 });
  }
  const hash = s => { let h = 2166136261; for (const c of s) h = Math.imul(h ^ c.charCodeAt(0), 16777619); return (h >>> 0) / 4294967295; };

  /* ---------------- data: planets from live views + scanner results ---------------- */
  function oppStats() {
    const by = new Map();
    for (const r of sc.results) {
      if (r.skip || !r.p) continue;
      const k = r.p.a.key, o = by.get(k) || { windows: 0, taken: 0, mkt: 0, flags: new Set(), pairs: [] };
      o.windows += r.real.length; o.taken += r.trades.length; o.mkt += r.mkt || 0; o.pairs.push(r);
      r.qual.flags.forEach(f => f[1] !== "info" && o.flags.add(f[0]));
      by.set(k, o);
    }
    return by;
  }
  function refreshData() {
    const stats = oppStats(), keep = new Set();
    const entry = +document.getElementById("scEntry").value || 10;
    const sizeBy = document.getElementById("mapSize").value;
    for (const v of state.views.values()) {
      if (!typeOk(v) || v.all.length < 2) continue;
      const ring = RINGS.find(r => r.cls === v.a.cls) || RINGS[0];
      const st = stats.get(v.a.key);
      let p = M.planets.get(v.a.key);
      if (!p) {
        const h = hash(v.a.key);
        p = { key: v.a.key, phase: h * Math.PI * 2, rJit: (hash(v.a.key + "r") - 0.5) * 60, yJit: (hash(v.a.key + "y") - 0.5) * 40, moonPhase: h * 10, rad: 6 };
        M.planets.set(v.a.key, p);
      }
      keep.add(v.a.key);
      Object.assign(p, { v, ring, st, exec: v.exec, label: v.a.label });
      const opps = st ? st.windows : 0;
      const target = sizeBy === "gap" ? 5 + Math.min(34, Math.max(0, v.exec ?? 0) * 0.9)
        : 5 + Math.min(36, Math.sqrt(opps) * 7);
      p.target = target;
      const age = v.execPair ? Math.max(ageOf(v.execPair[0]), ageOf(v.execPair[1])) : 0;
      p.stale = age > 10 ? { m: ageOf(v.execPair[0]) > ageOf(v.execPair[1]) ? v.execPair[0] : v.execPair[1], age } : null;
      p.state = v.exec == null ? "none" : v.exec >= entry && !p.stale ? "fill" : v.exec > 0 ? "near" : "inside";
      p.opps = opps;
    }
    for (const k of [...M.planets.keys()]) if (!keep.has(k)) M.planets.delete(k);
    renderLegendCounts(stats);
    renderList();
    if (M.selected) renderPanel();
  }

  /* ---------------- 3D → screen ---------------- */
  function worldPos(p, t) {
    const r = p.ring.r + p.rJit;
    const w = reduceMotion ? 0 : 0.035 * Math.sqrt(250 / r);          // inner rings orbit faster
    const th = p.phase + t * w;
    const x = r * Math.cos(th), z = r * Math.sin(th);
    return { x, y: z * TILT * (p.ring.r > 500 ? -1 : 1) + p.yJit, z };
  }
  function rot(x, y, z) {
    const cy = Math.cos(cam.yaw), sy = Math.sin(cam.yaw), cp = Math.cos(cam.pitch), sp = Math.sin(cam.pitch);
    const x1 = x * cy - z * sy, z1 = x * sy + z * cy;
    const y2 = y * cp - z1 * sp, z2 = y * sp + z1 * cp;
    return { x: x1, y: y2, z: z2 };
  }
  function project(x, y, z) {
    const r = rot(x, y, z), zc = r.z + cam.dist;
    if (zc < 50) return null;
    const f = Math.min(M.w, M.h) * 1.05;
    return { x: M.w / 2 + r.x * f / zc, y: M.h / 2 + r.y * f / zc, s: f / zc, z: zc };
  }

  /* ---------------- drawing ---------------- */
  const COL = { fill: "#5fe0bd", near: "#e6b35a", inside: "#5d7a93", none: "#4a5866" };
  let labelBoxes = [];
  function shade(hex, k) { const n = parseInt(hex.slice(1), 16), r = n >> 16, g = (n >> 8) & 255, b = n & 255, f = c => Math.max(0, Math.min(255, Math.round(c * k))); return `rgb(${f(r)},${f(g)},${f(b)})`; }

  function draw(now) {
    if (!M.running) return;
    const t = (now - M.t0) / 1000;
    if (cam.autoSpin && !M.drag) cam.yaw += 0.0009;
    ctx.setTransform(M.dpr, 0, 0, M.dpr, 0, 0);
    // space
    const bg = ctx.createRadialGradient(M.w / 2, M.h / 2, 0, M.w / 2, M.h / 2, Math.max(M.w, M.h) * 0.75);
    bg.addColorStop(0, "#0d1d22"); bg.addColorStop(1, "#05090b");
    ctx.fillStyle = bg; ctx.fillRect(0, 0, M.w, M.h);
    const f = Math.min(M.w, M.h) * 1.05;
    for (const s of M.stars) {
      const r = rot(s.x, s.y, s.z); if (r.z < 0.05) continue;
      ctx.globalAlpha = s.b * 0.8; ctx.fillStyle = "#cfe8e2";
      ctx.fillRect(M.w / 2 + r.x / r.z * f * 0.55, M.h / 2 + r.y / r.z * f * 0.55, s.s, s.s);
    }
    ctx.globalAlpha = 1;
    // orbit rings (only the types that have planets)
    const used = new Set([...M.planets.values()].map(p => p.ring.cls));
    ctx.lineWidth = 1;
    for (const ring of RINGS) {
      if (!used.has(ring.cls)) continue;
      ctx.beginPath(); let first = true;
      for (let i = 0; i <= 120; i++) {
        const a = i / 120 * Math.PI * 2, x = ring.r * Math.cos(a), z = ring.r * Math.sin(a);
        const q = project(x, z * TILT * (ring.r > 500 ? -1 : 1), z); if (!q) { first = true; continue; }
        if (first) { ctx.moveTo(q.x, q.y); first = false; } else ctx.lineTo(q.x, q.y);
      }
      ctx.strokeStyle = "rgba(95,224,189,0.16)"; ctx.stroke();
      // label each ring at its own spot on the near side, fanned out so they never stack
      const ri = RINGS.indexOf(ring), la = -Math.PI / 2 - cam.yaw + (ri - 2) * 0.22;   // nearest point to the camera, fanned
      const lq = project(ring.r * Math.cos(la), ring.r * Math.sin(la) * TILT * (ring.r > 500 ? -1 : 1), ring.r * Math.sin(la));
      if (lq) { ctx.fillStyle = "rgba(160,200,192,0.6)"; ctx.font = "11px JetBrains Mono, monospace"; ctx.textAlign = "center"; ctx.fillText(ring.label.toUpperCase(), lq.x, lq.y + 15); }
    }
    // planets + sun, back to front
    const items = [];
    const sun = project(0, 0, 0); if (sun) items.push({ sun: true, q: sun, z: sun.z });
    for (const p of M.planets.values()) {
      p.rad += (p.target - p.rad) * 0.08;                              // ease size changes
      const w = worldPos(p, t), q = project(w.x, w.y, w.z);
      if (q) items.push({ p, q, w, z: q.z });
    }
    items.sort((a, b) => b.z - a.z);
    M.proj = []; labelBoxes = [];
    // which planets get a name: the selected/hovered one, then the biggest, then fillable ones
    const sizeBy = document.getElementById("mapSize").value;
    const ranked = [...M.planets.values()].sort((a, b) => (sizeBy === "gap" ? (b.exec ?? -1e9) - (a.exec ?? -1e9) : (b.opps - a.opps) || ((b.exec ?? -1e9) - (a.exec ?? -1e9))));
    M.named = new Set(ranked.slice(0, 12).map(p => p.key));
    for (const it of items) {
      if (it.sun) { drawSun(it.q); continue; }
      drawPlanet(it.p, it.q, it.w, t);
      M.proj.push({ p: it.p, x: it.q.x, y: it.q.y, r: Math.max(5, it.p.rad * it.q.s), z: it.z });
    }
    requestAnimationFrame(draw);
  }
  function drawSun(q) {
    const R = 26 * q.s;
    const g = ctx.createRadialGradient(q.x, q.y, 0, q.x, q.y, R * 4);
    g.addColorStop(0, "rgba(255,236,190,0.95)"); g.addColorStop(0.18, "rgba(240,180,90,0.55)"); g.addColorStop(1, "rgba(240,138,108,0)");
    ctx.fillStyle = g; ctx.beginPath(); ctx.arc(q.x, q.y, R * 4, 0, Math.PI * 2); ctx.fill();
    ctx.fillStyle = "#ffe9c0"; ctx.beginPath(); ctx.arc(q.x, q.y, R * 0.7, 0, Math.PI * 2); ctx.fill();
  }
  function drawPlanet(p, q, w, t) {
    const R = Math.max(2.5, p.rad * q.s), base = COL[p.state];
    const sel = M.selected === p.key, hov = M.hover === p.key;
    const quiet = document.getElementById("mapSize").value === "opps" && sc.results.length && !p.opps && !sel && !hov;
    ctx.globalAlpha = quiet ? 0.5 : 1;
    // light comes from the sun at the centre
    const toSun = project(0, 0, 0), lx = toSun ? Math.sign(toSun.x - q.x) * 0.45 : -0.4, ly = toSun ? Math.sign(toSun.y - q.y) * 0.45 : -0.4;
    if (p.state === "fill") {                                         // fillable now: glow + pulse
      const pulse = reduceMotion ? 0.5 : (Math.sin(t * 3 + p.phase) + 1) / 2;
      const g = ctx.createRadialGradient(q.x, q.y, R * 0.8, q.x, q.y, R * (2.4 + pulse));
      g.addColorStop(0, "rgba(95,224,189,0.45)"); g.addColorStop(1, "rgba(95,224,189,0)");
      ctx.fillStyle = g; ctx.beginPath(); ctx.arc(q.x, q.y, R * (2.4 + pulse), 0, Math.PI * 2); ctx.fill();
    }
    const g = ctx.createRadialGradient(q.x + lx * R, q.y + ly * R, R * 0.1, q.x, q.y, R);
    g.addColorStop(0, shade(base, 1.55)); g.addColorStop(0.55, base); g.addColorStop(1, shade(base, 0.35));
    ctx.fillStyle = g; ctx.beginPath(); ctx.arc(q.x, q.y, R, 0, Math.PI * 2); ctx.fill();
    ctx.globalAlpha = 1;
    if (sel || hov) { ctx.strokeStyle = sel ? "#ffffff" : "rgba(255,255,255,0.6)"; ctx.lineWidth = sel ? 2 : 1; ctx.beginPath(); ctx.arc(q.x, q.y, R + 5, 0, Math.PI * 2); ctx.stroke(); }
    // moons = venues (on planets big enough to read, or when you point at one)
    const ms = R > 7 || sel || hov ? p.v.all : [];
    ms.forEach((m, i) => {
      const a = (reduceMotion ? 0 : t * 0.9) + p.moonPhase + i / ms.length * Math.PI * 2;
      const mr = p.rad * 1.8 + 10;
      const mq = project(w.x + mr * Math.cos(a), w.y + mr * 0.35 * Math.sin(a * 1.3), w.z + mr * Math.sin(a));
      if (!mq) return;
      ctx.fillStyle = exColor(m.exchange); ctx.globalAlpha = 0.9;
      ctx.beginPath(); ctx.arc(mq.x, mq.y, Math.max(1.6, 3.2 * mq.s), 0, Math.PI * 2); ctx.fill(); ctx.globalAlpha = 1;
    });
    // names: selected / hovered always, the 12 biggest otherwise, never on top of another name
    if (sel || hov || M.named.has(p.key)) {
      const text = p.label.replace(/ \(≈.*\)$/, "");
      ctx.font = `${sel || hov ? 600 : 500} ${Math.max(11, Math.min(14, 10 + R * 0.12))}px Figtree, system-ui, sans-serif`;
      const w = ctx.measureText(text).width, box = { x: q.x - w / 2 - 3, y: q.y + R + 3, w: w + 6, h: 15 };
      const clash = labelBoxes.some(b => box.x < b.x + b.w && box.x + box.w > b.x && box.y < b.y + b.h && box.y + box.h > b.y);
      if (!clash || sel || hov) {
        labelBoxes.push(box);
        ctx.textAlign = "center"; ctx.fillStyle = sel || hov ? "#ffffff" : "rgba(220,235,232,0.88)";
        ctx.fillText(text, q.x, q.y + R + 14);
      }
    }
  }

  /* ---------------- interaction ---------------- */
  function pick(x, y) {
    let best = null;
    for (const it of M.proj) { const d = Math.hypot(it.x - x, it.y - y); if (d <= it.r + 6 && (!best || it.z < best.z)) best = it; }
    return best;
  }
  const pt = e => { const b = cv.getBoundingClientRect(); return { x: e.clientX - b.left, y: e.clientY - b.top }; };
  cv.addEventListener("pointerdown", e => { cv.setPointerCapture(e.pointerId); M.drag = { x: e.clientX, y: e.clientY, moved: 0 }; });
  cv.addEventListener("pointermove", e => {
    if (M.drag) {
      const dx = e.clientX - M.drag.x, dy = e.clientY - M.drag.y; M.drag.moved += Math.abs(dx) + Math.abs(dy);
      cam.yaw -= dx * 0.005; cam.pitch = Math.max(-1.35, Math.min(1.35, cam.pitch + dy * 0.004));
      M.drag.x = e.clientX; M.drag.y = e.clientY; cam.autoSpin = false; tip.hidden = true; return;
    }
    const { x, y } = pt(e), hit = pick(x, y);
    M.hover = hit ? hit.p.key : null; cv.style.cursor = hit ? "pointer" : "grab";
    if (hit) {
      const p = hit.p, opps = p.st ? p.st.windows : null;
      tip.innerHTML = `<b>${esc(p.label)}</b> ${classTag(p.v.a.cls)}<br><span class="num">${p.exec == null ? "no bid/ask on two venues" : (p.exec >= 0 ? "fill gap " : "inside the spread, ") + fmtBps(p.exec) + " bps"}</span><br><span class="muted">${opps == null ? "run a scan for opportunity counts" : opps + " opportunit" + (opps === 1 ? "y" : "ies") + " in the scan"} · ${p.v.all.length} venues</span>`;
      tip.style.left = Math.min(M.w - 230, x + 14) + "px"; tip.style.top = Math.max(8, y - 10) + "px"; tip.hidden = false;
    } else tip.hidden = true;
  });
  cv.addEventListener("pointerup", e => {
    const d = M.drag; M.drag = null;
    if (d && d.moved < 6) { const { x, y } = pt(e), hit = pick(x, y); select(hit ? hit.p.key : null); }
  });
  cv.addEventListener("pointerleave", () => { M.hover = null; tip.hidden = true; });
  cv.addEventListener("wheel", e => { e.preventDefault(); cam.dist = Math.max(500, Math.min(3600, cam.dist * (1 + Math.sign(e.deltaY) * 0.08))); }, { passive: false });
  // pinch zoom
  const touches = new Map(); let pinch0 = null;
  cv.addEventListener("touchstart", e => { if (e.touches.length === 2) pinch0 = { d: Math.hypot(e.touches[0].clientX - e.touches[1].clientX, e.touches[0].clientY - e.touches[1].clientY), dist: cam.dist }; }, { passive: true });
  cv.addEventListener("touchmove", e => { if (e.touches.length === 2 && pinch0) { const d = Math.hypot(e.touches[0].clientX - e.touches[1].clientX, e.touches[0].clientY - e.touches[1].clientY); cam.dist = Math.max(500, Math.min(3600, pinch0.dist * pinch0.d / d)); } }, { passive: true });
  cv.addEventListener("touchend", () => { pinch0 = null; touches.clear(); }, { passive: true });
  document.getElementById("mapReset").addEventListener("click", () => { Object.assign(cam, { yaw: 0.6, pitch: 0.5, dist: 1080, autoSpin: !reduceMotion }); });
  document.getElementById("mapSpin").addEventListener("click", e => { cam.autoSpin = !cam.autoSpin; e.currentTarget.setAttribute("aria-pressed", cam.autoSpin); });
  document.getElementById("mapSize").addEventListener("change", refreshData);

  function select(key) {
    M.selected = key;
    panel.hidden = !key;
    if (key) renderPanel();
    renderList();
  }
  document.getElementById("mapClose").addEventListener("click", () => select(null));
  panel.addEventListener("click", e => {
    const b = e.target.closest("[data-act]"); if (!b) return;
    const k = M.selected;
    if (b.dataset.act === "dv") openDivergence(k);
    if (b.dataset.act === "watch") { addToRibbon(k); b.textContent = "On your watchlist"; b.disabled = true; }
    if (b.dataset.act === "scan") location.hash = "scanner";
  });
  document.getElementById("mapList").addEventListener("click", e => { const b = e.target.closest("[data-key]"); if (b) select(b.dataset.key); });

  /* ---------------- panel, legend, list ---------------- */
  function renderPanel() {
    const p = M.planets.get(M.selected);
    if (!p) { panel.hidden = true; return; }
    const v = p.v, entry = +document.getElementById("scEntry").value || 10;
    const pill = p.state === "fill" ? `<span class="pill go">Fillable now: ${fmtBps(v.exec)} bps</span>`
      : p.stale && v.exec >= entry ? `<span class="pill watch">${fmtBps(v.exec)} bps on paper: ${esc(p.stale.m.exchangeLabel)} quote is ${Math.round(p.stale.age)} s old, confirm first</span>`
      : p.state === "near" ? `<span class="pill watch">Fill gap ${fmtBps(v.exec)} bps, under your ${entry}</span>`
      : p.state === "inside" ? `<span class="pill avoid">Gap is inside the spreads (${fmtBps(v.exec)} bps)</span>` : `<span class="pill na">No bid/ask on two venues</span>`;
    const rows = [...v.all].sort((a, b) => (b.volume24h || 0) - (a.volume24h || 0)).map(m => `<tr>
        <td class="l"><span class="dot" style="background:${exColor(m.exchange)};display:inline-block;margin-right:6px"></span>${esc(m.exchangeLabel)}${ageTag(m)}</td>
        <td class="num">${m.bid != null ? fmtPx(m.bid) : "–"}</td><td class="num">${m.ask != null ? fmtPx(m.ask) : "–"}</td>
        <td class="num ${cls(v.gaps[m.exchange])}">${v.ref === m ? '<span class="ref-mark">REF</span>' : fmtBps(v.gaps[m.exchange])}</td>
        <td class="num ${cls(m.fundingApr)}">${m.fundingApr == null ? "–" : fmtPct(m.fundingApr)}</td><td class="num">${fmtUsd(m.volume24h)}</td></tr>`).join("");
    let opps = `<p class="hint">No scan yet. The scanner counts every gap that stayed fillable at your entry level for long enough. <button class="linkish" data-act="scan">Open the scanner</button></p>`;
    if (p.st) {
      const list = sc.opps.filter(o => o.p.a.key === p.key).sort((a, b) => b.op.start - a.op.start).slice(0, 8);
      opps = `<p class="hint">${p.st.windows} opportunit${p.st.windows === 1 ? "y" : "ies"} in the scan, ${p.st.taken} you could have taken: <b class="${p.st.mkt > 0 ? "above" : p.st.mkt < 0 ? "below" : ""}">${fmtUsd2(p.st.mkt)}</b> with market orders after fees.</p>` +
        (list.length ? `<table><thead><tr><th>Opened</th><th class="l">Sell / buy</th><th>Peak</th><th class="l">Outcome</th><th>Market $</th></tr></thead><tbody>${list.map(o => {
          const rich = o.op.side > 0 ? o.p.B : o.p.A, cheap = o.op.side > 0 ? o.p.A : o.p.B, t = o.tr;
          return `<tr><td class="num">${fmtTime(o.op.start)}</td><td class="l">${esc(rich.exchangeLabel)} / ${esc(cheap.exchangeLabel)}</td><td class="num">${o.op.peak.toFixed(1)}</td>
            <td class="l">${o.open ? "<b>still open</b>" : t.reason === "deadline" ? '<span class="above">not converged</span>' : t.reason === "flipped" ? "flipped" : "closed at exit"}</td>
            <td class="num ${t && t.netMkt > 0 ? "above" : t && t.netMkt < 0 ? "below" : ""}">${t ? (t.netMkt == null ? "too thin" : fmtUsd2(t.netMkt)) : "–"}</td></tr>`; }).join("")}</tbody></table>` : "") +
        (p.st.flags.size ? `<ul class="flags">${[...p.st.flags].slice(0, 4).map(f => `<li>${esc(f)}</li>`).join("")}</ul>` : "");
    }
    document.getElementById("mapPanelBody").innerHTML = `
      <div class="asset-cell" style="flex-wrap:wrap"><b style="font-size:18px">${esc(p.label)}</b>${classTag(v.a.cls)}${jumpTag(v)}</div>
      <div>${pill}</div>
      <p class="hint">${v.exec != null ? sideTxt(v) : ""}${v.maxGap != null ? ` · mid gap ${fmtBps(v.maxGap)} bps` : ""}</p>
      <div class="table-wrap"><table><thead><tr><th class="l">Venue</th><th>Bid</th><th>Ask</th><th>vs ref bps</th><th>Funding</th><th>24h vol</th></tr></thead><tbody>${rows}</tbody></table></div>
      <h3>Opportunities</h3>${opps}
      <div style="display:flex;gap:12px;flex-wrap:wrap;margin-top:4px">
        <button class="btn" type="button" data-act="dv">Open in Divergence</button>
        <button class="btn ghost-btn" type="button" data-act="watch"${state.ribbon.includes(p.key) ? " disabled" : ""}>${state.ribbon.includes(p.key) ? "On your watchlist" : "Add to watchlist"}</button>
      </div>`;
  }
  function renderLegendCounts(stats) {
    const ps = [...M.planets.values()];
    const n = s => ps.filter(p => p.state === s).length;
    document.getElementById("mapCounts").innerHTML =
      `<span><i style="background:${COL.fill}"></i>Fillable now (${n("fill")})</span><span><i style="background:${COL.near}"></i>Fillable, under entry (${n("near")})</span>` +
      `<span><i style="background:${COL.inside}"></i>Inside the spreads (${n("inside")})</span><span><i style="background:${COL.none}"></i>No bid/ask (${n("none")})</span>`;
    const st = document.getElementById("mapStatus");
    if (sc.running) st.textContent = `Agents are scanning: ${(sc.agents.history && sc.agents.history.msg) || "starting"}. Planets grow as results come in.`;
    else if (!sc.results.length) st.textContent = "Waiting for every venue to report, then the agents scan the last 7 days. Planets grow as results come in.";
    else st.textContent = `Sizes from the scan of the last ${document.getElementById("scDays").value} days (${sc.results.filter(r => !r.skip).length} pairs). ${[...stats.values()].reduce((s, o) => s + o.windows, 0)} opportunities in total.`;
  }
  function renderList() {
    const ps = [...M.planets.values()].sort((a, b) => (b.st ? b.st.windows : 0) - (a.st ? a.st.windows : 0) || (b.exec ?? -1e9) - (a.exec ?? -1e9)).slice(0, 16);
    document.getElementById("mapList").innerHTML = ps.map(p => `<button type="button" class="chip" data-key="${esc(p.key)}" aria-pressed="${M.selected === p.key}">
      <span class="sw" style="background:${COL[p.state]};border-radius:50%"></span>${esc(p.label.replace(/ \(≈.*\)$/, ""))} <span class="num muted">${p.st ? p.st.windows : "–"}</span></button>`).join("");
  }

  /* ---------------- lifecycle ---------------- */
  // only auto-scan once every venue has delivered its market list, or the scan misses pairs
  const venuesReady = () => VENUES.every(v => state.venueStatus[v.id]?.lastMsg || state.venueStatus[v.id]?.state === "err")
    && [...state.markets.values()].some(m => m.venue === "lighter" && m.lighterCat) && Date.now() - state.startedAt > 8000;
  const maybeScan = () => { if (!sc.results.length && !sc.running && venuesReady()) runScan(); };
  function resize() {
    const b = wrap.getBoundingClientRect();
    M.dpr = Math.min(2, window.devicePixelRatio || 1); M.w = b.width; M.h = b.height;
    cv.width = Math.round(M.w * M.dpr); cv.height = Math.round(M.h * M.dpr);
    cv.style.width = M.w + "px"; cv.style.height = M.h + "px";
  }
  addEventListener("resize", () => M.running && resize());
  function start() {
    if (M.running) return;
    M.running = true; resize(); refreshData();
    maybeScan();
    requestAnimationFrame(draw);
  }
  function stop() { M.running = false; tip.hidden = true; }
  document.addEventListener("visibilitychange", () => { if (document.hidden) stop(); else if (currentTab === "map") start(); });
  // hooks called by app.js
  window.mapRoute = tab => (tab === "map" ? start() : stop());
  window.mapRender = () => { if (!M.running) return; refreshData(); maybeScan(); };
  if (currentTab === "map") start();
})();
