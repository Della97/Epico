"""HTML renderer: one self-contained page per run.

No CDN, no build step, no dependencies — the model is embedded as JSON and
everything is drawn with hand-rolled SVG so the file works offline and can be
opened straight off disk as well as served. The palette matches the repo's
existing matplotlib plots (bench/plot_scaling.py) so a page and a plot of the
same run look like they belong together.
"""

from __future__ import annotations

import base64
import io
import json
import os

# Rendered height of the mark in the header, doubled for retina.
LOGO_PX = 104


def logo_data_uri(repo_root: str, target_h: int = LOGO_PX) -> str | None:
    """`assets/logo.png` as an inline data URI, or None if it isn't there.

    Downscaled first when Pillow happens to be installed: the source is a
    1118x910 PNG that base64s to ~200 KB, which would more than double a page
    to show a 52px mark. Pillow is NOT a dependency — without it the original
    bytes are embedded, which is merely heavier, not broken.
    """
    path = os.path.join(repo_root, "assets", "logo.png")
    if not os.path.isfile(path):
        return None
    raw = open(path, "rb").read()
    try:
        from PIL import Image                                  # noqa: PLC0415
        im = Image.open(io.BytesIO(raw))
        if im.height > target_h:
            w = max(1, round(im.width * target_h / im.height))
            im = im.convert("RGBA").resize((w, target_h), Image.LANCZOS)
            buf = io.BytesIO()
            im.save(buf, format="PNG", optimize=True)
            raw = buf.getvalue()
    except Exception:                                          # noqa: BLE001
        pass
    return "data:image/png;base64," + base64.b64encode(raw).decode("ascii")

CSS = """
:root{
  --bg:#0D1117; --panel:#161B22; --panel2:#1C2230; --line:#30363D;
  --fg:#E6EDF3; --dim:#C9D1D9; --mute:#8B949E;
  --accent:#58A6FF; --good:#3FB950; --warn:#D29922; --bad:#F85149;
  --c0:#58A6FF; --c1:#3FB950; --c2:#D29922; --c3:#BC8CFF; --c4:#F778BA;
  --c5:#39C5CF; --c6:#FF7B72; --c7:#A5D6FF;
}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--fg);
  font:14px/1.5 ui-sans-serif,-apple-system,"SF Pro Text",Segoe UI,Roboto,sans-serif}
code,.mono{font-family:ui-monospace,SFMono-Regular,"SF Mono",Menlo,monospace}
a{color:var(--accent)}
.wrap{max-width:1400px;margin:0 auto;padding:24px 20px 80px}
header.top{display:flex;flex-wrap:wrap;gap:16px;align-items:center;
  border-bottom:1px solid var(--line);padding-bottom:16px;margin-bottom:20px}
header.top h1{font-size:22px;margin:0;font-weight:600}
header.top .sub{color:var(--mute);font-size:13px}
/* The mark is set as a background so the data URI is written once and shared
   by both places it appears, instead of once per <img src>. */
.logo{background:var(--logo) center/contain no-repeat;flex:none}
header.top .logo{height:58px;width:71px;margin-right:2px}
header.top .titles{display:flex;flex-wrap:wrap;gap:12px;align-items:baseline}
footer .logo{display:inline-block;height:20px;width:24px;vertical-align:-5px;
  margin-right:6px;opacity:.65}
.badge{display:inline-block;padding:2px 8px;border-radius:999px;font-size:12px;
  border:1px solid var(--line);background:var(--panel2);color:var(--dim)}
.badge.ok{border-color:#238636;color:var(--good)}
.badge.warn{border-color:#9E6A03;color:var(--warn)}
.badge.bad{border-color:#DA3633;color:var(--bad)}
section{margin:26px 0}
h2{font-size:15px;text-transform:uppercase;letter-spacing:.08em;color:var(--mute);
  margin:0 0 12px;font-weight:600}
h3{font-size:13px;color:var(--dim);margin:18px 0 8px;font-weight:600}
.panel{background:var(--panel);border:1px solid var(--line);border-radius:10px;padding:16px}
.grid{display:grid;gap:14px}
.kpis{grid-template-columns:repeat(auto-fit,minmax(150px,1fr))}
.kpi{background:var(--panel);border:1px solid var(--line);border-radius:10px;padding:12px 14px}
.kpi .label{color:var(--mute);font-size:11px;text-transform:uppercase;letter-spacing:.06em}
.kpi .value{font-size:22px;font-weight:600;margin-top:4px;font-variant-numeric:tabular-nums}
.kpi .unit{font-size:12px;color:var(--mute);margin-left:3px;font-weight:400}
.kpi .foot{color:var(--mute);font-size:11px;margin-top:2px}
.cols2{grid-template-columns:repeat(auto-fit,minmax(420px,1fr))}
.cols3{grid-template-columns:repeat(auto-fit,minmax(300px,1fr))}
table{border-collapse:collapse;width:100%;font-size:12.5px}
th,td{text-align:right;padding:5px 9px;border-bottom:1px solid var(--line);
  font-variant-numeric:tabular-nums;white-space:nowrap}
th{color:var(--mute);font-weight:600;text-align:right;position:sticky;top:0;
  background:var(--panel);text-transform:uppercase;font-size:10.5px;letter-spacing:.05em}
th:first-child,td:first-child{text-align:left}
tbody tr:hover{background:var(--panel2)}
.scroll{overflow:auto;max-height:420px;border-radius:8px}
.overflow-x{overflow-x:auto}
dl.kv{display:grid;grid-template-columns:auto 1fr;gap:6px 14px;margin:0;font-size:13px}
dl.kv dt{color:var(--mute)}
dl.kv dd{margin:0;font-variant-numeric:tabular-nums}
.warnings{background:rgba(210,153,34,.08);border:1px solid #9E6A03;border-radius:10px;
  padding:12px 16px;margin-bottom:18px}
.warnings ul{margin:6px 0 0;padding-left:20px;color:var(--dim)}
.warnings li{margin:3px 0}
.controls{display:flex;gap:14px;align-items:center;flex-wrap:wrap;margin-bottom:12px}
button{background:var(--panel2);color:var(--fg);border:1px solid var(--line);
  border-radius:6px;padding:6px 14px;font-size:13px;cursor:pointer}
button:hover{border-color:var(--accent)}
input[type=range]{flex:1;min-width:260px;accent-color:var(--accent)}
.tnow{font-variant-numeric:tabular-nums;color:var(--accent);font-weight:600;min-width:86px}
.legend{display:flex;gap:14px;flex-wrap:wrap;font-size:12px;color:var(--dim);margin-top:8px}
.legend i{display:inline-block;width:10px;height:10px;border-radius:2px;margin-right:5px}
svg{display:block;max-width:100%}
.tip{position:fixed;pointer-events:none;background:#000;border:1px solid var(--line);
  border-radius:6px;padding:6px 9px;font-size:12px;opacity:0;transition:opacity .1s;z-index:99}
.node rect{transition:fill .12s,stroke .12s}
.muted{color:var(--mute)}
.right{text-align:right}
.pathnote{color:var(--mute);font-size:12px;margin-top:8px}
.tabs{display:flex;gap:6px;margin-bottom:10px;flex-wrap:wrap}
.tabs button.active{border-color:var(--accent);color:var(--accent)}
.hidden{display:none}
"""

JS = r"""
const M = window.__EPICO__;
const PAL = ['#58A6FF','#3FB950','#D29922','#BC8CFF','#F778BA','#39C5CF','#FF7B72','#A5D6FF',
             '#7EE787','#FFA657','#79C0FF','#D2A8FF','#FF9492','#56D4DD'];
const colorOf = (i) => PAL[i % PAL.length];
const fmt = (v, d=2) => {
  if (v===null||v===undefined||Number.isNaN(v)) return '—';
  if (Math.abs(v)>=1000) return Math.round(v).toLocaleString();
  const s = (+v).toFixed(d);
  // Strip trailing zeros only past a decimal point — otherwise "100" loses its
  // own zeros and reads as "1".
  return s.includes('.') ? s.replace(/\.?0+$/,'') : s;
};
const fmtInt = (v) => (v===null||v===undefined) ? '—' : Math.round(v).toLocaleString();
const el = (tag, attrs={}, kids=[]) => {
  const n = document.createElementNS('http://www.w3.org/2000/svg', tag);
  for (const [k,v] of Object.entries(attrs)) if (v!==null&&v!==undefined) n.setAttribute(k, v);
  for (const k of [].concat(kids)) n.appendChild(k);
  return n;
};
const txt = (s) => document.createTextNode(s);

// ── tooltip ──────────────────────────────────────────────────────────────────
const tip = document.createElement('div');
tip.className = 'tip'; document.body.appendChild(tip);
function showTip(e, html){ tip.innerHTML = html; tip.style.opacity = 1;
  tip.style.left = Math.min(e.clientX+14, innerWidth-260)+'px';
  tip.style.top = (e.clientY+14)+'px'; }
function hideTip(){ tip.style.opacity = 0; }
function attachTip(node, html){
  node.addEventListener('mousemove', e => showTip(e, html));
  node.addEventListener('mouseleave', hideTip);
}
// For content that changes as the slider moves: listeners are attached once and
// read the current text at hover time. Re-attaching per frame would pile up
// thousands of listeners during playback.
function attachLiveTip(node, get){
  node.addEventListener('mousemove', e => showTip(e, get()));
  node.addEventListener('mouseleave', hideTip);
}

// ── generic chart primitives ────────────────────────────────────────────────
function chart(w, h, pad){
  const svg = el('svg', {viewBox:`0 0 ${w} ${h}`, width:'100%', height:h});
  // Top padding leaves room for the y-axis label drawn above the plot area.
  return {svg, w, h, pad: Object.assign({l:56,r:14,t:24,b:26}, pad||{})};
}
function scales(c, xd, yd){
  const {l,r,t,b} = c.pad;
  const x0 = xd[0], x1 = (xd[1]===xd[0] ? xd[0]+1 : xd[1]);
  const y0 = yd[0], y1 = (yd[1]===yd[0] ? yd[0]+1 : yd[1]);
  return {
    x: v => l + (v-x0)/(x1-x0) * (c.w-l-r),
    y: v => c.h-b - (v-y0)/(y1-y0) * (c.h-t-b),
    xd:[x0,x1], yd:[y0,y1],
  };
}
// Round tick steps (1/2/2.5/5 x 10^k) so an axis reads 0,1,2,3 rather than
// 0,0.73,1.46. Returns values inside [lo,hi].
function niceTicks(lo, hi, n){
  if (!(hi > lo)) return [lo];
  const raw = (hi-lo)/Math.max(1,n);
  const mag = Math.pow(10, Math.floor(Math.log10(raw)));
  const norm = raw/mag;
  const step = (norm<=1?1:norm<=2?2:norm<=2.5?2.5:norm<=5?5:10)*mag;
  const out = [];
  for (let v = Math.ceil(lo/step)*step; v <= hi+step*1e-9; v += step) out.push(+v.toFixed(10));
  return out.length ? out : [lo, hi];
}
// Decimals sized to the axis range: a 0–3 ms axis needs two, a 0–6000 needs
// none. Sub-millisecond stage residencies otherwise print as a column of "0ms".
function tickFmt(span, unit=''){
  const d = span >= 100 ? 0 : span >= 10 ? 1 : span >= 1 ? 2 : span >= 0.1 ? 3 : 4;
  return v => fmt(v, d) + unit;
}
function axes(c, s, opts={}){
  const {l,r,t,b} = c.pad, g = el('g');
  const nY = opts.yticks ?? 4, nX = opts.xticks ?? 5;
  const yf = opts.yfmt || tickFmt(s.yd[1]-s.yd[0]);
  const xf = opts.xfmt || tickFmt(s.xd[1]-s.xd[0]);
  if (nY > 0) for (const v of niceTicks(s.yd[0], s.yd[1], nY)){
    const y = s.y(v);
    g.appendChild(el('line',{x1:l,x2:c.w-r,y1:y,y2:y,stroke:'#30363D','stroke-width':1,opacity:.55}));
    const lab = el('text',{x:l-6,y:y+3.5,fill:'#8B949E','font-size':10,'text-anchor':'end'});
    lab.appendChild(txt(yf(v))); g.appendChild(lab);
  }
  if (nX > 0) for (const v of niceTicks(s.xd[0], s.xd[1], nX)){
    const x = s.x(v);
    const lab = el('text',{x, y:c.h-b+14, fill:'#8B949E','font-size':10,'text-anchor':'middle'});
    lab.appendChild(txt(xf(v))); g.appendChild(lab);
  }
  if (opts.ylabel){
    // Above the plot area, not inside it — at x=12,y=t it lands on top of the
    // topmost y tick label.
    const lab = el('text',{x:4,y:t-8,fill:'#8B949E','font-size':10});
    lab.appendChild(txt(opts.ylabel)); g.appendChild(lab);
  }
  c.svg.appendChild(g);
}
function line(c, s, pts, color, step, width){
  if (!pts.length) return;
  let d = '';
  pts.forEach((p,i) => {
    const X = s.x(p[0]), Y = s.y(p[1]);
    if (i===0) d += `M${X},${Y}`;
    else if (step) d += `L${X},${s.y(pts[i-1][1])}L${X},${Y}`;
    else d += `L${X},${Y}`;
  });
  c.svg.appendChild(el('path',{d, fill:'none', stroke:color, 'stroke-width':width||1.6,
    'stroke-linejoin':'round'}));
}
function bars(c, s, pts, color){
  const bw = Math.max(1, (c.w-c.pad.l-c.pad.r)/Math.max(pts.length,1) - 1);
  pts.forEach(p => {
    const y = s.y(p[1]), y0 = s.y(s.yd[0]);
    c.svg.appendChild(el('rect',{x:s.x(p[0])-bw/2, y:Math.min(y,y0), width:bw,
      height:Math.abs(y0-y), fill:color, opacity:.85}));
  });
}
function emptyNote(host, msg){
  const d = document.createElement('div');
  d.className = 'muted'; d.style.padding = '18px 4px'; d.textContent = msg;
  host.appendChild(d);
}

// ── replica timeline lookup ─────────────────────────────────────────────────
function valueAt(series, t){
  let v = 0;
  for (const [ts, val] of series){ if (ts > t) break; v = val; }
  return v;
}
const T_MAX = Math.max(
  M.meta.duration_s || 0,
  ...Object.values(M.replicas).map(s => s.length ? s[s.length-1][0] : 0),
  ...Object.values(M.queue).map(s => s.length ? s[s.length-1][0] : 0), 1);

// ── DAG ─────────────────────────────────────────────────────────────────────
const NODE_W = 118, NODE_H = 46, GAP_X = 74, GAP_Y = 20;
let dagNodes = [], dagEls = {};

function layoutDag(){
  const byLayer = {};
  M.topology.nodes.forEach(n => (byLayer[n.layer] = byLayer[n.layer]||[]).push(n));
  const layers = Object.keys(byLayer).map(Number).sort((a,b)=>a-b);
  const maxRows = Math.max(...layers.map(l => byLayer[l].length));
  const H = maxRows*(NODE_H+GAP_Y) + 40;
  const W = layers.length*(NODE_W+GAP_X) + 40;
  dagNodes = [];
  layers.forEach((l, li) => {
    const col = byLayer[l];
    col.forEach((n, ri) => {
      const colH = col.length*(NODE_H+GAP_Y);
      dagNodes.push(Object.assign({}, n, {
        x: 20 + li*(NODE_W+GAP_X),
        y: 20 + (H-colH)/2 + ri*(NODE_H+GAP_Y),
      }));
    });
  });
  return {W, H};
}

function drawDag(){
  const host = document.getElementById('dag');
  host.innerHTML = '';
  const {W, H} = layoutDag();
  const svg = el('svg', {viewBox:`0 0 ${W} ${H}`, width:'100%'});
  const pos = {}; dagNodes.forEach(n => pos[n.name] = n);

  svg.appendChild(el('defs', {}, [ (() => {
      const m = el('marker',{id:'arrow',viewBox:'0 0 10 10',refX:9,refY:5,
        markerWidth:6,markerHeight:6,orient:'auto-start-reverse'});
      m.appendChild(el('path',{d:'M0,0 L10,5 L0,10 z', fill:'#484F58'}));
      return m; })() ]));

  // edges first so nodes sit on top
  M.topology.edges.forEach(e => {
    const a = pos[e.from], b = pos[e.to];
    if (!a || !b) return;
    const x1 = a.x+NODE_W, y1 = a.y+NODE_H/2, x2 = b.x, y2 = b.y+NODE_H/2;
    const mx = (x1+x2)/2;
    const p = el('path',{d:`M${x1},${y1} C${mx},${y1} ${mx},${y2} ${x2},${y2}`,
      fill:'none', stroke:'#484F58','stroke-width':1.4,'marker-end':'url(#arrow)'});
    attachTip(p, `<b>${e.from} → ${e.to}</b><br>transport: ${e.transport||'—'}` +
      (e.cap?`<br>ring cap: ${e.cap}`:'') + (e.producers?`<br>producer columns: ${e.producers} (base ${e.base})`:''));
    svg.appendChild(p);
  });

  dagNodes.forEach(n => {
    const g = el('g', {class:'node', transform:`translate(${n.x},${n.y})`});
    const rect = el('rect',{width:NODE_W,height:NODE_H,rx:8,fill:'#1C2230',
      stroke:'#30363D','stroke-width':1.5});
    const name = el('text',{x:NODE_W/2,y:19,fill:'#E6EDF3','font-size':12.5,
      'text-anchor':'middle','font-weight':600});
    name.appendChild(txt(n.name));
    const sub = el('text',{x:NODE_W/2,y:34,fill:'#8B949E','font-size':11,'text-anchor':'middle'});
    g.appendChild(rect); g.appendChild(name); g.appendChild(sub);
    svg.appendChild(g);
    dagEls[n.name] = {rect, sub, node:n, tipHtml:''};
    attachLiveTip(rect, () => dagEls[n.name].tipHtml);
  });

  host.appendChild(svg);
  updateDag(0);
}

function updateDag(t){
  const cfg = M.flags.autoscaler || {};
  for (const [name, o] of Object.entries(dagEls)){
    const reps = valueAt(M.replicas[name]||[], t);
    const qd = M.queue[name] ? valueAt(M.queue[name], t) : null;
    const max = (cfg[name]?.max) || Math.max(1, ...(M.replicas[name]||[[0,1]]).map(p=>p[1]));
    const frac = max ? reps/max : 0;
    o.rect.setAttribute('fill', reps === 0 ? '#161B22'
      : `color-mix(in srgb, #58A6FF ${12+frac*55}%, #161B22)`);
    o.rect.setAttribute('stroke', reps === 0 ? '#30363D' : '#58A6FF');
    o.sub.textContent = `${reps}/${max} rep` + (qd===null?'':`  ·  q ${qd}`);
    o.sub.setAttribute('fill', reps === 0 ? '#8B949E' : '#A5D6FF');
    o.tipHtml = `<b>${name}</b><br>replicas: ${reps} / ${max}` +
      (qd===null?'':`<br>queue depth: ${qd}`) +
      `<br>in-degree ${o.node.in_degree} · out-degree ${o.node.out_degree}` +
      `<br>paths in ${o.node.paths_in} · out ${o.node.paths_out}`;
  }
  document.getElementById('tnow').textContent = t.toFixed(2)+' s';
  const tot = Object.keys(dagEls).reduce((a,n)=>a+valueAt(M.replicas[n]||[],t),0);
  document.getElementById('livereps').textContent = tot;
  drawPlayhead(t);
}

// ── scrubbable time charts ──────────────────────────────────────────────────
// Every chart on the run clock shares this plot area, so a given instant sits
// at the same x in all of them — otherwise the playheads drift apart and a
// burst can't be lined up by eye with the scale-up it triggered.
const TPAD = {l:96, r:62};
// Any chart on the run's time axis can carry the playhead and accept a click
// to scrub, so the DAG, the replica bands and the offered load stay in sync.
let scrubbers = [];
function makeScrubbable(c, s){
  const ph = el('line',{x1:-10,x2:-10,y1:Math.max(0,c.pad.t-8),y2:c.h-c.pad.b,
    stroke:'#E6EDF3','stroke-width':1.5});
  c.svg.appendChild(ph);
  scrubbers.push({c, s, ph});
  c.svg.style.cursor = 'crosshair';
  c.svg.addEventListener('click', ev => {
    const box = c.svg.getBoundingClientRect();
    const vx = (ev.clientX-box.left)/box.width*c.w;
    const t = s.xd[0] + (vx-c.pad.l)/(c.w-c.pad.l-c.pad.r)*(s.xd[1]-s.xd[0]);
    setT(Math.max(0, Math.min(T_MAX, t)));
  });
}
function drawPlayhead(t){
  scrubbers.forEach(sc => {
    const x = sc.s.x(t);
    sc.ph.setAttribute('x1', x); sc.ph.setAttribute('x2', x);
  });
}

// ── offered load (loadgen) ──────────────────────────────────────────────────
function drawLoad(){
  const host = document.getElementById('load'); host.innerHTML='';
  const L = M.load || {cumulative:[], rate:[]};
  if (!L.cumulative.length)
    return emptyNote(host, 'No loadgen log for this run — offered load is unknown.');

  const c = chart(1160, 250, TPAD);
  const maxCum = Math.max(...L.cumulative.map(p=>p[1]));
  const maxRate = Math.max(1, ...L.rate.map(p=>p[1]));
  const sc = scales(c, [0, T_MAX], [0, maxCum*1.08]);
  const sr = scales(c, [0, T_MAX], [0, maxRate*1.15]);
  axes(c, sc, {xfmt:v=>fmt(v,1)+'s', yfmt:v=>fmtInt(v),
               ylabel:'events offered (cumulative)'});

  // Interval-average rate first, as a filled step behind the total. Skipped
  // when the samples are unevenly spaced (see model._load) — the numbers would
  // say more about where the sample boundaries fell than about the workload.
  if (L.rate.length && L.rate_uniform){
    let d = `M${sr.x(L.rate[0][0])},${sr.y(0)}`;
    L.rate.forEach((p,i) => {
      const X = sr.x(p[0]);
      if (i>0) d += `L${X},${sr.y(L.rate[i-1][1])}`;
      d += `L${X},${sr.y(p[1])}`;
    });
    d += `L${sr.x(L.rate[L.rate.length-1][0])},${sr.y(0)}Z`;
    c.svg.appendChild(el('path',{d, fill:'#D29922', opacity:.16}));
    line(c, sr, L.rate, '#D29922', true, 1.4);
    const lab = el('text',{x:c.w-c.pad.r+6,y:c.pad.t+2,fill:'#D29922','font-size':10});
    lab.appendChild(txt(fmtInt(maxRate))); c.svg.appendChild(lab);
    const lab2 = el('text',{x:c.w-c.pad.r+6,y:c.h-c.pad.b,fill:'#D29922','font-size':10});
    lab2.appendChild(txt('0 ev/s')); c.svg.appendChild(lab2);
  }

  line(c, sc, L.cumulative, '#3FB950', true, 2);
  (L.bursts||[]).forEach((b,i) => {
    const dot = el('circle',{cx:sc.x(b[0]),cy:sc.y(b[1]),r:3.6,fill:'#3FB950'});
    attachTip(dot, `<b>burst ${i+1}</b><br>${fmtInt(b[1])} events offered by ` +
      `t=${fmt(b[0],3)}s`);
    c.svg.appendChild(dot);
  });
  (M.markers||[]).filter(m=>m.kind==='eos').forEach(m => {
    const x = sc.x(m.t);
    c.svg.appendChild(el('line',{x1:x,x2:x,y1:c.pad.t,y2:c.h-c.pad.b,stroke:'#F778BA',
      'stroke-width':1,'stroke-dasharray':'3 3',opacity:.7}));
  });

  makeScrubbable(c, sc);
  host.appendChild(c.svg);
  const keys = [['cumulative offered','#3FB950']];
  if (L.rate.length && L.rate_uniform)
    keys.push(['offered rate, interval average (right axis)','#D29922']);
  legend(host, keys);

  const note = document.createElement('div');
  note.className = 'pathnote';
  const prof = (M.flags.source && M.flags.source.profile) || 'unknown profile';
  let text = `${fmtInt(L.total)} events offered across ${L.samples} logged sample(s) — ` +
    `${prof}. The curve is exact at each sample; its slope is the offered rate.`;
  if (L.rate.length && !L.rate_uniform)
    text += ` No rate curve is drawn: the loadgen logs its counter once per burst here, ` +
      `so an average between two samples would mostly reflect how much idle time the ` +
      `interval happened to include, not how fast the burst ran.`;
  note.innerHTML = text;
  host.appendChild(note);
}

// ── replica timeline strip ──────────────────────────────────────────────────
let ribbon = null;
function drawRibbon(){
  const host = document.getElementById('ribbon');
  host.innerHTML = '';
  const names = M.topology.nodes.map(n=>n.name);
  const maxRep = Math.max(1, ...names.map(n => Math.max(0,...(M.replicas[n]||[]).map(p=>p[1]))));
  const c = chart(1160, 40+names.length*22, Object.assign({}, TPAD, {b:24,t:8}));
  const s = scales(c, [0,T_MAX], [0, maxRep]);
  // No y ticks: the rows are stages, and numbers would print behind their names.
  axes(c, s, {yticks:0, xfmt:v=>fmt(v,1)+'s'});

  names.forEach((n,i) => {
    const rowY = 8 + i*22;
    const lab = el('text',{x:90,y:rowY+13,fill:'#C9D1D9','font-size':11,'text-anchor':'end'});
    lab.appendChild(txt(n)); c.svg.appendChild(lab);
    const series = M.replicas[n]||[];
    // one band per interval, opacity by replica count
    for (let k=0;k<series.length;k++){
      const t0 = series[k][0], t1 = (k+1<series.length ? series[k+1][0] : T_MAX);
      const v = series[k][1];
      if (v<=0 || t1<=t0) continue;
      const r = el('rect',{x:s.x(t0), y:rowY, width:Math.max(1,s.x(t1)-s.x(t0)), height:18,
        rx:3, fill:colorOf(i), opacity:0.25+0.75*(v/maxRep)});
      attachTip(r, `<b>${n}</b><br>${v} replica(s)<br>${fmt(t0,2)}s → ${fmt(t1,2)}s`);
      c.svg.appendChild(r);
    }
  });

  (M.markers||[]).forEach(m => {
    const x = s.x(m.t);
    const col = m.kind==='eos' ? '#F778BA' : m.kind==='slo' ? '#F85149' : '#8B949E';
    const l = el('line',{x1:x,x2:x,y1:6,y2:c.h-c.pad.b,stroke:col,'stroke-width':1,
      'stroke-dasharray':'3 3',opacity:.75});
    attachTip(l, `${m.label}<br>t=${fmt(m.t,2)}s`);
    c.svg.appendChild(l);
  });

  makeScrubbable(c, s);
  host.appendChild(c.svg);
  ribbon = {c, s};
}

// ── slider wiring ───────────────────────────────────────────────────────────
let playing = false, rafId = null, lastFrame = 0;
function setT(t, pushHash=true){
  t = Math.max(0, Math.min(T_MAX, t));
  document.getElementById('slider').value = String(t);
  updateDag(t);
  // Deep-link the scrubbed moment so a specific instant can be shared or
  // reopened. history.replaceState keeps the back button usable.
  if (pushHash && !playing)
    history.replaceState(null, '', '#t=' + t.toFixed(3));
}
function initialT(){
  const m = /[#&]t=([\d.]+)/.exec(location.hash || '');
  return m ? Math.max(0, Math.min(T_MAX, parseFloat(m[1]))) : 0;
}
function play(){
  playing = !playing;
  document.getElementById('play').textContent = playing ? '❚❚ Pause' : '▶ Play';
  lastFrame = performance.now();
  if (playing) step();
}
function step(){
  if (!playing) return;
  const now = performance.now();
  const dt = (now-lastFrame)/1000 * (+document.getElementById('speed').value);
  lastFrame = now;
  let t = (+document.getElementById('slider').value) + dt;
  if (t >= T_MAX){ t = 0; }
  setT(t);
  rafId = requestAnimationFrame(step);
}

// ── section charts ──────────────────────────────────────────────────────────
function drawQueue(){
  const host = document.getElementById('queuechart'); host.innerHTML='';
  const names = Object.keys(M.queue).filter(n => (M.queue[n]||[]).some(p=>p[1]>0));
  if (!names.length) return emptyNote(host, 'No queue-depth samples in this run.');
  const maxY = Math.max(...names.map(n => Math.max(...M.queue[n].map(p=>p[1]))));
  const c = chart(1160, 260, TPAD); const s = scales(c, [0,T_MAX], [0, maxY*1.05]);
  axes(c, s, {xfmt:v=>fmt(v,1)+'s', yfmt:v=>fmtInt(v), ylabel:'queue depth (events)'});
  names.forEach((n,i) => line(c, s, M.queue[n], colorOf(M.topology.nodes.findIndex(x=>x.name===n)), true, 1.4));
  host.appendChild(c.svg);
  legend(host, names.map(n => [n, colorOf(M.topology.nodes.findIndex(x=>x.name===n))]));
}
function legend(host, pairs){
  const d = document.createElement('div'); d.className='legend';
  pairs.forEach(([label,color]) => {
    const s = document.createElement('span');
    s.innerHTML = `<i style="background:${color}"></i>${label}`;
    d.appendChild(s);
  });
  host.appendChild(d);
}

function drawCdf(){
  const host = document.getElementById('cdf'); host.innerHTML='';
  const {x,y} = M.latency.cdf;
  if (!x.length) return emptyNote(host, 'No CDF data.');
  const pts = x.map((v,i)=>[v, y[i]*100]);
  const c = chart(560, 240); const s = scales(c, [x[0], x[x.length-1]], [0,100]);
  axes(c, s, {xfmt:tickFmt(s.xd[1]-s.xd[0],'ms'), yfmt:v=>fmt(v,0)+'%', ylabel:'percentile'});
  line(c, s, pts, '#58A6FF', false, 2);
  [['p50',M.latency.e2e.p50,'#3FB950'],['p99',M.latency.e2e.p99,'#D29922']].forEach(([lab,v,col])=>{
    if (!v) return;
    const X = s.x(Math.min(Math.max(v,s.xd[0]),s.xd[1]));
    c.svg.appendChild(el('line',{x1:X,x2:X,y1:c.pad.t,y2:c.h-c.pad.b,stroke:col,
      'stroke-width':1,'stroke-dasharray':'4 3',opacity:.8}));
    const t = el('text',{x:X+4,y:c.pad.t+11,fill:col,'font-size':10}); t.appendChild(txt(lab));
    c.svg.appendChild(t);
  });
  host.appendChild(c.svg);
}

function drawHist(){
  const host = document.getElementById('hist'); host.innerHTML='';
  const {labels,counts} = M.latency.hist;
  if (!labels.length) return emptyNote(host, 'No histogram data.');
  const pts = labels.map((v,i)=>[v, counts[i]]);
  const c = chart(560, 240); const s = scales(c, [labels[0], labels[labels.length-1]],
    [0, Math.max(...counts)*1.05]);
  axes(c, s, {xfmt:tickFmt(s.xd[1]-s.xd[0],'ms'), yfmt:v=>fmtInt(v), ylabel:'events'});
  bars(c, s, pts, '#BC8CFF');
  host.appendChild(c.svg);
}

function drawScatter(){
  const host = document.getElementById('scatter'); host.innerHTML='';
  const pts = M.scatter.points;
  if (!pts.length) return emptyNote(host, 'No per-event rows in this summary.');
  const maxY = Math.max(...pts.map(p=>p[1]));
  const c = chart(1160, 260, TPAD); const s = scales(c, [0, T_MAX], [0, maxY*1.05]);
  axes(c, s, {xfmt:v=>fmt(v,1)+'s', yfmt:tickFmt(s.yd[1]-s.yd[0],'ms'), ylabel:'e2e latency'});
  const g = el('g');
  pts.forEach(p => g.appendChild(el('circle',{cx:s.x(p[0]),cy:s.y(p[1]),r:1.1,
    fill:'#58A6FF',opacity:.35})));
  c.svg.appendChild(g);
  (M.markers||[]).filter(m=>m.kind!=='slo').forEach(m => {
    const x = s.x(m.t);
    c.svg.appendChild(el('line',{x1:x,x2:x,y1:c.pad.t,y2:c.h-c.pad.b,
      stroke:'#F778BA','stroke-width':1,'stroke-dasharray':'3 3',opacity:.5}));
  });
  host.appendChild(c.svg);
  const note = document.createElement('div');
  note.className='pathnote';
  note.textContent = `${fmtInt(M.scatter.shown)} of ${fmtInt(M.scatter.total)} sampled events plotted`;
  host.appendChild(note);
}

function drawThroughput(){
  const host = document.getElementById('tput'); host.innerHTML='';
  const rps = M.throughput.recv_per_second;
  if (!rps.length) return emptyNote(host, 'No per-second throughput series.');
  const pts = rps.map((v,i)=>[i, v]);
  const c = chart(560, 220); const s = scales(c, [0, Math.max(1,rps.length-1)], [0, Math.max(...rps)*1.05]);
  axes(c, s, {xfmt:v=>fmt(v,0)+'s', yfmt:v=>fmtInt(v), ylabel:'events/s at collector'});
  bars(c, s, pts, '#3FB950');
  host.appendChild(c.svg);
}

function drawColdStart(){
  const host = document.getElementById('coldchart'); host.innerHTML='';
  const boots = M.coldstart.boots.filter(b => b.t !== null);
  if (!boots.length) return emptyNote(host, 'No worker boots recorded (needs master.jsonl).');
  const maxY = Math.max(...boots.map(b=>b.cold_start_ms));
  const c = chart(1160, 240, TPAD); const s = scales(c, [0, T_MAX], [0, maxY*1.1]);
  axes(c, s, {xfmt:v=>fmt(v,1)+'s', yfmt:tickFmt(s.yd[1]-s.yd[0],'ms'), ylabel:'cold start'});
  const idx = {}; M.topology.nodes.forEach((n,i)=>idx[n.name]=i);
  boots.forEach(b => {
    const cx = s.x(b.t), cy = s.y(b.cold_start_ms);
    const dot = el('circle',{cx,cy,r:3.4,fill:colorOf(idx[b.stage]||0),opacity:.9});
    attachTip(dot, `<b>${b.stage}#${b.replica}</b> (${b.rid})<br>` +
      `cold start ${fmt(b.cold_start_ms,3)} ms<br>t=${fmt(b.t,2)}s<br>` +
      `spawn ${fmt(b.spawn_ms,3)} · instantiate ${fmt(b.instantiate_ms,3)} · ` +
      `export ${fmt(b.export_ms,3)} · sockets ${fmt(b.sockets_ms,3)} ms`);
    c.svg.appendChild(dot);
  });
  host.appendChild(c.svg);
}

function drawResources(){
  const host = document.getElementById('res'); host.innerHTML='';
  const {cpu, rss} = M.resources;
  if (!cpu.length && !rss.length)
    return emptyNote(host, 'Resource sampling was disabled for this run.');
  const c = chart(1160, 240, TPAD);
  const maxL = Math.max(1, ...cpu.map(p=>p[1])), maxR = Math.max(1, ...rss.map(p=>p[1]));
  const s = scales(c, [0, T_MAX], [0, maxL*1.1]);
  axes(c, s, {xfmt:v=>fmt(v,1)+'s', yfmt:v=>fmt(v,0)+'%',
              ylabel:'agent CPU (% of available cpus)'});
  line(c, s, cpu, '#D29922', false, 1.8);
  cpu.forEach(p => c.svg.appendChild(el('circle',{cx:s.x(p[0]),cy:s.y(p[1]),r:2.4,fill:'#D29922'})));
  if (rss.length){
    // RSS gets its own scale — MB and % share no units, and forcing one axis
    // would flatten whichever series is smaller.
    const s2 = scales(c, [0, T_MAX], [0, maxR*1.1]);
    line(c, s2, rss, '#39C5CF', false, 1.8);
    rss.forEach(p => c.svg.appendChild(el('circle',{cx:s2.x(p[0]),cy:s2.y(p[1]),r:2.4,fill:'#39C5CF'})));
    const lab = el('text',{x:c.w-c.pad.r,y:c.pad.t-8,fill:'#39C5CF','font-size':10,
      'text-anchor':'end'});
    lab.appendChild(txt(`RSS peak ${fmt(maxR,0)} MB`)); c.svg.appendChild(lab);
  }
  host.appendChild(c.svg);
  legend(host, [['CPU % of available','#D29922'], ['RSS MB (right-hand scale)','#39C5CF']]);
}

function drawStageLatency(){
  const host = document.getElementById('stagelat'); host.innerHTML='';
  const rows = Object.entries(M.latency.per_stage);
  if (!rows.length) return emptyNote(host, 'No per-stage latency.');
  const maxY = Math.max(...rows.map(([,v])=>v.p99));
  const c = chart(1160, 240, {l:56,b:60});
  const s = scales(c, [0, rows.length], [0, maxY*1.1]);
  axes(c, s, {xticks:0, yfmt:tickFmt(s.yd[1]-s.yd[0],'ms'), ylabel:'stage residency'});
  rows.forEach(([name,v],i) => {
    const x = s.x(i+0.5), bw = (c.w-c.pad.l-c.pad.r)/rows.length*0.55;
    const y99 = s.y(v.p99), y50 = s.y(v.p50), y0 = s.y(0);
    c.svg.appendChild(el('rect',{x:x-bw/2,y:y99,width:bw,height:y0-y99,fill:'#58A6FF',opacity:.28,rx:2}));
    c.svg.appendChild(el('rect',{x:x-bw/2,y:y50,width:bw,height:y0-y50,fill:'#58A6FF',opacity:.85,rx:2}));
    const lab = el('text',{x, y:c.h-c.pad.b+14, fill:'#8B949E','font-size':10,
      'text-anchor':'end', transform:`rotate(-40 ${x} ${c.h-c.pad.b+14})`});
    lab.appendChild(txt(name)); c.svg.appendChild(lab);
    const hit = el('rect',{x:x-bw/2,y:c.pad.t,width:bw,height:c.h-c.pad.t-c.pad.b,fill:'transparent'});
    attachTip(hit, `<b>${name}</b><br>p50 ${fmt(v.p50,3)} ms<br>p99 ${fmt(v.p99,3)} ms<br>` +
      `max ${fmt(v.max,3)} ms<br>n=${fmtInt(v.count)}`);
    c.svg.appendChild(hit);
  });
  host.appendChild(c.svg);
  legend(host, [['p50','#58A6FF'],['p99 (light)','rgba(88,166,255,.35)']]);
}

// ── tables ──────────────────────────────────────────────────────────────────
function table(host, cols, rows){
  const t = document.createElement('table');
  const thead = document.createElement('thead');
  const tr = document.createElement('tr');
  cols.forEach(c => { const th=document.createElement('th'); th.textContent=c.h; tr.appendChild(th); });
  thead.appendChild(tr); t.appendChild(thead);
  const tb = document.createElement('tbody');
  rows.forEach(r => {
    const tr = document.createElement('tr');
    cols.forEach(c => {
      const td = document.createElement('td');
      const v = c.f ? c.f(r) : r[c.k];
      if (v instanceof Node) td.appendChild(v); else td.innerHTML = v;
      tr.appendChild(td);
    });
    tb.appendChild(tr);
  });
  t.appendChild(tb);
  const wrap = document.createElement('div'); wrap.className='scroll';
  wrap.appendChild(t); host.appendChild(wrap);
}

function fillTables(){
  table(document.getElementById('t-conservation'),
    [{h:'stage',k:'stage'},
     {h:'paths in',f:r=>fmtInt(r.paths_in)},
     {h:'paths out',f:r=>fmtInt(r.paths_out)},
     {h:'expected',f:r=>r.expected===null?'—':fmtInt(r.expected)},
     {h:'actual',f:r=>fmtInt(r.actual)},
     {h:'Δ',f:r=> r.delta===null ? '—'
        : `<span class="${r.delta===0?'':'badge bad'}">${r.delta>0?'+':''}${fmtInt(r.delta)}</span>`}],
    M.counters.rows);

  table(document.getElementById('t-stagelat'),
    [{h:'stage',k:'stage'},{h:'n',f:r=>fmtInt(r.count)},{h:'mean',f:r=>fmt(r.mean,3)},
     {h:'p50',f:r=>fmt(r.p50,3)},{h:'p95',f:r=>fmt(r.p95,3)},{h:'p99',f:r=>fmt(r.p99,3)},
     {h:'p999',f:r=>fmt(r.p999,3)},{h:'max',f:r=>fmt(r.max,3)}],
    Object.entries(M.latency.per_stage).map(([stage,v])=>Object.assign({stage},v)));

  table(document.getElementById('t-inter'),
    [{h:'edge',f:r=>`${r.from} → ${r.to}`},{h:'n',f:r=>fmtInt(r.count)},
     {h:'mean',f:r=>fmt(r.mean,3)},{h:'p50',f:r=>fmt(r.p50,3)},{h:'p95',f:r=>fmt(r.p95,3)},
     {h:'p99',f:r=>fmt(r.p99,3)},{h:'max',f:r=>fmt(r.max,3)}],
    M.latency.inter_stage);

  table(document.getElementById('t-replica'),
    [{h:'stage',k:'stage'},{h:'replica',k:'replica'},{h:'n',f:r=>fmtInt(r.count)},
     {h:'mean',f:r=>fmt(r.mean,3)},{h:'p50',f:r=>fmt(r.p50,3)},{h:'p95',f:r=>fmt(r.p95,3)},
     {h:'p99',f:r=>fmt(r.p99,3)},{h:'max',f:r=>fmt(r.max,3)}],
    M.latency.per_replica);

  table(document.getElementById('t-boots'),
    [{h:'t (s)',f:r=>fmt(r.t,3)},{h:'stage',k:'stage'},{h:'replica',f:r=>'#'+r.replica},
     {h:'rid',f:r=>`<span class="mono">${r.rid||'—'}</span>`},
     {h:'cold start',f:r=>fmt(r.cold_start_ms,3)},{h:'spawn',f:r=>fmt(r.spawn_ms,3)},
     {h:'instantiate',f:r=>fmt(r.instantiate_ms,3)},{h:'export',f:r=>fmt(r.export_ms,3)},
     {h:'sockets',f:r=>fmt(r.sockets_ms,3)}],
    M.coldstart.boots);

  table(document.getElementById('t-scaling'),
    [{h:'t (s)',f:r=>fmt(r.t,3)},{h:'stage',k:'stage'},{h:'action',f:r=>
       `<span class="badge ${r.action==='drain'?'':'ok'}">${r.action}</span>`},
     {h:'replicas after',f:r=>fmtInt(r.new_count)},
     {h:'compile ms',f:r=>r.compile_ms===null?'—':fmt(r.compile_ms,2)},
     {h:'instantiate_pre ms',f:r=>r.instantiate_pre_ms===null?'—':fmt(r.instantiate_pre_ms,3)}],
    M.scaling_events);

  table(document.getElementById('t-stagecfg'),
    [{h:'stage',k:'stage'},{h:'min',f:r=>fmtInt(r.min)},{h:'max',f:r=>fmtInt(r.max)},
     {h:'queue up',f:r=>fmt(r.queue_up,0)},{h:'queue down',f:r=>fmt(r.queue_down,0)},
     {h:'mode',f:r=>r.mode||'—'},
     {h:'compile ms',f:r=>r.compile_ms===undefined?'—':fmt(r.compile_ms,2)},
     {h:'warmup ms',f:r=>r.warmup_ms===undefined?'—':fmt(r.warmup_ms,2)},
     {h:'ups',f:r=>fmtInt(r.ups)},{h:'downs',f:r=>fmtInt(r.downs)},
     {h:'events',f:r=>fmtInt(r.events)},{h:'eps',f:r=>fmt(r.eps,0)}],
    M.topology.nodes.map(n => {
      const a = (M.flags.autoscaler||{})[n.name] || {};
      const evs = M.scaling_events.filter(e=>e.stage===n.name);
      return {stage:n.name, min:a.min, max:a.max, queue_up:a.queue_up, queue_down:a.queue_down,
        mode:a.mode, compile_ms:(M.flags.compile_ms||{})[n.name],
        warmup_ms:(M.flags.warmup_ms||{})[n.name],
        ups:evs.filter(e=>e.action==='spawn'||e.action==='cold_start').length,
        downs:evs.filter(e=>e.action==='drain').length,
        events:(M.counters.rows.find(r=>r.stage===n.name)||{}).actual,
        eps:(M.throughput.per_stage_eps||{})[n.name]};
    }));

  const wt = Object.entries(M.worker_timing).map(([stage,v]) => ({
    stage, n:v.n, wasm_p50:v.wasm_us?.p50, wasm_p99:v.wasm_us?.p99, wasm_max:v.wasm_us?.max,
    serde_p50:v.serde_us?.p50, serde_p99:v.serde_us?.p99, total_p50:v.total_us?.p50,
    overhead_p50:v.overhead_us?.p50}));
  table(document.getElementById('t-worker'),
    [{h:'stage',k:'stage'},{h:'n',f:r=>fmtInt(r.n)},
     {h:'wasm p50 µs',f:r=>fmt(r.wasm_p50,3)},{h:'wasm p99 µs',f:r=>fmt(r.wasm_p99,3)},
     {h:'wasm max µs',f:r=>fmt(r.wasm_max,2)},{h:'serde p50 µs',f:r=>fmt(r.serde_p50,3)},
     {h:'serde p99 µs',f:r=>fmt(r.serde_p99,3)},{h:'total p50 µs',f:r=>fmt(r.total_p50,3)},
     {h:'overhead p50 µs',f:r=>fmt(r.overhead_p50,3)}],
    wt);

  table(document.getElementById('t-markers'),
    [{h:'t (s)',f:r=>fmt(r.t,3)},{h:'kind',f:r=>`<span class="badge">${r.kind}</span>`},
     {h:'event',k:'label'}], M.markers);

  if (M.latency.ingress_wait.length)
    table(document.getElementById('t-ingress'),
      [{h:'stage',k:'stage'},{h:'n',f:r=>fmtInt(r.count)},{h:'mean',f:r=>fmt(r.mean,3)},
       {h:'p50',f:r=>fmt(r.p50,3)},{h:'p95',f:r=>fmt(r.p95,3)},{h:'p99',f:r=>fmt(r.p99,3)},
       {h:'max',f:r=>fmt(r.max,3)}], M.latency.ingress_wait);
  else emptyNote(document.getElementById('t-ingress'), 'No ingress-wait data.');
}

// ── boot ────────────────────────────────────────────────────────────────────
function init(){
  document.getElementById('slider').max = String(T_MAX);
  document.getElementById('slider').step = String(Math.max(0.001, T_MAX/2000));
  document.getElementById('slider').addEventListener('input', e => {
    playing = false; document.getElementById('play').textContent = '▶ Play';
    updateDag(+e.target.value);
  });
  document.getElementById('play').addEventListener('click', play);
  document.getElementById('rewind').addEventListener('click', () => setT(0));
  drawDag(); drawLoad(); drawRibbon(); drawQueue(); drawCdf(); drawHist(); drawScatter();
  drawThroughput(); drawColdStart(); drawResources(); drawStageLatency();
  fillTables();
  setT(initialT(), false);
  document.querySelectorAll('.tabs button').forEach(b => {
    b.addEventListener('click', () => {
      const group = b.closest('section');
      group.querySelectorAll('.tabs button').forEach(x=>x.classList.remove('active'));
      group.querySelectorAll('.tabpane').forEach(p=>p.classList.add('hidden'));
      b.classList.add('active');
      group.querySelector('#'+b.dataset.pane).classList.remove('hidden');
    });
  });
}
document.addEventListener('DOMContentLoaded', init);
"""


def _esc(s) -> str:
    return (str(s).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
            if s is not None else "—")


def _kpi(label, value, unit="", foot="") -> str:
    return (f'<div class="kpi"><div class="label">{_esc(label)}</div>'
            f'<div class="value">{value}<span class="unit">{_esc(unit)}</span></div>'
            f'<div class="foot">{foot}</div></div>')


def _num(v, digits=0, dash="—") -> str:
    if v is None:
        return dash
    if digits == 0:
        return f"{round(v):,}"
    return f"{v:,.{digits}f}"


COMPILE_LABEL = {
    "aot": "AOT (.cwasm precompiled)",
    "jit": "cold-start JIT",
    "startup": "startup-JIT",
    "mixed": "mixed across stages",
}


def render(model: dict, logo: str | None = None) -> str:
    m, f, c = model["meta"], model["flags"], model["counters"]
    lat, cs, tp = model["latency"], model["coldstart"], model["throughput"]
    env = model["environment"]

    # ── flag chips ───────────────────────────────────────────────────────────
    req = f["compile_mode_requested"]
    eff = f["compile_mode_effective"]
    compile_txt = COMPILE_LABEL.get(req, req or "unknown")
    if eff and req and eff != req and not (req == "startup" and eff == "jit"):
        compile_txt += f" → ran as {eff}"

    cso = f["cold_start_opt"]
    cso_badge = ('<span class="badge ok">enabled</span>' if cso is True else
                 '<span class="badge">disabled</span>' if cso is False else
                 '<span class="badge warn">unknown</span>')

    conserved = c["conserved"]
    cons_badge = ('<span class="badge ok">conserved</span>' if conserved is True else
                  '<span class="badge bad">LEAK / DUPLICATE</span>' if conserved is False else
                  '<span class="badge">not checkable</span>')

    warnings_html = ""
    if model["warnings"]:
        items = "".join(f"<li>{_esc(w)}</li>" for w in model["warnings"])
        warnings_html = (f'<div class="warnings"><b>Notes on this run</b><ul>{items}</ul></div>')

    src = f["source"]
    src_bits = []
    if src["kind"]:
        src_bits.append(f'<dt>source</dt><dd>{_esc(src["kind"])}</dd>')
        src_bits.append(f'<dt>profile</dt><dd>{_esc(src["profile"])}</dd>')
        if src["rate"]:
            src_bits.append(f'<dt>rate</dt><dd>{_num(src["rate"])} ev/s</dd>')
        if src["count"]:
            src_bits.append(f'<dt>count</dt><dd>{_num(src["count"])}</dd>')
        src_bits.append(f'<dt>wire format</dt><dd>{_esc(src["format"])}</dd>')
        src_bits.append(f'<dt>ingress</dt><dd class="mono">{_esc(src["entry"])}</dd>')
        if src["sent"] is not None:
            src_bits.append(f'<dt>sent / dropped</dt>'
                            f'<dd>{_num(src["sent"])} / {_num(src["dropped"])}</dd>')
    else:
        src_bits.append('<dt>source</dt><dd class="muted">no loadgen log for this run</dd>')

    kpis = "".join([
        _kpi("events at collector", _num(c["received"]), "",
             f'{_num(c["paths"])} source→sink path(s)'),
        _kpi("offered", _num(c["sent"]) if c["sent"] is not None else "—", "",
             cons_badge),
        _kpi("duration", _num(m["duration_s"], 2), "s",
             f'sustained {_num(tp["sustained_eps"])} ev/s'),
        _kpi("e2e p50", _num(lat["e2e"]["p50"], 2), "ms",
             f'mean {_num(lat["e2e"]["mean"], 2)} ms'),
        _kpi("e2e p99", _num(lat["e2e"]["p99"], 2), "ms",
             f'p999 {_num(lat["e2e"]["p999"], 2)} · max {_num(lat["e2e"]["max"], 2)} ms'),
        _kpi("cold starts", _num(cs["count"]), "",
             f'p50 {_num(cs["p50"], 3)} · max {_num(cs["max"], 3)} ms'),
        _kpi("stages", _num(m["stage_count"]), "",
             f'{len(model["topology"]["edges"])} edges'),
        _kpi("scale ups / downs",
             f'{sum(1 for e in model["scaling_events"] if e["action"] in ("spawn", "cold_start"))}'
             f' / {sum(1 for e in model["scaling_events"] if e["action"] == "drain")}',
             "", "across the run"),
    ])

    flag_rows = [
        ("compile mode", _esc(compile_txt)),
        ("cold-start-opt", cso_badge),
        ("edge transport", _esc(f["edge_impl"] or "—") +
         (f' · cap {_num(f["edge_cap"])}' if f["edge_cap"] else "")),
        ("ingress", _esc(f["ingress"]["mode"]) +
         (f' · cap {_num(f["ingress"]["cap"])}' if f["ingress"]["cap"] else "")),
        ("egress", _esc(f["egress"]["mode"])),
        ("dispatchers", ", ".join(f["dispatchers"]) if f["dispatchers"]
         else '<span class="muted">none (in-process spine)</span>'),
        ("credit window", _num(f["credit_window"]) if f["credit_window"] else "—"),
        ("batch events", _num(f["batch_events"]) if f["batch_events"] else "—"),
        ("typed dispatch", f'{len(f["typed_dispatch"])} stage(s)'
         if f["typed_dispatch"] else '<span class="muted">off</span>'),
        ("resource sampling", "on" if f["resource_sampling"] else "off"),
        ("max replicas total", _num(f["engine"].get("max_replicas_total"))
         if f["engine"] else "—"),
        ("topology source", _esc(model["topology"]["edge_source"])),
    ]
    flags_html = "".join(f"<dt>{_esc(k)}</dt><dd>{v}</dd>" for k, v in flag_rows)

    env_rows = [
        ("host", env.get("host")), ("cpu", env.get("cpu_model")),
        ("cores", f'{env.get("cpu_cores_physical")} physical / {env.get("cpu_cores_logical")} logical'),
        ("ram", f'{env.get("ram_total_mb")} MB' if env.get("ram_total_mb") else None),
        ("os", f'{env.get("os_name")} {env.get("os_version")} (kernel {env.get("kernel")})'),
        ("rustc", env.get("rustc")), ("wasmtime", env.get("wasmtime")),
        ("git commit", (env.get("git_commit") or "")[:12] +
         (" (dirty)" if env.get("git_dirty") else "")),
    ]
    env_html = "".join(f"<dt>{_esc(k)}</dt><dd>{_esc(v)}</dd>"
                       for k, v in env_rows if v)

    # `</` inside the JSON (a path, a log message) would close the script tag
    # early; the escape is invisible to JSON.parse.
    data = json.dumps(model, separators=(",", ":"), default=str).replace("</", "<\\/")

    logo_img = '<div class="logo" role="img" aria-label="Epico"></div>' if logo else ""
    logo_foot = '<span class="logo"></span>' if logo else ""
    logo_icon = f'<link rel="icon" href="{logo}">' if logo else ""
    logo_css = f":root{{--logo:url({logo})}}" if logo else ""

    return f"""<!doctype html>
<html lang="en"><head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>{_esc(m['pipeline'])} · {_esc(m['run_id'])} · epico-viz</title>
{logo_icon}
<style>{logo_css}{CSS}</style>
</head><body>
<div class="wrap">

<header class="top">
  {logo_img}
  <div class="titles">
    <h1>{_esc(m['pipeline'])}</h1>
    <span class="badge">{_esc(m['run_id'])}</span>
    <span class="sub">{_esc(m['started_iso'] or 'unknown start')} · {_num(m['duration_s'], 2)} s ·
      {_esc(m['layout'])} layout</span>
  </div>
</header>

{warnings_html}

<section>
  <h2>Run at a glance</h2>
  <div class="grid kpis">{kpis}</div>
</section>

<section>
  <h2>Configuration &amp; flags</h2>
  <div class="grid cols3">
    <div class="panel"><h3>Runtime flags</h3><dl class="kv">{flags_html}</dl></div>
    <div class="panel"><h3>Source</h3><dl class="kv">{''.join(src_bits)}</dl></div>
    <div class="panel"><h3>Environment</h3><dl class="kv">{env_html}</dl></div>
  </div>
</section>

<section>
  <h2>Execution — replicas over time</h2>
  <div class="panel">
    <div class="controls">
      <button id="play">▶ Play</button>
      <button id="rewind">⏮ Reset</button>
      <input id="slider" type="range" min="0" value="0">
      <span class="tnow" id="tnow">0.00 s</span>
      <label class="muted">speed
        <select id="speed">
          <option value="0.25">0.25×</option>
          <option value="0.5">0.5×</option>
          <option value="1" selected>1×</option>
          <option value="2">2×</option>
          <option value="4">4×</option>
        </select>
      </label>
      <span class="muted">live replicas: <b id="livereps">0</b></span>
    </div>
    <div class="overflow-x" id="dag"></div>
    <div class="pathnote">Node fill tracks live replicas at the scrubbed time;
      the second line reads <span class="mono">replicas/max · queue depth</span>.
      Hover an edge for its transport and ring geometry.</div>
  </div>
  <div class="panel" style="margin-top:14px">
    <h3>Offered load (click to scrub)</h3>
    <div class="overflow-x" id="load"></div>
  </div>
  <div class="panel" style="margin-top:14px">
    <h3>Replica bands (click to scrub)</h3>
    <div class="overflow-x" id="ribbon"></div>
    <div class="pathnote">Band opacity is the replica count; dashed lines mark
      loadgen bursts, EOS, and SLO breaches. Shares its time axis with the
      offered-load chart above, so a burst and the scale-up it triggers line up.</div>
  </div>
</section>

<section>
  <h2>Queue depth</h2>
  <div class="panel"><div class="overflow-x" id="queuechart"></div></div>
</section>

<section>
  <h2>Latency</h2>
  <div class="grid cols2">
    <div class="panel"><h3>e2e CDF</h3><div id="cdf"></div></div>
    <div class="panel"><h3>e2e histogram</h3><div id="hist"></div></div>
  </div>
  <div class="panel" style="margin-top:14px">
    <h3>Per-event e2e over time</h3><div class="overflow-x" id="scatter"></div>
  </div>
  <div class="panel" style="margin-top:14px">
    <h3>Per-stage residency</h3><div class="overflow-x" id="stagelat"></div>
  </div>
  <div class="tabs" style="margin-top:14px">
    <button class="active" data-pane="p-stagelat">per stage</button>
    <button data-pane="p-inter">inter-stage edges</button>
    <button data-pane="p-replica">per replica</button>
    <button data-pane="p-ingress">ingress wait</button>
  </div>
  <div class="panel">
    <div class="tabpane" id="p-stagelat"><div id="t-stagelat"></div></div>
    <div class="tabpane hidden" id="p-inter"><div id="t-inter"></div></div>
    <div class="tabpane hidden" id="p-replica"><div id="t-replica"></div></div>
    <div class="tabpane hidden" id="p-ingress"><div id="t-ingress"></div></div>
  </div>
</section>

<section>
  <h2>Cold start</h2>
  <div class="panel"><div class="overflow-x" id="coldchart"></div></div>
  <div class="panel" style="margin-top:14px"><h3>Every worker boot</h3>
    <div id="t-boots"></div></div>
</section>

<section>
  <h2>Throughput &amp; resources</h2>
  <div class="grid cols2">
    <div class="panel"><h3>Collector throughput</h3><div id="tput"></div></div>
    <div class="panel"><h3>Agent resources</h3><div id="res"></div></div>
  </div>
</section>

<section>
  <h2>Scaling &amp; stage configuration</h2>
  <div class="tabs">
    <button class="active" data-pane="p-stagecfg">per-stage config</button>
    <button data-pane="p-scaling">scaling events</button>
    <button data-pane="p-markers">run timeline</button>
  </div>
  <div class="panel">
    <div class="tabpane" id="p-stagecfg"><div id="t-stagecfg"></div></div>
    <div class="tabpane hidden" id="p-scaling"><div id="t-scaling"></div></div>
    <div class="tabpane hidden" id="p-markers"><div id="t-markers"></div></div>
  </div>
</section>

<section>
  <h2>Event conservation</h2>
  <div class="panel">
    <div class="pathnote" style="margin:0 0 10px">
      Under broadcast fan-out a stage is traversed once per path through it, so the
      expected count is <span class="mono">paths_in × paths_out × offered</span>.
      {cons_badge}
    </div>
    <div id="t-conservation"></div>
  </div>
</section>

<section>
  <h2>Worker timing</h2>
  <div class="panel"><div id="t-worker"></div></div>
</section>

<footer class="pathnote">
  {logo_foot}
  Generated by epico-viz from
  <span class="mono">{_esc(m['summary_path'])}</span>
  {'· <span class="mono">' + _esc(m['master_log']) + '</span>' if m['master_log'] else ''}
</footer>
</div>
<script>window.__EPICO__ = {data};</script>
<script>{JS}</script>
</body></html>
"""
