"""HTML renderer: one self-contained page per run.

No CDN, no build step, no dependencies — the model is embedded as JSON and
everything is drawn with hand-rolled SVG so the file works offline and can be
opened straight off disk as well as served.

Design direction — "bench instrument". The page is a logic-analyser readout of
one run, not a dashboard: a warm graphite housing, a single cool readout hue for
data, and reserved status lamps. Stage identity is carried by position and label
(the left gutter every time chart shares), so colour is free to encode magnitude
instead of naming things. The four-slot categorical set is only spent where
series genuinely overlap in one plot area, and it is validated for colour-vision
separation rather than eyeballed.
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


# ── design tokens ────────────────────────────────────────────────────────────
# One source of truth: the CSS custom properties and the JS `TK` object are both
# generated from this, so a colour can never drift between the page chrome and
# the SVG the charts draw.
#
# Chosen in OKLCH and checked with the data-viz validator against the chart
# surface (#181412, dark mode): the categorical four pass the lightness band,
# chroma floor, protan/deutan separation, normal-vision floor and 3:1 contrast
# on ALL pairs, not just adjacent ones. Adding a fifth hue fails the
# normal-vision floor on this surface, which is why series past four are
# faceted into small multiples rather than given a new colour.
TOKENS = {
    # housing
    "ground":  "#0E0C0A",
    "panel":   "#181412",
    "raise":   "#241F1C",
    "rule":    "#342E29",
    "ruleSoft": "#241F1C",
    # ink — warm off-white, never a series colour
    "ink":     "#F2EEE9",
    "ink2":    "#B6B0A9",
    "ink3":    "#807972",
    # the readout hue: every default data mark
    "signal":   "#0DB3C1",
    "signalHi": "#56D2DF",
    "signalDim": "#06717A",
    # sequential ramp, one hue light→dark, for magnitude
    "seq0": "#015259", "seq1": "#06717A", "seq2": "#03919D",
    "seq3": "#0DB3C1", "seq4": "#56D2DF",
    # status — reserved, always shipped with a text label, never colour alone
    "good":    "#4AB962",
    "caution": "#E7AC2A",
    "fault":   "#E6443A",
    # The one alternate series hue, for the rare chart with exactly two
    # measures. Deliberately far from every status hue so it can never be
    # misread as a state.
    "alt": "#7258CA",
}


def _css_vars() -> str:
    import re
    def kebab(k):
        # Uppercase only: `signalHi` → `signal-hi`, but `seq0` stays `seq0`.
        return re.sub(r"([A-Z])", r"-\1", k).lower()
    return ":root{" + "".join(f"--{kebab(k)}:{v};" for k, v in TOKENS.items()) + "}"


def _js_tokens() -> str:
    return "const TK=" + json.dumps(TOKENS, separators=(",", ":")) + ";"


CSS = """
*{box-sizing:border-box}
html{-webkit-text-size-adjust:100%}
body{
  margin:0;background:var(--ground);color:var(--ink);
  font:400 13px/1.55 var(--sans);
  -webkit-font-smoothing:antialiased;
}
:root{
  --mono:ui-monospace,"SF Mono",SFMono-Regular,"JetBrains Mono",Menlo,Consolas,monospace;
  --sans:ui-sans-serif,-apple-system,"SF Pro Text","Segoe UI",Roboto,sans-serif;
  /* The gutter every time-based chart reserves for channel names. Section
     labels sit in the same column, so the page has one structural spine. */
  --gutter:132px;
  --page:1360px;
}
.mono,code{font-family:var(--mono)}

/* Everything that is a number, an identifier or a label is set in the mono
   face; the sans is reserved for prose and is deliberately subordinate. */
h1,h2,h3,th,td,button,select,dt,dd,.label,.ro-v,.badge,.tnow{font-family:var(--mono)}

a{color:var(--signal-hi);text-underline-offset:3px}
:focus-visible{outline:2px solid var(--signal-hi);outline-offset:2px;border-radius:3px}

.wrap{max-width:var(--page);margin:0 auto;padding:0 24px 96px}

/* ── nameplate ─────────────────────────────────────────────────────────── */
.plate{padding:34px 0 22px;display:grid;gap:18px}
.plate-id{display:flex;align-items:flex-end;gap:18px;flex-wrap:wrap}
.logo{background:var(--logo) center/contain no-repeat;flex:none}
.plate .logo{height:46px;width:56px;opacity:.9}
.plate h1{
  font-size:31px;line-height:1;margin:0;font-weight:500;letter-spacing:-.02em;
}
.serial{
  font-size:11px;letter-spacing:.14em;text-transform:uppercase;color:var(--ink3);
  border:1px solid var(--rule);border-radius:3px;padding:4px 8px;
  background:var(--panel);font-family:var(--mono);
}
.plate-meta{color:var(--ink3);font-size:12px;padding-bottom:3px}
.plate-meta b{color:var(--ink2);font-weight:400}

/* the one-line verdict: what this run did, in the page's own voice */
.verdict{
  display:flex;flex-wrap:wrap;gap:10px 22px;align-items:center;
  font-family:var(--mono);font-size:12.5px;color:var(--ink2);
}
.verdict .n{color:var(--ink);font-weight:500}

/* status lamp — a colour is never the only signal, a word always follows */
.lamp{display:inline-flex;align-items:center;gap:7px;white-space:nowrap}
.lamp::before{
  content:"";width:7px;height:7px;border-radius:50%;background:var(--ink3);
  box-shadow:0 0 0 3px rgba(255,255,255,.04);flex:none;
}
.lamp--good::before{background:var(--good);box-shadow:0 0 0 3px rgba(74,185,98,.16)}
.lamp--caution::before{background:var(--caution);box-shadow:0 0 0 3px rgba(231,172,42,.16)}
.lamp--fault::before{background:var(--fault);box-shadow:0 0 0 3px rgba(230,68,58,.18)}

/* ── the run tape: the hero, and the scrubber ──────────────────────────── */
.tape-frame{
  background:var(--panel);border:1px solid var(--rule);border-radius:6px;
  box-shadow:inset 0 1px 0 rgba(255,255,255,.035);
  padding:12px 14px 10px;
}
.tape-head{
  display:flex;align-items:baseline;gap:14px;flex-wrap:wrap;margin-bottom:8px;
}
.tape-head .label{color:var(--ink2)}
.tape-head .snote{font-size:11px;color:var(--ink3);flex:1}
#tape{cursor:crosshair}

/* ── sticky transport rail ─────────────────────────────────────────────── */
.rail{
  position:sticky;top:0;z-index:40;margin:0 -24px 0;padding:9px 24px;
  background:color-mix(in srgb,var(--ground) 88%,transparent);
  backdrop-filter:blur(14px);
  border-bottom:1px solid var(--rule);
  display:flex;align-items:center;gap:14px;
  transform:translateY(-100%);opacity:0;pointer-events:none;
  transition:transform .22s ease,opacity .22s ease;
}
.rail.stuck{transform:none;opacity:1;pointer-events:auto}
.rail #railtape{flex:1;min-width:160px;height:30px}
@media (prefers-reduced-motion:reduce){.rail{transition:none}}

.transport{display:flex;align-items:center;gap:8px}
button{
  background:var(--raise);color:var(--ink);border:1px solid var(--rule);
  border-radius:4px;padding:5px 11px;font-size:12px;cursor:pointer;
  letter-spacing:.02em;transition:border-color .12s,color .12s;
}
button:hover{border-color:var(--signal-dim);color:var(--signal-hi)}
select{
  background:var(--raise);color:var(--ink2);border:1px solid var(--rule);
  border-radius:4px;padding:4px 6px;font-size:11.5px;
}
.tnow{
  font-size:19px;font-weight:500;color:var(--signal-hi);letter-spacing:-.01em;
  font-variant-numeric:tabular-nums;min-width:92px;text-align:right;
}
.tnow small{font-size:11px;color:var(--ink3);margin-left:3px;font-weight:400}
.railstat{
  font-family:var(--mono);font-size:11px;color:var(--ink3);white-space:nowrap;
  letter-spacing:.04em;
}
.railstat b{color:var(--ink);font-weight:500;font-variant-numeric:tabular-nums}

/* ── section rhythm ───────────────────────────────────────────────────── */
section{margin:44px 0 0}
.shead{
  display:grid;grid-template-columns:var(--gutter) 1fr auto;align-items:center;
  gap:16px;margin-bottom:16px;
}
.shead h2{
  font-size:11px;text-transform:uppercase;letter-spacing:.17em;color:var(--ink);
  margin:0;font-weight:500;
}
.shead .srule{height:1px;background:var(--rule)}
.shead .snote{font-size:11px;color:var(--ink3);letter-spacing:.06em}
@media (max-width:720px){
  .shead{grid-template-columns:1fr auto}
  .shead .srule{display:none}
}

h3{
  font-size:10.5px;text-transform:uppercase;letter-spacing:.14em;color:var(--ink2);
  margin:0 0 10px;font-weight:500;
}

.panel{
  background:var(--panel);border:1px solid var(--rule);border-radius:6px;
  padding:16px;box-shadow:inset 0 1px 0 rgba(255,255,255,.03);
}
.panel + .panel{margin-top:12px}
.grid{display:grid;gap:12px;align-items:start}
.cols2{grid-template-columns:repeat(auto-fit,minmax(400px,1fr))}
.cols3{grid-template-columns:repeat(auto-fit,minmax(290px,1fr))}

/* ── readout strip — four primary meters, then the spec plate ─────────── */
.readouts{
  display:grid;grid-template-columns:repeat(auto-fit,minmax(165px,1fr));
  gap:1px;border:1px solid var(--rule);border-radius:6px;
  background:var(--rule);overflow:hidden;
}
.ro{padding:15px 18px;background:var(--panel)}
.label{
  font-size:9.5px;text-transform:uppercase;letter-spacing:.15em;color:var(--ink3);
  font-family:var(--mono);
}
.ro-v{
  font-size:27px;line-height:1.15;font-weight:500;margin-top:7px;
  font-variant-numeric:tabular-nums;letter-spacing:-.02em;
}
.ro-v .unit{font-size:12px;color:var(--ink3);margin-left:4px;font-weight:400;letter-spacing:0}
.ro-f{font-size:11px;color:var(--ink3);margin-top:5px;font-family:var(--mono)}

/* spec plate: the rest of the numbers, dense, aligned, no cards */
dl.kv{
  display:grid;grid-template-columns:minmax(110px,auto) 1fr;gap:0;margin:0;
  font-size:12px;
}
dl.kv dt{
  color:var(--ink3);padding:5px 14px 5px 0;border-top:1px solid var(--rule-soft);
  letter-spacing:.03em;
}
dl.kv dd{
  margin:0;padding:5px 0;border-top:1px solid var(--rule-soft);
  font-variant-numeric:tabular-nums;color:var(--ink);word-break:break-word;
}
dl.kv dt:first-of-type,dl.kv dd:first-of-type{border-top:0}

/* ── tables ───────────────────────────────────────────────────────────── */
table{border-collapse:collapse;width:100%;font-size:11.5px}
th,td{
  text-align:right;padding:6px 10px;border-bottom:1px solid var(--rule-soft);
  font-variant-numeric:tabular-nums;white-space:nowrap;
}
th{
  color:var(--ink3);font-weight:500;position:sticky;top:0;background:var(--panel);
  text-transform:uppercase;font-size:9.5px;letter-spacing:.1em;
  border-bottom:1px solid var(--rule);
}
th:first-child,td:first-child{text-align:left;color:var(--ink)}
td{color:var(--ink2)}
tbody tr:hover td{background:var(--raise);color:var(--ink)}
.scroll{overflow:auto;max-height:430px;border-radius:4px}
.overflow-x{overflow-x:auto}

/* ── notes / warnings ─────────────────────────────────────────────────── */
.notes{
  border:1px solid var(--rule);border-left:2px solid var(--caution);
  border-radius:6px;background:var(--panel);padding:13px 16px;margin-top:18px;
}
.notes .label{color:var(--caution)}
.notes ul{margin:8px 0 0;padding-left:17px;color:var(--ink2);font-size:12px}
.notes li{margin:4px 0}

.badge{
  display:inline-block;padding:2px 7px;border-radius:3px;font-size:10.5px;
  border:1px solid var(--rule);background:var(--raise);color:var(--ink2);
  letter-spacing:.05em;
}
.badge.ok{border-color:color-mix(in srgb,var(--good) 45%,transparent);color:var(--good)}
.badge.warn{border-color:color-mix(in srgb,var(--caution) 45%,transparent);color:var(--caution)}
.badge.bad{border-color:color-mix(in srgb,var(--fault) 50%,transparent);color:var(--fault)}

.legend{
  display:flex;gap:8px 18px;flex-wrap:wrap;font-size:11px;color:var(--ink2);
  margin-top:10px;font-family:var(--mono);
}
.legend span{display:inline-flex;align-items:center;gap:6px}
.legend i{width:11px;height:3px;border-radius:2px;flex:none}
.legend .ramp{display:inline-flex;gap:0;border-radius:2px;overflow:hidden}
.legend .ramp i{width:15px;height:8px;border-radius:0}

/* Charts carry a viewBox and no height, so they scale uniformly to the column
   width — text included. Anything that needs a fixed pixel height instead opts
   in with .stretch, and those have no text to distort. */
svg{display:block;height:auto;max-width:100%;margin-inline:auto}
svg text{font-family:var(--mono)}
svg.stretch{width:100%;height:100%}
/* Charts on the run clock span the column so their shared gutter lines up. */
#tape svg,#load svg,#loadrate svg,#scatter svg,#coldchart svg{
  width:100%;min-width:660px}
.facet svg{width:100%}
.muted{color:var(--ink3)}
.note{
  color:var(--ink3);font-size:11.5px;margin-top:10px;max-width:78ch;
  line-height:1.6;
}
.note b{color:var(--ink2);font-weight:400}

.tip{
  position:fixed;pointer-events:none;z-index:99;opacity:0;
  background:var(--raise);border:1px solid var(--rule);border-radius:5px;
  padding:8px 10px;font:11.5px/1.5 var(--mono);color:var(--ink2);
  box-shadow:0 8px 24px rgba(0,0,0,.5);transition:opacity .1s;max-width:280px;
}
.tip b{color:var(--ink);font-weight:500}

.tabs{display:flex;gap:0;margin-bottom:-1px;flex-wrap:wrap;position:relative;z-index:1}
.tabs button{
  background:transparent;border:1px solid transparent;border-bottom:0;
  border-radius:4px 4px 0 0;color:var(--ink3);padding:7px 13px;font-size:11px;
  letter-spacing:.06em;text-transform:uppercase;
}
.tabs button:hover{color:var(--ink2)}
.tabs button.active{
  background:var(--panel);border-color:var(--rule);color:var(--ink);
}
.tabs + .panel{border-top-left-radius:0}
.hidden{display:none}

/* small multiples grid for faceted per-stage series */
.facets{display:grid;grid-template-columns:repeat(auto-fit,minmax(260px,1fr));gap:10px}
.facet{border:1px solid var(--rule-soft);border-radius:4px;padding:9px 10px 6px;background:var(--ground)}
.facet .fh{display:flex;justify-content:space-between;align-items:baseline;gap:8px;margin-bottom:5px}
.facet .fn{font-family:var(--mono);font-size:11px;color:var(--ink)}
.facet .fv{font-family:var(--mono);font-size:10.5px;color:var(--ink3);font-variant-numeric:tabular-nums}

footer{
  margin-top:56px;padding-top:18px;border-top:1px solid var(--rule);
  color:var(--ink3);font-size:11px;font-family:var(--mono);line-height:1.8;
}
footer .logo{display:inline-block;height:15px;width:19px;vertical-align:-3px;
  margin-right:6px;opacity:.5}
"""

JS = r"""
const M = window.__EPICO__;

// Magnitude → one hue, light→dark. Used wherever colour encodes "how much".
// The top step is held back for the playhead and direct labels, so a series
// sitting at its maximum still reads as data rather than as a highlight.
const SEQ = [TK.seq0, TK.seq1, TK.seq2];
const seqOf = (frac) => SEQ[Math.max(0, Math.min(SEQ.length-1,
  Math.round(frac * (SEQ.length-1))))];

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
const svgText = (attrs, s) => { const t = el('text', attrs); t.appendChild(txt(s)); return t; };

// ── tooltip ──────────────────────────────────────────────────────────────────
const tip = document.createElement('div');
tip.className = 'tip'; document.body.appendChild(tip);
function showTip(e, html){
  tip.innerHTML = html; tip.style.opacity = 1;
  const w = tip.offsetWidth || 260, h = tip.offsetHeight || 60;
  tip.style.left = Math.max(6, Math.min(e.clientX+16, innerWidth-w-8))+'px';
  tip.style.top  = Math.max(6, Math.min(e.clientY+16, innerHeight-h-8))+'px';
}
function hideTip(){ tip.style.opacity = 0; }
function attachTip(node, html){
  node.addEventListener('mousemove', e => showTip(e, html));
  node.addEventListener('mouseleave', hideTip);
}
// For content that changes as the playhead moves: listeners are attached once
// and read the current text at hover time. Re-attaching per frame would pile up
// thousands of listeners during playback.
function attachLiveTip(node, get){
  node.addEventListener('mousemove', e => showTip(e, get()));
  node.addEventListener('mouseleave', hideTip);
}

// ── chart primitives ────────────────────────────────────────────────────────
function chart(w, h, pad, stretch){
  const svg = el('svg', {viewBox:`0 0 ${w} ${h}`, width:w, height:h});
  if (stretch){ svg.setAttribute('preserveAspectRatio','none');
                svg.setAttribute('class','stretch');
                svg.removeAttribute('width'); svg.removeAttribute('height'); }
  return {svg, w, h, pad: Object.assign({l:64,r:18,t:22,b:26}, pad||{})};
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
// The graticule: a recessive measurement grid that belongs to the instrument.
// Horizontal rules only — vertical ones fight the playhead.
function axes(c, s, opts={}){
  const {l,r,t,b} = c.pad, g = el('g');
  const nY = opts.yticks ?? 4, nX = opts.xticks ?? 5;
  const yf = opts.yfmt || tickFmt(s.yd[1]-s.yd[0]);
  const xf = opts.xfmt || tickFmt(s.xd[1]-s.xd[0]);
  if (nY > 0) for (const v of niceTicks(s.yd[0], s.yd[1], nY)){
    const y = s.y(v);
    g.appendChild(el('line',{x1:l,x2:c.w-r,y1:y,y2:y,stroke:TK.rule,'stroke-width':1,
      'shape-rendering':'crispEdges'}));
    g.appendChild(svgText({x:l-8,y:y+3.5,fill:TK.ink3,'font-size':9.5,
      'text-anchor':'end'}, yf(v)));
  }
  // Baseline sits a shade brighter than the grid: it is the zero reference.
  g.appendChild(el('line',{x1:l,x2:c.w-r,y1:c.h-b,y2:c.h-b,stroke:TK.ink3,
    'stroke-width':1,opacity:.45,'shape-rendering':'crispEdges'}));
  if (nX > 0) for (const v of niceTicks(s.xd[0], s.xd[1], nX)){
    const x = s.x(v);
    g.appendChild(el('line',{x1:x,x2:x,y1:c.h-b,y2:c.h-b+4,stroke:TK.ink3,opacity:.5,
      'shape-rendering':'crispEdges'}));
    g.appendChild(svgText({x, y:c.h-b+15, fill:TK.ink3,'font-size':9.5,
      'text-anchor':'middle'}, xf(v)));
  }
  if (opts.ylabel)
    g.appendChild(svgText({x:l, y:t-9, fill:TK.ink3,'font-size':9.5,
      'letter-spacing':'.1em'}, opts.ylabel.toUpperCase()));
  c.svg.appendChild(g);
}
function line(c, s, pts, color, step, width){
  if (!pts.length) return null;
  let d = '';
  pts.forEach((p,i) => {
    const X = s.x(p[0]), Y = s.y(p[1]);
    if (i===0) d += `M${X},${Y}`;
    else if (step) d += `L${X},${s.y(pts[i-1][1])}L${X},${Y}`;
    else d += `L${X},${Y}`;
  });
  const path = el('path',{d, fill:'none', stroke:color, 'stroke-width':width||2,
    'stroke-linejoin':'round','stroke-linecap':'round'});
  c.svg.appendChild(path);
  return path;
}
// Bars: 2px surface gap between neighbours, 2px rounded data-end anchored to
// the baseline (so the rounding never floats free of zero).
function bars(c, s, pts, color, opacity){
  const slot = (c.w-c.pad.l-c.pad.r)/Math.max(pts.length,1);
  const bw = Math.max(1, slot - 2);
  const y0 = s.y(s.yd[0]), r = Math.min(2, bw/2);
  pts.forEach(p => {
    const y = s.y(p[1]), h = Math.abs(y0-y);
    if (h < .3) return;
    const x = s.x(p[0]) - bw/2, top = Math.min(y,y0), rr = Math.min(r, h);
    c.svg.appendChild(el('path',{
      d:`M${x},${top+h} L${x},${top+rr} Q${x},${top} ${x+rr},${top} `+
        `L${x+bw-rr},${top} Q${x+bw},${top} ${x+bw},${top+rr} L${x+bw},${top+h} Z`,
      fill:color, opacity:opacity??1}));
  });
}
function emptyNote(host, msg){
  const d = document.createElement('div');
  d.className = 'note'; d.style.padding = '20px 2px'; d.textContent = msg;
  host.appendChild(d);
}
function legend(host, pairs){
  if (pairs.length < 2) return;             // one series is named by its title
  const d = document.createElement('div'); d.className='legend';
  pairs.forEach(([label,color]) => {
    const s = document.createElement('span');
    const i = document.createElement('i'); i.style.background = color;
    s.appendChild(i); s.appendChild(document.createTextNode(label));
    d.appendChild(s);
  });
  host.appendChild(d);
}
function noteUnder(host, html){
  const n = document.createElement('div'); n.className='note'; n.innerHTML = html;
  host.appendChild(n); return n;
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

const STAGES = M.topology.nodes.map(n => n.name);
const MAXREP = Math.max(1, ...STAGES.map(n =>
  Math.max(0, ...(M.replicas[n]||[]).map(p=>p[1]))));

// ── shared time geometry ────────────────────────────────────────────────────
// Every chart on the run clock uses this plot area, so a given instant sits at
// the same x in all of them and a burst lines up by eye with the scale-up it
// triggered. The left pad is the page's channel-name gutter.
const TW = 1200, TPAD = {l:132, r:26};
let scrubbers = [];
function makeScrubbable(c, s, opts={}){
  const ph = el('line',{x1:-20,x2:-20,y1:Math.max(0,c.pad.t-10),y2:c.h-c.pad.b,
    stroke:TK.signalHi,'stroke-width':1.5,'pointer-events':'none'});
  c.svg.appendChild(ph);
  scrubbers.push({c, s, ph});
  c.svg.style.cursor = 'crosshair';
  const toT = ev => {
    const box = c.svg.getBoundingClientRect();
    const vx = (ev.clientX-box.left)/box.width*c.w;
    return Math.max(0, Math.min(T_MAX,
      s.xd[0] + (vx-c.pad.l)/(c.w-c.pad.l-c.pad.r)*(s.xd[1]-s.xd[0])));
  };
  c.svg.addEventListener('click', ev => setT(toT(ev)));
  if (opts.hover !== false){
    // Crosshair readout: hovering any time chart reports the run clock, so the
    // axis can be read without committing to a scrub.
    c.svg.addEventListener('mousemove', ev => {
      const t = toT(ev);
      showTip(ev, opts.readout ? opts.readout(t) : `t <b>${fmt(t,3)} s</b>`);
    });
    c.svg.addEventListener('mouseleave', hideTip);
  }
}
function drawPlayhead(t){
  scrubbers.forEach(sc => {
    const x = sc.s.x(t);
    sc.ph.setAttribute('x1', x); sc.ph.setAttribute('x2', x);
  });
}

// ── the run tape ────────────────────────────────────────────────────────────
// The whole run in one strip: replica occupancy per stage as sequential
// intensity, offered load as a hairline over it, markers pinned. It is the hero
// and it is the scrubber; the sticky rail carries a thin copy of the same thing.
function drawTape(hostId, h, detail){
  const host = document.getElementById(hostId);
  if (!host) return;
  host.innerHTML = '';
  const rows = STAGES.length;
  const pad = detail ? {l:TPAD.l, r:TPAD.r, t:10, b:22} : {l:8, r:8, t:4, b:4};
  const c = chart(detail ? TW : 900, h, pad, !detail);
  const s = scales(c, [0, T_MAX], [0, 1]);
  const plotH = c.h - pad.t - pad.b;
  const rowH = plotH / Math.max(rows, 1);

  STAGES.forEach((n,i) => {
    const y = pad.t + i*rowH;
    c.svg.appendChild(el('rect',{x:pad.l, y:y+0.5, width:c.w-pad.l-pad.r,
      height:Math.max(1,rowH-2), fill:TK.ground, rx:2}));
    const series = M.replicas[n]||[];
    for (let k=0;k<series.length;k++){
      const t0 = series[k][0], t1 = (k+1<series.length ? series[k+1][0] : T_MAX);
      const v = series[k][1];
      if (v<=0 || t1<=t0) continue;
      const rect = el('rect',{x:s.x(t0), y:y+0.5, width:Math.max(1,s.x(t1)-s.x(t0)),
        height:Math.max(1,rowH-2), fill:seqOf(MAXREP>1 ? (v-1)/(MAXREP-1) : 1), rx:2});
      c.svg.appendChild(rect);
      if (detail)
        attachTip(rect, `<b>${n}</b><br>${v} replica${v===1?'':'s'}<br>`+
          `${fmt(t0,2)} s → ${fmt(t1,2)} s`);
    }
    if (detail && rowH >= 11)
      c.svg.appendChild(svgText({x:pad.l-10,y:y+rowH/2+3.5,fill:TK.ink2,'font-size':10.5,
        'text-anchor':'end'}, n));
  });

  // Offered load as a hairline over the occupancy: the cause drawn on top of
  // the effect, on the one axis they share.
  const L = M.load||{cumulative:[]};
  if (L.cumulative && L.cumulative.length > 1){
    const maxC = Math.max(...L.cumulative.map(p=>p[1])) || 1;
    const sl = scales(c, [0,T_MAX], [0, maxC*1.05]);
    line(c, sl, L.cumulative, TK.ink, true, detail?1.6:1.2);
  }
  (M.markers||[]).forEach(m => {
    const col = m.kind==='slo' ? TK.fault : m.kind==='eos' ? TK.caution : TK.ink3;
    const x = s.x(m.t);
    c.svg.appendChild(el('line',{x1:x,x2:x,y1:pad.t,y2:c.h-pad.b,stroke:col,
      'stroke-width':1,'stroke-dasharray':'2 3',opacity:.85}));
    if (detail){
      const hit = el('rect',{x:x-5,y:pad.t,width:10,height:plotH,fill:'transparent'});
      attachTip(hit, `${m.label}<br>t <b>${fmt(m.t,3)} s</b>`);
      c.svg.appendChild(hit);
    }
  });
  for (const v of niceTicks(0, T_MAX, detail ? 8 : 5)){
    const x = s.x(v);
    c.svg.appendChild(el('line',{x1:x,x2:x,y1:pad.t,y2:c.h-pad.b,stroke:TK.ground,
      'stroke-width':1,opacity:.65,'shape-rendering':'crispEdges',
      'pointer-events':'none'}));
  }
  if (detail) axes(c, s, {yticks:0, xfmt:v=>fmt(v,1)+'s'});

  makeScrubbable(c, s, {readout: t => {
    const tot = STAGES.reduce((a,n)=>a+valueAt(M.replicas[n]||[],t),0);
    return `t <b>${fmt(t,3)} s</b><br>live replicas <b>${tot}</b><br>`+
           `<span style="color:${TK.ink3}">click to scrub</span>`;
  }});
  host.appendChild(c.svg);

  if (!detail) return;
  // Sequential scale, so the key is a ramp rather than a list of names.
  const key = document.createElement('div');
  key.className = 'legend';
  const ramp = `<span class="ramp">` +
    SEQ.map(col => `<i style="background:${col}"></i>`).join('') + `</span>`;
  let html = `<span>replicas 1 ${ramp} ${MAXREP}</span>`;
  if (L.cumulative && L.cumulative.length > 1)
    html += `<span><i style="background:${TK.ink}"></i>offered, cumulative</span>`;
  const kinds = new Set((M.markers||[]).map(m=>m.kind));
  if (kinds.has('eos')) html += `<span><i style="background:${TK.caution}"></i>EOS</span>`;
  if (kinds.has('slo')) html += `<span><i style="background:${TK.fault}"></i>SLO breach</span>`;
  if (kinds.has('burst')) html += `<span><i style="background:${TK.ink3}"></i>loadgen burst</span>`;
  key.innerHTML = html;
  host.appendChild(key);
}

// ── DAG ─────────────────────────────────────────────────────────────────────
// Nodes are channel modules: the name, a segment meter for live replicas, and a
// queue bar. Discrete replicas get discrete segments rather than a fill ramp —
// you can count them.
const NODE_W = 138, NODE_H = 62, GAP_X = 78, GAP_Y = 22, SEG_MAX = 14;
let dagNodes = [], dagEls = {};

function layoutDag(){
  const byLayer = {};
  M.topology.nodes.forEach(n => (byLayer[n.layer] = byLayer[n.layer]||[]).push(n));
  const layers = Object.keys(byLayer).map(Number).sort((a,b)=>a-b);
  const maxRows = Math.max(...layers.map(l => byLayer[l].length));
  const H = maxRows*(NODE_H+GAP_Y) + 52;
  const W = layers.length*(NODE_W+GAP_X) + 40;
  dagNodes = [];
  layers.forEach((l, li) => {
    const col = byLayer[l];
    col.forEach((n, ri) => {
      const colH = col.length*(NODE_H+GAP_Y);
      dagNodes.push(Object.assign({}, n, {
        x: 20 + li*(NODE_W+GAP_X),
        y: 32 + (H-32-colH)/2 + ri*(NODE_H+GAP_Y),
      }));
    });
  });
  return {W, H, layers};
}

function drawDag(){
  const host = document.getElementById('dag');
  host.innerHTML = '';
  const {W, H, layers} = layoutDag();
  const svg = el('svg', {viewBox:`0 0 ${W} ${H}`, width:'100%'});
  const pos = {}; dagNodes.forEach(n => pos[n.name] = n);

  const m = el('marker',{id:'arrow',viewBox:'0 0 10 10',refX:9,refY:5,
    markerWidth:5,markerHeight:5,orient:'auto-start-reverse'});
  m.appendChild(el('path',{d:'M0,0 L10,5 L0,10 z', fill:TK.ink3}));
  svg.appendChild(el('defs', {}, [m]));

  // Layer index is real information — the DAG's depth — so it is labelled.
  layers.forEach((l, li) => {
    svg.appendChild(svgText({x:20+li*(NODE_W+GAP_X), y:16, fill:TK.ink3,
      'font-size':9,'letter-spacing':'.14em'},
      'LAYER '+l));
  });

  // edges first so nodes sit on top
  M.topology.edges.forEach(e => {
    const a = pos[e.from], b = pos[e.to];
    if (!a || !b) return;
    const x1 = a.x+NODE_W, y1 = a.y+NODE_H/2, x2 = b.x, y2 = b.y+NODE_H/2;
    const mx = (x1+x2)/2;
    const d = `M${x1},${y1} C${mx},${y1} ${mx},${y2} ${x2},${y2}`;
    const p = el('path',{d, fill:'none', stroke:TK.rule,'stroke-width':1.4,
      'marker-end':'url(#arrow)'});
    // A fat invisible sibling gives the hairline a real hit target.
    const hit = el('path',{d, fill:'none', stroke:'transparent','stroke-width':12});
    attachTip(hit, `<b>${e.from} → ${e.to}</b><br>transport ${e.transport||'—'}` +
      (e.cap?`<br>ring cap ${e.cap}`:'') +
      (e.producers?`<br>producer columns ${e.producers} (base ${e.base})`:''));
    svg.appendChild(p); svg.appendChild(hit);
  });

  dagNodes.forEach(n => {
    const g = el('g', {transform:`translate(${n.x},${n.y})`});
    const rect = el('rect',{width:NODE_W,height:NODE_H,rx:4,fill:TK.panel,
      stroke:TK.rule,'stroke-width':1});
    g.appendChild(rect);
    g.appendChild(svgText({x:11,y:19,fill:TK.ink,'font-size':11.5}, n.name));
    const count = svgText({x:NODE_W-11,y:19,fill:TK.ink3,'font-size':10.5,
      'text-anchor':'end'}, '0');
    g.appendChild(count);

    // replica segment meter
    const segs = [];
    const cfg = (M.flags.autoscaler||{})[n.name] || {};
    const cap = Math.min(SEG_MAX, Math.max(1, cfg.max ||
      Math.max(1, ...(M.replicas[n.name]||[[0,1]]).map(p=>p[1]))));
    const sw = Math.min(9, (NODE_W-22-(cap-1)*2)/cap);
    for (let i=0;i<cap;i++){
      const r = el('rect',{x:11+i*(sw+2), y:29, width:sw, height:9, rx:1,
        fill:TK.raise});
      segs.push(r); g.appendChild(r);
    }
    // queue bar
    const qtrack = el('rect',{x:11,y:46,width:NODE_W-22,height:3,rx:1.5,fill:TK.raise});
    const qfill  = el('rect',{x:11,y:46,width:0,height:3,rx:1.5,fill:TK.caution});
    g.appendChild(qtrack); g.appendChild(qfill);

    const hit = el('rect',{width:NODE_W,height:NODE_H,fill:'transparent'});
    g.appendChild(hit);
    svg.appendChild(g);
    dagEls[n.name] = {rect, count, segs, qfill, cap, node:n, tipHtml:''};
    attachLiveTip(hit, () => dagEls[n.name].tipHtml);
  });

  host.appendChild(svg);
}

function updateDag(t){
  const cfg = M.flags.autoscaler || {};
  let peakQ = 1;
  for (const n of STAGES) if (M.queue[n])
    peakQ = Math.max(peakQ, ...M.queue[n].map(p=>p[1]));
  for (const [name, o] of Object.entries(dagEls)){
    const reps = valueAt(M.replicas[name]||[], t);
    const qd = M.queue[name] ? valueAt(M.queue[name], t) : null;
    const max = (cfg[name]?.max) ||
      Math.max(1, ...(M.replicas[name]||[[0,1]]).map(p=>p[1]));
    o.segs.forEach((r,i) => r.setAttribute('fill',
      i < reps ? seqOf(0.35 + 0.65*((i+1)/Math.max(o.cap,1))) : TK.raise));
    o.rect.setAttribute('stroke', reps === 0 ? TK.rule : TK.signalDim);
    o.count.textContent = `${reps}/${max}`;
    o.count.setAttribute('fill', reps === 0 ? TK.ink3 : TK.ink);
    o.qfill.setAttribute('width', qd ? (NODE_W-22)*Math.min(1, qd/peakQ) : 0);
    o.tipHtml = `<b>${name}</b><br>replicas ${reps} / ${max}` +
      (qd===null?'':`<br>queue depth ${qd}`) +
      `<br>in-degree ${o.node.in_degree} · out-degree ${o.node.out_degree}` +
      `<br>paths in ${o.node.paths_in} · out ${o.node.paths_out}`;
  }
  const tot = STAGES.reduce((a,n)=>a+valueAt(M.replicas[n]||[],t),0);
  const qtot = STAGES.reduce((a,n)=>a+(M.queue[n]?valueAt(M.queue[n],t):0),0);
  document.querySelectorAll('.tnow').forEach(e =>
    e.innerHTML = fmt(t,2)+'<small>s</small>');
  document.querySelectorAll('[data-live=reps]').forEach(e => e.textContent = tot);
  document.querySelectorAll('[data-live=queue]').forEach(e => e.textContent = qtot);
  drawPlayhead(t);
}

// ── offered load ────────────────────────────────────────────────────────────
// Two aligned panels rather than one chart with two y-scales: events and
// events/second share no units and a second axis would only invite the reader
// to compare two arbitrary scalings.
function drawLoad(){
  const host = document.getElementById('load');
  const rateHost = document.getElementById('loadrate');
  host.innerHTML = ''; rateHost.innerHTML = '';
  const L = M.load || {cumulative:[], rate:[]};
  if (!L.cumulative.length){
    // Nothing to plot: one line of explanation, not two empty frames.
    document.getElementById('loadrate-panel').classList.add('hidden');
    const p = document.getElementById('load-panel');
    p.querySelector('h3').textContent = 'Offered load';
    noteUnder(host, 'No loadgen log next to this run, so what was offered — and '+
      'therefore the conservation check — is unknown. Runs launched through the '+
      'loadgen write <span class="mono">loadgen.jsonl</span> beside the summary.');
    return;
  }

  const maxCum = Math.max(...L.cumulative.map(p=>p[1]));
  const c = chart(TW, 190, TPAD);
  const sc = scales(c, [0, T_MAX], [0, maxCum*1.08]);
  axes(c, sc, {xfmt:v=>fmt(v,1)+'s', yfmt:v=>fmtInt(v), ylabel:'events offered'});

  // area under the curve, then the curve: the fill reads as accumulated volume
  let d = `M${sc.x(L.cumulative[0][0])},${sc.y(0)}`;
  L.cumulative.forEach((p,i) => {
    const X = sc.x(p[0]);
    if (i>0) d += `L${X},${sc.y(L.cumulative[i-1][1])}`;
    d += `L${X},${sc.y(p[1])}`;
  });
  d += `L${sc.x(L.cumulative[L.cumulative.length-1][0])},${sc.y(0)}Z`;
  c.svg.appendChild(el('path',{d, fill:TK.signal, opacity:.12}));
  line(c, sc, L.cumulative, TK.signal, true, 2);

  (L.bursts||[]).forEach((b,i) => {
    const dot = el('circle',{cx:sc.x(b[0]),cy:sc.y(b[1]),r:4,fill:TK.signalHi,
      stroke:TK.panel,'stroke-width':2});
    attachTip(dot, `<b>burst ${i+1}</b><br>${fmtInt(b[1])} events offered by `+
      `t ${fmt(b[0],3)} s`);
    c.svg.appendChild(dot);
  });
  // direct label on the final value beats a legend for a single series
  const last = L.cumulative[L.cumulative.length-1];
  c.svg.appendChild(svgText({x:sc.x(last[0])+7, y:sc.y(last[1])+3.5, fill:TK.ink,
    'font-size':10.5}, fmtInt(last[1])));

  makeScrubbable(c, sc, {readout: t => {
    const v = valueAt(L.cumulative, t);
    return `t <b>${fmt(t,3)} s</b><br>offered <b>${fmtInt(v)}</b> events`;
  }});
  host.appendChild(c.svg);

  const prof = (M.flags.source && M.flags.source.profile) || 'unknown profile';
  noteUnder(host, `<b>${fmtInt(L.total)}</b> events offered across ${L.samples} `+
    `logged sample(s) — ${prof}. The curve is exact at each sample; its slope is `+
    `the offered rate.`);

  // Interval-average rate, its own panel, its own axis. Skipped when the samples
  // are unevenly spaced (see model._load) — the numbers would say more about
  // where the sample boundaries fell than about the workload.
  if (L.rate.length && L.rate_uniform){
    const maxRate = Math.max(1, ...L.rate.map(p=>p[1]));
    const cr = chart(TW, 150, TPAD);
    const sr = scales(cr, [0, T_MAX], [0, maxRate*1.15]);
    axes(cr, sr, {xfmt:v=>fmt(v,1)+'s', yfmt:v=>fmtInt(v), ylabel:'offered ev/s'});
    let rd = `M${sr.x(L.rate[0][0])},${sr.y(0)}`;
    L.rate.forEach((p,i) => {
      const X = sr.x(p[0]);
      if (i>0) rd += `L${X},${sr.y(L.rate[i-1][1])}`;
      rd += `L${X},${sr.y(p[1])}`;
    });
    rd += `L${sr.x(L.rate[L.rate.length-1][0])},${sr.y(0)}Z`;
    cr.svg.appendChild(el('path',{d:rd, fill:TK.caution, opacity:.14}));
    line(cr, sr, L.rate, TK.caution, true, 2);
    makeScrubbable(cr, sr, {readout: t =>
      `t <b>${fmt(t,3)} s</b><br>offered rate <b>${fmtInt(valueAt(L.rate,t))}</b> ev/s`});
    rateHost.appendChild(cr.svg);
    noteUnder(rateHost, 'Interval average between two logged counter samples, '+
      'held as a step across the interval it was measured over.');
  } else {
    rateHost.innerHTML = '';
    noteUnder(rateHost, 'No rate panel for this run: the loadgen logs its counter '+
      'once per burst here, so an average between two samples would mostly reflect '+
      'how much idle time the interval happened to include, not how fast the burst ran.');
  }
}

// ── queue depth ─────────────────────────────────────────────────────────────
// Up to four stages overlay in one plot with the categorical set; past that the
// page facets into small multiples rather than inventing a fifth hue.
function drawQueue(){
  const host = document.getElementById('queuechart'); host.innerHTML='';
  const names = STAGES.filter(n => (M.queue[n]||[]).some(p=>p[1]>0));
  if (!names.length) return emptyNote(host, 'No queue-depth samples in this run.');

  // One panel per stage on a shared scale. Overlaying them would need a hue per
  // stage, and every hue far enough apart on this surface is already spoken for
  // by the status lamps — so the stages separate by position, as everywhere else.
  const grid = document.createElement('div'); grid.className = 'facets';
  const maxY = Math.max(...names.map(n => Math.max(...M.queue[n].map(p=>p[1]))));
  names.forEach(n => {
    const f = document.createElement('div'); f.className = 'facet';
    const peak = Math.max(...M.queue[n].map(p=>p[1]));
    f.innerHTML = `<div class="fh"><span class="fn">${n}</span>`+
      `<span class="fv">peak ${fmtInt(peak)}</span></div>`;
    const c = chart(300, 82, {l:4,r:4,t:6,b:12});
    const s = scales(c, [0,T_MAX], [0, maxY*1.08]);
    let d = `M${s.x(0)},${s.y(0)}`;
    M.queue[n].forEach((p,i) => {
      const X = s.x(p[0]);
      if (i>0) d += `L${X},${s.y(M.queue[n][i-1][1])}`;
      d += `L${X},${s.y(p[1])}`;
    });
    d += `L${s.x(T_MAX)},${s.y(0)}Z`;
    c.svg.appendChild(el('path',{d, fill:TK.signal, opacity:.18}));
    line(c, s, M.queue[n], TK.signal, true, 1.4);
    c.svg.appendChild(el('line',{x1:c.pad.l,x2:c.w-c.pad.r,y1:s.y(0),y2:s.y(0),
      stroke:TK.ink3,'stroke-width':1,opacity:.35}));
    makeScrubbable(c, s, {readout: t =>
      `<b>${n}</b><br>t ${fmt(t,3)} s<br>queued <b>${fmtInt(valueAt(M.queue[n],t))}</b>`});
    f.appendChild(c.svg);
    grid.appendChild(f);
  });
  host.appendChild(grid);
  noteUnder(host, `${names.length} stage(s) queued, each on a shared `+
    `0–${fmtInt(maxY)} scale so peaks compare directly across panels. `+
    `All share the run clock and the playhead.`);
}

// ── latency ─────────────────────────────────────────────────────────────────
function drawCdf(){
  const host = document.getElementById('cdf'); host.innerHTML='';
  const {x,y} = M.latency.cdf;
  if (!x.length) return emptyNote(host, 'No CDF data.');
  const pts = x.map((v,i)=>[v, y[i]*100]);
  const c = chart(600, 230, {l:60,r:52,t:24,b:30});
  const s = scales(c, [x[0], x[x.length-1]], [0,100]);
  axes(c, s, {xfmt:tickFmt(s.xd[1]-s.xd[0],' ms'), yfmt:v=>fmt(v,0)+'%',
              ylabel:'percentile'});
  line(c, s, pts, TK.signal, false, 2);
  // p50/p99 marked and directly labelled — no legend needed for one curve
  [['p50',M.latency.e2e.p50],['p99',M.latency.e2e.p99]].forEach(([lab,v])=>{
    if (!v) return;
    const X = s.x(Math.min(Math.max(v,s.xd[0]),s.xd[1]));
    c.svg.appendChild(el('line',{x1:X,x2:X,y1:c.pad.t,y2:c.h-c.pad.b,stroke:TK.ink3,
      'stroke-width':1,'stroke-dasharray':'3 3'}));
    c.svg.appendChild(svgText({x:X+5,y:c.pad.t+10,fill:TK.ink2,'font-size':10}, `${lab} ${fmt(v,2)}`));
  });
  const rows = pts.length;
  c.svg.addEventListener('mousemove', ev => {
    const box = c.svg.getBoundingClientRect();
    const vx = (ev.clientX-box.left)/box.width*c.w;
    const t = s.xd[0] + (vx-c.pad.l)/(c.w-c.pad.l-c.pad.r)*(s.xd[1]-s.xd[0]);
    let i = 0; while (i<rows-1 && pts[i][0] < t) i++;
    showTip(ev, `<b>${fmt(pts[i][0],3)} ms</b><br>${fmt(pts[i][1],1)}% of events at or below`);
  });
  c.svg.addEventListener('mouseleave', hideTip);
  c.svg.style.cursor = 'crosshair';
  host.appendChild(c.svg);
}

function drawHist(){
  const host = document.getElementById('hist'); host.innerHTML='';
  const {labels,counts} = M.latency.hist;
  if (!labels.length) return emptyNote(host, 'No histogram data.');
  const pts = labels.map((v,i)=>[v, counts[i]]);
  const c = chart(600, 230, {l:60,r:52,t:24,b:30});
  const s = scales(c, [labels[0], labels[labels.length-1]],
    [0, Math.max(...counts)*1.05]);
  axes(c, s, {xfmt:tickFmt(s.xd[1]-s.xd[0],' ms'), yfmt:v=>fmtInt(v), ylabel:'events'});
  bars(c, s, pts, TK.signal, .85);
  // per-bin hover, hit target wider than the mark
  const slot = (c.w-c.pad.l-c.pad.r)/pts.length;
  pts.forEach(p => {
    const hit = el('rect',{x:s.x(p[0])-slot/2,y:c.pad.t,width:Math.max(slot,8),
      height:c.h-c.pad.t-c.pad.b,fill:'transparent'});
    attachTip(hit, `<b>${fmt(p[0],3)} ms</b><br>${fmtInt(p[1])} events`);
    c.svg.appendChild(hit);
  });
  host.appendChild(c.svg);
}

function drawScatter(){
  const host = document.getElementById('scatter'); host.innerHTML='';
  const pts = M.scatter.points;
  if (!pts.length) return emptyNote(host, 'No per-event rows in this summary.');
  const maxY = Math.max(...pts.map(p=>p[1]));
  const c = chart(TW, 230, TPAD);
  const s = scales(c, [0, T_MAX], [0, maxY*1.05]);
  axes(c, s, {xfmt:v=>fmt(v,1)+'s', yfmt:tickFmt(s.yd[1]-s.yd[0],' ms'),
              ylabel:'e2e latency'});
  const g = el('g');
  pts.forEach(p => g.appendChild(el('circle',{cx:s.x(p[0]),cy:s.y(p[1]),r:1.2,
    fill:TK.signal,opacity:.4})));
  c.svg.appendChild(g);
  [['p50',M.latency.e2e.p50],['p99',M.latency.e2e.p99]].forEach(([lab,v]) => {
    if (!v || v > maxY) return;
    const Y = s.y(v);
    c.svg.appendChild(el('line',{x1:c.pad.l,x2:c.w-c.pad.r,y1:Y,y2:Y,stroke:TK.ink2,
      'stroke-width':1,'stroke-dasharray':'4 4',opacity:.7}));
    c.svg.appendChild(svgText({x:c.w-c.pad.r+5,y:Y+3.5,fill:TK.ink2,'font-size':9.5}, lab));
  });
  (M.markers||[]).filter(m=>m.kind!=='slo').forEach(m => {
    const x = s.x(m.t);
    c.svg.appendChild(el('line',{x1:x,x2:x,y1:c.pad.t,y2:c.h-c.pad.b,
      stroke:TK.caution,'stroke-width':1,'stroke-dasharray':'2 3',opacity:.6}));
  });
  makeScrubbable(c, s, {readout: t => `t <b>${fmt(t,3)} s</b>`});
  host.appendChild(c.svg);
  noteUnder(host, `${fmtInt(M.scatter.shown)} of ${fmtInt(M.scatter.total)} `+
    `sampled events plotted. Dashed rules are the run's e2e p50 and p99.`);
}

function drawStageLatency(){
  const host = document.getElementById('stagelat'); host.innerHTML='';
  const rows = Object.entries(M.latency.per_stage);
  if (!rows.length) return emptyNote(host, 'No per-stage latency.');
  const maxY = Math.max(...rows.map(([,v])=>v.p99));
  const W = Math.min(TW, TPAD.l + TPAD.r + rows.length*128);
  const c = chart(W, 250, {l:TPAD.l, r:TPAD.r, t:26, b:64});
  const s = scales(c, [0, rows.length], [0, maxY*1.12]);
  axes(c, s, {xticks:0, yfmt:tickFmt(s.yd[1]-s.yd[0],' ms'), ylabel:'stage residency'});
  const slot = (c.w-c.pad.l-c.pad.r)/rows.length;
  rows.forEach(([name,v],i) => {
    const x = s.x(i+0.5), bw = Math.min(46, slot*0.5);
    // A p50→p99 range bar with a p50 tick: two translucent bars stacked on the
    // same baseline made the p99 read as a separate, larger series.
    const y99 = s.y(v.p99), y50 = s.y(v.p50);
    c.svg.appendChild(el('rect',{x:x-bw/2, y:y99, width:bw, height:Math.max(1,y50-y99),
      rx:2, fill:TK.signal, opacity:.3}));
    c.svg.appendChild(el('rect',{x:x-bw/2, y:y50-1.5, width:bw, height:3, rx:1.5,
      fill:TK.signalHi}));
    c.svg.appendChild(el('line',{x1:x, x2:x, y1:y50, y2:s.y(0), stroke:TK.signal,
      'stroke-width':1, opacity:.45}));
    c.svg.appendChild(svgText({x, y:c.h-c.pad.b+16, fill:TK.ink2,'font-size':9.5,
      'text-anchor':'end',
      transform:`rotate(-38 ${x} ${c.h-c.pad.b+16})`}, name));
    const hit = el('rect',{x:x-slot/2,y:c.pad.t,width:Math.max(slot,10),
      height:c.h-c.pad.t-c.pad.b,fill:'transparent'});
    attachTip(hit, `<b>${name}</b><br>p50 ${fmt(v.p50,3)} ms<br>p99 ${fmt(v.p99,3)} ms<br>`+
      `max ${fmt(v.max,3)} ms<br>n ${fmtInt(v.count)}`);
    c.svg.appendChild(hit);
  });
  host.appendChild(c.svg);
  legend(host, [['p50', TK.signalHi], ['p50 → p99 range', TK.signalDim]]);
}

// ── cold start ──────────────────────────────────────────────────────────────
function drawColdStart(){
  const host = document.getElementById('coldchart'); host.innerHTML='';
  const boots = M.coldstart.boots.filter(b => b.t !== null);
  if (!boots.length)
    return emptyNote(host, 'No worker boots recorded (needs master.jsonl).');
  const maxY = Math.max(...boots.map(b=>b.cold_start_ms));
  const c = chart(TW, 210, TPAD);
  const s = scales(c, [0, T_MAX], [0, maxY*1.12]);
  axes(c, s, {xfmt:v=>fmt(v,1)+'s', yfmt:tickFmt(s.yd[1]-s.yd[0],' ms'),
              ylabel:'cold start'});
  // One hue: every dot is the same kind of event and the stage is on the
  // tooltip and in the table below, so a per-stage colour would be decoration.
  boots.forEach(b => {
    const cx = s.x(b.t), cy = s.y(b.cold_start_ms);
    const dot = el('circle',{cx,cy,r:4,fill:TK.signal,stroke:TK.panel,'stroke-width':2});
    attachTip(dot, `<b>${b.stage}#${b.replica}</b> <span style="color:${TK.ink3}">`+
      `${b.rid||''}</span><br>cold start <b>${fmt(b.cold_start_ms,3)} ms</b><br>`+
      `t ${fmt(b.t,2)} s<br>spawn ${fmt(b.spawn_ms,3)} · instantiate `+
      `${fmt(b.instantiate_ms,3)} · export ${fmt(b.export_ms,3)} · sockets `+
      `${fmt(b.sockets_ms,3)} ms`);
    c.svg.appendChild(dot);
  });
  if (M.coldstart.p50){
    const Y = s.y(M.coldstart.p50);
    c.svg.appendChild(el('line',{x1:c.pad.l,x2:c.w-c.pad.r,y1:Y,y2:Y,stroke:TK.ink2,
      'stroke-width':1,'stroke-dasharray':'4 4',opacity:.7}));
    c.svg.appendChild(svgText({x:c.w-c.pad.r+5,y:Y+3.5,fill:TK.ink2,'font-size':9.5}, 'p50'));
  }
  makeScrubbable(c, s, {readout: t => `t <b>${fmt(t,3)} s</b>`});
  host.appendChild(c.svg);
  noteUnder(host, `${boots.length} worker boot(s). Every phase breakdown is in `+
    `the table below.`);
}

// ── throughput & resources ──────────────────────────────────────────────────
function drawThroughput(){
  const host = document.getElementById('tput'); host.innerHTML='';
  const rps = M.throughput.recv_per_second;
  if (!rps.length) return emptyNote(host, 'No per-second throughput series.');
  const pts = rps.map((v,i)=>[i, v]);
  const c = chart(600, 210, {l:64,r:20,t:24,b:30});
  const s = scales(c, [-0.5, rps.length-0.5], [0, Math.max(...rps)*1.05]);
  axes(c, s, {xfmt:v=>fmt(v,0)+'s', yfmt:v=>fmtInt(v), ylabel:'events/s at collector'});
  bars(c, s, pts, TK.signal, .85);
  const slot = (c.w-c.pad.l-c.pad.r)/pts.length;
  pts.forEach(p => {
    const hit = el('rect',{x:s.x(p[0])-slot/2,y:c.pad.t,width:Math.max(slot,8),
      height:c.h-c.pad.t-c.pad.b,fill:'transparent'});
    attachTip(hit, `second <b>${p[0]}</b><br>${fmtInt(p[1])} events`);
    c.svg.appendChild(hit);
  });
  host.appendChild(c.svg);
}

// CPU and RSS get one panel each. They share no units, and a second y-axis
// would let an arbitrary scaling imply a relationship neither series claims.
function drawResources(){
  const cpuHost = document.getElementById('res-cpu');
  const rssHost = document.getElementById('res-rss');
  cpuHost.innerHTML = ''; rssHost.innerHTML = '';
  const {cpu, rss} = M.resources;
  if (!cpu.length && !rss.length){
    emptyNote(cpuHost, 'Resource sampling was off for this run '+
      '(resource_sample_interval_ms: 0).');
    rssHost.closest('.panel').classList.add('hidden');
    return;
  }
  const mk = (host, series, color, label, unit, digits) => {
    if (!series.length) return emptyNote(host, 'No samples.');
    const c = chart(600, 190, {l:64,r:20,t:24,b:30});
    const maxV = Math.max(1, ...series.map(p=>p[1]));
    const s = scales(c, [0, T_MAX], [0, maxV*1.12]);
    axes(c, s, {xfmt:v=>fmt(v,1)+'s', yfmt:v=>fmt(v,digits), ylabel:label});
    let d = `M${s.x(series[0][0])},${s.y(0)}`;
    series.forEach(p => { d += `L${s.x(p[0])},${s.y(p[1])}`; });
    d += `L${s.x(series[series.length-1][0])},${s.y(0)}Z`;
    c.svg.appendChild(el('path',{d, fill:color, opacity:.12}));
    line(c, s, series, color, false, 2);
    series.forEach(p => {
      const dot = el('circle',{cx:s.x(p[0]),cy:s.y(p[1]),r:3,fill:color,
        stroke:TK.panel,'stroke-width':1.5});
      attachTip(dot, `t <b>${fmt(p[0],2)} s</b><br>${fmt(p[1],digits)} ${unit}`);
      c.svg.appendChild(dot);
    });
    const peak = series.reduce((a,p)=>p[1]>a[1]?p:a, series[0]);
    c.svg.appendChild(svgText({x:s.x(peak[0]), y:s.y(peak[1])-9, fill:TK.ink,
      'font-size':10,'text-anchor':'middle'},
      `peak ${fmt(peak[1],digits)} ${unit}`));
    host.appendChild(c.svg);
  };
  mk(cpuHost, cpu, TK.signal, 'agent cpu %', '%', 0);
  mk(rssHost, rss, TK.alt, 'agent rss', 'MB', 0);
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

// ── transport ───────────────────────────────────────────────────────────────
let playing = false, lastFrame = 0;
function setT(t, pushHash=true){
  t = Math.max(0, Math.min(T_MAX, t));
  updateDag(t);
  window.__T = t;
  // Deep-link the scrubbed moment so a specific instant can be shared or
  // reopened. history.replaceState keeps the back button usable.
  if (pushHash && !playing)
    history.replaceState(null, '', '#t=' + t.toFixed(3));
}
function initialT(){
  const m = /[#&]t=([\d.]+)/.exec(location.hash || '');
  if (m) return Math.max(0, Math.min(T_MAX, parseFloat(m[1])));
  // Without a deep link, open at the first instant the pipeline is carrying as
  // many replicas as it ever will. At t=0 the autoscaler has not acted yet, so
  // the DAG would read 0/N and the page would look broken on arrival rather
  // than idle-before-start.
  const edges = new Set([0]);
  for (const n of STAGES) for (const [t] of (M.replicas[n]||[])) edges.add(t);
  const at = t => STAGES.reduce((a,n)=>a+valueAt(M.replicas[n]||[],t), 0);
  let best = 0, bestV = -1;
  for (const t of [...edges].sort((a,b)=>a-b)){
    const v = at(t);
    if (v > bestV){ bestV = v; best = t; }
  }
  return bestV > 0 ? Math.min(T_MAX, best) : 0;
}
function setPlaying(v){
  playing = v;
  document.querySelectorAll('.play').forEach(b => {
    b.textContent = playing ? '❙❙ Pause' : '▶ Play';
    b.setAttribute('aria-pressed', String(playing));
  });
  lastFrame = performance.now();
  if (playing) requestAnimationFrame(step);
}
function speed(){
  const s = document.querySelector('.speed');
  return s ? +s.value : 1;
}
function step(){
  if (!playing) return;
  const now = performance.now();
  const dt = (now-lastFrame)/1000 * speed();
  lastFrame = now;
  let t = (window.__T || 0) + dt;
  if (t >= T_MAX) t = 0;
  setT(t, false);
  requestAnimationFrame(step);
}

// ── boot ────────────────────────────────────────────────────────────────────
function init(){
  document.querySelectorAll('.play').forEach(b =>
    b.addEventListener('click', () => setPlaying(!playing)));
  document.querySelectorAll('.rewind').forEach(b =>
    b.addEventListener('click', () => { setPlaying(false); setT(0); }));
  document.querySelectorAll('.speed').forEach(sel =>
    sel.addEventListener('change', e => {
      document.querySelectorAll('.speed').forEach(s => s.value = e.target.value);
    }));

  drawTape('tape', 32 + STAGES.length*Math.max(11, Math.min(26, 260/STAGES.length)), true);
  drawTape('railtape', 30, false);
  drawDag(); drawLoad(); drawQueue(); drawCdf(); drawHist();
  drawScatter(); drawThroughput(); drawColdStart(); drawResources();
  drawStageLatency(); fillTables();
  setT(initialT(), false);

  // The rail is the tape's understudy: it appears only once the tape itself has
  // scrolled away, so there is never a second scrubber competing on screen.
  const rail = document.getElementById('rail'), tape = document.getElementById('tape');
  if (rail && tape && 'IntersectionObserver' in window){
    new IntersectionObserver(([e]) => rail.classList.toggle('stuck', !e.isIntersecting),
      {rootMargin:'-60px 0px 0px 0px'}).observe(tape);
  }

  document.querySelectorAll('.tabs button').forEach(b => {
    b.addEventListener('click', () => {
      const group = b.closest('section');
      group.querySelectorAll('.tabs button').forEach(x=>{
        x.classList.remove('active'); x.setAttribute('aria-selected','false'); });
      group.querySelectorAll('.tabpane').forEach(p=>p.classList.add('hidden'));
      b.classList.add('active'); b.setAttribute('aria-selected','true');
      group.querySelector('#'+b.dataset.pane).classList.remove('hidden');
    });
  });

  // Transport from the keyboard: space plays, arrows step a frame of run time.
  addEventListener('keydown', e => {
    const tag = (e.target.tagName||'').toLowerCase();
    if (tag === 'input' || tag === 'select' || tag === 'textarea') return;
    if (e.code === 'Space'){ e.preventDefault(); setPlaying(!playing); }
    else if (e.key === 'ArrowLeft'){ e.preventDefault(); setPlaying(false);
      setT((window.__T||0) - T_MAX/200); }
    else if (e.key === 'ArrowRight'){ e.preventDefault(); setPlaying(false);
      setT((window.__T||0) + T_MAX/200); }
    else if (e.key === 'Home'){ e.preventDefault(); setPlaying(false); setT(0); }
  });
}
document.addEventListener('DOMContentLoaded', init);
"""


def _esc(s) -> str:
    return (str(s).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
            if s is not None else "—")


def _num(v, digits=0, dash="—") -> str:
    if v is None:
        return dash
    if digits == 0:
        return f"{round(v):,}"
    return f"{v:,.{digits}f}"


def _readout(label, value, unit="", foot="") -> str:
    unit_html = f'<span class="unit">{_esc(unit)}</span>' if unit else ""
    return (f'<div class="ro"><div class="label">{_esc(label)}</div>'
            f'<div class="ro-v">{value}{unit_html}</div>'
            f'<div class="ro-f">{foot}</div></div>')


def _shead(title, note="") -> str:
    return (f'<div class="shead"><h2>{_esc(title)}</h2><span class="srule"></span>'
            f'<span class="snote">{_esc(note)}</span></div>')


def _transport(compact=False) -> str:
    """The transport controls. Rendered twice — under the tape and in the rail —
    and both copies drive the same clock, so scrubbing works wherever you are."""
    speed = ('<select class="speed" aria-label="playback speed">'
             '<option value="0.25">0.25×</option><option value="0.5">0.5×</option>'
             '<option value="1" selected>1×</option><option value="2">2×</option>'
             '<option value="4">4×</option></select>')
    return (f'<div class="transport">'
            f'<button class="play" aria-pressed="false">▶ Play</button>'
            f'<button class="rewind">⏮ Reset</button>{speed}</div>'
            f'<div class="tnow" role="status" aria-live="off">0.00<small>s</small></div>'
            + ("" if compact else
               '<div class="railstat">REPLICAS <b data-live="reps">0</b>'
               '&nbsp;&nbsp;QUEUED <b data-live="queue">0</b></div>'))


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
    cons_lamp = ('<span class="lamp lamp--good">events conserved</span>'
                 if conserved is True else
                 '<span class="lamp lamp--fault">leak or duplicate</span>'
                 if conserved is False else
                 '<span class="lamp">conservation not checkable</span>')

    notes_html = ""
    if model["warnings"]:
        items = "".join(f"<li>{_esc(w)}</li>" for w in model["warnings"])
        notes_html = (f'<div class="notes"><div class="label">Notes on this run</div>'
                      f'<ul>{items}</ul></div>')

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

    ups = sum(1 for e in model["scaling_events"] if e["action"] in ("spawn", "cold_start"))
    downs = sum(1 for e in model["scaling_events"] if e["action"] == "drain")

    # Four primary meters. The rest of the numbers live on the spec plate below —
    # eight equal cards made nothing primary.
    readouts = "".join([
        _readout("events at collector", _num(c["received"]), "",
                 f'{_num(c["paths"])} source→sink path(s)'),
        _readout("run duration", _num(m["duration_s"], 2), "s",
                 f'{_num(tp["sustained_eps"])} ev/s sustained'),
        _readout("e2e p50", _num(lat["e2e"]["p50"], 2), "ms",
                 f'mean {_num(lat["e2e"]["mean"], 2)} ms'),
        _readout("e2e p99", _num(lat["e2e"]["p99"], 2), "ms",
                 f'p999 {_num(lat["e2e"]["p999"], 2)} · max {_num(lat["e2e"]["max"], 2)} ms'),
    ])

    run_rows = [
        ("offered", (_num(c["sent"]) if c["sent"] is not None else "—")),
        ("expected at sink", (_num(c["expected_received"])
                              if c["expected_received"] is not None else "—")),
        ("stages / edges", f'{_num(m["stage_count"])} / {len(model["topology"]["edges"])}'),
        ("scale ups / downs", f'{ups} / {downs}'),
        ("cold starts", f'{_num(cs["count"])}'),
        ("cold start p50 / max", f'{_num(cs["p50"], 3)} / {_num(cs["max"], 3)} ms'),
        ("e2e min / p95", f'{_num(lat["e2e"]["min"], 2)} / {_num(lat["e2e"]["p95"], 2)} ms'),
        ("sampled events", _num(lat["e2e"]["count"])),
    ]
    run_html = "".join(f"<dt>{_esc(k)}</dt><dd>{v}</dd>" for k, v in run_rows)

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
<meta name="color-scheme" content="dark">
<title>{_esc(m['pipeline'])} · {_esc(m['run_id'])} · epico-viz</title>
{logo_icon}
<style>{_css_vars()}{logo_css}{CSS}</style>
</head><body>
<div class="wrap">

<header class="plate">
  <div class="plate-id">
    {logo_img}
    <h1>{_esc(m['pipeline'])}</h1>
    <span class="serial">{_esc(m['run_id'])}</span>
    <span class="plate-meta">{_esc(m['started_iso'] or 'start unknown')} ·
      <b>{_num(m['duration_s'], 2)} s</b> · {_esc(m['layout'])} layout</span>
  </div>
  <div class="verdict">
    <span><span class="n">{_num(c['received'])}</span> events through
      <span class="n">{_num(m['stage_count'])}</span> stages</span>
    <span>{cons_lamp}</span>
    <span>e2e p99 <span class="n">{_num(lat['e2e']['p99'], 2)} ms</span></span>
    <span><span class="n">{_num(cs['count'])}</span> cold start(s),
      p50 <span class="n">{_num(cs['p50'], 3)} ms</span></span>
  </div>
</header>

<div class="rail" id="rail">
  {_transport(compact=True)}
  <div id="railtape"></div>
  <div class="railstat">REPLICAS <b data-live="reps">0</b></div>
</div>

<div class="tape-frame">
  <div class="tape-head">
    <span class="label">Run tape</span>
    <span class="snote">one row per stage · replica occupancy · offered load over it ·
      click or drag to scrub</span>
  </div>
  <div class="overflow-x" id="tape"></div>
  <div class="tape-head" style="margin:12px 0 0">{_transport()}</div>
</div>

{notes_html}

<section>
  {_shead("Run", "what came out the other end")}
  <div class="readouts">{readouts}</div>
  <div class="grid cols2" style="margin-top:12px">
    <div class="panel"><h3>Totals</h3><dl class="kv">{run_html}</dl></div>
    <div class="panel"><h3>Environment</h3><dl class="kv">{env_html}</dl></div>
  </div>
</section>

<section>
  {_shead("Configuration", "how the run was told to behave")}
  <div class="grid cols2">
    <div class="panel"><h3>Runtime flags</h3><dl class="kv">{flags_html}</dl></div>
    <div class="panel"><h3>Source</h3><dl class="kv">{''.join(src_bits)}</dl></div>
  </div>
</section>

<section>
  {_shead("Execution", "scrub the run clock — space plays, ← → step")}
  <div class="panel">
    <h3>Pipeline at the scrubbed instant</h3>
    <div class="overflow-x" id="dag"></div>
    <div class="note">Each node is a stage: the segment meter counts live
      replicas out of its ceiling, the bar under it is queue depth relative to
      this run's peak. Hover an edge for its transport and ring geometry.</div>
  </div>
  <div class="panel" id="load-panel">
    <h3>Offered load — cumulative</h3>
    <div class="overflow-x" id="load"></div>
  </div>
  <div class="panel" id="loadrate-panel">
    <h3>Offered rate</h3>
    <div class="overflow-x" id="loadrate"></div>
  </div>
</section>

<section>
  {_shead("Backpressure", "queue depth per stage")}
  <div class="panel"><div class="overflow-x" id="queuechart"></div></div>
</section>

<section>
  {_shead("Latency", "end to end, then where the time went")}
  <div class="grid cols2">
    <div class="panel"><h3>e2e distribution (CDF)</h3><div id="cdf"></div></div>
    <div class="panel"><h3>e2e histogram</h3><div id="hist"></div></div>
  </div>
  <div class="panel">
    <h3>Per-event e2e over the run</h3><div class="overflow-x" id="scatter"></div>
  </div>
  <div class="panel">
    <h3>Per-stage residency</h3><div class="overflow-x" id="stagelat"></div>
  </div>
  <div class="tabs" style="margin-top:18px" role="tablist">
    <button class="active" role="tab" aria-selected="true" data-pane="p-stagelat">per stage</button>
    <button role="tab" aria-selected="false" data-pane="p-inter">inter-stage edges</button>
    <button role="tab" aria-selected="false" data-pane="p-replica">per replica</button>
    <button role="tab" aria-selected="false" data-pane="p-ingress">ingress wait</button>
  </div>
  <div class="panel">
    <div class="tabpane" id="p-stagelat"><div id="t-stagelat"></div></div>
    <div class="tabpane hidden" id="p-inter"><div id="t-inter"></div></div>
    <div class="tabpane hidden" id="p-replica"><div id="t-replica"></div></div>
    <div class="tabpane hidden" id="p-ingress"><div id="t-ingress"></div></div>
  </div>
</section>

<section>
  {_shead("Cold start", "every worker boot, and what it cost")}
  <div class="panel"><div class="overflow-x" id="coldchart"></div></div>
  <div class="panel"><h3>Boot phases</h3><div id="t-boots"></div></div>
</section>

<section>
  {_shead("Throughput", "what the agent moved, and what it cost to move it")}
  <div class="grid cols3">
    <div class="panel"><h3>Collector throughput</h3><div id="tput"></div></div>
    <div class="panel"><h3>Agent CPU</h3><div id="res-cpu"></div></div>
    <div class="panel"><h3>Agent resident memory</h3><div id="res-rss"></div></div>
  </div>
</section>

<section>
  {_shead("Scaling", "thresholds, actions, and the run's timeline")}
  <div class="tabs" role="tablist">
    <button class="active" role="tab" aria-selected="true" data-pane="p-stagecfg">per-stage config</button>
    <button role="tab" aria-selected="false" data-pane="p-scaling">scaling events</button>
    <button role="tab" aria-selected="false" data-pane="p-markers">run timeline</button>
  </div>
  <div class="panel">
    <div class="tabpane" id="p-stagecfg"><div id="t-stagecfg"></div></div>
    <div class="tabpane hidden" id="p-scaling"><div id="t-scaling"></div></div>
    <div class="tabpane hidden" id="p-markers"><div id="t-markers"></div></div>
  </div>
</section>

<section>
  {_shead("Conservation", "did every event survive the DAG")}
  <div class="panel">
    <div class="note" style="margin:0 0 12px">
      Under broadcast fan-out a stage is traversed once per path through it, so the
      expected count is <span class="mono">paths_in × paths_out × offered</span>.
      &nbsp; {cons_lamp}
    </div>
    <div id="t-conservation"></div>
  </div>
</section>

<section>
  {_shead("Worker timing", "wasm and serde, per stage")}
  <div class="panel"><div id="t-worker"></div></div>
</section>

<footer>
  {logo_foot}
  Generated by epico-viz from <span class="mono">{_esc(m['summary_path'])}</span>
  {'<br><span class="mono">' + _esc(m['master_log']) + '</span>' if m['master_log'] else ''}
</footer>
</div>
<script>window.__EPICO__ = {data};</script>
<script>{_js_tokens()}{JS}</script>
</body></html>
"""
