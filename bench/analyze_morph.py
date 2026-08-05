#!/usr/bin/env python3
"""Break-even analysis for runtime topology morphs (roadmap M2).

The M2 deliverable is not "fusion works" — it is a NUMBER: how long must the
rewritten topology run before the rewrite has paid for itself? This script
computes it from the standard `master_*_summary.json`, with no telemetry that
did not already exist.

    bench/analyze_morph.py logs/run_*/master_summary.json
    bench/analyze_morph.py --json results.json logs/**/master_summary.json

What it reads
-------------
* `scaling.per_stage[<subject>].events` — every transition is bracketed by
  `morph_begin` … `<verb>` … `morph_end` on the subject stage's timeline, so the
  switch window is recoverable exactly.
* `events.rows` — `[recv_t_s, e2e_ms, [[stage#replica, enter_ts, exit_ts], …]]`,
  sorted by arrival. Per-event wall timestamps at whatever resolution the event
  rate supports. `recv_per_second` is 1 s-granular and useless for a switch that
  may last microseconds; this is not.

What it reports
---------------
**S, the switch cost**, three ways, because they answer different questions:

  S_control   the actuator's own teardown+deploy timer. What the control plane
              did.
  S_gap       the largest hole in the ARRIVAL stream around the switch, minus
              the median inter-arrival gap in the same window. What the output
              stream saw.
  S_e2e       the worst e2e latency of any event crossing the switch, minus the
              pre-switch median. What a single unlucky event paid.

  S_control brackets from below (it cannot see queueing), S_e2e from above (it
  includes the backlog the stall built). A large divergence means the stall is
  somewhere the actuator's timer does not cover.

**Break-even**, two framings, because which one is meaningful depends on what
was the bottleneck:

  throughput  The pipeline was the limit, so deleting edge cost raises the
              ceiling. The stall processes nothing for S seconds, leaving the
              rewrite `lambda_post * S` events in debt, repaid at
              `lambda_post - lambda_pre` events per second:

                  T = lambda_post * S / (lambda_post - lambda_pre)

  latency     The SOURCE was the limit, so throughput cannot move no matter how
              much edge cost is deleted, and the gain shows up as latency
              instead. A stall of S seconds injects a backlog whose total excess
              latency is about `lambda * S^2 / 2` event-seconds, repaid at
              `lambda * delta_e2e` event-seconds per second:

                  T = S^2 / (2 * delta_e2e)

Read the throughput number when `lambda_post > lambda_pre`; otherwise read the
latency one and say so. An infinite value means the rewrite never pays for
itself at this workload — a real and publishable outcome, not an error. If BOTH
are infinite the rewrite is pure cost here, which is the honest finding.

**The contracted edge**, before and after: the per-event `inter_stage` gap that
fusion is supposed to delete. This is the mechanism evidence. If break-even is
good but this gap did not move, the improvement came from somewhere else and
the conclusion is wrong.

Caveats, stated because they bound what these numbers mean
----------------------------------------------------------
* `events.rows` is a uniform reservoir sample, then subsampled. Uniform sampling
  preserves relative density, so rates and ratios are sound — but every absolute
  inter-arrival gap is inflated by the sampling factor. S_gap therefore subtracts
  the median gap of the SAME series, which cancels that factor to first order,
  and the reported `gap_resolution_ms` says how fine a stall this run could have
  resolved at all. A S_gap near that floor is a bound, not a measurement.
* Ordered DAG pairs matter. Run `identity` first: a redeploy with no topology
  change has zero expected steady-state delta BY CONSTRUCTION, so a non-zero
  gain there means the harness is wrong, not that redeploying helps.
"""

from __future__ import annotations

import argparse
import json
import math
import statistics
import sys
from pathlib import Path

VERBS = ("fuse", "split", "identity")

# Seconds of steady state sampled on each side of a switch.
WINDOW_S = 2.0
# Skip after `morph_end` before the "after" window starts: a freshly deployed
# stage is cold-starting replicas and working off the backlog the stall built.
# Measuring steady state during recovery would attribute recovery cost to the
# new topology.
SETTLE_S = 1.0


# ── summary access ──────────────────────────────────────────────────────────

def load_rows(summary: dict) -> list[tuple[float, float, list]]:
    """Per-event rows sorted by arrival time."""
    rows = summary.get("events", {}).get("rows", []) or []
    return [r for r in rows if isinstance(r, (list, tuple)) and len(r) >= 2]


def scale_factor(summary: dict, rows: list) -> float:
    """Events actually received per row present in the log."""
    received = summary.get("counters", {}).get("events_received", 0)
    return (received / len(rows)) if rows and received else 1.0


def find_morphs(summary: dict) -> list[dict]:
    """Every transition, with its control-plane window.

    A transition writes `morph_begin`, then its verb, then `morph_end` on the
    subject stage's timeline. Reading them back in order recovers both what
    happened and exactly when.
    """
    out = []
    per_stage = summary.get("scaling", {}).get("per_stage", {}) or {}
    for stage, blk in per_stage.items():
        evs = sorted(blk.get("events", []), key=lambda e: e["t_s"])
        open_begin = None
        pending_verb = None
        for e in evs:
            action = e["action"]
            if action == "morph_begin":
                open_begin, pending_verb = e["t_s"], None
            elif action in VERBS and open_begin is not None:
                pending_verb = (action, e["t_s"], e.get("new_count"))
            elif action in ("morph_end", "morph_failed"):
                if open_begin is None:
                    continue
                verb, t_verb, count = pending_verb or ("unknown", open_begin, None)
                out.append({
                    "subject": stage,
                    "verb": verb,
                    "ok": action == "morph_end",
                    "t_begin": open_begin,
                    "t_verb": t_verb,
                    "t_end": e["t_s"],
                    "s_control_ms": (e["t_s"] - open_begin) * 1000.0,
                    "replicas_after": count,
                })
                open_begin, pending_verb = None, None
    out.sort(key=lambda m: m["t_begin"])
    return out


# ── window statistics ───────────────────────────────────────────────────────

def window(rows: list, lo: float, hi: float) -> list:
    return [r for r in rows if lo <= r[0] < hi]


def throughput(rows_in_window: list, span_s: float, scale: float) -> float:
    if span_s <= 0:
        return 0.0
    return len(rows_in_window) * scale / span_s


def edge_gaps_ms(rows_in_window: list, frm: str, to: str) -> list[float]:
    """Per-event `frm -> to` transport gap, in ms.

    The gap between two CONSECUTIVE hops is `next.enter - prev.exit`: serialize,
    enqueue, transport, dequeue, deserialize — everything between two
    `process-event` boundaries. Fusion deletes all of it for the contracted
    edge, because the two hops become adjacent calls in one worker thread.

    Hop labels are `stage#replica`; the replica suffix is stripped so the edge
    key stays the topology edge.
    """
    gaps = []
    for row in rows_in_window:
        hops = row[2] if len(row) > 2 else []
        for a, b in zip(hops, hops[1:]):
            if len(a) < 3 or len(b) < 3:
                continue
            if a[0].split("#")[0] == frm and b[0].split("#")[0] == to:
                gap = (b[1] - a[2]) * 1000.0
                if gap >= 0:
                    gaps.append(gap)
    return gaps


def pct(xs: list[float], q: float) -> float:
    if not xs:
        return float("nan")
    s = sorted(xs)
    i = min(len(s) - 1, max(0, int(round(q * (len(s) - 1)))))
    return s[i]


def arrival_gap_stats(rows: list, lo: float, hi: float) -> tuple[float, float]:
    """(largest hole, median hole) in the arrival stream over [lo, hi), in ms.

    Both come from the same series so the reservoir's sampling factor divides
    out of their difference.
    """
    ts = [r[0] for r in rows if lo <= r[0] < hi]
    if len(ts) < 3:
        return (float("nan"), float("nan"))
    ts.sort()
    holes = [(b - a) * 1000.0 for a, b in zip(ts, ts[1:])]
    return (max(holes), statistics.median(holes))


# ── per-morph analysis ──────────────────────────────────────────────────────

def analyse_morph(summary: dict, rows: list, scale: float, m: dict,
                  edge: tuple[str, str] | None) -> dict:
    t_begin, t_end = m["t_begin"], m["t_end"]

    pre_lo, pre_hi = t_begin - WINDOW_S, t_begin
    post_lo = t_end + SETTLE_S
    post_hi = post_lo + WINDOW_S

    pre_rows  = window(rows, pre_lo, pre_hi)
    post_rows = window(rows, post_lo, post_hi)

    lam_pre  = throughput(pre_rows,  pre_hi - pre_lo,   scale)
    lam_post = throughput(post_rows, post_hi - post_lo, scale)

    # ── S, the data-plane view ──────────────────────────────────────────────
    # Search a little either side of the control-plane window: the stall the
    # output stream sees starts when the last pre-switch event drains and ends
    # when the new topology's first event lands, which brackets the actuator's
    # own timer on both sides.
    hole_max, hole_med = arrival_gap_stats(rows, t_begin - 0.25, t_end + 1.0)
    s_gap_ms = (hole_max - hole_med) if not math.isnan(hole_max) else float("nan")

    switch_rows = window(rows, t_begin, t_end + 1.0)
    e2e_pre = statistics.median([r[1] for r in pre_rows]) if pre_rows else float("nan")
    e2e_switch_max = max((r[1] for r in switch_rows), default=float("nan"))
    s_e2e_ms = (e2e_switch_max - e2e_pre) if pre_rows and switch_rows else float("nan")

    # ── Break-even ──────────────────────────────────────────────────────────
    # Prefer the data-plane gap: it is the cost the event stream actually paid,
    # where the control-plane timer only sees what the actuator did.
    s_seconds = (s_gap_ms if not math.isnan(s_gap_ms) else m["s_control_ms"]) / 1000.0
    s_seconds = max(s_seconds, 0.0)

    delta_lam = lam_post - lam_pre
    if delta_lam > 0 and s_seconds > 0:
        breakeven_tp_s = lam_post * s_seconds / delta_lam
    elif s_seconds <= 0:
        breakeven_tp_s = 0.0            # free switch
    else:
        breakeven_tp_s = float("inf")   # no throughput gain at this workload

    e2e_post = statistics.median([r[1] for r in post_rows]) if post_rows else float("nan")
    delta_e2e_ms = (e2e_pre - e2e_post) if post_rows and pre_rows else float("nan")
    if math.isnan(delta_e2e_ms) or delta_e2e_ms <= 0:
        breakeven_lat_s = float("inf")
    elif s_seconds <= 0:
        breakeven_lat_s = 0.0
    else:
        breakeven_lat_s = (s_seconds ** 2) / (2.0 * delta_e2e_ms / 1000.0)

    # Where the gain actually showed up, which decides which framing is
    # meaningful. Throughput can only move if the PIPELINE was the limit; when
    # the source is, the same deleted edge cost surfaces as latency instead.
    # 2% of the pre-rate is the noise floor between two 2-second windows.
    if delta_lam > 0.02 * max(lam_pre, 1.0):
        gain_in, breakeven_s = "throughput", breakeven_tp_s
    elif not math.isnan(delta_e2e_ms) and delta_e2e_ms > 0:
        gain_in, breakeven_s = "latency", breakeven_lat_s
    else:
        gain_in, breakeven_s = "neither", float("inf")

    res = {
        **m,
        "window_s": WINDOW_S,
        "settle_s": SETTLE_S,
        "s_gap_ms": s_gap_ms,
        "s_e2e_ms": s_e2e_ms,
        "gap_resolution_ms": hole_med,
        "lambda_pre_eps": lam_pre,
        "lambda_post_eps": lam_post,
        "e2e_pre_ms": e2e_pre,
        "e2e_post_ms": e2e_post,
        "delta_e2e_ms": delta_e2e_ms,
        "gain_in": gain_in,
        "breakeven_throughput_s": breakeven_tp_s,
        "breakeven_latency_s": breakeven_lat_s,
        "breakeven_s": breakeven_s,
        "pre_samples": len(pre_rows),
        "post_samples": len(post_rows),
    }

    if edge:
        frm, to = edge
        g_pre  = edge_gaps_ms(pre_rows,  frm, to)
        g_post = edge_gaps_ms(post_rows, frm, to)
        res["edge"] = f"{frm} -> {to}"
        res["edge_gap_pre_p50_ms"]  = pct(g_pre, 0.50)
        res["edge_gap_pre_p99_ms"]  = pct(g_pre, 0.99)
        res["edge_gap_post_p50_ms"] = pct(g_post, 0.50)
        res["edge_gap_post_p99_ms"] = pct(g_post, 0.99)
        res["edge_gap_pre_n"]  = len(g_pre)
        res["edge_gap_post_n"] = len(g_post)
    return res


def analyse_summary(path: Path, edge: tuple[str, str] | None) -> dict:
    summary = json.load(open(path))
    rows  = load_rows(summary)
    scale = scale_factor(summary, rows)
    morphs = find_morphs(summary)
    return {
        "summary": str(path),
        "events_received": summary.get("counters", {}).get("events_received"),
        "rows": len(rows),
        "row_scale": scale,
        "transport": (summary.get("run_config") or {}).get("edge_impl"),
        "morphs": [analyse_morph(summary, rows, scale, m, edge) for m in morphs],
    }


# ── reporting ───────────────────────────────────────────────────────────────

def f(x, nd=3):
    if x is None:
        return "n/a"
    if isinstance(x, float):
        if math.isnan(x):
            return "n/a"
        if math.isinf(x):
            return "never"
    return f"{x:.{nd}f}" if isinstance(x, float) else str(x)


def report(results: list[dict]) -> None:
    for r in results:
        print(f"\n=== {r['summary']}")
        print(f"    events_received={r['events_received']}  "
              f"log_rows={r['rows']} (1 row ≈ {r['row_scale']:.1f} events)")
        if not r["morphs"]:
            print("    no morphs in this run")
            continue
        for m in r["morphs"]:
            status = "" if m["ok"] else "  [FAILED]"
            print(f"\n    {m['verb'].upper()} -> {m['subject']}  at t={m['t_verb']:.3f}s{status}")
            print(f"      S_control   {f(m['s_control_ms'])} ms   (actuator: teardown + deploy)")
            print(f"      S_gap       {f(m['s_gap_ms'])} ms   (largest hole in the arrival stream)")
            print(f"      S_e2e       {f(m['s_e2e_ms'])} ms   (worst event crossing the switch)")
            print(f"      resolution  {f(m['gap_resolution_ms'])} ms   "
                  f"(median sampled inter-arrival — S_gap below this is a bound, not a measurement)")
            if m["pre_samples"] == 0 or m["post_samples"] == 0:
                print(f"      (no traffic in the before/after window — this morph "
                      f"landed on an idle pipeline and measures nothing)")
                continue
            print(f"      throughput  {f(m['lambda_pre_eps'], 0)} -> {f(m['lambda_post_eps'], 0)} ev/s")
            print(f"      e2e p50     {f(m['e2e_pre_ms'])} -> {f(m['e2e_post_ms'])} ms"
                  f"   (delta {f(m['delta_e2e_ms'])} ms/event)")
            print(f"      gain shows up in: {m['gain_in']}"
                  f"   (throughput can only move if the PIPELINE was the limit)")
            print(f"      BREAK-EVEN  {f(m['breakeven_s'])} s"
                  f"   [throughput {f(m['breakeven_throughput_s'])} s, "
                  f"latency {f(m['breakeven_latency_s'])} s]")
            if "edge" in m:
                print(f"      contracted edge {m['edge']}:")
                print(f"        p50  {f(m['edge_gap_pre_p50_ms'])} -> {f(m['edge_gap_post_p50_ms'])} ms"
                      f"   (n={m['edge_gap_pre_n']} -> {m['edge_gap_post_n']})")
                print(f"        p99  {f(m['edge_gap_pre_p99_ms'])} -> {f(m['edge_gap_post_p99_ms'])} ms")


def aggregate(results: list[dict]) -> None:
    """Distributions across runs, not single numbers.

    One run's S is an anecdote. The mechanism's cost is a distribution, and the
    tail is the part a controller has to respect: never propose a morph faster
    than break-even.
    """
    by_verb: dict[str, list[dict]] = {}
    for r in results:
        for m in r["morphs"]:
            if m["ok"]:
                by_verb.setdefault(m["verb"], []).append(m)
    if not by_verb:
        return
    print("\n" + "=" * 72)
    print("AGGREGATE across runs (n = number of morphs observed)")
    print("=" * 72)
    for verb, ms in sorted(by_verb.items()):
        def col(key):
            xs = [m[key] for m in ms if isinstance(m.get(key), float)
                  and not math.isnan(m[key]) and not math.isinf(m[key])]
            return xs
        print(f"\n  {verb}  (n={len(ms)})")
        for key, label in (("s_control_ms", "S_control ms"),
                           ("s_gap_ms",     "S_gap     ms"),
                           ("s_e2e_ms",     "S_e2e     ms"),
                           ("breakeven_s",  "breakeven  s")):
            xs = col(key)
            if not xs:
                print(f"    {label}:  n/a")
                continue
            print(f"    {label}:  p50={pct(xs, 0.50):9.3f}  p95={pct(xs, 0.95):9.3f}  "
                  f"max={max(xs):9.3f}  n={len(xs)}")
        never = sum(1 for m in ms if math.isinf(m.get("breakeven_s", 0.0)))
        if never:
            print(f"    breakeven never reached in {never}/{len(ms)} morphs "
                  f"(no throughput gain at this workload)")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("summaries", nargs="+", type=Path,
                    help="master_*_summary.json files")
    ap.add_argument("--edge", default="normalize:detect",
                    help="contracted edge as FROM:TO, or 'none' to skip "
                         "(default: normalize:detect)")
    ap.add_argument("--json", type=Path,
                    help="also write the full result set here")
    args = ap.parse_args()

    edge = None
    if args.edge.lower() != "none":
        if ":" not in args.edge:
            print(f"--edge expects FROM:TO, got {args.edge!r}", file=sys.stderr)
            return 2
        frm, to = args.edge.split(":", 1)
        edge = (frm, to)

    results = []
    for p in args.summaries:
        if not p.exists():
            print(f"skipping missing {p}", file=sys.stderr)
            continue
        try:
            results.append(analyse_summary(p, edge))
        except (json.JSONDecodeError, KeyError, TypeError) as e:
            print(f"skipping unreadable {p}: {e}", file=sys.stderr)

    if not results:
        print("no readable summaries", file=sys.stderr)
        return 1

    report(results)
    aggregate(results)

    if args.json:
        args.json.write_text(json.dumps(results, indent=2))
        print(f"\nwrote {args.json}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
