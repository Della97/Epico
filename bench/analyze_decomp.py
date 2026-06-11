#!/usr/bin/env python3
"""Analyze the Epico service-time decomposition runs (native_decomp.sh).

Usage: python3 analyze_decomp.py <results_dir> [--windflow-us 0.4723]

For each (arm, mode) it reports two independent estimates of the per-stage
service time T:
  * 1/throughput  — from sustained_eps_warmup_trimmed (valid only while the
    WORKER is the bottleneck; in serde/passthrough modes the ingest path may
    cap first, which the script flags when the two estimates disagree >25%)
  * total_us p50  — from worker_timing (the worker's own stopwatch; valid in
    every mode and per stage)

It then prints the decomposition waterfall per arm:
  passthrough            = recv/push + transport plumbing
  serde - passthrough    = JSON parse + hop bookkeeping + serialize
  wasm  - serde          = Val construction + component call (dynamic dispatch)
and the multiple vs a reference native system (default: WindFlow 0.4723 us).

Writes decomp_waterfall.png if matplotlib is available.
"""
import argparse
import json
import statistics as st
from collections import defaultdict
from pathlib import Path

MODES = ["passthrough", "serde", "wasm"]


def load(root):
    runs = defaultdict(list)   # (arm, mode) -> [summary]
    for meta_path in Path(root).rglob("meta.json"):
        summ_path = meta_path.with_name("summary.json")
        if not summ_path.exists():
            continue
        try:
            meta = json.loads(meta_path.read_text())
            summ = json.loads(summ_path.read_text())
        except json.JSONDecodeError:
            continue
        if "mode" in meta:
            runs[(meta["arm"], meta["mode"])].append(summ)
    return runs


def mean_sd(vals):
    vals = [v for v in vals if v is not None]
    if not vals:
        return float("nan"), float("nan")
    return st.fmean(vals), (st.stdev(vals) if len(vals) > 1 else 0.0)


def eps(s):
    return s.get("throughput", {}).get("sustained_eps_warmup_trimmed")


def total_us_p50(s, stage):
    return s.get("worker_timing", {}).get(stage, {}).get("total_us", {}).get("p50")


def serde_us_p50(s, stage):
    return s.get("worker_timing", {}).get(stage, {}).get("serde_us", {}).get("p50")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("root")
    ap.add_argument("--windflow-us", type=float, default=0.4723)
    args = ap.parse_args()

    runs = load(args.root)
    if not runs:
        print(f"no (summary.json, meta.json) pairs with a 'mode' under {args.root}")
        return

    arms = sorted({a for a, _ in runs})
    print("=== SERVICE TIME PER STAGE (us) — two independent estimates ===")
    print(f"  {'arm':<11} {'mode':<12} {'1/eps':>9} {'sd':>6}   "
          f"{'relay tot p50':>13} {'fwd tot p50':>12} {'relay serde p50':>15}  note")

    T = {}      # (arm, mode) -> chosen service-time estimate (us)
    for arm in arms:
        for mode in MODES:
            ss = runs.get((arm, mode), [])
            if not ss:
                continue
            inv_eps, inv_sd = mean_sd([1e6 / eps(s) if eps(s) else None for s in ss])
            rel, _ = mean_sd([total_us_p50(s, "relay") for s in ss])
            fwd, _ = mean_sd([total_us_p50(s, "forward") for s in ss])
            sed, _ = mean_sd([serde_us_p50(s, "relay") for s in ss])

            # Choose the estimate: worker stopwatch when available, else 1/eps.
            stopwatch = rel if rel == rel else fwd            # NaN-safe
            chosen = stopwatch if stopwatch == stopwatch else inv_eps
            note = ""
            if stopwatch == stopwatch and inv_eps == inv_eps:
                if inv_eps > 1.25 * stopwatch:
                    note = "ingest-bound: 1/eps >> worker time, using worker time"
                elif stopwatch > 1.25 * inv_eps:
                    note = "worker stopwatch > 1/eps (?) — check pipelining/overlap"
            T[(arm, mode)] = chosen
            print(f"  {arm:<11} {mode:<12} {inv_eps:>9.2f} {inv_sd:>6.2f}   "
                  f"{rel:>13.2f} {fwd:>12.2f} {sed:>15.2f}  {note}")

    print("\n=== DECOMPOSITION WATERFALL (per stage, us) ===")
    bars = {}
    for arm in arms:
        p  = T.get((arm, "passthrough"))
        sd = T.get((arm, "serde"))
        w  = T.get((arm, "wasm"))
        if None in (p, sd, w) or any(x != x for x in (p, sd, w)):
            print(f"  {arm}: incomplete mode set, skipping")
            continue
        seg = {
            "recv/push + transport plumbing": p,
            "JSON parse + hop + serialize":   max(sd - p, 0.0),
            "Val construction + wasm call":   max(w - sd, 0.0),
        }
        bars[arm] = seg
        print(f"\n  {arm}  (total {w:.2f} us  ->  {1e6/w:,.0f} ev/s per worker)")
        for k, v in seg.items():
            print(f"    {k:<34} {v:>7.2f} us  ({100*v/w:>4.1f}%)")
        print(f"    {'vs WindFlow ' + format(args.windflow_us, '.4f') + ' us':<34} "
              f"{w/args.windflow_us:>6.1f}x  "
              f"(native floor would be {1e6/p:,.0f} ev/s)")

    # plot
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        print("\n(matplotlib not installed — skipping decomp_waterfall.png)")
        return
    if bars:
        fig, ax = plt.subplots(figsize=(7, 3.6))
        ax.set_facecolor("#f4f4f6")
        colors = ["#7f7f7f", "#e1812c", "#4878cf"]
        ys = list(bars)
        for yi, arm in enumerate(ys):
            left = 0.0
            for (k, v), c in zip(bars[arm].items(), colors):
                ax.barh(yi, v, left=left, color=c, edgecolor="white",
                        height=0.55, label=k if yi == 0 else None)
                left += v
            ax.text(left, yi, f"  {left:.1f} \u00b5s", va="center", fontsize=9)
        ax.axvline(args.windflow_us, color="#c03d3e", linestyle="--", linewidth=1.2)
        ax.text(args.windflow_us, len(ys) - 0.4, " WindFlow", color="#c03d3e",
                fontsize=8, rotation=90, va="top")
        ax.set_yticks(range(len(ys)))
        ax.set_yticklabels(ys)
        ax.invert_yaxis()
        ax.set_xlabel("per-stage service time (\u00b5s)")
        ax.set_title("Where the stage service time goes (identity stage, R=1)")
        ax.grid(axis="x", color="white", linewidth=1.0)
        ax.set_axisbelow(True)
        ax.legend(fontsize=8, frameon=False, loc="lower right")
        fig.tight_layout()
        out = Path(args.root) / "decomp_waterfall.png"
        fig.savefig(out, dpi=150)
        print(f"\nPlot -> {out}")


if __name__ == "__main__":
    main()
