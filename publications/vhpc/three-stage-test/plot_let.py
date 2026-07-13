#!/usr/bin/env python3
"""
plot_stage_latencies.py

Plot per-event intra-stage and inter-stage latencies as time series,
binned over the run.

Sources from a Metapod agent summary JSON file. Reads `per_event_log`
(every entry is `[recv_t_s, e2e_ms, [[stage, enter_wall, exit_wall], ...]]`)
to compute:

  - intra-stage[stage] = exit_wall - enter_wall (per stage, per event)
  - inter-stage[edge]  = enter_wall[i+1] - exit_wall[i]  (per edge, per event)

Then bins by recv_t_s and plots median + p95 bands over time.

Usage:
  python3 plot_stage_latencies.py logs/run/agent_*_summary.json [--out figures]
  python3 plot_stage_latencies.py logs/run/agent_*_summary.json --bin 0.5

The script also writes a CSV (`stage_latencies_long.csv`) alongside the
figures so you can inspect or re-plot the raw binned data in pandas.
"""

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

# ──────────────────────────────────────────────────────────────────────────────
# Style
# ──────────────────────────────────────────────────────────────────────────────

plt.rcParams.update({
    "font.size": 10,
    "axes.titlesize": 11,
    "axes.titleweight": "bold",
    "axes.spines.top": False,
    "axes.spines.right": False,
    "axes.grid": True,
    "grid.alpha": 0.3,
    "grid.linestyle": "--",
})

# Tab10-derived palette, deterministic per name so the same stage gets
# the same color across all figures.
def color_for(name: str) -> str:
    palette = [
        "#1f77b4", "#ff7f0e", "#2ca02c", "#d62728",
        "#9467bd", "#8c564b", "#e377c2", "#7f7f7f",
        "#bcbd22", "#17becf",
    ]
    return palette[hash(name) % len(palette)]


# ──────────────────────────────────────────────────────────────────────────────
# Load
# ──────────────────────────────────────────────────────────────────────────────

def load_events(summary_path: Path) -> pd.DataFrame:
    """Flatten per_event_log into a long-form DataFrame.

    Returns columns:
      recv_t_s    : seconds since test start (float)
      stage       : stage name
      kind        : "intra" | "inter"
      edge        : for inter, "<from>→<to>"; for intra, the stage name
      latency_ms  : the measured latency in milliseconds
    """
    with summary_path.open() as f:
        summary = json.load(f)

    # The per-event row list lives in different places depending on the
    # summary writer version:
    #   - newer: events.rows  (list of [recv_t_s, e2e_ms, hops])
    #   - older: per_event_log (same shape, top-level)
    #   - some intermediate variants stash it under e2e.per_event_log
    per_event = None
    if isinstance(summary.get("events"), dict):
        per_event = summary["events"].get("rows")
    if per_event is None:
        per_event = summary.get("per_event_log")
    if per_event is None:
        per_event = summary.get("e2e", {}).get("per_event_log")

    if not per_event:
        raise SystemExit(
            f"No per-event rows found in {summary_path}. Tried: "
            f"events.rows, per_event_log, e2e.per_event_log. "
            f"Top-level keys: {sorted(summary.keys())}"
        )

    rows = []
    for entry in per_event:
        if not isinstance(entry, list) or len(entry) < 3:
            continue
        recv_t_s, _e2e_ms, hops = entry
        if not hops:
            continue

        # Intra-stage: exit - enter for each hop.
        for (name, enter, exit_ts) in hops:
            if exit_ts >= enter:
                rows.append({
                    "recv_t_s":   recv_t_s,
                    "stage":      name,
                    "kind":       "intra",
                    "edge":       name,
                    "latency_ms": (exit_ts - enter) * 1000.0,
                })

        # Inter-stage: enter[i+1] - exit[i] between adjacent hops.
        for i in range(len(hops) - 1):
            from_name, _, from_exit = hops[i]
            to_name,   to_enter, _ = hops[i + 1]
            if to_enter >= from_exit:
                rows.append({
                    "recv_t_s":   recv_t_s,
                    "stage":      to_name,             # for grouping/coloring
                    "kind":       "inter",
                    "edge":       f"{from_name}→{to_name}",
                    "latency_ms": (to_enter - from_exit) * 1000.0,
                })

    if not rows:
        raise SystemExit(
            "No valid hops in per_event_log — the events have no bench_hops "
            "fields. Are you reading a summary from a non-instrumented run?"
        )

    return pd.DataFrame(rows)


# ──────────────────────────────────────────────────────────────────────────────
# Binning
# ──────────────────────────────────────────────────────────────────────────────

def bin_series(df: pd.DataFrame, bin_s: float) -> pd.DataFrame:
    """Bin events into time buckets and compute median + p95 per (kind, edge).

    Returns columns: t_s, kind, edge, n, p50, p95.
    """
    out = df.copy()
    out["t_bin"] = (out["recv_t_s"] // bin_s) * bin_s + (bin_s / 2.0)

    g = out.groupby(["t_bin", "kind", "edge"], as_index=False).agg(
        n=("latency_ms", "size"),
        p50=("latency_ms", lambda x: float(np.percentile(x, 50))),
        p95=("latency_ms", lambda x: float(np.percentile(x, 95))),
    )
    g = g.rename(columns={"t_bin": "t_s"}).sort_values(["edge", "t_s"])
    return g


# ──────────────────────────────────────────────────────────────────────────────
# Plotting
# ──────────────────────────────────────────────────────────────────────────────

def plot_kind(binned: pd.DataFrame, kind: str, title: str, out_path: Path):
    """Time-series plot: one line per edge, median ± p95 shading."""
    sub = binned[binned["kind"] == kind]
    if sub.empty:
        print(f"[skip] no data for kind={kind}")
        return

    edges = list(sub["edge"].unique())
    fig, ax = plt.subplots(figsize=(11, 4.2))

    for edge in edges:
        es = sub[sub["edge"] == edge].sort_values("t_s")
        color = color_for(edge)
        ax.plot(es["t_s"], es["p50"], color=color, linewidth=1.6,
                label=f"{edge}  (p50)")
        ax.fill_between(es["t_s"], es["p50"], es["p95"], color=color,
                        alpha=0.15, linewidth=0)

    ax.set_xlabel("time since test start (s)")
    ax.set_ylabel("latency (ms)")
    ax.set_title(title)
    ax.set_ylim(bottom=0)
    ax.legend(loc="upper right", fontsize=8, framealpha=0.9, ncol=2)
    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)
    print(f"[write] {out_path}")


def plot_combined(binned: pd.DataFrame, out_path: Path):
    """One figure, two stacked panels — intra-stage above, inter-stage below.

    Useful when reviewing both at once and comparing absolute scales.
    """
    fig, axes = plt.subplots(2, 1, figsize=(11, 7), sharex=True)

    for ax, kind, title in [
        (axes[0], "intra", "Intra-stage latency (worker process-event call)"),
        (axes[1], "inter", "Inter-stage latency (transport + dispatcher queue)"),
    ]:
        sub = binned[binned["kind"] == kind]
        if sub.empty:
            ax.text(0.5, 0.5, f"no {kind}-stage data", ha="center", va="center",
                    transform=ax.transAxes, fontsize=11, color="gray")
            ax.set_title(title)
            continue

        for edge in sub["edge"].unique():
            es = sub[sub["edge"] == edge].sort_values("t_s")
            color = color_for(edge)
            ax.plot(es["t_s"], es["p50"], color=color, linewidth=1.5,
                    label=f"{edge}")
            ax.fill_between(es["t_s"], es["p50"], es["p95"], color=color,
                            alpha=0.15, linewidth=0)
        ax.set_ylabel("latency (ms)")
        ax.set_title(title)
        ax.set_ylim(bottom=0)
        ax.legend(loc="upper right", fontsize=8, framealpha=0.9, ncol=2)

    axes[-1].set_xlabel("time since test start (s)")
    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)
    print(f"[write] {out_path}")


# ──────────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("summary", type=Path,
                    help="Path to agent_*_summary.json")
    ap.add_argument("--out", type=Path, default=None,
                    help="Output directory (default: <summary's dir>/figures)")
    ap.add_argument("--bin", type=float, default=1.0,
                    help="Bin size in seconds (default: 1.0; use 0.1-0.5 "
                         "for short runs, 5-10 for long runs)")
    args = ap.parse_args()

    if not args.summary.exists():
        sys.exit(f"summary not found: {args.summary}")

    out_dir = args.out or args.summary.parent / "figures"
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"[load] {args.summary}")
    df = load_events(args.summary)
    print(f"[load] {len(df):,} hop measurements "
          f"({(df['kind']=='intra').sum():,} intra, "
          f"{(df['kind']=='inter').sum():,} inter)")
    print(f"       time span: {df['recv_t_s'].min():.2f}s "
          f"→ {df['recv_t_s'].max():.2f}s")

    binned = bin_series(df, args.bin)
    print(f"[bin]  {len(binned):,} bin × edge rows at {args.bin}s bin")

    # Persist binned data for downstream analysis.
    csv_path = out_dir / "stage_latencies_long.csv"
    binned.to_csv(csv_path, index=False)
    print(f"[write] {csv_path}")

    plot_kind(binned, "intra",
              "Intra-stage latency over time (median, shaded to p95)",
              out_dir / "intra_stage_timeseries.pdf")
    plot_kind(binned, "inter",
              "Inter-stage latency over time (median, shaded to p95)",
              out_dir / "inter_stage_timeseries.pdf")
    plot_combined(binned, out_dir / "stage_latencies_combined.pdf")

    print("[done]")


if __name__ == "__main__":
    main()