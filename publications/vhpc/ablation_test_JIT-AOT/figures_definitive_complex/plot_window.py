#!/usr/bin/env python3
"""
plot_window.py — Metapod run time-series: loadgen throughput + active replicas.

For a given run (default: most recent) in the log directory, plots:
  Panel 1 (top):    Active replicas per stage over time (step chart)
  Panel 2 (bottom): Load-generator throughput (events/s) over time

Loadgen files:           loadgen_YYYYMMDD_HHMMSS.jsonl
Dispatcher files:        dispatcher-dispatch-{stage}_YYYYMMDD_HHMMSS.jsonl
Agent files (run key):   agent_YYYYMMDD_HHMMSS.jsonl

Usage:
  python3 plot_window.py [--log-dir DIR] [--run YYYYMMDD_HHMMSS] [--output-dir DIR]
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

# ── Design tokens (mirrored from plot.py) ────────────────────────────────────

COLOR_AXIS    = "#333333"
COLOR_NEUTRAL = "#8a8a8a"
COLOR_PANEL_BG = "#f4f4f6"

LNCS_COL_W = 4.8

# One color per stage; reuses the AOT blue for the first / primary stage
STAGE_COLORS = ["#1f4e79", "#d1495b", "#2e8b57", "#e07b39", "#6a3d9a"]

PREFERRED_STAGE_ORDER = ["warmup", "normalize", "detect", "finalize"]


def _ordered_stages(stages: list[str]) -> list[str]:
    preferred = [s for s in PREFERRED_STAGE_ORDER if s in stages]
    extras = sorted(s for s in stages if s not in PREFERRED_STAGE_ORDER)
    return preferred + extras


# ── Style (identical to plot.py) ─────────────────────────────────────────────

def setup_style():
    plt.rcParams.update({
        "font.family": "sans-serif",
        "font.sans-serif": ["DejaVu Sans", "Arial", "Helvetica"],
        "mathtext.fontset": "dejavusans",
        "font.size": 8.5,
        "axes.titlesize": 10.0,
        "axes.titleweight": "bold",
        "axes.titlelocation": "center",
        "axes.titlepad": 8,
        "axes.labelsize": 8.5,
        "axes.labelweight": "regular",
        "xtick.labelsize": 8.0,
        "ytick.labelsize": 8.0,
        "legend.fontsize": 8.0,
        "legend.title_fontsize": 8.0,
        "axes.edgecolor": COLOR_AXIS,
        "axes.labelcolor": COLOR_AXIS,
        "axes.linewidth": 0.6,
        "xtick.color": COLOR_AXIS,
        "ytick.color": COLOR_AXIS,
        "xtick.major.width": 0.6,
        "ytick.major.width": 0.6,
        "xtick.major.size": 2.5,
        "ytick.major.size": 2.5,
        "axes.spines.top": False,
        "axes.spines.right": False,
        "axes.grid": True,
        "axes.grid.axis": "y",
        "grid.color": "white",
        "grid.linewidth": 0.6,
        "grid.linestyle": "-",
        "axes.axisbelow": True,
        "legend.frameon": False,
        "legend.borderpad": 0.4,
        "figure.dpi": 120,
        "savefig.dpi": 300,
        "savefig.bbox": "tight",
        "savefig.pad_inches": 0.05,
        "pdf.fonttype": 42,
        "ps.fonttype": 42,
    })


def _tint_panel(ax, color=COLOR_PANEL_BG):
    ax.set_facecolor(color)


def save(fig, out_dir: Path, name: str):
    for ext in ("pdf", "png"):
        path = out_dir / f"{name}.{ext}"
        fig.savefig(path)
        print(f"  wrote {path}")
    plt.close(fig)


# ── File discovery ────────────────────────────────────────────────────────────

AGENT_RE      = re.compile(r"^agent_(\d{8}_\d{6})\.jsonl$")
LOADGEN_RE    = re.compile(r"^loadgen_(\d{8}_\d{6})\.jsonl$")
DISPATCHER_RE = re.compile(r"^dispatcher-dispatch-(\w+)_(\d{8}_\d{6})\.jsonl$")


def _ts_seconds(ts_str: str) -> int:
    """Convert YYYYMMDD_HHMMSS to an integer seconds value for proximity matching."""
    d, t = ts_str.split("_")
    return int(d) * 100000 + int(t)


def find_runs(log_dir: Path) -> list[str]:
    """Return sorted list of run timestamps from agent files."""
    runs = []
    for f in log_dir.iterdir():
        m = AGENT_RE.match(f.name)
        if m:
            runs.append(m.group(1))
    return sorted(runs)


def _find_closest(log_dir: Path, pattern: re.Pattern, run_ts: str,
                  window_s: int = 5) -> Path | None:
    """Find the file matching pattern whose timestamp is within window_s of run_ts."""
    ref = _ts_seconds(run_ts)
    best_path, best_dist = None, window_s + 1
    for f in log_dir.iterdir():
        m = pattern.match(f.name)
        if not m:
            continue
        dist = abs(_ts_seconds(m.group(1)) - ref)
        if dist < best_dist:
            best_dist, best_path = dist, f
    return best_path


# ── Parsing ───────────────────────────────────────────────────────────────────

def parse_loadgen(path: Path) -> pd.DataFrame:
    """
    Parse loadgen JSONL → dataframe columns: ts, t (relative), throughput (ev/s),
    dropped (cumulative), anomalies (cumulative).
    """
    rows = []
    with path.open() as f:
        for line in f:
            try:
                d = json.loads(line)
            except json.JSONDecodeError:
                continue
            if d.get("msg") == "progress":
                rows.append({
                    "ts":        float(d["ts"]),
                    "sent":      int(d["sent"]),
                    "dropped":   int(d.get("dropped", 0)),
                    "anomalies": int(d.get("anomalies", 0)),
                })
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows).sort_values("ts").reset_index(drop=True)
    dt = df["ts"].diff()
    ds = df["sent"].diff()
    df["throughput"] = (ds / dt).where(dt > 0)
    df["t"] = df["ts"] - df["ts"].iloc[0]
    return df


def parse_replicas(log_dir: Path, run_ts: str, t0: float,
                   window_s: int = 5) -> dict[str, pd.DataFrame]:
    """
    For every dispatcher-dispatch-{stage} file belonging to run_ts (±window_s),
    build a step-function dataframe with columns [t, active].
    """
    ref = _ts_seconds(run_ts)
    stage_rows: dict[str, list[dict]] = {}

    for f in sorted(log_dir.iterdir()):
        m = DISPATCHER_RE.match(f.name)
        if not m:
            continue
        stage, fts = m.group(1), m.group(2)
        if abs(_ts_seconds(fts) - ref) > window_s:
            continue

        rows = []
        with f.open() as fh:
            for line in fh:
                try:
                    d = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if d.get("msg") in ("worker connected", "worker gone"):
                    rows.append({
                        "t":      float(d["ts"]) - t0,
                        "active": int(d.get("active", 0)),
                    })
        if rows:
            stage_rows.setdefault(stage, []).extend(rows)

    result = {}
    for stage, rows in stage_rows.items():
        df = pd.DataFrame(rows).sort_values("t").reset_index(drop=True)
        result[stage] = df
    return result


# ── Plot ──────────────────────────────────────────────────────────────────────

def plot_window(log_dir: Path, run_ts: str, out_dir: Path):
    # ── Load data ──────────────────────────────────────────────────────────
    loadgen_path = _find_closest(log_dir, LOADGEN_RE, run_ts)
    if loadgen_path is None:
        print(f"No loadgen file near run {run_ts}", file=sys.stderr)
        return

    lg = parse_loadgen(loadgen_path)
    if lg.empty:
        print("No loadgen progress data found.", file=sys.stderr)
        return

    t0    = lg["ts"].iloc[0]
    t_end = lg["t"].iloc[-1]

    replica_dfs = parse_replicas(log_dir, run_ts, t0)
    stages = _ordered_stages(list(replica_dfs.keys()))
    stage_color = {s: STAGE_COLORS[i % len(STAGE_COLORS)] for i, s in enumerate(stages)}

    # ── Figure layout ──────────────────────────────────────────────────────
    fig, (ax_rep, ax_load) = plt.subplots(
        2, 1,
        figsize=(LNCS_COL_W * 1.5, 4.4),
        sharex=True,
        gridspec_kw={"height_ratios": [1, 1.8], "hspace": 0.08},
    )

    # ── Panel 1: Active replicas per stage ─────────────────────────────────
    _tint_panel(ax_rep)

    for stage in stages:
        df = replica_dfs[stage]
        color = stage_color[stage]

        # Prepend t=0 with 0 active; append t=t_end with last known value
        ts_vals = np.concatenate([[0.0], df["t"].to_numpy(), [t_end]])
        ac_vals = np.concatenate([[0],   df["active"].to_numpy(),
                                   [df["active"].iloc[-1]]])

        ax_rep.step(ts_vals, ac_vals, where="post",
                    color=color, linewidth=1.6, label=stage, alpha=0.9)

    # Total active (sum across all stages at every change event)
    if stages:
        all_events = pd.concat(
            [replica_dfs[s].assign(stage=s) for s in stages],
            ignore_index=True,
        ).sort_values("t").reset_index(drop=True)

        current = {s: 0 for s in stages}
        total_ts = [0.0]
        total_ac = [0]
        for _, ev in all_events.iterrows():
            current[ev["stage"]] = ev["active"]
            total_ts.append(ev["t"])
            total_ac.append(sum(current.values()))
        total_ts.append(t_end)
        total_ac.append(total_ac[-1])

        ax_rep.step(total_ts, total_ac, where="post",
                    color=COLOR_AXIS, linewidth=1.8, linestyle="--",
                    label="total", alpha=0.65, zorder=5)

    ax_rep.set_ylabel("active replicas")
    ax_rep.yaxis.set_major_locator(plt.MaxNLocator(integer=True))
    ax_rep.set_ylim(bottom=0)
    ax_rep.grid(True, axis="x", color="white", linewidth=0.5)
    ax_rep.grid(True, axis="y", color="white", linewidth=0.6)
    ax_rep.set_axisbelow(True)
    ax_rep.legend(
        loc="upper right", ncol=len(stages) + 1,
        fontsize=7.5, frameon=False,
        handlelength=1.6, handletextpad=0.5, columnspacing=1.2,
    )

    # ── Panel 2: Load-generator throughput ────────────────────────────────
    _tint_panel(ax_load)

    color_load = "#1f4e79"
    valid = lg.dropna(subset=["throughput"])

    ax_load.fill_between(valid["t"], 0, valid["throughput"],
                         color=color_load, alpha=0.15, step="mid")
    ax_load.step(valid["t"], valid["throughput"], where="mid",
                 color=color_load, linewidth=1.4, alpha=0.9, label="events/s")

    # Dropped events on secondary y-axis (only if any drops occurred)
    if int(lg["dropped"].max()) > 0:
        ax2 = ax_load.twinx()
        ax2.plot(lg["t"], lg["dropped"],
                 color="#d1495b", linewidth=1.0, linestyle=":",
                 alpha=0.85, label="dropped (cum.)")
        ax2.set_ylabel("dropped (cum.)", fontsize=7.5, color="#d1495b")
        ax2.tick_params(axis="y", colors="#d1495b", labelsize=7.5)
        ax2.spines["right"].set_visible(True)
        ax2.spines["right"].set_color("#d1495b")
        ax2.spines["top"].set_visible(False)

    ax_load.set_ylabel("events / s")
    ax_load.set_xlabel("time (s)")
    ax_load.set_xlim(0, t_end)
    ax_load.set_ylim(bottom=0)
    ax_load.grid(True, axis="x", color="white", linewidth=0.5)
    ax_load.grid(True, axis="y", color="white", linewidth=0.6)
    ax_load.set_axisbelow(True)

    # ── Title ─────────────────────────────────────────────────────────────
    date_part = run_ts[:8]
    hh, mm, ss = run_ts[9:11], run_ts[11:13], run_ts[13:15]
    fig.suptitle(
        f"Load & replicas — {date_part[:4]}-{date_part[4:6]}-{date_part[6:]}  "
        f"{hh}:{mm}:{ss}\n(Metapod - Three-stage pipeline)",
        fontsize=10, fontweight="bold", y=0.99,
    )

    fig.tight_layout(rect=(0, 0, 1, 0.95))
    save(fig, out_dir, f"window_{run_ts}")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--log-dir", type=Path,
        default=Path("/home/ubuntu/git/metapod/examples/three-stage-test/logs"),
        help="Directory containing agent/loadgen/dispatcher JSONL files",
    )
    parser.add_argument(
        "--run", type=str, default=None,
        help="Run timestamp YYYYMMDD_HHMMSS (default: most recent)",
    )
    parser.add_argument(
        "--output-dir", type=Path, default=None,
        help="Output directory for PDF/PNG (default: <log-dir>/../figures)",
    )
    args = parser.parse_args()

    log_dir = args.log_dir
    runs = find_runs(log_dir)
    if not runs:
        print(f"No agent files found in {log_dir}", file=sys.stderr)
        sys.exit(1)

    run_ts = args.run if args.run else runs[-1]
    if run_ts not in runs:
        print(f"Run '{run_ts}' not found.\nAvailable runs:\n  " +
              "\n  ".join(runs), file=sys.stderr)
        sys.exit(1)

    out_dir = args.output_dir or (log_dir.parent / "figures")
    out_dir.mkdir(parents=True, exist_ok=True)

    setup_style()
    print(f"Plotting run {run_ts}  →  {out_dir}")
    plot_window(log_dir, run_ts, out_dir)
    print("done.")


if __name__ == "__main__":
    main()
