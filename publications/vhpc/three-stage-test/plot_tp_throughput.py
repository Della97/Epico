#!/usr/bin/env python3
"""
Metapod throughput sweep — simple vs complex pipeline × credit_window.

Reads agent summary JSONs from a directory layout of the form:

    logs/
      simple_window_1/agent_*_summary.json
      simple_window_16/agent_*_summary.json
      simple_window_32/agent_*_summary.json
      complex_window_1/agent_*_summary.json
      complex_window_16/agent_*_summary.json
      complex_window_32/agent_*_summary.json

Produces two figures, both as PDF + PNG, styled like the rest of the
paper's ablation plots:

    tp_throughput_comparison.{pdf,png}   — bar chart, two pipeline facets
    tp_throughput_timeseries.{pdf,png}   — time-series overlay per pipeline

Visual idioms (matched to the 01/03/05 ablation plots in this repo):
- DejaVu Sans, weighted titles, smaller axis labels
- Restrained palette: deep navy / muted coral / muted teal
- Tinted panel background with white gridlines
- Top-centered legend with colored squares and bold labels
- Suptitle below the legend with a (Metapod — ...) context line
- Bold facet titles per pipeline panel
- Annotated bars with the headline value + secondary metadata

Usage:
    python3 plot_tp_throughput.py [--logs DIR] [--out DIR]

Defaults: --logs=. --out=./figures
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Optional

import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import matplotlib.ticker as mticker
import numpy as np

# --- Defaults ---------------------------------------------------------------

DEFAULT_LOGS_DIR = Path(".")
DEFAULT_OUT_DIR  = Path("./figures")

# Color palette matched to the ablation plots.
COLOR_CW = {
    "1":  "#1f4e79",   # deep navy
    "16": "#d1495b",   # muted coral-red
    "32": "#5e8a87",   # muted teal
}

COLOR_GRID      = "#d8d8d8"
COLOR_AXIS      = "#333333"
COLOR_ANNOT     = "#1a1a1a"
COLOR_NEUTRAL   = "#8a8a8a"
COLOR_PANEL_BG  = "#f4f4f6"

# LNCS text-column width ~12.2 cm = 4.8 in
LNCS_COL_W = 4.8

# Directory-name regex: matches simple_window_1, complex_window_16, etc.
DIR_RE = re.compile(
    r"^(?P<pipeline>simple|complex)_window_(?P<cw>\d+)$",
    re.IGNORECASE,
)

PIPELINE_ORDER = ["simple", "complex"]
PIPELINE_TITLE = {"simple": "Simple pipeline", "complex": "Complex pipeline"}
CW_ORDER = ["1", "16", "32"]


# --- Style ------------------------------------------------------------------

def setup_style() -> None:
    """LNCS single-column, sans-serif, Metapod-style aesthetic."""
    plt.rcParams.update({
        # Typography
        "font.family":       "sans-serif",
        "font.sans-serif":   ["DejaVu Sans", "Arial", "Helvetica"],
        "mathtext.fontset":  "dejavusans",
        "font.size":         8.5,
        "axes.titlesize":    10.0,
        "axes.titleweight":  "bold",
        "axes.titlelocation": "center",
        "axes.titlepad":     8,
        "axes.labelsize":    8.5,
        "axes.labelweight":  "regular",
        "xtick.labelsize":   8.0,
        "ytick.labelsize":   8.0,
        "legend.fontsize":   8.0,
        "legend.title_fontsize": 8.0,
        # Color and lines
        "axes.edgecolor":    COLOR_AXIS,
        "axes.labelcolor":   COLOR_AXIS,
        "axes.linewidth":    0.6,
        "xtick.color":       COLOR_AXIS,
        "ytick.color":       COLOR_AXIS,
        "xtick.major.width": 0.6,
        "ytick.major.width": 0.6,
        "xtick.major.size":  2.5,
        "ytick.major.size":  2.5,
        # Grid and spines — grid drawn in white over the tinted panel bg
        "axes.spines.top":   False,
        "axes.spines.right": False,
        "axes.grid":         True,
        "axes.grid.axis":    "y",
        "grid.color":        "white",
        "grid.linewidth":    0.6,
        "grid.linestyle":    "-",
        "axes.axisbelow":    True,
        # Legend
        "legend.frameon":    False,
        "legend.borderpad":  0.4,
        # Output
        "figure.dpi":        120,
        "savefig.dpi":       300,
        "savefig.bbox":      "tight",
        "savefig.pad_inches": 0.05,
        "pdf.fonttype":      42,
        "ps.fonttype":       42,
    })


def _tint_panel(ax, color: str = COLOR_PANEL_BG) -> None:
    """Apply the standard light tinted background to a single axes."""
    ax.set_facecolor(color)


def _top_legend(fig, items: list[tuple[str, str]], y: float = 0.97) -> None:
    """Top-centered figure legend with colored squares and bold labels.

    items: list of (label, color)
    """
    handles = [
        mpatches.Patch(facecolor=c, edgecolor="none", label=lab)
        for lab, c in items
    ]
    leg = fig.legend(
        handles=handles, loc="upper center",
        bbox_to_anchor=(0.5, y),
        ncol=len(items),
        handlelength=1.0, handleheight=1.0,
        columnspacing=2.0, handletextpad=0.6,
        frameon=False, fontsize=9,
    )
    for txt in leg.get_texts():
        txt.set_fontweight("bold")


def save(fig: plt.Figure, out_dir: Path, name: str) -> None:
    """Emit both PDF (paper) and PNG (preview) versions of a figure."""
    for ext in ("pdf", "png"):
        path = out_dir / f"{name}.{ext}"
        fig.savefig(path)
        print(f"  wrote {path}")
    plt.close(fig)


# --- Extraction -------------------------------------------------------------

def find_summary(dir_path: Path) -> Optional[Path]:
    """Return the most-recent agent summary JSON inside a window directory."""
    candidates = sorted(dir_path.glob("agent_*_summary*.json"))
    if not candidates:
        return None
    return max(candidates, key=lambda p: p.stat().st_mtime)


def load_run(path: Path) -> dict:
    with path.open() as f:
        return json.load(f)


def extract_metrics(data: dict) -> dict:
    """Pull throughput and duration from a summary JSON."""
    duration = data.get("duration", 0.0)
    recv     = (data.get("counters") or {}).get("events_received", 0)
    tp_block = data.get("throughput") or {}

    if duration > 0 and recv > 0:
        raw_eps = recv / duration
    else:
        raw_eps = tp_block.get("sustained_eps", 0.0)

    return {
        "events":          recv,
        "duration_s":      duration,
        "throughput_eps":  raw_eps,
        "throughput_keps": raw_eps / 1_000,
    }


def discover(logs_dir: Path) -> dict[str, dict[str, dict]]:
    """Walk the logs directory and return runs[pipeline][cw] = metrics dict."""
    out: dict[str, dict[str, dict]] = {p: {} for p in PIPELINE_ORDER}
    for sub in sorted(logs_dir.iterdir()):
        if not sub.is_dir():
            continue
        m = DIR_RE.match(sub.name)
        if not m:
            continue
        pipeline = m.group("pipeline").lower()
        cw       = m.group("cw")
        if pipeline not in out or cw not in CW_ORDER:
            continue
        summary = find_summary(sub)
        if summary is None:
            print(f"warning: no summary JSON in {sub}")
            continue
        data    = load_run(summary)
        metrics = extract_metrics(data)
        if metrics["events"] == 0:
            print(f"warning: {summary.name} has events_received=0, skipping")
            continue
        metrics["summary_path"]    = summary
        metrics["recv_per_second"] = data.get("recv_per_second")
        out[pipeline][cw] = metrics
        print(f"  {pipeline:>7}/cw={cw:>2}  "
              f"{metrics['events']:>9,} events  "
              f"{metrics['duration_s']:7.2f} s  "
              f"→ {metrics['throughput_keps']:.2f} k ev/s")
    return out


# --- Plot 1: throughput bar chart -------------------------------------------

def plot_throughput_bars(runs: dict, out_dir: Path) -> None:
    """Two panel facets (simple, complex), three bars per panel (cw=1,16,32).

    Shared y-axis so the two pipelines are directly comparable. Each bar is
    labelled with its throughput on top (bold) and its event count + duration
    below in muted gray, matching the multi-fact bar style of figure 04.
    """
    pipelines = [p for p in PIPELINE_ORDER if runs.get(p)]
    if not pipelines:
        print("error: no pipelines with data; skipping bar plot")
        return

    all_keps = [
        runs[p][cw]["throughput_keps"]
        for p in pipelines for cw in CW_ORDER if cw in runs[p]
    ]
    if not all_keps:
        print("error: no throughput samples; skipping bar plot")
        return
    ymax = max(all_keps)

    fig, axes = plt.subplots(
        1, len(pipelines),
        figsize=(LNCS_COL_W, LNCS_COL_W * 0.70),
        sharey=True,
    )
    if len(pipelines) == 1:
        axes = [axes]

    x = np.arange(len(CW_ORDER))

    for ax, pipeline in zip(axes, pipelines):
        _tint_panel(ax)
        pipeline_runs = runs[pipeline]
        keps   = [pipeline_runs.get(cw, {}).get("throughput_keps", 0.0) for cw in CW_ORDER]
        dur_s  = [pipeline_runs.get(cw, {}).get("duration_s", 0.0) for cw in CW_ORDER]
        events = [pipeline_runs.get(cw, {}).get("events", 0) for cw in CW_ORDER]

        bars = ax.bar(
            x, keps,
            color=[COLOR_CW[cw] for cw in CW_ORDER],
            width=0.62, edgecolor="white", linewidth=0.6, zorder=3,
        )
        for bar, k, d, ev in zip(bars, keps, dur_s, events):
            if k <= 0:
                continue
            h = bar.get_height()
            # Headline value (rounded to whole k ev/s — sub-unit precision
            # would be visual noise at this scale).
            ax.text(
                bar.get_x() + bar.get_width() / 2,
                h + ymax * 0.015,
                f"{k:.1f}",
                ha="center", va="bottom",
                fontsize=8.5, fontweight="bold", color=COLOR_ANNOT,
            )
            # Secondary metadata — total event count in k and run duration.
            ax.text(
                bar.get_x() + bar.get_width() / 2,
                h + ymax * 0.015 + ymax * 0.085,
                f"{ev/1e3:.0f} k ev\n{d:.1f} s",
                ha="center", va="bottom",
                fontsize=6.8, color=COLOR_NEUTRAL,
            )

        ax.set_xticks(x)
        ax.set_xticklabels([f"cw={c}" for c in CW_ORDER])
        ax.set_title(PIPELINE_TITLE[pipeline])
        ax.set_ylim(0, ymax * 1.40)
        ax.yaxis.set_major_formatter(mticker.FormatStrFormatter("%.0f"))

    axes[0].set_ylabel("Throughput (k events / s)")

    # Top-centered colored-square legend with bold labels — same idiom as the
    # AOT/JIT swatches in the reference plots.
    # legend_items = [(f"cw = {cw}", COLOR_CW[cw]) for cw in CW_ORDER]
    # _top_legend(fig, legend_items, y=0.99)

    # Suptitle below the legend with a Metapod context line, also matching
    # the reference plots' (Metapod — Simple pipeline) idiom.
    fig.suptitle(
        "Throughput vs credit window\n(Metapod)",
        y=0.92, fontsize=10.5, fontweight="bold",
    )
    fig.subplots_adjust(top=0.74, bottom=0.16, wspace=0.10)

    save(fig, out_dir, "tp_throughput_comparison")


# --- Plot 2: recv_per_second time-series overlay ----------------------------

def plot_timeseries(runs: dict, out_dir: Path) -> None:
    """One row per pipeline (simple, complex), each with three overlaid
    time-series lines (one per cw)."""
    pipelines = [p for p in PIPELINE_ORDER if runs.get(p)]
    has_ts = any(
        runs[p].get(cw, {}).get("recv_per_second")
        for p in pipelines for cw in CW_ORDER
    )
    if not has_ts:
        print("  no recv_per_second data; skipping time-series plot")
        return

    fig, axes = plt.subplots(
        len(pipelines), 1,
        figsize=(LNCS_COL_W, 1.7 * len(pipelines) + 0.8),
        sharex=True,
    )
    if len(pipelines) == 1:
        axes = [axes]

    for ax, pipeline in zip(axes, pipelines):
        _tint_panel(ax)
        pipeline_runs = runs[pipeline]
        for cw in CW_ORDER:
            r = pipeline_runs.get(cw)
            if not r:
                continue
            rps = r.get("recv_per_second") or []
            if not rps:
                continue
            t = np.arange(len(rps))
            ax.plot(
                t, np.array(rps) / 1_000,
                color=COLOR_CW[cw], linewidth=1.4,
                label=f"cw = {cw}",
            )
        ax.set_title(PIPELINE_TITLE[pipeline])
        ax.set_ylabel("k events / s")

    axes[-1].set_xlabel("Time (s)")

    legend_items = [(f"cw = {cw}", COLOR_CW[cw]) for cw in CW_ORDER]
    _top_legend(fig, legend_items, y=0.99)

    fig.suptitle(
        "Throughput over time\n(Metapod — tp profile)",
        y=0.93, fontsize=10.5, fontweight="bold",
    )
    fig.subplots_adjust(top=0.83, bottom=0.12, hspace=0.45)

    save(fig, out_dir, "tp_throughput_timeseries")


# --- CLI --------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--logs", type=Path, default=DEFAULT_LOGS_DIR,
                   help="Directory containing <pipeline>_window_<cw>/ subdirs "
                        "(default: current dir).")
    p.add_argument("--out", type=Path, default=DEFAULT_OUT_DIR,
                   help="Output directory for figures (default: ./figures).")
    return p.parse_args()


def main() -> None:
    args = parse_args()

    if not args.logs.exists():
        sys.exit(f"error: logs dir not found: {args.logs}")

    args.out.mkdir(parents=True, exist_ok=True)
    setup_style()

    runs = discover(args.logs)
    if not any(runs[p] for p in PIPELINE_ORDER):
        sys.exit(
            f"error: no <pipeline>_window_<cw> directories found under "
            f"{args.logs}. Expected names like simple_window_1, "
            f"complex_window_16, etc."
        )

    plot_throughput_bars(runs, args.out)
    plot_timeseries(runs, args.out)


if __name__ == "__main__":
    main()