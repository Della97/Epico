#!/usr/bin/env python3
"""
plot_tp_distribution.py — throughput distribution across N runs × credit_window.

Reads agent summary JSONs from:

    tp-logs/
      simple-1/    agent_*_summary.json  (×N)
      simple-8/    ...
      simple-16/
      simple-32/
      complex-1/
      complex-8/
      complex-16/
      complex-32/

Each JSON must contain:
    duration                  (float, seconds)
    counters.events_received  (int)

Produces a single figure (PDF + PNG):
    tp_distribution.{pdf,png}  — notched boxplot + weighted-mean diamond,
                                  two pipeline panels, four cw groups each.
                                  Y-axis is broken: upper sub-axis zooms on
                                  high values, lower sub-axis on low values.

Usage:
    python3 plot_tp_distribution.py [--logs DIR] [--out DIR]

Defaults: --logs=./tp-logs  --out=./figures
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import matplotlib.gridspec as gridspec
import numpy as np

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

DEFAULT_LOGS = Path("./tp-logs")
DEFAULT_OUT  = Path("./figures")

CW_ORDER   = ["1", "8", "16", "32"]
PIPELINES  = ["simple", "complex"]
PIPE_TITLE = {"simple": "Simple pipeline", "complex": "Complex pipeline"}

COLOR_CW = {
    "1":  "#1f4e79",
    "8":  "#e08c2e",
    "16": "#d1495b",
    "32": "#5e8a87",
}

COLOR_AXIS      = "#333333"
COLOR_ANNOT     = "#1a1a1a"
COLOR_PANEL_BG  = "#f4f4f6"
COLOR_RATIO_BOX  = "#ffffff"
COLOR_RATIO_EDGE = "#bcbcbc"

LNCS_COL_W = 4.8

# ---------------------------------------------------------------------------
# Broken-axis Y-range config (k ev/s)
# ---------------------------------------------------------------------------
# Per-pipeline upper sub-axis limits  [ylo, yhi]  — span ≈ requested value.
# Per-pipeline lower sub-axis limits  [ylo, yhi].
UPPER_YLIM = {
    "simple":  (37.9, 41.5),   # span ≈ 39
    "complex": (25.5, 28.0),   # span ≈ 26.6
}
LOWER_YLIM = {
    "simple":  (9.3, 9.8),    # span = 9
    "complex": (8.3, 8.7),    # span = 9
}

# Height ratio upper : lower (visual real-estate split)
UPPER_LOWER_RATIO = 2.5


# ---------------------------------------------------------------------------
# Style helpers
# ---------------------------------------------------------------------------

def setup_style() -> None:
    """LNCS single-column, sans-serif, Metapod-style aesthetic."""
    plt.rcParams.update({
        "font.family":        "sans-serif",
        "font.sans-serif":    ["DejaVu Sans", "Arial", "Helvetica"],
        "mathtext.fontset":   "dejavusans",
        "font.size":          8.5,
        "axes.titlesize":     10.0,
        "axes.titleweight":   "bold",
        "axes.titlelocation": "center",
        "axes.titlepad":      8,
        "axes.labelsize":     8.5,
        "axes.labelweight":   "regular",
        "xtick.labelsize":    8.0,
        "ytick.labelsize":    8.0,
        "legend.fontsize":    8.0,
        "legend.title_fontsize": 8.0,
        "axes.edgecolor":     COLOR_AXIS,
        "axes.labelcolor":    COLOR_AXIS,
        "axes.linewidth":     0.6,
        "xtick.color":        COLOR_AXIS,
        "ytick.color":        COLOR_AXIS,
        "xtick.major.width":  0.6,
        "ytick.major.width":  0.6,
        "xtick.major.size":   2.5,
        "ytick.major.size":   2.5,
        "axes.spines.top":    False,
        "axes.spines.right":  False,
        "axes.grid":          True,
        "axes.grid.axis":     "y",
        "grid.color":         "white",
        "grid.linewidth":     0.6,
        "grid.linestyle":     "-",
        "axes.axisbelow":     True,
        "legend.frameon":     False,
        "legend.borderpad":   0.4,
        "figure.dpi":         120,
        "savefig.dpi":        300,
        "savefig.bbox":       "tight",
        "savefig.pad_inches": 0.05,
        "pdf.fonttype":       42,
        "ps.fonttype":        42,
    })


def _tint_panel(ax, color=COLOR_PANEL_BG):
    ax.set_facecolor(color)


def _top_legend(fig, items, y=0.97):
    handles = [
        mpatches.Patch(facecolor=c, edgecolor=c, label=lab, linewidth=0)
        for lab, c in items
    ]
    fig.legend(
        handles=handles, loc="upper center", ncol=len(handles),
        bbox_to_anchor=(0.5, y),
        frameon=False, fontsize=8.5,
        handlelength=1.4, handleheight=1.0,
        handletextpad=0.5, columnspacing=0.8,
        prop={"weight": "bold"},
    )


def _ratio_box(ax, x_frac, y_frac, text, transform=None):
    if transform is None:
        transform = ax.transAxes
    ax.text(
        x_frac, y_frac, text,
        transform=transform, ha="center", va="center",
        fontsize=7.8, color=COLOR_ANNOT, fontweight="bold",
        bbox=dict(facecolor=COLOR_RATIO_BOX, edgecolor=COLOR_RATIO_EDGE,
                  boxstyle="round,pad=0.25", linewidth=0.5),
    )


# ---------------------------------------------------------------------------
# Broken-axis diagonal break marks
# ---------------------------------------------------------------------------

def _draw_break_marks(ax_upper, ax_lower, d=0.012):
    """Draw diagonal zig-zag break marks between two stacked axes."""
    kw_up = dict(transform=ax_upper.transAxes, color=COLOR_AXIS,
                 clip_on=False, linewidth=0.8, zorder=10)
    ax_upper.plot((-d, +d), (-d, +d), **kw_up)
    ax_upper.plot((1 - d, 1 + d), (-d, +d), **kw_up)

    kw_lo = dict(transform=ax_lower.transAxes, color=COLOR_AXIS,
                 clip_on=False, linewidth=0.8, zorder=10)
    ax_lower.plot((-d, +d), (1 - d, 1 + d), **kw_lo)
    ax_lower.plot((1 - d, 1 + d), (1 - d, 1 + d), **kw_lo)


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def extract_sample(path: Path) -> tuple[float, int] | None:
    try:
        with path.open() as f:
            d = json.load(f)
    except Exception as e:
        print(f"  warning: could not read {path}: {e}")
        return None

    duration = d.get("duration", 0.0)
    recv     = int((d.get("counters") or {}).get("events_received", 0))
    tp_block = d.get("throughput") or {}

    if duration > 0 and recv > 0:
        eps = recv / duration
    else:
        eps = tp_block.get("sustained_eps", 0.0)

    if eps <= 0:
        print(f"  warning: zero throughput in {path.name}, skipping")
        return None
    return eps / 1_000, recv


def load_all(logs_dir: Path) -> dict[str, dict[str, list[tuple[float, int]]]]:
    data: dict[str, dict[str, list[tuple[float, int]]]] = {
        p: {cw: [] for cw in CW_ORDER} for p in PIPELINES
    }
    for pipeline in PIPELINES:
        for cw in CW_ORDER:
            folder = logs_dir / f"{pipeline}-{cw}"
            if not folder.exists():
                print(f"  warning: missing {folder}")
                continue
            jsons = sorted(folder.glob("agent_*_summary.json"))
            if not jsons:
                print(f"  warning: no summary JSONs in {folder}")
                continue
            for j in jsons:
                s = extract_sample(j)
                if s is not None:
                    data[pipeline][cw].append(s)
            n = len(data[pipeline][cw])
            if n:
                keps_arr = np.array([s[0] for s in data[pipeline][cw]])
                mu = float(np.mean(keps_arr))
                sd = float(np.std(keps_arr, ddof=1)) if n > 1 else 0.0
                print(f"  {pipeline:>7}/cw={cw:>2}  n={n:>2}  "
                      f"mean={mu:.2f}  std={sd:.2f}  k ev/s")
    return data


# ---------------------------------------------------------------------------
# Core drawing: paint one pipeline column onto (ax_up, ax_lo)
# ---------------------------------------------------------------------------

def _draw_pipeline_column(ax_up, ax_lo, pipeline, data):
    """Draw notched boxplots + weighted-mean diamonds on a broken-Y column."""
    positions = list(range(len(CW_ORDER)))
    width = 0.28
    means: dict[str, float] = {}

    for ax in (ax_up, ax_lo):
        _tint_panel(ax)

    for pos, cw in zip(positions, CW_ORDER):
        samples = data[pipeline][cw]
        color   = COLOR_CW[cw]
        if not samples:
            continue

        keps_arr   = np.array([s[0] for s in samples])
        events_arr = np.array([s[1] for s in samples], dtype=float)

        bp_kw = dict(
            positions=[pos], widths=width * 1.8,
            notch=True, bootstrap=2000,
            patch_artist=True, showfliers=True,
            medianprops=dict(color=color, linewidth=1.8,
                             solid_capstyle="butt"),
            boxprops=dict(facecolor=color, edgecolor=color,
                          linewidth=0.9, alpha=0.55),
            whiskerprops=dict(color=color, linewidth=0.9),
            capprops=dict(color=color, linewidth=0.9),
            flierprops=dict(marker="o", markersize=2.5,
                            markerfacecolor=color,
                            markeredgecolor=color, alpha=0.6),
            zorder=3,
        )

        # Draw on both axes — matplotlib clips to each axis's ylim automatically
        for ax in (ax_up, ax_lo):
            bp = ax.boxplot(keps_arr, **bp_kw)
            for patch in bp["boxes"]:
                patch.set_facecolor(color)
                patch.set_alpha(0.55)
                patch.set_edgecolor(color)
                patch.set_linewidth(0.9)

        # Weighted mean
        wsum  = events_arr.sum()
        wmean = (float(np.average(keps_arr, weights=events_arr))
                 if wsum > 0 else float(np.mean(keps_arr)))
        means[cw] = wmean

        # Diamond on both axes
        for ax in (ax_up, ax_lo):
            ax.plot([pos], [wmean], marker="D", markersize=5,
                    markerfacecolor="white", markeredgecolor=color,
                    markeredgewidth=1.2, zorder=5, linestyle="none")

        # Annotate weighted-mean label on the upper axis only
        q1, q3 = np.percentile(keps_arr, [25, 75])
        iqr = q3 - q1
        wt_candidate = q3 + 1.5 * iqr
        in_range = keps_arr[keps_arr <= wt_candidate]
        whisker_top = (float(np.max(in_range)) if in_range.size else float(q3))
        ax_up.annotate(
            f"{wmean:.1f}",
            xy=(pos, whisker_top), xytext=(0, 5),
            textcoords="offset points",
            ha="center", va="bottom",
            fontsize=7.8, color=color, fontweight="bold",
        )

    # --- upper sub-axis appearance ---
    ax_up.set_xticks(positions)
    ax_up.set_xticklabels([])           # labels only on lower axis
    ax_up.set_xlim(-0.55, len(CW_ORDER) - 1 + 1.05)
    ax_up.set_ylim(*UPPER_YLIM[pipeline])
    ax_up.spines["bottom"].set_visible(False)
    ax_up.tick_params(bottom=False)
    ax_up.set_title(PIPE_TITLE[pipeline], color=COLOR_ANNOT, pad=8)

    # Ratio callout
    if len(means) >= 2:
        mx = max(means.values())
        mn = min(means.values())
        if mn > 0:
            _ratio_box(ax_up, 0.5, 0.93, f"{mx / mn:.1f}\u00d7 range")

    # --- lower sub-axis appearance ---
    ax_lo.set_xticks(positions)
    ax_lo.set_xticklabels([f"cw = {c}" for c in CW_ORDER],
                          fontweight="bold", fontsize=7.5)
    ax_lo.set_xlim(-0.55, len(CW_ORDER) - 1 + 1.05)
    ax_lo.set_ylim(*LOWER_YLIM[pipeline])
    ax_lo.spines["top"].set_visible(False)

    # Diagonal break marks at the axis junction
    _draw_break_marks(ax_up, ax_lo)


# ---------------------------------------------------------------------------
# Main plot builder
# ---------------------------------------------------------------------------

def plot_distribution(data: dict, out_dir: Path) -> None:
    n_pipes = len(PIPELINES)
    R = UPPER_LOWER_RATIO

    fig = plt.figure(figsize=(LNCS_COL_W * 1.75, 4.4))

    outer_gs = gridspec.GridSpec(
        1, n_pipes,
        figure=fig,
        wspace=0.18,
        left=0.10, right=0.97,
        top=0.80, bottom=0.10,
    )

    ax_up_list, ax_lo_list = [], []
    for col_idx, pipeline in enumerate(PIPELINES):
        inner = gridspec.GridSpecFromSubplotSpec(
            2, 1,
            subplot_spec=outer_gs[col_idx],
            hspace=0.06,
            height_ratios=[R, 1],
        )
        ax_up = fig.add_subplot(inner[0])
        ax_lo = fig.add_subplot(inner[1])
        ax_up_list.append(ax_up)
        ax_lo_list.append(ax_lo)
        _draw_pipeline_column(ax_up, ax_lo, pipeline, data)

    # Shared Y-label on the leftmost column
    ax_up_list[0].set_ylabel("Throughput (k events / s)")
    ax_lo_list[0].set_ylabel("")

    # Suppress duplicate y-tick labels on right panel
    for ax in (ax_up_list[1], ax_lo_list[1]):
        ax.set_ylabel("")

    n_runs = max(
        len(data[p][cw]) for p in PIPELINES for cw in CW_ORDER
        if data[p][cw]
    )
    _top_legend(fig, [(f"cw = {cw}", COLOR_CW[cw]) for cw in CW_ORDER], y=0.985)
    fig.suptitle(
        f"Throughput vs credit window  ({n_runs} runs each)\n"
        f"box = IQR, notch = 95% CI of median, \u25c6 = event-weighted mean",
        y=0.92, fontsize=10.0, fontweight="bold",
    )
    save(fig, out_dir, "tp_distribution")


# ---------------------------------------------------------------------------
# Save helper
# ---------------------------------------------------------------------------

def save(fig: plt.Figure, out_dir: Path, name: str) -> None:
    for ext in ("pdf", "png"):
        p = out_dir / f"{name}.{ext}"
        fig.savefig(p)
        print(f"  wrote {p}")
    plt.close(fig)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--logs", type=Path, default=DEFAULT_LOGS,
                   help="tp-logs root directory (default: ./tp-logs)")
    p.add_argument("--out",  type=Path, default=DEFAULT_OUT,
                   help="Output directory for figures (default: ./figures)")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    if not args.logs.exists():
        sys.exit(f"error: logs dir not found: {args.logs}")
    args.out.mkdir(parents=True, exist_ok=True)

    setup_style()
    data = load_all(args.logs)

    if not any(data[p][cw] for p in PIPELINES for cw in CW_ORDER):
        sys.exit("error: no throughput samples found; nothing to plot")

    plot_distribution(data, args.out)


if __name__ == "__main__":
    main()