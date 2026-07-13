#!/usr/bin/env python3
"""
plot_replicas.py — replica count over time: serverfull vs serverless-AOT vs serverless-JIT.

Reads summary JSONs from:
    data/serverfull/*.json
    data/serverless_aot/*.json
    data/serverless_jit/*.json

Produces:
    figures/replicas_comparison.{pdf,png}

Usage:
    python3 plot_replicas.py [--serverfull DIR] [--serverless-aot DIR]
                             [--serverless-jit DIR] [--out DIR]
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np


COLOR_SERVERFULL     = "#d6604d"
COLOR_SERVERLESS_AOT = "#2166ac"
COLOR_SERVERLESS_JIT = "#1b7837"

COLOR_PANEL_BG = "#f4f4f6"
COLOR_GRID     = "#d8d8d8"
COLOR_AXIS     = "#333333"

ALPHA_INDIVIDUAL = 0.15
LW_INDIVIDUAL    = 0.7
LW_MEDIAN        = 2.0

STAGES     = ["normalize", "detect", "finalize"]
STAGE_MAX  = {"normalize": 5, "detect": 5, "finalize": 6}
LNCS_COL_W = 4.8


def setup_style():
    plt.rcParams.update({
        "font.family":        "sans-serif",
        "font.sans-serif":    ["DejaVu Sans", "Arial", "Helvetica"],
        "mathtext.fontset":   "dejavusans",
        "font.size":          8.5,
        "axes.titlesize":     9.5,
        "axes.titleweight":   "bold",
        "axes.titlelocation": "center",
        "axes.titlepad":      5,
        "axes.labelsize":     8.5,
        "xtick.labelsize":    8.0,
        "ytick.labelsize":    8.0,
        "legend.fontsize":    8.5,
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
        "grid.color":         COLOR_GRID,
        "grid.linewidth":     0.5,
        "grid.linestyle":     "-",
        "axes.axisbelow":     True,
        "legend.frameon":     False,
        "figure.dpi":         120,
        "savefig.dpi":        300,
        "savefig.bbox":       "tight",
        "savefig.pad_inches": 0.05,
        "pdf.fonttype":       42,
        "ps.fonttype":        42,
    })


def load_summaries(data_dir: Path) -> list[dict]:
    jsons = sorted(data_dir.glob("*.json"))
    if not jsons:
        raise FileNotFoundError(f"No JSON files in {data_dir}")
    return [json.loads(p.read_text()) for p in jsons]


def extract_step(summary: dict, stage: str) -> tuple[np.ndarray, np.ndarray]:
    """Return (ts, counts) step-function for one stage. Closed at duration."""
    events   = summary["scaling"]["per_stage"].get(stage, {}).get("events", [])
    duration = float(summary.get("duration", 1.0))
    ts     = [0.0]
    counts = [0]
    for ev in events:
        if ev["action"] == "init_jit":
            continue
        ts.append(float(ev["t_s"]))
        counts.append(int(ev["new_count"]))
    ts.append(duration)
    counts.append(counts[-1])
    return np.array(ts), np.array(counts)


def resample_step(ts: np.ndarray, counts: np.ndarray,
                  grid: np.ndarray) -> np.ndarray:
    out = np.empty(len(grid), dtype=float)
    for i, t in enumerate(grid):
        idx = int(np.searchsorted(ts, t, side="right")) - 1
        out[i] = counts[idx] if idx >= 0 else 0.0
    return out


def median_step(
    summaries: list[dict],
    stage: str,
    n_grid: int = 600,
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    steps = [extract_step(s, stage) for s in summaries]
    t_max = max(float(ts[-1]) for ts, _ in steps)
    grid  = np.linspace(0.0, t_max, n_grid)
    matrix = np.stack([resample_step(ts, c, grid) for ts, c in steps])
    return (
        grid,
        np.median(matrix, axis=0),
        np.percentile(matrix, 10, axis=0),
        np.percentile(matrix, 90, axis=0),
    )


def avg_replicas(summary: dict) -> float:
    """Total average replicas across all stages for one run."""
    duration = float(summary.get("duration", 1.0))
    total    = 0.0
    for stage in STAGES:
        ts, counts = extract_step(summary, stage)
        area = sum(counts[i] * (ts[i + 1] - ts[i]) for i in range(len(ts) - 1))
        total += area / duration
    return total


def plot_replicas(
    sf_summaries:     list[dict],
    sl_aot_summaries: list[dict],
    sl_jit_summaries: list[dict],
    out_dir: Path,
):
    setup_style()

    n_stages = len(STAGES)
    fig, axes = plt.subplots(
        n_stages, 1,
        figsize=(LNCS_COL_W, 1.0 + 1.7 * n_stages),
        sharex=True,
        gridspec_kw={"hspace": 0.12},
    )
    fig.patch.set_facecolor("white")

    groups = [
        ("serverfull",     sf_summaries,     COLOR_SERVERFULL),
        ("serverless-AOT", sl_aot_summaries, COLOR_SERVERLESS_AOT),
        ("serverless-JIT", sl_jit_summaries, COLOR_SERVERLESS_JIT),
    ]

    for ax, stage in zip(axes, STAGES):
        ax.set_facecolor(COLOR_PANEL_BG)

        for label, summaries, color in groups:
            for s in summaries:
                ts, counts = extract_step(s, stage)
                ax.step(ts, counts, where="post",
                        color=color, linewidth=LW_INDIVIDUAL,
                        alpha=ALPHA_INDIVIDUAL, zorder=2)

            grid, med, p10, p90 = median_step(summaries, stage)
            ax.fill_between(grid, p10, p90,
                            step="post", color=color,
                            alpha=0.12, linewidth=0, zorder=3)
            ax.step(grid, med, where="post",
                    color=color, linewidth=LW_MEDIAN,
                    alpha=1.0, zorder=4, label=label)

        y_max = STAGE_MAX.get(stage, 8) + 1
        ax.set_ylim(0, y_max)
        ax.yaxis.set_major_locator(ticker.MultipleLocator(2))
        ax.set_ylabel(stage, fontweight="bold", labelpad=4)

    axes[-1].set_xlabel("time (s)")
    axes[-1].xaxis.set_major_locator(ticker.MultipleLocator(60))
    axes[-1].xaxis.set_minor_locator(ticker.MultipleLocator(30))

    # Avg replica annotations in top panel
    ax0 = axes[0]
    sf_avg  = np.mean([avg_replicas(s) for s in sf_summaries])
    aot_avg = np.mean([avg_replicas(s) for s in sl_aot_summaries])
    jit_avg = np.mean([avg_replicas(s) for s in sl_jit_summaries])
    ax0.set_title(
        f"Replica count over time — serverfull vs serverless (AOT / JIT)\n"
        f"avg replicas: sf={sf_avg:.1f}  aot={aot_avg:.1f}  jit={jit_avg:.1f}  "
        f"({len(sf_summaries)} runs each, band=p10–p90)",
        pad=6,
    )
    axes[0].legend(
        loc="upper right",
        handlelength=1.2,
        handletextpad=0.4,
        prop={"weight": "bold"},
    )

    out_dir.mkdir(parents=True, exist_ok=True)
    stem = out_dir / "replicas_comparison"
    for ext in ("pdf", "png"):
        p = str(stem) + f".{ext}"
        fig.savefig(p, facecolor=fig.get_facecolor())
        print(f"Saved -> {p}")
    plt.close(fig)


def main():
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    here = Path(__file__).parent
    ap.add_argument("--serverfull",     type=Path, default=here / "data" / "serverfull")
    ap.add_argument("--serverless-aot", type=Path, default=here / "data" / "serverless_aot")
    ap.add_argument("--serverless-jit", type=Path, default=here / "data" / "serverless_jit")
    ap.add_argument("--out",            type=Path, default=here / "figures")
    args = ap.parse_args()

    print(f"Loading serverfull     : {args.serverfull}")
    sf = load_summaries(args.serverfull)
    print(f"  {len(sf)} summaries")

    print(f"Loading serverless-AOT : {args.serverless_aot}")
    sl_aot = load_summaries(args.serverless_aot)
    print(f"  {len(sl_aot)} summaries")

    print(f"Loading serverless-JIT : {args.serverless_jit}")
    sl_jit = load_summaries(args.serverless_jit)
    print(f"  {len(sl_jit)} summaries")

    print()
    print("Average replicas across all stages:")
    print(f"  serverfull     : {np.mean([avg_replicas(s) for s in sf]):.2f}")
    print(f"  serverless-AOT : {np.mean([avg_replicas(s) for s in sl_aot]):.2f}")
    print(f"  serverless-JIT : {np.mean([avg_replicas(s) for s in sl_jit]):.2f}")
    print()

    plot_replicas(sf, sl_aot, sl_jit, args.out)
    print("done.")


if __name__ == "__main__":
    main()
