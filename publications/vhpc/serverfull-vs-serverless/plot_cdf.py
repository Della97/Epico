#!/usr/bin/env python3
"""
plot_cdf.py — CDF comparison: serverfull vs serverless-AOT vs serverless-JIT.

Reads summary JSONs from:
    data/serverfull/*.json
    data/serverless_aot/*.json
    data/serverless_jit/*.json

Produces:
    figures/cdf_comparison.{pdf,png}

Usage:
    python3 plot_cdf.py [--serverfull DIR] [--serverless-aot DIR]
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


COLOR_SERVERFULL    = "#d6604d"   # warm red
COLOR_SERVERLESS_AOT = "#2166ac"  # cool blue
COLOR_SERVERLESS_JIT = "#1b7837"  # green

COLOR_PANEL_BG = "#f4f4f6"
COLOR_GRID     = "#d8d8d8"
COLOR_AXIS     = "#333333"

ALPHA_INDIVIDUAL = 0.18
ALPHA_MEDIAN     = 1.0
LW_INDIVIDUAL    = 0.8
LW_MEDIAN        = 2.0

LNCS_COL_W = 4.8


def setup_style():
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
        "axes.grid.axis":     "both",
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
        raise FileNotFoundError(f"No JSON files found in {data_dir}")
    return [json.loads(p.read_text()) for p in jsons]


def extract_cdf(summary: dict) -> tuple[np.ndarray, np.ndarray]:
    x = np.array(summary["cdf_x"], dtype=float)
    y = np.array(summary["cdf_y"], dtype=float)
    order = np.argsort(x)
    return x[order], y[order] / 100.0


def median_cdf(
    summaries: list[dict],
    n_grid: int = 1000,
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    cdfs = [extract_cdf(s) for s in summaries]
    x_min = min(float(x[0])  for x, _ in cdfs)
    x_max = max(float(x[-1]) for x, _ in cdfs)
    x_grid = np.logspace(np.log10(max(x_min, 0.01)), np.log10(x_max), n_grid)

    interp_matrix = np.zeros((len(cdfs), n_grid))
    for i, (x, y) in enumerate(cdfs):
        interp_matrix[i] = np.interp(x_grid, x, y, left=0.0, right=1.0)

    return (
        x_grid,
        np.median(interp_matrix, axis=0),
        np.percentile(interp_matrix, 10, axis=0),
        np.percentile(interp_matrix, 90, axis=0),
    )


def percentile_from_cdf(x: np.ndarray, y: np.ndarray, q: float) -> float:
    idx = np.searchsorted(y, q)
    if idx == 0:
        return float(x[0])
    if idx >= len(y):
        return float(x[-1])
    x0, y0 = float(x[idx - 1]), float(y[idx - 1])
    x1, y1 = float(x[idx]),     float(y[idx])
    if y1 == y0:
        return x0
    return x0 + (x1 - x0) * (q - y0) / (y1 - y0)


def _fmt_ms(v: float) -> str:
    if v >= 1000:
        return f"{v / 1000:.2f} s"
    if v >= 10:
        return f"{v:.0f} ms"
    return f"{v:.1f} ms"


def plot_cdf_comparison(
    sf_summaries:      list[dict],
    sl_aot_summaries:  list[dict],
    sl_jit_summaries:  list[dict],
    out_dir: Path,
):
    setup_style()
    fig, ax = plt.subplots(figsize=(LNCS_COL_W * 1.55, 1.75))
    ax.set_facecolor(COLOR_PANEL_BG)
    fig.patch.set_facecolor("white")

    groups = [
        ("serverfull",     sf_summaries,     COLOR_SERVERFULL),
        ("serverless-AOT", sl_aot_summaries, COLOR_SERVERLESS_AOT),
        ("serverless-JIT", sl_jit_summaries, COLOR_SERVERLESS_JIT),
    ]

    # Label y-positions (data coords) for each percentile × group combination.
    # Three non-overlapping bands keep all 9 horizontal labels separated.
    #   band p50  → y ∈ [0.05, 0.23]
    #   band p99  → y ∈ [0.38, 0.56]
    #   band p999 → y ∈ [0.65, 0.83]
    label_y = {
        0.50:  [0.23, 0.14, 0.05],   # [sf, aot, jit]
        0.99:  [0.56, 0.47, 0.38],
        0.999: [0.83, 0.74, 0.65],
    }

    for gi, (label, summaries, color) in enumerate(groups):
        for s in summaries:
            x, y = extract_cdf(s)
            ax.plot(x, y, color=color, linewidth=LW_INDIVIDUAL,
                    alpha=ALPHA_INDIVIDUAL, zorder=2)

        x_grid, med, p10, p90 = median_cdf(summaries)
        ax.fill_between(x_grid, p10, p90,
                        color=color, alpha=0.12, zorder=3, linewidth=0)
        ax.plot(x_grid, med,
                color=color, linewidth=LW_MEDIAN, alpha=ALPHA_MEDIAN,
                zorder=4, label=label)

        for q, ql in [(0.50, "p50"), (0.99, "p99"), (0.999, "p99.9")]:
            v   = percentile_from_cdf(x_grid, med, q)
            y_l = label_y[q][gi]

            # drop line from the dot straight down to just above the label
            ax.plot([v, v], [y_l + 0.025, q],
                    color=color, lw=0.6, ls=(0, (4, 2)),
                    alpha=0.75, zorder=3)

            # dot at the CDF crossing
            ax.scatter([v], [q], s=22, color=color,
                       edgecolor="white", linewidth=0.6, zorder=6)

            # horizontal label at a unique vertical position
            ax.text(v, y_l, f"{ql}={_fmt_ms(v)}",
                    ha="center", va="center",
                    fontsize=7.0, color=color, fontweight="bold",
                    zorder=7,
                    bbox=dict(facecolor="white", edgecolor="none",
                              alpha=0.75, pad=1.2))

    ax.set_xscale("log")
    ax.set_xlabel("end-to-end latency (ms, log scale)")
    ax.set_ylabel("empirical CDF")
    ax.set_ylim(-0.02, 1.05)
    ax.yaxis.set_major_formatter(
        ticker.FuncFormatter(lambda v, _: f"{v:.0%}")
    )
    ax.grid(True, axis="x", which="major", color=COLOR_GRID, linewidth=0.5)
    ax.grid(True, axis="x", which="minor", color=COLOR_GRID,
            linewidth=0.3, alpha=0.6)
    ax.grid(True, axis="y", which="major", color=COLOR_GRID, linewidth=0.5)
    ax.set_axisbelow(True)

    n_sf  = len(sf_summaries)
    n_aot = len(sl_aot_summaries)
    n_jit = len(sl_jit_summaries)
    ax.set_title(
        f"End-to-end latency: serverfull vs serverless (AOT / JIT)\n"
        f"(sf={n_sf}, aot={n_aot}, jit={n_jit} runs; band = p10–p90)",
        pad=8,
    )
    ax.legend(
        loc="upper left",
        handlelength=1.4,
        handletextpad=0.5,
        prop={"weight": "bold"},
    )

    out_dir.mkdir(parents=True, exist_ok=True)
    stem = out_dir / "cdf_comparison"
    for ext in ("pdf", "png"):
        p = str(stem) + f".{ext}"
        fig.savefig(p, facecolor=fig.get_facecolor())
        print(f"Saved -> {p}")
    plt.close(fig)


def print_stats(label: str, summaries: list[dict]):
    x_all = np.concatenate([extract_cdf(s)[0] for s in summaries])
    x_grid = np.logspace(
        np.log10(max(float(x_all.min()), 0.01)),
        np.log10(float(x_all.max())),
        2000,
    )
    interp = [np.interp(x_grid, *extract_cdf(s), left=0.0, right=1.0)
               for s in summaries]
    med = np.median(interp, axis=0)

    p50  = percentile_from_cdf(x_grid, med, 0.50)
    p99  = percentile_from_cdf(x_grid, med, 0.99)
    p999 = percentile_from_cdf(x_grid, med, 0.999)
    print(f"  {label:18s}  n={len(summaries):2d}  "
          f"p50={_fmt_ms(p50):>10}  p99={_fmt_ms(p99):>10}  "
          f"p99.9={_fmt_ms(p999):>10}")


def main():
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    here = Path(__file__).parent
    ap.add_argument("--serverfull",    type=Path, default=here / "data" / "serverfull")
    ap.add_argument("--serverless-aot", type=Path, default=here / "data" / "serverless_aot")
    ap.add_argument("--serverless-jit", type=Path, default=here / "data" / "serverless_jit")
    ap.add_argument("--out",           type=Path, default=here / "figures")
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
    print("Median CDF statistics:")
    print_stats("serverfull",     sf)
    print_stats("serverless-AOT", sl_aot)
    print_stats("serverless-JIT", sl_jit)
    print()

    plot_cdf_comparison(sf, sl_aot, sl_jit, args.out)
    print("done.")


if __name__ == "__main__":
    main()
