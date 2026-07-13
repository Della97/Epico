#!/usr/bin/env python3
"""
plot_combined.py — combined plots from a separate warm run and a separate cold run.

Reads:
    <warm_dir>/parents.csv            (loadgen output)
    <cold_dir>/parents.csv            (cold-probe loadgen output)
    <warm_dir>/children/*.json        (optional, from fetch_children.sh)
    <cold_dir>/children/*.json        (optional, from fetch_children.sh)

Writes (in --output-dir):
    01_warm_cdf.{pdf,png}             warm e2e CDF with p50/p95/p99 callouts
    02_cold_stages.{pdf,png}          per-stage cold init breakdown

Usage:
    python3 plot_combined.py WARM_DIR COLD_DIR [--output-dir DIR]

Aesthetic matches plot_comb.py (deep navy warm, coral cold, LNCS column width).
"""
from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np

# ---------------------------------------------------------------------------
# Palette and style — matched to plot_comb.py
# ---------------------------------------------------------------------------

COLOR_WARM = "#1f4e79"
COLOR_COLD = "#d1495b"
COLOR_GRID = "#e8e8e8"
COLOR_AXIS = "#404040"
COLOR_ANNOT = "#2a2a2a"

STAGE_ORDER = ["normalize", "detect", "finalize"]
STAGE_MAP = {"normalize_rs": "normalize", "detect_rs": "detect", "finalize_rs": "finalize"}


def setup_style():
    plt.rcParams.update({
        "font.family": "DejaVu Sans",
        "font.size": 10,
        "axes.titlesize": 11.5,
        "axes.titleweight": "semibold",
        "axes.titlelocation": "left",
        "axes.titlepad": 10,
        "axes.labelsize": 10,
        "xtick.labelsize": 9.5,
        "ytick.labelsize": 9.5,
        "legend.fontsize": 9.5,
        "axes.edgecolor": COLOR_AXIS,
        "axes.labelcolor": COLOR_AXIS,
        "axes.linewidth": 0.8,
        "xtick.color": COLOR_AXIS,
        "ytick.color": COLOR_AXIS,
        "xtick.major.width": 0.8,
        "ytick.major.width": 0.8,
        "xtick.major.size": 3,
        "ytick.major.size": 3,
        "axes.spines.top": False,
        "axes.spines.right": False,
        "axes.grid": True,
        "axes.grid.axis": "y",
        "grid.color": COLOR_GRID,
        "grid.linewidth": 0.7,
        "grid.linestyle": "-",
        "axes.axisbelow": True,
        "legend.frameon": False,
        "figure.dpi": 110,
        "savefig.dpi": 240,
        "savefig.bbox": "tight",
        "savefig.pad_inches": 0.15,
        "pdf.fonttype": 42,
        "ps.fonttype": 42,
    })


def save(fig, out_dir: Path, name: str):
    for ext in ("pdf", "png"):
        path = out_dir / f"{name}.{ext}"
        fig.savefig(path)
        print(f"  wrote {path}")
    plt.close(fig)


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_e2e_from_parents(parents_csv: Path, only_main: bool = True) -> np.ndarray:
    """Load e2e_ms from a parents.csv. Skips prewarm rows and failures."""
    vals = []
    with open(parents_csv) as f:
        for r in csv.DictReader(f):
            if only_main and r.get("is_prewarm") == "1":
                continue
            if r.get("success") != "true":
                continue
            try:
                vals.append(float(r["e2e_ms"]))
            except (KeyError, ValueError):
                continue
    return np.array(vals)


def load_stage_inits(run_dir: Path) -> dict:
    """Returns dict: short_stage_name -> list of init_ms (>0 only)."""
    out = {s: [] for s in STAGE_ORDER}
    children_dir = run_dir / "children"
    if not children_dir.exists():
        return out
    for jf in children_dir.glob("*.json"):
        try:
            b = json.loads(jf.read_text())
        except Exception:
            continue
        for c in b.get("children", []):
            short = STAGE_MAP.get(c.get("name", ""))
            if not short:
                continue
            for ann in c.get("annotations", []) or []:
                if ann.get("key") == "initTime":
                    v = float(ann.get("value", 0.0))
                    if v > 0.0:
                        out[short].append(v)
                    break
    return out


# ---------------------------------------------------------------------------
# Plot 1: warm e2e CDF
# ---------------------------------------------------------------------------

def plot_warm_cdf(warm: np.ndarray, out_dir: Path):
    if warm.size == 0:
        print("  no warm samples; skipping warm CDF")
        return

    xs = np.sort(warm)
    ys = np.arange(1, xs.size + 1) / xs.size

    fig, ax = plt.subplots(figsize=(6.5, 3.6))

    ax.step(
        np.concatenate([[xs[0]], xs]),
        np.concatenate([[0], ys]),
        where="post",
        color=COLOR_WARM,
        linewidth=2.0,
        solid_capstyle="round",
    )

    for q, label in [(0.5, "p50"), (0.95, "p95"), (0.99, "p99")]:
        v = xs[min(xs.size - 1, int(xs.size * q))]
        ax.scatter([v], [q], s=44, color=COLOR_WARM,
                   edgecolor="white", linewidth=1.0, zorder=5)
        ax.annotate(
            f"{label} = {v:.1f} ms",
            xy=(v, q), xytext=(8, -2),
            textcoords="offset points",
            ha="left", va="top",
            fontsize=9, color=COLOR_ANNOT, fontweight="semibold",
        )

    for q in (0.5, 0.95):
        ax.axhline(q, color=COLOR_GRID, linewidth=0.7, zorder=0)

    ax.set_xscale("log")
    ax.set_xlabel("end-to-end latency (ms, log scale)")
    ax.set_ylabel("empirical CDF")
    ax.set_ylim(-0.02, 1.05)
    ax.grid(True, axis="x", which="major", color=COLOR_GRID, linewidth=0.6)
    ax.grid(True, axis="x", which="minor", color=COLOR_GRID, linewidth=0.4, alpha=0.5)
    ax.set_axisbelow(True)

    fig.suptitle(
        f"Warm pipeline end-to-end latency (n = {xs.size})",
        fontsize=12.5, fontweight="semibold",
        x=0.06, y=1.0, ha="left",
    )
    fig.tight_layout()
    save(fig, out_dir, "01_warm_cdf")


# ---------------------------------------------------------------------------
# Plot 2: per-stage cold init (half-violin + jitter + median)
# ---------------------------------------------------------------------------

def plot_cold_stages(stage_inits: dict, out_dir: Path):
    stages_with_data = [s for s in STAGE_ORDER if stage_inits.get(s)]
    if not stages_with_data:
        print("  no cold-stage samples; skipping cold_stages")
        return

    fig, ax = plt.subplots(figsize=(6.5, 4.0))

    for i, stage in enumerate(STAGE_ORDER):
        vals = np.array(stage_inits.get(stage, []))
        if vals.size == 0:
            continue

        parts = ax.violinplot(
            vals, positions=[i], widths=0.7, showmeans=False,
            showmedians=False, showextrema=False,
        )
        for pc in parts["bodies"]:
            m = np.mean(pc.get_paths()[0].vertices[:, 0])
            pc.get_paths()[0].vertices[:, 0] = np.clip(
                pc.get_paths()[0].vertices[:, 0], m, np.inf
            )
            pc.set_facecolor(COLOR_COLD)
            pc.set_edgecolor(COLOR_COLD)
            pc.set_alpha(0.25)
            pc.set_linewidth(0)

        rng = np.random.default_rng(42 + i)
        x_jitter = i - 0.05 - rng.uniform(0, 0.22, size=vals.size)
        ax.scatter(
            x_jitter, vals,
            s=22, color=COLOR_COLD, alpha=0.7,
            edgecolor="white", linewidth=0.6, zorder=3,
        )

        med = np.median(vals)
        ax.hlines(med, i - 0.32, i + 0.32,
                  color=COLOR_COLD, linewidth=2.2, zorder=4)
        ax.annotate(
            f"{med/1000:.2f} s",
            xy=(i + 0.34, med),
            xytext=(4, 0), textcoords="offset points",
            ha="left", va="center",
            fontsize=9, color=COLOR_COLD, fontweight="semibold",
        )

    n_total = sum(len(stage_inits.get(s, [])) for s in STAGE_ORDER)
    ax.set_xticks(range(len(STAGE_ORDER)))
    ax.set_xticklabels(STAGE_ORDER)
    ax.set_xlim(-0.6, len(STAGE_ORDER) - 0.4)
    ax.set_ylabel("cold init time per stage (ms)")
    ax.set_xlabel("")

    all_vals = np.concatenate([np.array(stage_inits.get(s, [0])) for s in STAGE_ORDER])
    if all_vals.max() > 0:
        ax.set_ylim(0, all_vals.max() * 1.15)

    fig.suptitle(
        f"Per-stage cold initialization (n = {n_total} stage observations)",
        fontsize=12.5, fontweight="semibold",
        x=0.06, y=1.0, ha="left",
    )
    fig.tight_layout()
    save(fig, out_dir, "02_cold_stages")


# ---------------------------------------------------------------------------
# Stats summary
# ---------------------------------------------------------------------------

def print_summary(warm: np.ndarray, cold: np.ndarray, stage_inits: dict):
    def pct(xs, q):
        if len(xs) == 0:
            return float("nan")
        xs = np.sort(xs)
        return float(xs[min(len(xs) - 1, int(len(xs) * q))])

    print()
    print("Warm e2e (ms):")
    if warm.size:
        print(f"  n={warm.size}  p50={pct(warm,0.5):.2f}  p95={pct(warm,0.95):.2f}  "
              f"p99={pct(warm,0.99):.2f}  max={warm.max():.2f}")
    else:
        print("  no warm samples")

    print()
    print("Cold e2e (ms):")
    if cold.size:
        print(f"  n={cold.size}  p50={pct(cold,0.5):.0f}  p95={pct(cold,0.95):.0f}  "
              f"max={cold.max():.0f}")
    else:
        print("  no cold samples")

    print()
    print("Per-stage cold init (ms):")
    for stage in STAGE_ORDER:
        vals = np.array(stage_inits.get(stage, []))
        if vals.size:
            print(f"  {stage:10}  n={vals.size:3}  "
                  f"p50={pct(vals,0.5):.0f}  p95={pct(vals,0.95):.0f}  max={vals.max():.0f}")
        else:
            print(f"  {stage:10}  n=0")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("warm_dir", type=Path, help="warm run dir (with parents.csv)")
    ap.add_argument("cold_dir", type=Path, help="cold run dir (with parents.csv)")
    ap.add_argument("--output-dir", type=Path, default=Path("figures_combined"),
                    help="output dir (default: ./figures_combined)")
    args = ap.parse_args()

    warm_pc = args.warm_dir / "parents.csv"
    cold_pc = args.cold_dir / "parents.csv"
    if not warm_pc.exists():
        print(f"missing {warm_pc}", file=sys.stderr); sys.exit(1)
    if not cold_pc.exists():
        print(f"missing {cold_pc}", file=sys.stderr); sys.exit(1)

    out_dir = args.output_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    setup_style()
    warm = load_e2e_from_parents(warm_pc)
    cold = load_e2e_from_parents(cold_pc)

    # Per-stage cold inits come from the cold run's children (if available).
    stage_inits = load_stage_inits(args.cold_dir)
    # If cold children weren't fetched, fall back to warm children that recorded
    # any nonzero init (rare, but covers the case where the warm run captured
    # the very first activations before everything was hot).
    if not any(stage_inits.values()):
        stage_inits = load_stage_inits(args.warm_dir)

    print(f"Warm dir: {args.warm_dir}")
    print(f"Cold dir: {args.cold_dir}")
    print(f"Output:   {out_dir}")
    print()
    print(f"Loaded: warm n={warm.size}, cold n={cold.size}")

    plot_warm_cdf(warm, out_dir)
    plot_cold_stages(stage_inits, out_dir)

    print_summary(warm, cold, stage_inits)
    print()
    print("done.")


if __name__ == "__main__":
    main()