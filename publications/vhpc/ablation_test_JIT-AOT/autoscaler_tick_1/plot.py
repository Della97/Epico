#!/usr/bin/env python3
"""
Metapod AOT vs JIT ablation — redesigned plots.

Aesthetic principles:
- Clean typography (DejaVu Sans, weighted titles, smaller axis labels)
- Restrained palette: deep blue for AOT, warm coral for JIT, neutral grays elsewhere
- No box plots where the data is small enough to show every point honestly
- Annotated medians and ratios — readers shouldn't have to squint
- Consistent margins, gridline weights, and panel sizing across figures
- Both PDF (vector, paper) and PNG (preview) outputs

Usage: python3 plot_ablation_v2.py [--input-dir DIR] [--output-dir DIR]
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
import numpy as np
import pandas as pd
import seaborn as sns

# --- Defaults ---------------------------------------------------------------

DEFAULT_INPUT_DIR = Path(__file__).parent
DEFAULT_OUTPUT_DIR = DEFAULT_INPUT_DIR / "figures"

FILENAME_RE = re.compile(
    r"agent_(?P<ts>\d{8}_\d{6})_summary-(?P<mode>AOT|JIT)-(?P<iter>\d+)\.json$"
)

# Restrained, complementary palette. AOT is the "good" baseline (deep blue,
# trustworthy); JIT is the "hot" treatment (warm coral); accents in muted gray.
COLOR_AOT = "#1f4e79"   # deep navy
COLOR_JIT = "#d1495b"   # muted coral-red
COLOR_GRID = "#e8e8e8"
COLOR_AXIS = "#404040"
COLOR_ANNOT = "#2a2a2a"
COLOR_NEUTRAL = "#8a8a8a"

MODE_PALETTE = {"AOT": COLOR_AOT, "JIT": COLOR_JIT}
MODE_ORDER = ["AOT", "JIT"]
PREFERRED_STAGE_ORDER = ["warmup", "normalize", "detect", "finalize"]


def ordered_stages_present(df: pd.DataFrame) -> list[str]:
    """Return stage order limited to stages that actually appear in data.

    Uses PREFERRED_STAGE_ORDER for legacy pipeline stages when present;
    falls back to alphabetical for unknown stage names. Either way, the
    returned list contains *exactly* the stages present in the data, so
    callers can iterate it as their canonical stage axis.
    """
    if df.empty or "stage" not in df.columns:
        return []
    present = sorted(df["stage"].dropna().astype(str).unique().tolist())
    # Preferred-first, then anything else in alphabetical order.
    preferred = [s for s in PREFERRED_STAGE_ORDER if s in present]
    extras = [s for s in present if s not in PREFERRED_STAGE_ORDER]
    return preferred + extras


# --- Extraction (unchanged from v1) -----------------------------------------


def extract_run(path: Path) -> Optional[dict]:
    m = FILENAME_RE.search(path.name)
    if not m:
        return None
    mode, iteration = m.group("mode"), int(m.group("iter"))
    try:
        with path.open() as f:
            data = json.load(f)
    except Exception as e:
        print(f"  skipping {path.name}: {e}", file=sys.stderr)
        return None

    record = {
        "file": path.name, "mode": mode, "iteration": iteration,
        "p99_e2e_ms":   data.get("p99"),
        "p999_e2e_ms":  data.get("p999"),
        "median_e2e_ms": data.get("p50"),
        "max_e2e_ms":   data.get("max"),
        "duration_s":   data.get("duration"),
        "_summary_path": str(path),  # keep for time-series replay
        "stages": {},
    }

    for stage, payload in data.get("scaling", {}).get("per_stage", {}).items():
        events = payload.get("events", [])
        init_event = next(
            (e for e in events if e.get("action", "").startswith("init_")), None
        )
        compile_ms = init_event.get("compile_ms") if init_event else None
        instantiate_pre_ms = init_event.get("instantiate_pre_ms") if init_event else None

        cold_events = sorted(
            [e for e in events if e.get("action") == "cold_start"],
            key=lambda e: e.get("t_s", 0),
        )
        cold_ms = [e["cold_start_ms"] for e in cold_events
                   if e.get("cold_start_ms") is not None]

        record["stages"][stage] = {
            "compile_ms": compile_ms,
            "instantiate_pre_ms": instantiate_pre_ms,
            "first_cold_start_ms": cold_ms[0] if cold_ms else None,
            "warm_cold_starts_ms": cold_ms[1:] if len(cold_ms) > 1 else [],
            "n_cold_starts": len(cold_ms),
        }
    return record


def build_dataframes(runs: list[dict]) -> tuple[pd.DataFrame, pd.DataFrame]:
    rows_first, rows_warm, rows_run = [], [], []
    for r in runs:
        rows_run.append({
            "mode": r["mode"], "iteration": r["iteration"],
            "p99_e2e_ms":    r["p99_e2e_ms"],
            "p999_e2e_ms":   r["p999_e2e_ms"],
            "median_e2e_ms": r["median_e2e_ms"],
            "max_e2e_ms":    r.get("max_e2e_ms"),
            "_summary_path": r.get("_summary_path"),
        })
        for stage, s in r["stages"].items():
            if s["first_cold_start_ms"] is not None:
                rows_first.append({
                    "mode": r["mode"], "iteration": r["iteration"], "stage": stage,
                    "cold_start_ms": s["first_cold_start_ms"],
                    "compile_ms": s["compile_ms"],
                    "instantiate_pre_ms": s["instantiate_pre_ms"],
                    "spawn_kind": "first",
                })
            for w in s["warm_cold_starts_ms"]:
                rows_warm.append({
                    "mode": r["mode"], "iteration": r["iteration"], "stage": stage,
                    "cold_start_ms": w,
                    "spawn_kind": "warm",
                })
    df_cold = pd.concat([pd.DataFrame(rows_first), pd.DataFrame(rows_warm)],
                        ignore_index=True)
    df_run = pd.DataFrame(rows_run)
    return df_cold, df_run


def load_long_csv(path: Path) -> pd.DataFrame:
    """Fallback: read pre-computed long-form CSV if no JSON dir is given."""
    df = pd.read_csv(path)
    df["spawn_kind"] = df["spawn_kind"].replace({
        "first (scale-from-zero)": "first",
        "warm (re-spawn)": "warm",
    })
    return df


# --- Style ------------------------------------------------------------------

def setup_style():
    """Tighter, more polished defaults than seaborn's stock 'paper' theme."""
    plt.rcParams.update({
        # Typography
        "font.family": "DejaVu Sans",
        "font.size": 10,
        "axes.titlesize": 11.5,
        "axes.titleweight": "semibold",
        "axes.titlelocation": "left",
        "axes.titlepad": 10,
        "axes.labelsize": 10,
        "axes.labelweight": "regular",
        "xtick.labelsize": 9.5,
        "ytick.labelsize": 9.5,
        "legend.fontsize": 9.5,
        "legend.title_fontsize": 9.5,
        # Color and lines
        "axes.edgecolor": COLOR_AXIS,
        "axes.labelcolor": COLOR_AXIS,
        "axes.linewidth": 0.8,
        "xtick.color": COLOR_AXIS,
        "ytick.color": COLOR_AXIS,
        "xtick.major.width": 0.8,
        "ytick.major.width": 0.8,
        "xtick.major.size": 3,
        "ytick.major.size": 3,
        # Grid and spines
        "axes.spines.top": False,
        "axes.spines.right": False,
        "axes.grid": True,
        "axes.grid.axis": "y",
        "grid.color": COLOR_GRID,
        "grid.linewidth": 0.7,
        "grid.linestyle": "-",
        "axes.axisbelow": True,
        # Legend
        "legend.frameon": False,
        "legend.borderpad": 0.4,
        # Output
        "figure.dpi": 110,
        "savefig.dpi": 240,
        "savefig.bbox": "tight",
        "savefig.pad_inches": 0.15,
        "pdf.fonttype": 42,   # editable text in PDF
        "ps.fonttype": 42,
    })


def save(fig, out_dir: Path, name: str):
    for ext in ("pdf", "png"):
        path = out_dir / f"{name}.{ext}"
        fig.savefig(path)
        print(f"  wrote {path}")
    plt.close(fig)


# --- Plot 1: First-spawn comparison (raincloud-style) -----------------------

def plot_first_spawn(df_cold: pd.DataFrame, out_dir: Path):
    """
    First-spawn cold-start latency by mode, faceted by stage.

    Combines:
    - half-violin (kernel density) showing distribution shape
    - jittered scatter showing individual runs (n=20 each)
    - horizontal bar at the median, annotated with the value
    - ratio annotation (JIT median / AOT median) above each panel
    """
    df = df_cold[df_cold["spawn_kind"] == "first"]
    if df.empty:
        return

    stages = ordered_stages_present(df)
    n_stages = len(stages)
    fig, axes = plt.subplots(1, n_stages, figsize=(3.5 * n_stages, 4.4), sharey=False)
    if n_stages == 1:
        axes = [axes]

    for ax, stage in zip(axes, stages):
        sub = df[df["stage"] == stage]
        medians = {}

        for i, mode in enumerate(MODE_ORDER):
            vals = sub[sub["mode"] == mode]["cold_start_ms"].to_numpy()
            if vals.size == 0:
                continue
            color = MODE_PALETTE[mode]
            medians[mode] = np.median(vals)

            # Half-violin (right side only) with subtle fill
            parts = ax.violinplot(
                vals, positions=[i], widths=0.7, showmeans=False,
                showmedians=False, showextrema=False,
            )
            for pc in parts["bodies"]:
                # Clip to right half only
                m = np.mean(pc.get_paths()[0].vertices[:, 0])
                pc.get_paths()[0].vertices[:, 0] = np.clip(
                    pc.get_paths()[0].vertices[:, 0], m, np.inf
                )
                pc.set_facecolor(color)
                pc.set_edgecolor(color)
                pc.set_alpha(0.25)
                pc.set_linewidth(0)

            # Jittered scatter on the left side
            rng = np.random.default_rng(42 + i)
            x_jitter = i - 0.05 - rng.uniform(0, 0.22, size=vals.size)
            ax.scatter(
                x_jitter, vals,
                s=22, color=color, alpha=0.7,
                edgecolor="white", linewidth=0.6, zorder=3,
            )

            # Median marker — short horizontal bar
            ax.hlines(
                medians[mode], i - 0.32, i + 0.32,
                color=color, linewidth=2.2, zorder=4,
            )
            # Median value annotation
            ax.annotate(
                f"{medians[mode]:.2f}",
                xy=(i + 0.34, medians[mode]),
                xytext=(4, 0), textcoords="offset points",
                ha="left", va="center",
                fontsize=9, color=color, fontweight="semibold",
            )

        # Axis cosmetics
        ax.set_xticks(range(len(MODE_ORDER)))
        ax.set_xticklabels(MODE_ORDER)
        ax.set_xlim(-0.6, 1.7)
        ax.set_title(stage, color=COLOR_ANNOT)
        ax.set_xlabel("")
        # Tasteful y-axis padding. Guard against empty subsets (a stage in
        # STAGE_ORDER that has no data in this run, e.g. mixing old and new
        # JSON files): fall back to a 1 ms ceiling and skip the panel body.
        max_val = sub["cold_start_ms"].max()
        if pd.isna(max_val) or max_val <= 0:
            ax.set_ylim(0, 1.0)
            ax.text(0.5, 0.5, "no data", transform=ax.transAxes,
                    ha="center", va="center", color=COLOR_NEUTRAL, fontsize=10)
        else:
            ax.set_ylim(0, max_val * 1.15)

        # Ratio annotation (JIT / AOT)
        if "AOT" in medians and "JIT" in medians and medians["AOT"] > 0:
            ratio = medians["JIT"] / medians["AOT"]
            ax.text(
                0.5, 0.97, f"JIT / AOT = {ratio:.1f}×",
                transform=ax.transAxes, ha="center", va="top",
                fontsize=9.5, color=COLOR_ANNOT,
                bbox=dict(facecolor="white", edgecolor=COLOR_GRID,
                          boxstyle="round,pad=0.3", linewidth=0.7),
            )

    axes[0].set_ylabel("cold-start latency (ms)")
    fig.suptitle(
        "First-spawn cold-start latency (n = 20 per mode per stage)",
        fontsize=12.5, fontweight="semibold",
        x=0.06, y=1.02, ha="left",
    )
    fig.tight_layout()
    save(fig, out_dir, "01_first_spawn")


# --- Plot 2: CDF ------------------------------------------------------------

def plot_cdf(df_cold: pd.DataFrame, out_dir: Path):
    """
    Empirical CDF of first-spawn cold_start_ms.

    Single panel, log x. Mode encoded by color (the big story), stage encoded
    by line style (the secondary detail). Median markers annotated inline
    with the value, no drop lines.
    """
    df = df_cold[df_cold["spawn_kind"] == "first"]
    if df.empty:
        return
    stages = ordered_stages_present(df)

    fig, ax = plt.subplots(figsize=(8.5, 4.6))

    # Assign line styles to whatever stages this run contains. Order is
    # taken from `stages` (the order returned by ordered_stages_present),
    # so the most-loaded / first stage consistently gets "solid", keeping
    # the visual emphasis stable across different pipelines without
    # hardcoding any specific stage names.
    style_pool = [
        "solid",
        (0, (4, 1.5)),            # dashed
        (0, (1, 1.5)),            # dotted
        (0, (3, 1, 1, 1)),        # dash-dot
        (0, (5, 1, 1, 1, 1, 1)),  # dash-dot-dot
        (0, (2, 1)),              # short dash
    ]
    stage_styles = {s: style_pool[i % len(style_pool)]
                    for i, s in enumerate(stages)}

    # We'll plot all 6 (mode, stage) combinations on one axis.
    for mode in MODE_ORDER:
        color = MODE_PALETTE[mode]
        for stage in stages:
            sub = df[(df["mode"] == mode) & (df["stage"] == stage)]
            vals = np.sort(sub["cold_start_ms"].to_numpy())
            if vals.size == 0:
                continue

            # Step ECDF
            y = np.arange(1, vals.size + 1) / vals.size
            ax.step(
                np.concatenate([[vals[0]], vals]),
                np.concatenate([[0], y]),
                where="post",
                color=color,
                linewidth=2.0,
                linestyle=stage_styles[stage],
                solid_capstyle="round",
                dash_capstyle="round",
                alpha=0.95,
            )

            # Median marker
            med = np.median(vals)
            ax.scatter([med], [0.5], s=44, color=color,
                       edgecolor="white", linewidth=1.0, zorder=5)

    ax.set_xscale("log")
    ax.set_xlabel("cold-start latency (ms, log scale)")
    ax.set_ylabel("empirical CDF")
    ax.set_ylim(-0.02, 1.05)

    # Light reference lines at common percentile levels.
    for q in (0.5, 0.95):
        ax.axhline(q, color=COLOR_GRID, linewidth=0.7, zorder=0)
    ax.text(ax.get_xlim()[1], 0.5, " p50",
            va="center", ha="left", fontsize=8.5, color=COLOR_NEUTRAL)
    ax.text(ax.get_xlim()[1], 0.95, " p95",
            va="center", ha="left", fontsize=8.5, color=COLOR_NEUTRAL)

    # Group annotations - identify the AOT cluster and JIT cluster
    aot_max = df[df["mode"] == "AOT"]["cold_start_ms"].max()
    jit_min = df[df["mode"] == "JIT"]["cold_start_ms"].min()
    # Geometric mean of AOT region and JIT region for label x-positions
    aot_label_x = np.sqrt(df[df["mode"] == "AOT"]["cold_start_ms"].min() * aot_max)
    jit_label_x = np.sqrt(jit_min * df[df["mode"] == "JIT"]["cold_start_ms"].max())

    ax.text(aot_label_x, 1.08, "AOT", ha="center", va="bottom",
            fontsize=12, fontweight="semibold", color=COLOR_AOT)
    ax.text(jit_label_x, 1.08, "JIT", ha="center", va="bottom",
            fontsize=12, fontweight="semibold", color=COLOR_JIT)

    # Stage legend (line styles only, in neutral gray).
    from matplotlib.lines import Line2D
    stage_handles = [
        Line2D([0], [0], color=COLOR_NEUTRAL, linewidth=2.0,
               linestyle=stage_styles[s], label=s)
        for s in stages
    ]
    legend = ax.legend(handles=stage_handles, loc="lower right",
                       bbox_to_anchor=(1.0, 0.0),
                       title="stage", frameon=True, fontsize=9.5,
                       title_fontsize=9.5,
                       facecolor="white", edgecolor=COLOR_GRID,
                       framealpha=0.95)
    legend.get_title().set_color(COLOR_AXIS)

    ax.grid(True, axis="x", which="major", color=COLOR_GRID, linewidth=0.6)
    ax.grid(True, axis="x", which="minor", color=COLOR_GRID, linewidth=0.4, alpha=0.5)
    ax.set_axisbelow(True)

    fig.suptitle(
        "First-spawn cold-start CDF (n = 20 per mode per stage)",
        fontsize=12.5, fontweight="semibold",
        x=0.06, y=1.0, ha="left",
    )
    fig.tight_layout()
    save(fig, out_dir, "02_cdf")


# --- Plot 3: First vs warm spawns -------------------------------------------

def plot_first_vs_warm(df_cold: pd.DataFrame, out_dir: Path):
    """
    First-spawn vs warm-spawn distributions per mode/stage.
    Uses log y. Highlights JIT warm tail (the recompile-like outliers).
    """
    if df_cold.empty:
        return

    stages = ordered_stages_present(df_cold)
    n_stages = len(stages)
    fig, axes = plt.subplots(1, n_stages, figsize=(3.5 * n_stages, 4.2), sharey=True)
    if n_stages == 1:
        axes = [axes]

    # Position layout: 4 columns per panel
    # AOT-first, AOT-warm | JIT-first, JIT-warm
    positions = {
        ("AOT", "first"): 0, ("AOT", "warm"): 1,
        ("JIT", "first"): 2.4, ("JIT", "warm"): 3.4,
    }

    for ax, stage in zip(axes, stages):
        sub = df_cold[df_cold["stage"] == stage]

        for (mode, kind), pos in positions.items():
            vals = sub[(sub["mode"] == mode) & (sub["spawn_kind"] == kind)]["cold_start_ms"].to_numpy()
            if vals.size == 0:
                continue
            color = MODE_PALETTE[mode]
            alpha_fill = 0.85 if kind == "first" else 0.45

            # Skinny boxplot at this position
            bp = ax.boxplot(
                vals, positions=[pos], widths=0.55,
                patch_artist=True, showfliers=False,
                medianprops=dict(color="white", linewidth=1.5),
                whiskerprops=dict(color=color, linewidth=1.0),
                capprops=dict(color=color, linewidth=1.0),
                boxprops=dict(facecolor=color, edgecolor=color,
                              alpha=alpha_fill, linewidth=0),
            )

            # Overlay scatter with jitter
            rng = np.random.default_rng(hash((stage, mode, kind)) % (2**32))
            x_jitter = pos + rng.uniform(-0.18, 0.18, size=vals.size)
            ax.scatter(
                x_jitter, vals,
                s=12, color=color, alpha=0.5,
                edgecolor="none", zorder=3,
            )

        ax.set_yscale("log")
        ax.set_title(stage, color=COLOR_ANNOT)
        ax.set_xticks([0.5, 2.9])
        ax.set_xticklabels(["AOT", "JIT"])
        ax.set_xlim(-0.7, 4.1)

        ax.grid(True, axis="y", which="both", color=COLOR_GRID, linewidth=0.5)
        ax.grid(True, axis="y", which="major", color=COLOR_GRID, linewidth=0.8)

    # Add fine sub-labels (first/warm) below each pair.
    # Place them on a secondary x-axis with custom ticks at minor positions.
    for ax in axes:
        sec = ax.secondary_xaxis(-0.10)
        sec.set_xticks([0, 1, 2.4, 3.4])
        sec.set_xticklabels(["first", "warm", "first", "warm"])
        sec.tick_params(axis="x", length=0, labelsize=8.5,
                        colors=COLOR_NEUTRAL)
        for label in sec.get_xticklabels():
            label.set_style("italic")

    axes[0].set_ylabel("cold-start latency (ms, log scale)")

    fig.suptitle(
        "First-spawn vs warm-spawn cold-start latency",
        fontsize=12.5, fontweight="semibold",
        x=0.06, y=1.02, ha="left",
    )
    fig.tight_layout()
    fig.subplots_adjust(bottom=0.16)
    save(fig, out_dir, "03_first_vs_warm")


# --- Plot 4: Decomposition --------------------------------------------------

def plot_decomposition(df_cold: pd.DataFrame, out_dir: Path):
    """
    Where the time goes. Stacked horizontal bars per (stage, mode), median.

    Decomposition of cold_start_ms:
      compile + instantiate_pre
        + first_worker_penalty (WASI / Store / pool first-slot, paid only by
                                whichever stage spawns the first worker)
        + baseline_residual (thread setup, instantiate, page faults — paid
                             by every cold start)

    The first_worker_penalty is estimated per-mode as
    `cold_start_ms[stage] - min(cold_start_ms across stages)`. Whichever
    stage's autoscaler spawns first absorbs the WASI/Store first-time-in-
    process penalty; subsequent stages don't pay it because the OS, libc,
    glibc allocator, and Wasmtime engine internals are already warmed up.

    For AOT, compile happens outside the cold-start window (shown ghosted
    to the left of zero). For JIT, compile + instantiate_pre are inside.
    """
    df = df_cold[df_cold["spawn_kind"] == "first"].copy()
    if df.empty:
        return

    agg = (df.groupby(["mode", "stage"])
           .agg(cold_start_ms=("cold_start_ms", "median"),
                compile_ms=("compile_ms", "median"),
                instantiate_pre_ms=("instantiate_pre_ms", "median"))
           .reset_index())
    stages = ordered_stages_present(df)

    # Per-mode baseline = min cold_start_ms across stages of that mode.
    # We attribute everything above the baseline (after subtracting
    # compile + instantiate_pre, which are stage-independent costs in JIT
    # mode) to the first-worker penalty.
    baseline_per_mode: dict[str, float] = {}
    for mode in MODE_ORDER:
        sub = agg[agg["mode"] == mode]
        if sub.empty:
            continue
        if mode == "JIT":
            # The JIT baseline is the warm-stage cold_start_ms after
            # subtracting that stage's compile + instantiate_pre, which
            # are paid by *every* stage in JIT mode regardless of order.
            tmp = (sub["cold_start_ms"]
                   - sub["compile_ms"].fillna(0.0)
                   - sub["instantiate_pre_ms"].fillna(0.0)).clip(lower=0.0)
            baseline_per_mode[mode] = float(tmp.min())
        else:
            baseline_per_mode[mode] = float(sub["cold_start_ms"].min())

    fig, ax = plt.subplots(figsize=(10, 5))

    bar_height = 0.36
    y_positions = []
    y_labels = []
    y = 0

    def tint(base_hex: str, mix: float):
        from matplotlib.colors import to_rgb
        r, g, b = to_rgb(base_hex)
        return (r + (1 - r) * mix, g + (1 - g) * mix, b + (1 - b) * mix)

    for stage in stages:
        for mode in MODE_ORDER:
            row = agg[(agg["mode"] == mode) & (agg["stage"] == stage)]
            if row.empty:
                continue
            row = row.iloc[0]
            color = MODE_PALETTE[mode]
            base = baseline_per_mode.get(mode, 0.0)

            if mode == "JIT":
                comp  = row["compile_ms"] or 0.0
                inst  = row["instantiate_pre_ms"] or 0.0
                # Everything not explained by compile + instantiate_pre.
                residual_total = max(row["cold_start_ms"] - comp - inst, 0.0)
                # Split residual into baseline (every stage pays it) and
                # first-worker penalty (only the leading stage pays it).
                baseline_resid = min(residual_total, base)
                fw_penalty     = max(residual_total - base, 0.0)

                # Bar order (left to right):
                #   baseline_resid | first_worker_penalty | instantiate_pre | compile
                x = 0.0
                ax.barh(y, baseline_resid, height=bar_height, left=x,
                        color=color, alpha=0.95,
                        edgecolor="white", linewidth=0.5)
                x += baseline_resid
                if fw_penalty > 0:
                    ax.barh(y, fw_penalty, height=bar_height, left=x,
                            color=tint(color, 0.25),
                            edgecolor="white", linewidth=0.5,
                            hatch="..", alpha=0.95)
                    x += fw_penalty
                ax.barh(y, inst, height=bar_height, left=x,
                        color=tint(color, 0.45),
                        edgecolor="white", linewidth=0.5)
                x += inst
                ax.barh(y, comp, height=bar_height, left=x,
                        color=tint(color, 0.7),
                        edgecolor="white", linewidth=0.5)
                total = x + comp
                ax.text(total + 0.3, y, f"{total:.2f} ms",
                        va="center", ha="left", fontsize=9,
                        color=COLOR_ANNOT, fontweight="semibold")
            else:  # AOT
                cold = row["cold_start_ms"] or 0.0
                baseline_resid = min(cold, base)
                fw_penalty     = max(cold - base, 0.0)

                x = 0.0
                ax.barh(y, baseline_resid, height=bar_height, left=x,
                        color=color, alpha=0.95,
                        edgecolor="white", linewidth=0.5)
                x += baseline_resid
                if fw_penalty > 0:
                    ax.barh(y, fw_penalty, height=bar_height, left=x,
                            color=tint(color, 0.25),
                            edgecolor="white", linewidth=0.5,
                            hatch="..", alpha=0.95)
                    x += fw_penalty
                # Show AOT compile as a ghosted bar to the left of zero.
                comp_aot = row["compile_ms"] or 0.0
                ax.barh(y, -comp_aot, height=bar_height, left=0,
                        color=tint(color, 0.7), edgecolor="white",
                        linewidth=0.5, hatch="///",  alpha=0.55)
                ax.text(x + 0.3, y, f"{x:.2f} ms",
                        va="center", ha="left", fontsize=9,
                        color=COLOR_ANNOT, fontweight="semibold")

            y_positions.append(y)
            y_labels.append(f"{stage}  ·  {mode}")
            y += 1
        y += 0.5  # gap between stages

    # Vertical line at zero to separate "outside window" (left) from "inside" (right)
    ax.axvline(0, color=COLOR_AXIS, linewidth=0.9, zorder=1)

    ax.set_yticks(y_positions)
    ax.set_yticklabels(y_labels)
    ax.invert_yaxis()
    ax.set_xlabel("time (ms) — left of 0: paid before cold-start window  ·  right of 0: inside cold_start_ms")

    # Annotations explaining the regions
    xlim = ax.get_xlim()
    ax.text(xlim[0] * 0.5, -0.7, "outside window\n(autoscaler startup)",
            ha="center", va="bottom", fontsize=8.5, color=COLOR_NEUTRAL, style="italic")
    ax.text(xlim[1] * 0.5, -0.7, "inside cold_start_ms\n(measured cost)",
            ha="center", va="bottom", fontsize=8.5, color=COLOR_NEUTRAL, style="italic")

    # Legend
    legend_handles = [
        mpatches.Patch(facecolor=COLOR_NEUTRAL, alpha=0.95,
                       label="baseline residual (thread + instantiate + page faults)"),
        mpatches.Patch(facecolor=tint(COLOR_NEUTRAL, 0.25), hatch="..", alpha=0.95,
                       label="first-worker penalty (WASI / Store / pool first-slot)"),
        mpatches.Patch(facecolor=tint(COLOR_NEUTRAL, 0.45),
                       label="instantiate_pre (JIT only, inside window)"),
        mpatches.Patch(facecolor=tint(COLOR_NEUTRAL, 0.7),
                       label="compile (JIT: inside window)"),
        mpatches.Patch(facecolor=tint(COLOR_NEUTRAL, 0.7), hatch="///", alpha=0.55,
                       label="compile (AOT: outside window)"),
    ]
    ax.legend(handles=legend_handles,
              loc="upper center", bbox_to_anchor=(0.5, -0.13),
              ncol=2, fontsize=9, title=None,
              columnspacing=1.8, handletextpad=0.6)

    ax.grid(True, axis="x", color=COLOR_GRID, linewidth=0.6)
    ax.set_axisbelow(True)

    fig.suptitle(
        "Cold-start time decomposition (median first-spawn, ms)",
        fontsize=12.5, fontweight="semibold",
        x=0.06, y=1.0, ha="left",
    )
    fig.tight_layout()
    save(fig, out_dir, "04_decomposition")


# --- Plot 5: p99 e2e --------------------------------------------------------

def plot_p99_e2e(df_run: pd.DataFrame, out_dir: Path):
    """Run-level p99 latency by mode — slim, focused."""
    if df_run.empty or df_run["p99_e2e_ms"].isna().all():
        return

    fig, ax = plt.subplots(figsize=(7.5, 4.6))

    for i, mode in enumerate(MODE_ORDER):
        vals = df_run[df_run["mode"] == mode]["p99_e2e_ms"].dropna().to_numpy()
        if vals.size == 0:
            continue
        color = MODE_PALETTE[mode]
        med = np.median(vals)

        # Half-violin
        parts = ax.violinplot(vals, positions=[i], widths=0.65,
                              showmeans=False, showmedians=False, showextrema=False)
        for pc in parts["bodies"]:
            m = np.mean(pc.get_paths()[0].vertices[:, 0])
            pc.get_paths()[0].vertices[:, 0] = np.clip(
                pc.get_paths()[0].vertices[:, 0], m, np.inf
            )
            pc.set_facecolor(color)
            pc.set_edgecolor(color)
            pc.set_alpha(0.25)
            pc.set_linewidth(0)

        rng = np.random.default_rng(101 + i)
        x_jitter = i - 0.05 - rng.uniform(0, 0.22, size=vals.size)
        ax.scatter(x_jitter, vals, s=28, color=color, alpha=0.75,
                   edgecolor="white", linewidth=0.6, zorder=3)

        # Median: solid horizontal bar + annotation on the right.
        ax.hlines(med, i - 0.32, i + 0.32, color=color, linewidth=2.2, zorder=4)
        ax.annotate(f"med {med:.2f}",
                    xy=(i + 0.34, med), xytext=(4, 0), textcoords="offset points",
                    ha="left", va="center",
                    fontsize=9.5, color=color, fontweight="semibold")

        # Max of the p99-distribution (across the 20 iterations): dashed bar.
        mx = float(np.max(vals))
        ax.hlines(mx, i - 0.32, i + 0.32, color=color, linewidth=1.5,
                  linestyle=(0, (3, 2)), alpha=0.85, zorder=4)
        relative_gap = (mx - med) / max(med, 1e-9)
        y_offset_pts = 0 if relative_gap >= 0.08 else 8
        ax.annotate(f"max(p99) {mx:.2f}",
                    xy=(i + 0.34, mx), xytext=(4, y_offset_pts),
                    textcoords="offset points",
                    ha="left", va="center",
                    fontsize=9.0, color=color, alpha=0.85)

        # Absolute max e2e latency observed *across all events of all runs*
        # for this mode. This captures the cold-start spike that p99
        # truncates away. Marked as a downward triangle on a twin axis
        # placeholder so it sits visually on the right edge regardless of
        # how large the value is — but we still need it on the same y-scale
        # for direct comparison, so we let matplotlib autoscale to include
        # it. The annotation reports the exact number.
        if "max_e2e_ms" in df_run.columns:
            abs_max_vals = (df_run[df_run["mode"] == mode]["max_e2e_ms"]
                            .dropna().to_numpy())
            if abs_max_vals.size > 0:
                abs_max = float(np.max(abs_max_vals))
                ax.scatter([i + 0.05], [abs_max], marker="v", s=72,
                           color=color, edgecolor="black", linewidth=0.8,
                           zorder=5, label=None)
                ax.annotate(f"max(all events) {abs_max:.2f}",
                            xy=(i + 0.34, abs_max), xytext=(4, 0),
                            textcoords="offset points",
                            ha="left", va="center",
                            fontsize=9.0, color="black", alpha=0.9)

    ax.set_xticks(range(len(MODE_ORDER)))
    ax.set_xticklabels(MODE_ORDER)
    ax.set_xlim(-0.6, 1.7)
    ax.set_ylabel("end-to-end latency (ms, log scale)")
    ax.set_xlabel("")
    ax.set_yscale("log")

    fig.suptitle(
        "Run-level p99 end-to-end latency (n = 20 per mode)",
        fontsize=12.5, fontweight="semibold",
        x=0.07, y=1.0, ha="left",
    )
    fig.tight_layout()
    save(fig, out_dir, "05_p99_e2e")


def plot_e2e_timeseries(df_run: pd.DataFrame, out_dir: Path):
    """Per-event end-to-end latency over time, one panel per mode.

    Reads `events.rows` from each summary JSON so we can see *where* the
    p99/max outliers live. Cold-start spikes show up as a tall first
    sample near t=0; steady-state noise shows up as a flat band; backpressure
    shows up as a rising slope.

    Subsamples to ~5000 points per run so the figure stays light, but
    preserves the per-second max so cold-start spikes aren't smoothed away.
    """
    if df_run.empty or "_summary_path" not in df_run.columns:
        return

    modes = [m for m in MODE_ORDER if (df_run["mode"] == m).any()]
    if not modes:
        return

    fig, axes = plt.subplots(1, len(modes), figsize=(6.5 * len(modes), 4.2),
                             sharey=True)
    if len(modes) == 1:
        axes = [axes]

    for ax, mode in zip(axes, modes):
        sub = df_run[df_run["mode"] == mode]
        color = MODE_PALETTE[mode]
        plotted_any = False
        global_max_y = 0.0

        for _, row in sub.iterrows():
            path = row["_summary_path"]
            if path is None or pd.isna(path):
                continue
            try:
                with open(path) as f:
                    s = json.load(f)
            except Exception:
                continue
            rows = s.get("events", {}).get("rows", [])
            if not rows:
                continue

            ts  = np.array([r[0] for r in rows], dtype=np.float64)
            lat = np.array([r[1] for r in rows], dtype=np.float64)

            # Per-second max-preserving subsample + uniform stride for body.
            order = np.argsort(ts)
            ts, lat = ts[order], lat[order]
            bins = np.floor(ts).astype(np.int64)
            uniq, first_idx = np.unique(bins, return_index=True)
            idx_max_per_bin = []
            for b, start in zip(uniq, first_idx):
                end = start + int(np.sum(bins == b))
                idx_max_per_bin.append(start + int(np.argmax(lat[start:end])))
            idx_max_per_bin = np.array(idx_max_per_bin, dtype=np.int64)

            stride = max(1, len(ts) // 5000)
            keep_idx = np.unique(np.concatenate([
                idx_max_per_bin,
                np.arange(0, len(ts), stride, dtype=np.int64),
            ]))
            ts_p  = ts[keep_idx]
            lat_p = lat[keep_idx]

            ax.scatter(ts_p, lat_p, s=4, color=color, alpha=0.35,
                       edgecolor="none", rasterized=True)
            plotted_any = True
            if len(lat) > 0:
                global_max_y = max(global_max_y, float(np.max(lat)))

        if not plotted_any:
            ax.text(0.5, 0.5, "no per-event rows in summaries",
                    ha="center", va="center", transform=ax.transAxes,
                    color="gray", fontsize=10)

        ax.set_title(mode, color=COLOR_ANNOT)
        ax.set_xlabel("time since collector start (s)")
        ax.set_yscale("log")
        ax.set_xlim(left=0)
        if global_max_y > 0:
            ax.axhline(global_max_y, color=color, linestyle=":",
                       linewidth=1.0, alpha=0.6)
            ax.annotate(f"max {global_max_y:.2f} ms",
                        xy=(0.99, global_max_y),
                        xycoords=("axes fraction", "data"),
                        xytext=(-4, 4), textcoords="offset points",
                        ha="right", va="bottom",
                        fontsize=9, color=color, alpha=0.9)

    axes[0].set_ylabel("e2e latency (ms, log scale)")
    fig.suptitle(
        "End-to-end latency over time (every run overlaid; "
        "subsampled, per-second max preserved)",
        fontsize=12, fontweight="semibold",
        x=0.07, y=1.0, ha="left",
    )
    fig.tight_layout()
    save(fig, out_dir, "06_e2e_timeseries")


# --- Stats summary ----------------------------------------------------------

def write_summary_stats(df_cold: pd.DataFrame, out_dir: Path):
    if df_cold.empty:
        return

    def stats(g):
        return pd.Series({
            "n": len(g),
            "mean_ms": g["cold_start_ms"].mean(),
            "median_ms": g["cold_start_ms"].median(),
            "p95_ms": g["cold_start_ms"].quantile(0.95),
            "p99_ms": g["cold_start_ms"].quantile(0.99),
            "min_ms": g["cold_start_ms"].min(),
            "max_ms": g["cold_start_ms"].max(),
            "std_ms": g["cold_start_ms"].std(),
        })

    summary = (df_cold.groupby(["mode", "stage", "spawn_kind"], group_keys=False)
                      .apply(stats)
                      .reset_index())
    path = out_dir / "summary_stats.csv"
    summary.to_csv(path, index=False, float_format="%.4f")
    print(f"  wrote {path}")
    print()
    print("Median first-spawn cold_start_ms (JIT/AOT ratio):")
    first = summary[summary["spawn_kind"] == "first"]
    if first.empty:
        print("(no first-spawn rows)")
        return

    pivot = first.pivot(index="stage", columns="mode", values="median_ms")
    stages = ordered_stages_present(first)
    pivot = pivot.reindex(stages)

    ratio = pivot.get("JIT", pd.Series(index=pivot.index, dtype=float)) / \
        pivot.get("AOT", pd.Series(index=pivot.index, dtype=float))
    pivot["ratio (JIT/AOT)"] = ratio

    def _fmt(v: float) -> str:
        return "n/a" if pd.isna(v) else f"{v:.3f}"

    print(pivot.to_string(float_format=_fmt))


# --- Main -------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-dir", type=Path, default=DEFAULT_INPUT_DIR,
                        help="directory containing agent_*_summary-{AOT,JIT}-*.json files")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--from-csv", type=Path, default=None,
                        help="skip JSON parsing; load from cold_starts_long.csv")
    args = parser.parse_args()

    out_dir = args.output_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    if args.from_csv:
        df_cold = load_long_csv(args.from_csv)
        df_run = pd.DataFrame()
        print(f"Loaded {len(df_cold)} rows from {args.from_csv}")
    else:
        in_dir = args.input_dir
        files = sorted(in_dir.glob("agent_*_summary-*.json"))
        if not files:
            print(f"No matching files in {in_dir}", file=sys.stderr)
            sys.exit(1)
        print(f"Found {len(files)} files in {in_dir}")
        runs = [r for p in files if (r := extract_run(p)) is not None]
        if not runs:
            print("No usable runs.", file=sys.stderr)
            sys.exit(1)
        n_aot = sum(1 for r in runs if r["mode"] == "AOT")
        n_jit = sum(1 for r in runs if r["mode"] == "JIT")
        print(f"Parsed {len(runs)} runs: {n_aot} AOT, {n_jit} JIT")
        df_cold, df_run = build_dataframes(runs)
        if not df_run.empty:
            # _summary_path is used internally for the time-series plot
            # but should not pollute the human-readable CSV.
            csv_cols = [c for c in df_run.columns if c != "_summary_path"]
            df_run[csv_cols].to_csv(out_dir / "run_summary_long.csv",
                                    index=False, float_format="%.4f")
            print(f"  wrote {out_dir / 'run_summary_long.csv'}")

    setup_style()
    print(f"\nWriting figures to {out_dir}")
    plot_first_spawn(df_cold, out_dir)
    plot_cdf(df_cold, out_dir)
    plot_first_vs_warm(df_cold, out_dir)
    plot_decomposition(df_cold, out_dir)
    if not df_run.empty:
        plot_p99_e2e(df_run, out_dir)
        plot_e2e_timeseries(df_run, out_dir)

    write_summary_stats(df_cold, out_dir)
    print("\ndone.")


if __name__ == "__main__":
    main()