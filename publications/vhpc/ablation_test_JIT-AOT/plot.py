#!/usr/bin/env python3
"""
Epico AOT vs JIT ablation — redesigned plots.

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
COLOR_GRID = "#d8d8d8"
COLOR_AXIS = "#333333"
COLOR_ANNOT = "#1a1a1a"
COLOR_NEUTRAL = "#8a8a8a"
COLOR_PANEL_BG = "#f4f4f6"
COLOR_RATIO_BOX = "#ffffff"
COLOR_RATIO_EDGE = "#bcbcbc"

# LNCS text block ~12.2 cm = 4.8 in
LNCS_COL_W = 4.8

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
    """LNCS single-column, sans-serif, Epico-style aesthetic."""
    plt.rcParams.update({
        # Typography
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
        # Color and lines
        "axes.edgecolor": COLOR_AXIS,
        "axes.labelcolor": COLOR_AXIS,
        "axes.linewidth": 0.6,
        "xtick.color": COLOR_AXIS,
        "ytick.color": COLOR_AXIS,
        "xtick.major.width": 0.6,
        "ytick.major.width": 0.6,
        "xtick.major.size": 2.5,
        "ytick.major.size": 2.5,
        # Grid and spines (grid drawn in white over the tinted panel bg)
        "axes.spines.top": False,
        "axes.spines.right": False,
        "axes.grid": True,
        "axes.grid.axis": "y",
        "grid.color": "white",
        "grid.linewidth": 0.6,
        "grid.linestyle": "-",
        "axes.axisbelow": True,
        # Legend
        "legend.frameon": False,
        "legend.borderpad": 0.4,
        # Output
        "figure.dpi": 120,
        "savefig.dpi": 300,
        "savefig.bbox": "tight",
        "savefig.pad_inches": 0.05,
        "pdf.fonttype": 42,
        "ps.fonttype": 42,
    })


def _tint_panel(ax, color=COLOR_PANEL_BG):
    """Apply the standard light tinted background to a single axes."""
    ax.set_facecolor(color)


def _top_legend(fig, items, y=0.97):
    """Top-centered figure legend with colored squares, bold labels, no frame.
    items: list of (label, color)
    """
    handles = [
        mpatches.Patch(facecolor=c, edgecolor=c, label=lab, linewidth=0)
        for lab, c in items
    ]
    fig.legend(
        handles=handles, loc="upper center", ncol=len(handles),
        bbox_to_anchor=(0.5, y),
        frameon=False, fontsize=8.5,
        handlelength=1.4, handleheight=1.0,
        handletextpad=0.5, columnspacing=2.0,
        prop={"weight": "bold"},
    )


def _ratio_box(ax, x_frac, y_frac, text, transform=None):
    """White-fill bordered callout for ratio/headline numbers."""
    if transform is None:
        transform = ax.transAxes
    ax.text(
        x_frac, y_frac, text,
        transform=transform, ha="center", va="center",
        fontsize=7.8, color=COLOR_ANNOT, fontweight="bold",
        bbox=dict(facecolor=COLOR_RATIO_BOX, edgecolor=COLOR_RATIO_EDGE,
                  boxstyle="round,pad=0.25", linewidth=0.5),
    )


def _halfviolin(ax, vals, x_center, color, width=0.30):
    """Half-violin facing right of x_center."""
    if vals.size < 2:
        return
    try:
        parts = ax.violinplot(
            vals, positions=[x_center], widths=width * 2,
            showmeans=False, showmedians=False, showextrema=False,
        )
    except Exception:
        return
    for pc in parts["bodies"]:
        verts = pc.get_paths()[0].vertices
        m = float(np.mean(verts[:, 0]))
        verts[:, 0] = np.clip(verts[:, 0], m, np.inf)
        pc.set_facecolor(color)
        pc.set_edgecolor(color)
        pc.set_alpha(0.22)
        pc.set_linewidth(0)


def _jitter_strip(ax, vals, x_center, color,
                  width=0.25, point_size=14, alpha=0.6, seed=0):
    """Jittered dots placed to the left of x_center."""
    if vals.size == 0:
        return
    rng = np.random.default_rng(seed)
    x = x_center - rng.uniform(0.05, width, size=vals.size)
    ax.scatter(x, vals, s=point_size, color=color, alpha=alpha,
               edgecolor="white", linewidth=0.4, zorder=3)


def _median_bar(ax, med, x_center, color, half=0.30, lw=2.0):
    ax.hlines(med, x_center - half, x_center + half,
              color=color, linewidth=lw, zorder=4)


def _median_label(ax, med, x_center, color, half=0.30, unit="ms"):
    if med >= 1000:
        txt = f"{med/1000:.2f} s"
    elif med >= 10:
        txt = f"{med:.1f} {unit}"
    else:
        txt = f"{med:.2f} {unit}"
    ax.annotate(
        txt, xy=(x_center + half + 0.02, med),
        xytext=(2, 0), textcoords="offset points",
        ha="left", va="center",
        fontsize=7.8, color=color, fontweight="bold",
    )


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

    LNCS-styled: tinted panel background, top-centered colored-square legend,
    bold sans titles per facet, ratio callout above each panel.
    """
    df = df_cold[df_cold["spawn_kind"] == "first"]
    if df.empty:
        return

    stages = ordered_stages_present(df)
    n_stages = len(stages)
    # Lay out at LNCS column width regardless of n_stages so the figure
    # fits \textwidth. Per-facet width shrinks if there are many stages.
    fig, axes = plt.subplots(
        1, n_stages, figsize=(LNCS_COL_W, 2.8),
        sharey=False,
    )
    if n_stages == 1:
        axes = [axes]

    width = 0.30
    for ax, stage in zip(axes, stages):
        _tint_panel(ax)
        sub = df[df["stage"] == stage]
        medians = {}

        for i, mode in enumerate(MODE_ORDER):
            vals = sub[sub["mode"] == mode]["cold_start_ms"].to_numpy()
            if vals.size == 0:
                continue
            color = MODE_PALETTE[mode]
            medians[mode] = float(np.median(vals))

            _halfviolin(ax, vals, i, color, width=width)
            _jitter_strip(ax, vals, i, color,
                          width=width, point_size=10, alpha=0.65,
                          seed=42 + i)
            _median_bar(ax, medians[mode], i, color, half=width, lw=2.0)
            _median_label(ax, medians[mode], i, color, half=width)

        ax.set_xticks(range(len(MODE_ORDER)))
        ax.set_xticklabels(MODE_ORDER, fontweight="bold", fontsize=7.5)
        ax.set_xlim(-0.55, 1.7)
        ax.set_title(stage, color=COLOR_ANNOT)
        ax.set_xlabel("")

        max_val = sub["cold_start_ms"].max()
        if pd.isna(max_val) or max_val <= 0:
            ax.set_ylim(0, 1.0)
            ax.text(0.5, 0.5, "no data", transform=ax.transAxes,
                    ha="center", va="center", color=COLOR_NEUTRAL, fontsize=9)
        else:
            # Add headroom for the in-panel ratio box
            ax.set_ylim(0, max_val * 1.28)

        # Ratio annotation (JIT / AOT) — inside the panel near the top
        if "AOT" in medians and "JIT" in medians and medians["AOT"] > 0:
            ratio = medians["JIT"] / medians["AOT"]
            _ratio_box(ax, 0.5, 0.93, f"{ratio:.1f}\u00d7")

    axes[0].set_ylabel("cold-start latency (ms)")
    _top_legend(fig,
                [("AOT", COLOR_AOT), ("JIT", COLOR_JIT)],
                y=0.97)
    fig.tight_layout(rect=(0, 0, 1, 0.92))
    save(fig, out_dir, "01_first_spawn")


# --- Plot 2: CDF ------------------------------------------------------------

def plot_cdf(df_cold: pd.DataFrame, out_dir: Path):
    """
    Empirical CDF of first-spawn cold_start_ms. Mode by color, stage by
    line style. LNCS-styled, single column.
    """
    df = df_cold[df_cold["spawn_kind"] == "first"]
    if df.empty:
        return
    stages = ordered_stages_present(df)

    fig, ax = plt.subplots(figsize=(LNCS_COL_W, 3.0))
    _tint_panel(ax)

    style_pool = [
        "solid",
        (0, (4, 1.5)),
        (0, (1, 1.5)),
        (0, (3, 1, 1, 1)),
        (0, (5, 1, 1, 1, 1, 1)),
        (0, (2, 1)),
    ]
    stage_styles = {s: style_pool[i % len(style_pool)]
                    for i, s in enumerate(stages)}

    for mode in MODE_ORDER:
        color = MODE_PALETTE[mode]
        for stage in stages:
            sub = df[(df["mode"] == mode) & (df["stage"] == stage)]
            vals = np.sort(sub["cold_start_ms"].to_numpy())
            if vals.size == 0:
                continue
            y = np.arange(1, vals.size + 1) / vals.size
            ax.step(
                np.concatenate([[vals[0]], vals]),
                np.concatenate([[0], y]),
                where="post",
                color=color, linewidth=1.5,
                linestyle=stage_styles[stage],
                solid_capstyle="round", dash_capstyle="round",
                alpha=0.95,
            )
            med = np.median(vals)
            ax.scatter([med], [0.5], s=20, color=color,
                       edgecolor="white", linewidth=0.7, zorder=5)

    ax.set_xscale("log")
    ax.set_xlabel("cold-start latency (ms, log scale)")
    ax.set_ylabel("empirical CDF")
    ax.set_ylim(-0.02, 1.06)

    ax.grid(True, axis="x", which="major", color="white", linewidth=0.6)
    ax.grid(True, axis="x", which="minor", color="white",
            linewidth=0.4, alpha=0.7)
    ax.grid(True, axis="y", which="major", color="white", linewidth=0.6)
    ax.set_axisbelow(True)

    # p50 / p95 reference lines (drawn lightly in white over the tint)
    for q, qlabel in ((0.5, "p50"), (0.95, "p95")):
        ax.axhline(q, color="white", linewidth=0.8, zorder=0)
        ax.text(ax.get_xlim()[1], q, f" {qlabel}",
                va="center", ha="left", fontsize=7.5,
                color=COLOR_NEUTRAL, fontweight="bold")

    # Stage legend (line styles only) sits in the lower-right corner with
    # a white-fill frame so it reads cleanly over the tint.
    from matplotlib.lines import Line2D
    stage_handles = [
        Line2D([0], [0], color=COLOR_NEUTRAL, linewidth=1.6,
               linestyle=stage_styles[s], label=s)
        for s in stages
    ]
    legend = ax.legend(
        handles=stage_handles, loc="lower right",
        bbox_to_anchor=(1.0, 0.0),
        title="stage", frameon=True, fontsize=7.5,
        title_fontsize=7.5,
        facecolor="white", edgecolor=COLOR_GRID,
        framealpha=0.95, borderpad=0.4, handlelength=2.4,
    )
    legend.get_title().set_color(COLOR_AXIS)
    legend.get_title().set_fontweight("bold")

    ax.set_title("First-spawn cold-start CDF", pad=10)
    _top_legend(fig,
                [("AOT", COLOR_AOT), ("JIT", COLOR_JIT)],
                y=0.97)
    fig.tight_layout(rect=(0, 0, 1, 0.92))
    save(fig, out_dir, "02_cdf")


# --- Plot 3: First vs warm spawns -------------------------------------------

def plot_first_vs_warm(df_cold: pd.DataFrame, out_dir: Path):
    """
    First-spawn vs warm-spawn distributions per mode/stage. Log y.
    LNCS-styled: half-violin + jitter (replacing skinny boxplots for
    aesthetic consistency with the rest of the figure set).
    """
    if df_cold.empty:
        return

    stages = ordered_stages_present(df_cold)
    n_stages = len(stages)
    fig, axes = plt.subplots(
        1, n_stages, figsize=(LNCS_COL_W, 3.0), sharey=True,
    )
    if n_stages == 1:
        axes = [axes]

    # x positions: AOT-first, AOT-warm | JIT-first, JIT-warm
    positions = {
        ("AOT", "first"): 0.0, ("AOT", "warm"): 0.9,
        ("JIT", "first"): 2.2, ("JIT", "warm"): 3.1,
    }
    width = 0.30

    for ax, stage in zip(axes, stages):
        _tint_panel(ax)
        sub = df_cold[df_cold["stage"] == stage]

        for (mode, kind), pos in positions.items():
            vals = sub[(sub["mode"] == mode) &
                       (sub["spawn_kind"] == kind)]["cold_start_ms"].to_numpy()
            if vals.size == 0:
                continue
            color = MODE_PALETTE[mode]
            alpha_violin = 0.28 if kind == "first" else 0.16

            # Half-violin
            if vals.size >= 2:
                try:
                    parts = ax.violinplot(
                        vals, positions=[pos], widths=width * 2,
                        showmeans=False, showmedians=False, showextrema=False,
                    )
                    for pc in parts["bodies"]:
                        verts = pc.get_paths()[0].vertices
                        m = float(np.mean(verts[:, 0]))
                        verts[:, 0] = np.clip(verts[:, 0], m, np.inf)
                        pc.set_facecolor(color)
                        pc.set_edgecolor(color)
                        pc.set_alpha(alpha_violin)
                        pc.set_linewidth(0)
                except Exception:
                    pass

            # Jitter
            rng = np.random.default_rng(hash((stage, mode, kind)) & 0xFFFF)
            x_jitter = pos - rng.uniform(0.05, width, size=vals.size)
            ax.scatter(x_jitter, vals, s=8, color=color,
                       alpha=0.55 if kind == "first" else 0.35,
                       edgecolor="white", linewidth=0.3, zorder=3)

            # Median bar
            med = float(np.median(vals))
            ax.hlines(med, pos - width, pos + width,
                      color=color, linewidth=1.8, zorder=4)

        ax.set_yscale("log")
        ax.set_title(stage, color=COLOR_ANNOT)
        ax.set_xticks([0.45, 2.65])
        ax.set_xticklabels(["AOT", "JIT"], fontweight="bold", fontsize=7.5)
        ax.set_xlim(-0.6, 3.7)
        ax.grid(True, axis="y", which="major", color="white", linewidth=0.6)
        ax.grid(True, axis="y", which="minor", color="white",
                linewidth=0.4, alpha=0.7)
        ax.set_axisbelow(True)

    # Sub-labels (first/warm) under each pair via secondary x-axis
    for ax in axes:
        sec = ax.secondary_xaxis(-0.10)
        sec.set_xticks([0.0, 0.9, 2.2, 3.1])
        sec.set_xticklabels(["first", "warm", "first", "warm"],
                            fontsize=6.5)
        sec.tick_params(axis="x", length=0, colors=COLOR_NEUTRAL)
        for label in sec.get_xticklabels():
            label.set_style("italic")

    axes[0].set_ylabel("cold-start latency (ms, log)")
    _top_legend(fig,
                [("AOT", COLOR_AOT), ("JIT", COLOR_JIT)],
                y=0.97)
    fig.tight_layout(rect=(0, 0, 1, 0.92))
    fig.subplots_adjust(bottom=0.18)
    save(fig, out_dir, "03_first_vs_warm")


# --- Plot 4: Decomposition --------------------------------------------------

def plot_decomposition(df_cold: pd.DataFrame, out_dir: Path):
    """
    Where the time goes. Stacked horizontal bars per (stage, mode), median.

    LNCS-styled: bold sans title, tinted panel, complex bottom-center legend
    (5 items wouldn't fit a top-center placement at column width).
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

    baseline_per_mode: dict[str, float] = {}
    for mode in MODE_ORDER:
        sub = agg[agg["mode"] == mode]
        if sub.empty:
            continue
        if mode == "JIT":
            tmp = (sub["cold_start_ms"]
                   - sub["compile_ms"].fillna(0.0)
                   - sub["instantiate_pre_ms"].fillna(0.0)).clip(lower=0.0)
            baseline_per_mode[mode] = float(tmp.min())
        else:
            baseline_per_mode[mode] = float(sub["cold_start_ms"].min())

    # Compute n_bars to size figure height: 2 bars per stage + small inter-stage gap
    n_bars = 2 * len(stages)
    fig_h = max(3.0, 0.5 * n_bars + 1.7)
    fig, ax = plt.subplots(figsize=(LNCS_COL_W, fig_h))
    _tint_panel(ax)

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
                comp = row["compile_ms"] or 0.0
                inst = row["instantiate_pre_ms"] or 0.0
                residual_total = max(row["cold_start_ms"] - comp - inst, 0.0)
                baseline_resid = min(residual_total, base)
                fw_penalty = max(residual_total - base, 0.0)

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
                ax.text(total + 0.3, y, f"{total:.1f} ms",
                        va="center", ha="left", fontsize=7.5,
                        color=COLOR_ANNOT, fontweight="bold")
            else:  # AOT
                cold = row["cold_start_ms"] or 0.0
                baseline_resid = min(cold, base)
                fw_penalty = max(cold - base, 0.0)

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
                comp_aot = row["compile_ms"] or 0.0
                ax.barh(y, -comp_aot, height=bar_height, left=0,
                        color=tint(color, 0.7), edgecolor="white",
                        linewidth=0.5, hatch="///", alpha=0.55)
                ax.text(x + 0.3, y, f"{x:.1f} ms",
                        va="center", ha="left", fontsize=7.5,
                        color=COLOR_ANNOT, fontweight="bold")

            y_positions.append(y)
            y_labels.append(f"{stage}  ·  {mode}")
            y += 1
        y += 0.5

    ax.axvline(0, color=COLOR_AXIS, linewidth=0.8, zorder=1)

    ax.set_yticks(y_positions)
    ax.set_yticklabels(y_labels, fontsize=7.5)
    ax.invert_yaxis()
    ax.set_xlabel("time (ms) — left of 0: pre-window  ·  right of 0: cold_start_ms",
                  fontsize=7.5)

    # Legend at bottom-center (5 segments are too many for top-center at 4.8")
    legend_handles = [
        mpatches.Patch(facecolor=COLOR_NEUTRAL, alpha=0.95,
                       label="baseline residual"),
        mpatches.Patch(facecolor=tint(COLOR_NEUTRAL, 0.25), hatch="..", alpha=0.95,
                       label="first-worker penalty"),
        mpatches.Patch(facecolor=tint(COLOR_NEUTRAL, 0.45),
                       label="instantiate_pre (JIT)"),
        mpatches.Patch(facecolor=tint(COLOR_NEUTRAL, 0.7),
                       label="compile (JIT, in-window)"),
        mpatches.Patch(facecolor=tint(COLOR_NEUTRAL, 0.7), hatch="///", alpha=0.55,
                       label="compile (AOT, pre-window)"),
    ]
    ax.legend(
        handles=legend_handles,
        loc="upper center", bbox_to_anchor=(0.5, -0.10 - 0.10 / max(fig_h, 1)),
        ncol=2, fontsize=7.0, title=None,
        columnspacing=1.2, handletextpad=0.5,
        frameon=False,
    )

    ax.grid(True, axis="x", color="white", linewidth=0.6)
    ax.set_axisbelow(True)

    ax.set_title("Cold-start time decomposition (median first-spawn)", pad=10)
    _top_legend(fig,
                [("AOT", COLOR_AOT), ("JIT", COLOR_JIT)],
                y=0.97)

    fig.tight_layout(rect=(0, 0.04, 1, 0.93))
    save(fig, out_dir, "04_decomposition")


# --- Plot 5: p99 e2e --------------------------------------------------------

def plot_p99_e2e(df_run: pd.DataFrame, out_dir: Path):
    """Run-level p99 latency by mode — LNCS-styled half-violin + jitter."""
    if df_run.empty or df_run["p99_e2e_ms"].isna().all():
        return

    fig, ax = plt.subplots(figsize=(LNCS_COL_W, 3.0))
    _tint_panel(ax)

    width = 0.30
    for i, mode in enumerate(MODE_ORDER):
        vals = df_run[df_run["mode"] == mode]["p99_e2e_ms"].dropna().to_numpy()
        if vals.size == 0:
            continue
        color = MODE_PALETTE[mode]

        _halfviolin(ax, vals, i, color, width=width)
        _jitter_strip(ax, vals, i, color,
                      width=width, point_size=12, alpha=0.7,
                      seed=101 + i)

        med = float(np.median(vals))
        _median_bar(ax, med, i, color, half=width, lw=2.0)
        ax.annotate(
            f"med {med:.1f} ms",
            xy=(i + width + 0.02, med), xytext=(2, 0),
            textcoords="offset points",
            ha="left", va="center",
            fontsize=7.8, color=color, fontweight="bold",
        )

        # Max(p99) as a dashed bar
        mx = float(np.max(vals))
        ax.hlines(mx, i - width, i + width, color=color, linewidth=1.2,
                  linestyle=(0, (3, 2)), alpha=0.85, zorder=4)
        relative_gap = (mx - med) / max(med, 1e-9)
        y_offset_pts = 0 if relative_gap >= 0.10 else 9
        ax.annotate(
            f"max(p99) {mx:.1f}",
            xy=(i + width + 0.02, mx),
            xytext=(2, y_offset_pts), textcoords="offset points",
            ha="left", va="center",
            fontsize=7.2, color=color, alpha=0.85,
        )

        # Absolute max across all events
        if "max_e2e_ms" in df_run.columns:
            abs_max_vals = (df_run[df_run["mode"] == mode]["max_e2e_ms"]
                            .dropna().to_numpy())
            if abs_max_vals.size > 0:
                abs_max = float(np.max(abs_max_vals))
                ax.scatter([i + 0.05], [abs_max], marker="v", s=44,
                           color=color, edgecolor="black", linewidth=0.6,
                           zorder=5)
                ax.annotate(
                    f"abs max {abs_max:.1f}",
                    xy=(i + width + 0.02, abs_max),
                    xytext=(2, 0), textcoords="offset points",
                    ha="left", va="center",
                    fontsize=7.2, color="black", alpha=0.85,
                )

    ax.set_xticks(range(len(MODE_ORDER)))
    ax.set_xticklabels(MODE_ORDER, fontweight="bold", fontsize=7.5)
    ax.set_xlim(-0.55, 1.7)
    ax.set_ylabel("end-to-end latency (ms, log)")
    ax.set_xlabel("")
    ax.set_yscale("log")
    ax.grid(True, axis="y", which="major", color="white", linewidth=0.6)
    ax.grid(True, axis="y", which="minor", color="white",
            linewidth=0.4, alpha=0.7)
    ax.set_axisbelow(True)

    ax.set_title("Run-level p99 end-to-end latency", pad=10)
    _top_legend(fig,
                [("AOT", COLOR_AOT), ("JIT", COLOR_JIT)],
                y=0.97)
    fig.tight_layout(rect=(0, 0, 1, 0.92))
    save(fig, out_dir, "05_p99_e2e")


def plot_e2e_timeseries(df_run: pd.DataFrame, out_dir: Path):
    """Per-event end-to-end latency over time, one panel per mode.

    LNCS-styled: tinted panels, top-centered legend, bold sans per-panel
    titles. Subsamples to ~5000 points per run; preserves per-second max
    so cold-start spikes aren't smoothed away.
    """
    if df_run.empty or "_summary_path" not in df_run.columns:
        return

    modes = [m for m in MODE_ORDER if (df_run["mode"] == m).any()]
    if not modes:
        return

    # Stack panels vertically at LNCS column width
    fig, axes = plt.subplots(
        len(modes), 1, figsize=(LNCS_COL_W, 1.6 * len(modes) + 0.6),
        sharex=False, sharey=True,
    )
    if len(modes) == 1:
        axes = [axes]

    for ax, mode in zip(axes, modes):
        _tint_panel(ax)
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

            ts = np.array([r[0] for r in rows], dtype=np.float64)
            lat = np.array([r[1] for r in rows], dtype=np.float64)

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
            ts_p = ts[keep_idx]
            lat_p = lat[keep_idx]

            ax.scatter(ts_p, lat_p, s=2.5, color=color, alpha=0.35,
                       edgecolor="none", rasterized=True)
            plotted_any = True
            if len(lat) > 0:
                global_max_y = max(global_max_y, float(np.max(lat)))

        if not plotted_any:
            ax.text(0.5, 0.5, "no per-event rows in summaries",
                    ha="center", va="center", transform=ax.transAxes,
                    color=COLOR_NEUTRAL, fontsize=9)

        ax.set_title(mode, color=COLOR_ANNOT)
        ax.set_xlabel("time since collector start (s)", fontsize=7.8)
        ax.set_yscale("log")
        ax.set_xlim(left=0)
        ax.grid(True, axis="y", which="major", color="white", linewidth=0.6)
        ax.grid(True, axis="y", which="minor", color="white",
                linewidth=0.4, alpha=0.7)
        ax.grid(True, axis="x", which="major", color="white", linewidth=0.6)
        ax.set_axisbelow(True)

        if global_max_y > 0:
            ax.axhline(global_max_y, color=color, linestyle=":",
                       linewidth=0.9, alpha=0.6)
            ax.annotate(f"max {global_max_y:.1f} ms",
                        xy=(0.99, global_max_y),
                        xycoords=("axes fraction", "data"),
                        xytext=(-4, 4), textcoords="offset points",
                        ha="right", va="bottom",
                        fontsize=7.5, color=color, alpha=0.9,
                        fontweight="bold")

    axes[0].set_ylabel("e2e latency (ms, log)")
    if len(axes) > 1:
        axes[-1].set_ylabel("e2e latency (ms, log)")

    _top_legend(fig,
                [("AOT", COLOR_AOT), ("JIT", COLOR_JIT)],
                y=0.97)
    fig.tight_layout(rect=(0, 0, 1, 0.93))
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