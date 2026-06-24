#!/usr/bin/env python3
"""
plot_combined.py — combined plots from a warm run and a cold run.

Restyled to match the Metapod paper aesthetic:
    - bold sans-serif panel titles
    - top-centered legend on white above the chart
    - light tinted panel backgrounds
    - boxed ratio callouts above paired distributions
    - half-violins + jitter strips + thick median bars
    - LNCS-friendly column width and font sizes

Reads:
    <warm_dir>/parents.csv            (loadgen output)
    <cold_dir>/parents.csv            (cold-probe loadgen output)
    <warm_dir>/children/*.json        (optional, from fetch_children.sh)
    <cold_dir>/children/*.json        (optional, from fetch_children.sh)

Writes (in --output-dir):
    01_e2e_cdf.{pdf,png}              warm + cold e2e CDFs overlaid
    02_stage_inits.{pdf,png}          per-stage init: warm vs cold (broken Y)
    03_cold_vs_warm.{pdf,png}         warm + cold e2e violins (log y)

Usage:
    python3 plot_combined.py WARM_DIR COLD_DIR [--output-dir DIR]
"""
from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path

import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np


# ---------------------------------------------------------------------------
# Palette + style
# ---------------------------------------------------------------------------

COLOR_WARM = "#d1495b"          # AOT analog in Metapod plots
COLOR_COLD = "#1f4e79"          # JIT analog in Metapod plots
COLOR_GRID = "#d8d8d8"
COLOR_AXIS = "#333333"
COLOR_ANNOT = "#1a1a1a"
COLOR_PANEL_BG = "#f4f4f6"      # neutral pale tint, single panel
COLOR_RATIO_BOX = "#ffffff"
COLOR_RATIO_EDGE = "#bcbcbc"

# LNCS text block ~12.2 cm = ~4.8 in. We render at exactly column width so
# \includegraphics[width=\textwidth]{...} is 1:1 with no rescaling.
LNCS_COL_W = 4.8

STAGE_ORDER = ["normalize", "detect", "finalize"]
STAGE_MAP = {"normalize_rs": "normalize",
             "detect_rs": "detect",
             "finalize_rs": "finalize"}


def setup_style():
    """Matplotlib rc tuned for LNCS single-column figures."""
    plt.rcParams.update({
        # Body sans-serif — DejaVu Sans is universally available. Panel titles
        # use bold weight; axis labels and ticks use regular weight.
        "font.family": "sans-serif",
        "font.sans-serif": ["DejaVu Sans", "Arial", "Helvetica"],
        "mathtext.fontset": "dejavusans",
        "font.size": 8.5,
        "axes.titlesize": 10.0,
        "axes.titleweight": "bold",
        "axes.titlelocation": "center",
        "axes.titlepad": 8,
        "axes.labelsize": 8.5,
        "xtick.labelsize": 8.0,
        "ytick.labelsize": 8.0,
        "legend.fontsize": 8.0,
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
        "grid.color": COLOR_GRID,
        "grid.linewidth": 0.5,
        "grid.linestyle": "-",
        "axes.axisbelow": True,
        "legend.frameon": False,
        "figure.dpi": 120,
        "savefig.dpi": 300,
        "savefig.bbox": "tight",
        "savefig.pad_inches": 0.05,
        "pdf.fonttype": 42,
        "ps.fonttype": 42,
    })


def save(fig, out_dir: Path, name: str):
    for ext in ("pdf", "png"):
        path = out_dir / f"{name}.{ext}"
        fig.savefig(path)
        print(f"  wrote {path}")
    plt.close(fig)


def _tint_panel(ax, color=COLOR_PANEL_BG):
    """Apply the Metapod-style light tinted background to an axes."""
    ax.set_facecolor(color)


def _top_legend(fig, items, y=1.0):
    """Top-centered legend with bold-edged color squares, no frame.
    items: list of (label, color)
    """
    handles = [
        mpatches.Patch(facecolor=c, edgecolor=c, label=lab, linewidth=0)
        for lab, c in items
    ]
    leg = fig.legend(
        handles=handles, loc="upper center", ncol=len(handles),
        bbox_to_anchor=(0.5, y),
        frameon=False, fontsize=8.5,
        handlelength=1.4, handleheight=1.0,
        handletextpad=0.5, columnspacing=2.0,
        prop={"weight": "bold"},
    )
    return leg


def _ratio_box(ax, x, y_axes_frac, text):
    """Draw a small white-fill ratio callout box centered at axes-fraction y."""
    ax.text(
        x, y_axes_frac, text,
        transform=ax.get_xaxis_transform(),  # x in data coords, y in axes frac
        ha="center", va="bottom",
        fontsize=8.0, color=COLOR_ANNOT, fontweight="bold",
        bbox=dict(facecolor=COLOR_RATIO_BOX, edgecolor=COLOR_RATIO_EDGE,
                  boxstyle="round,pad=0.25", linewidth=0.5),
        clip_on=False,
    )


def _halfviolin(ax, vals, x_center, color, side="right", width=0.4):
    """One-sided violin: filled half-density facing left or right of x_center."""
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
        if side == "right":
            verts[:, 0] = np.clip(verts[:, 0], m, np.inf)
        else:
            verts[:, 0] = np.clip(verts[:, 0], -np.inf, m)
        pc.set_facecolor(color)
        pc.set_edgecolor(color)
        pc.set_alpha(0.22)
        pc.set_linewidth(0)


def _jitter_strip(ax, vals, x_center, color, side="left",
                  width=0.3, point_size=6, alpha=0.55, seed=0):
    """Jittered points placed on one side of x_center."""
    if vals.size == 0:
        return
    rng = np.random.default_rng(seed)
    if side == "left":
        x = x_center - rng.uniform(0.05, width, size=vals.size)
    else:
        x = x_center + rng.uniform(0.05, width, size=vals.size)
    ax.scatter(x, vals, s=point_size, color=color, alpha=alpha,
               edgecolor="white", linewidth=0.3, zorder=3)


def _median_bar(ax, med, x_center, color, half=0.35, lw=2.2, zorder=4):
    ax.hlines(med, x_center - half, x_center + half,
              color=color, linewidth=lw, zorder=zorder)


def _median_label(ax, med, x_center, color, half=0.35,
                  fmt_below_ms=True):
    if med >= 1000:
        txt = f"{med/1000:.2f} s"
    elif med >= 10:
        txt = f"{med:.0f} ms"
    else:
        txt = f"{med:.1f} ms" if fmt_below_ms else f"{med:.2f} ms"
    ax.annotate(
        txt, xy=(x_center + half + 0.02, med),
        xytext=(2, 0), textcoords="offset points",
        ha="left", va="center",
        fontsize=8.0, color=color, fontweight="bold",
    )


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_e2e_from_parents(parents_csv: Path) -> np.ndarray:
    vals = []
    with open(parents_csv) as f:
        for r in csv.DictReader(f):
            if r.get("is_prewarm") == "1":
                continue
            if r.get("success") != "true":
                continue
            try:
                vals.append(float(r["e2e_ms"]))
            except (KeyError, ValueError):
                continue
    return np.array(vals)


def load_stage_inits(run_dir: Path, only_nonzero: bool = True) -> dict:
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
            init_ms = 0.0
            for ann in c.get("annotations", []) or []:
                if ann.get("key") == "initTime":
                    init_ms = float(ann.get("value", 0.0))
                    break
            if only_nonzero and init_ms <= 0.0:
                continue
            out[short].append(init_ms)
    return out


# ---------------------------------------------------------------------------
# Plot 1: warm + cold e2e CDF
# ---------------------------------------------------------------------------

def plot_e2e_cdf(warm: np.ndarray, cold: np.ndarray, out_dir: Path):
    if warm.size == 0 and cold.size == 0:
        print("  no samples; skipping e2e_cdf")
        return
    fig, ax = plt.subplots(figsize=(LNCS_COL_W, 2.9))
    _tint_panel(ax)

    # Plot cold first so warm callouts sit visually on top
    for label, vals, color in [("cold", cold, COLOR_COLD),
                               ("warm", warm, COLOR_WARM)]:
        if vals.size == 0:
            continue
        xs = np.sort(vals)
        ys = np.arange(1, xs.size + 1) / xs.size
        ax.step(np.concatenate([[xs[0]], xs]),
                np.concatenate([[0], ys]),
                where="post", color=color, linewidth=1.5,
                solid_capstyle="round")
        # Quantile callouts. The cold distribution is tight: p95 and p99 sit
        # nearly on top of each other on log-x. Stagger the labels vertically
        # by quantile so they don't collide. Cold uses LEFT placement (avoids
        # right-edge clip), warm uses RIGHT.
        place_left = (label == "cold")
        # Per-quantile y offsets (in points) — large enough that successive
        # labels never overlap even when their dots coincide.
        offsets = {0.5: 0, 0.95: -18, 0.99: 12}
        for q, qlabel in [(0.5, "p50"), (0.95, "p95"), (0.99, "p99")]:
            v = xs[min(xs.size - 1, int(xs.size * q))]
            ax.scatter([v], [q], s=20, color=color,
                       edgecolor="white", linewidth=0.7, zorder=5)
            txt = (f"{qlabel} = {v:.1f} ms" if v < 1000
                   else f"{qlabel} = {v/1000:.2f} s")
            ax.annotate(
                txt, xy=(v, q),
                xytext=(-7 if place_left else 7, offsets[q]),
                textcoords="offset points",
                ha="right" if place_left else "left",
                va="center",
                fontsize=7.2, color=color, fontweight="bold",
            )

    ax.set_xscale("log")
    ax.set_xlabel("end-to-end latency (ms, log scale)")
    ax.set_ylabel("empirical CDF")
    ax.set_ylim(-0.02, 1.06)

    # Add a bit of right-edge x padding so cold callouts breathe.
    if cold.size:
        ax.set_xlim(right=float(cold.max()) * 1.8)

    ax.grid(True, axis="x", which="major", color="white", linewidth=0.6)
    ax.grid(True, axis="x", which="minor", color="white",
            linewidth=0.4, alpha=0.7)
    ax.grid(True, axis="y", which="major", color="white", linewidth=0.6)
    ax.set_axisbelow(True)

    ax.set_title("End-to-end latency (OpenWhisk)", pad=10)
    _top_legend(fig,
                [("warm", COLOR_WARM), ("cold", COLOR_COLD)],
                y=0.3)
    fig.tight_layout(rect=(0, 0, 1, 0.93))
    save(fig, out_dir, "01_e2e_cdf")


# ---------------------------------------------------------------------------
# Plot 2: per-stage init, broken Y axis, paired warm/cold per stage
# ---------------------------------------------------------------------------

def plot_stage_inits(warm_inits: dict, cold_inits: dict, out_dir: Path):
    if not (any(warm_inits.values()) or any(cold_inits.values())):
        print("  no stage-init samples; skipping stage_inits")
        return

    cold_all = np.concatenate(
        [np.array(cold_inits.get(s, []), dtype=float) for s in STAGE_ORDER]
    )

    if cold_all.size:
        cold_lo = max(0.0, float(np.percentile(cold_all, 1)) - 50)
        cold_hi = float(np.percentile(cold_all, 99)) + 50
    else:
        cold_lo, cold_hi = 600, 900

    fig, (ax_top, ax_bot) = plt.subplots(
        2, 1, sharex=True, figsize=(LNCS_COL_W, 3.3),
        gridspec_kw={"height_ratios": [3.2, 1.0], "hspace": 0.12},
    )
    _tint_panel(ax_top)
    _tint_panel(ax_bot)

    # Y limits: cold panel data-driven; warm panel a thin band around 0.
    # We add modest headroom so the violins don't crash into the title.
    ax_top.set_ylim(cold_lo, cold_hi + (cold_hi - cold_lo) * 0.08)
    ax_bot.set_ylim(-1.0, 1.5)

    # Break-axis marks
    ax_top.spines["bottom"].set_visible(False)
    ax_bot.spines["top"].set_visible(False)
    ax_top.tick_params(labelbottom=False, bottom=False)
    d = 0.4
    kw = dict(marker=[(-1, -d), (1, d)], markersize=6,
              linestyle="none", color=COLOR_AXIS, mec=COLOR_AXIS,
              mew=0.7, clip_on=False)
    ax_top.plot([0, 1], [0, 0], transform=ax_top.transAxes, **kw)
    ax_bot.plot([0, 1], [1, 1], transform=ax_bot.transAxes, **kw)

    n_warm_total = 0
    n_cold_total = 0
    warm_nonzero_max = 0.0
    warm_nonzero_count = 0
    ratio_lines = []  # for the boxed ratio callouts in the top panel

    width = 0.30  # half-width for each violin/jitter pair

    for i, stage in enumerate(STAGE_ORDER):
        w = np.array(warm_inits.get(stage, []), dtype=float)
        c = np.array(cold_inits.get(stage, []), dtype=float)
        n_warm_total += w.size
        n_cold_total += c.size
        if w.size:
            wnz = w[w > 0.0]
            warm_nonzero_count += int(wnz.size)
            if wnz.size:
                warm_nonzero_max = max(warm_nonzero_max, float(wnz.max()))

        # --- Cold (top panel): half-violin (right) + jitter (left) + median bar
        if c.size:
            _halfviolin(ax_top, c, i, COLOR_COLD, side="right", width=width)
            _jitter_strip(ax_top, c, i, COLOR_COLD, side="left",
                          width=width, point_size=8, alpha=0.6,
                          seed=hash(("cold", stage)) & 0xFFFF)
            med_c = float(np.median(c))
            _median_bar(ax_top, med_c, i, COLOR_COLD, half=width, lw=2.2)
            _median_label(ax_top, med_c, i, COLOR_COLD, half=width)

        # --- Warm (bottom panel): flat 0 line + count label + outliers as dots
        if w.size:
            n = w.size
            zeros = int(np.sum(w == 0.0))
            _median_bar(ax_bot, 0.0, i, COLOR_WARM, half=width, lw=2.6)
            ax_bot.scatter([i], [0], s=22, color=COLOR_WARM,
                           edgecolor="white", linewidth=0.6, zorder=5)
            label = f"0 ms"
            ax_bot.annotate(
                label, xy=(i + width + 0.02, 0),
                xytext=(2, -2), textcoords="offset points",
                ha="left", va="top",
                fontsize=7.8, color=COLOR_WARM, fontweight="bold",
            )

        # --- Ratio callout (top panel)
        # Use the OVERALL warm median (not median of nonzeros). For OW, warm
        # reuse → initTime=0, so overall median is typically 0 and the ratio
        # is infinite. Skip the box in that case — the cold median label
        # inside the violin already shows the magnitude, and "0 ms (N/N)"
        # in the warm panel makes the contrast explicit.
        if c.size and w.size:
            med_c = float(np.median(c))
            med_w = float(np.median(w))
            if med_w > 0:
                ratio_lines.append((i, f"{med_c / med_w:.0f}\u00d7"))

    # Draw all ratio boxes inside the cold panel near the top (we extended
    # ax_top.ylim above to leave room for them). x in data coords (stage
    # index), y in axes-frac for consistent placement across stages.
    for x, txt in ratio_lines:
        ax_top.text(
            x, 0.93, txt,
            transform=ax_top.get_xaxis_transform(),
            ha="center", va="center",
            fontsize=7.5, color=COLOR_ANNOT, fontweight="bold",
            bbox=dict(facecolor=COLOR_RATIO_BOX, edgecolor=COLOR_RATIO_EDGE,
                      boxstyle="round,pad=0.22", linewidth=0.5),
        )

    # X axis & labels
    ax_bot.set_xticks(range(len(STAGE_ORDER)))
    ax_bot.set_xticklabels(STAGE_ORDER, fontweight="bold")
    ax_bot.set_xlim(-0.55, len(STAGE_ORDER) - 0.45)

    ax_top.set_ylabel("cold init (ms)", color=COLOR_COLD,
                      fontsize=8.5, fontweight="bold")
    ax_bot.set_ylabel("warm (ms)", color=COLOR_WARM,
                      fontsize=8.5, fontweight="bold")
    ax_top.tick_params(axis="y", colors=COLOR_COLD)
    ax_bot.tick_params(axis="y", colors=COLOR_WARM)

    # Sample-count badge bottom-right of cold panel (above the broken-axis
    # diagonal break, inside the cold area)


    # Title + top-centered legend, with clear vertical separation.
    # Legend sits above the title, both above the chart.
    ax_top.set_title("Per-stage container initialization (OpenWhisk)", pad=10)
    _top_legend(fig,
                [("warm", COLOR_WARM), ("cold", COLOR_COLD)],
                y=0.9)

    fig.tight_layout(rect=(0, 0, 1, 0.93))
    save(fig, out_dir, "02_stage_inits")


# ---------------------------------------------------------------------------
# Plot 3: warm vs cold e2e, half-violin + jitter, log Y
# ---------------------------------------------------------------------------

def plot_cold_vs_warm(warm: np.ndarray, cold: np.ndarray, out_dir: Path):
    if warm.size == 0 and cold.size == 0:
        print("  no samples; skipping cold_vs_warm")
        return
    fig, ax = plt.subplots(figsize=(LNCS_COL_W, 2.9))
    _tint_panel(ax)

    width = 0.30
    for i, (label, vals, color) in enumerate(
        [("warm", warm, COLOR_WARM),
         ("cold", cold, COLOR_COLD)]
    ):
        if vals.size == 0:
            continue
        _halfviolin(ax, vals, i, color, side="right", width=width)
        _jitter_strip(ax, vals, i, color, side="left",
                      width=width, point_size=6, alpha=0.5,
                      seed=hash(("vs", label)) & 0xFFFF)
        med = float(np.median(vals))
        _median_bar(ax, med, i, color, half=width, lw=2.2)
        if med >= 1000:
            mtxt = f"{med/1000:.2f} s"
        elif med >= 10:
            mtxt = f"{med:.0f} ms"
        else:
            mtxt = f"{med:.1f} ms"
        ax.annotate(
            mtxt, xy=(i + width + 0.02, med),
            xytext=(2, 0), textcoords="offset points",
            ha="left", va="center",
            fontsize=8.0, color=color, fontweight="bold",
        )

    # Ratio callout — placed INSIDE the chart, top-center, well below the title
    if warm.size and cold.size:
        m_w, m_c = float(np.median(warm)), float(np.median(cold))
        ratio = m_c / m_w if m_c >= m_w else m_w / m_c
        ax.text(
            0.5, 0.55,
            f"cold/warm = {ratio:.1f}\u00d7",
            transform=ax.transAxes, ha="center", va="center",
            fontsize=8.0, color=COLOR_ANNOT, fontweight="bold",
            bbox=dict(facecolor=COLOR_RATIO_BOX, edgecolor=COLOR_RATIO_EDGE,
                      boxstyle="round,pad=0.25", linewidth=0.5),
        )

    ax.set_yscale("log")
    ax.set_xticks([0, 1])
    ax.set_xticklabels([f"warm ($n={warm.size}$)",
                        f"cold ($n={cold.size}$)"],
                       fontweight="bold")
    ax.set_xlim(-0.55, 1.55)
    ax.set_ylabel("end-to-end latency (ms, log scale)")
    ax.set_xlabel("")
    ax.grid(True, axis="y", which="major", color="white", linewidth=0.5)
    ax.grid(True, axis="y", which="minor", color="white",
            linewidth=0.3, alpha=0.7)
    ax.set_axisbelow(True)

    # Headroom so the top label/ratio box doesn't crash the cold cluster
    if warm.size and cold.size:
        ymax = max(float(warm.max()), float(cold.max()))
        ymin = min(float(warm[warm > 0].min()) if (warm > 0).any()
                   else float(warm.min()),
                   float(cold.min()))
        ax.set_ylim(ymin * 0.6, ymax * 2.8)

    ax.set_title("Cold vs warm end-to-end latency (OpenWhisk)", pad=10)
    _top_legend(fig,
                [("warm", COLOR_WARM), ("cold", COLOR_COLD)],
                y=0.8)

    fig.tight_layout(rect=(0, 0, 1, 0.93))
    save(fig, out_dir, "03_cold_vs_warm")


# ---------------------------------------------------------------------------
# Stats summary (printed to stdout)
# ---------------------------------------------------------------------------

def print_summary(warm, cold, warm_inits, cold_inits):
    def pct(xs, q):
        if len(xs) == 0:
            return float("nan")
        xs = np.sort(xs)
        return float(xs[min(len(xs) - 1, int(len(xs) * q))])

    print()
    print("Warm e2e (ms):")
    if warm.size:
        print(f"  n={warm.size}  p50={pct(warm,0.5):.2f}  "
              f"p95={pct(warm,0.95):.2f}  p99={pct(warm,0.99):.2f}  "
              f"max={warm.max():.2f}")
    else:
        print("  no warm samples")
    print()
    print("Cold e2e (ms):")
    if cold.size:
        print(f"  n={cold.size}  p50={pct(cold,0.5):.0f}  "
              f"p95={pct(cold,0.95):.0f}  max={cold.max():.0f}")
    else:
        print("  no cold samples")
    print()
    print("Per-stage init (ms):")
    print(f"  {'stage':10}  {'warm':>20}  {'cold':>22}")
    for stage in STAGE_ORDER:
        w = np.array(warm_inits.get(stage, []))
        c = np.array(cold_inits.get(stage, []))
        w_txt = (f"n={w.size:4}  p50={pct(w,0.5):.0f}"
                 if w.size else "n=   0")
        c_txt = (f"n={c.size:4}  p50={pct(c,0.5):.0f}  p95={pct(c,0.95):.0f}"
                 if c.size else "n=   0")
        print(f"  {stage:10}  {w_txt:>20}  {c_txt:>22}")
    print()
    if warm.size and cold.size:
        ratio = np.median(cold) / np.median(warm)
        print(f"Cold/warm median ratio: {ratio:.1f}x")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument("warm_dir", type=Path, help="warm run dir (with parents.csv)")
    ap.add_argument("cold_dir", type=Path, help="cold run dir (with parents.csv)")
    ap.add_argument("--output-dir", type=Path, default=Path("figures_combined"),
                    help="output dir (default: ./figures_combined)")
    args = ap.parse_args()

    for label, p in (("warm_dir", args.warm_dir),
                     ("cold_dir", args.cold_dir)):
        if str(p) in ("", "."):
            print(f"error: {label} resolved to '{p}' "
                  "— was $WARM_DIR or $COLD_DIR empty?",
                  file=sys.stderr)
            sys.exit(2)
        if not p.exists():
            print(f"error: {label} does not exist: {p}", file=sys.stderr)
            sys.exit(2)

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
    warm_inits = load_stage_inits(args.warm_dir, only_nonzero=False)
    cold_inits = load_stage_inits(args.cold_dir, only_nonzero=True)

    print(f"Warm dir: {args.warm_dir}")
    print(f"Cold dir: {args.cold_dir}")
    print(f"Output:   {out_dir}")
    print()
    print(f"Loaded: warm n={warm.size}, cold n={cold.size}")

    plot_e2e_cdf(warm, cold, out_dir)
    plot_stage_inits(warm_inits, cold_inits, out_dir)
    plot_cold_vs_warm(warm, cold, out_dir)
    print_summary(warm, cold, warm_inits, cold_inits)
    print()
    print("done.")


if __name__ == "__main__":
    main()