#!/usr/bin/env python3
"""
plot_scaling.py — plot stage replica count, queue depth, and loadgen throughput
from an Epico run directory, proving that each stage scales independently.

Supports two run formats:
  • wave/autoscale runs — uses _summary.json (rich: scaling events, queue depth
    time series, recv_per_second), expected for ZeroMQ + sinusoidal load runs
  • fixed-replica runs  — falls back to JSONL-only parsing (worker booted events
    + collector progress + loadgen progress)

Usage:
    python3 bench/plot_scaling.py [run_dir] [--out output.png]

    run_dir  path to a results sub-directory (auto-detected if omitted)
"""

import argparse
import glob
import json
import os
import sys
from collections import defaultdict

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import matplotlib.ticker as ticker
from matplotlib.lines import Line2D
import numpy as np


# Paper-style colors — chosen to match the Epico/Metapod paper palette and
# remain distinguishable in greyscale print.
COLOR_PANEL_BG = "#f4f4f6"
COLOR_GRID     = "#d8d8d8"
COLOR_AXIS     = "#333333"
COLOR_TP_RECV  = "#1a7c3f"   # pipeline received (dark green)
COLOR_TP_SENT  = "#777777"   # loadgen sent (neutral grey)

STAGE_COLORS = {
    "normalize": "#2166ac",   # blue   (ColorBrewer RdYlBu)
    "detect":    "#d6604d",   # red    (ColorBrewer RdYlBu)
    "finalize":  "#1a9641",   # green  (ColorBrewer RdYlGn)
    "relay":     "#2166ac",
    "forward":   "#d6604d",
}
DEFAULT_COLORS = ["#762a83", "#e08214", "#4393c3", "#f1a340"]


def stage_color(name: str, idx: int) -> str:
    return STAGE_COLORS.get(name, DEFAULT_COLORS[idx % len(DEFAULT_COLORS)])


def setup_style():
    """rcParams matching the Epico paper aesthetic (LNCS single-column)."""
    plt.rcParams.update({
        "font.family":          "sans-serif",
        "font.sans-serif":      ["DejaVu Sans", "Arial", "Helvetica"],
        "mathtext.fontset":     "dejavusans",
        "font.size":            8.5,
        "axes.titlesize":       10.0,
        "axes.titleweight":     "bold",
        "axes.titlelocation":   "center",
        "axes.titlepad":        7,
        "axes.labelsize":       8.5,
        "axes.labelcolor":      COLOR_AXIS,
        "axes.edgecolor":       COLOR_AXIS,
        "axes.linewidth":       0.6,
        "axes.spines.top":      False,
        "axes.spines.right":    False,
        "axes.grid":            True,
        "axes.grid.axis":       "y",
        "axes.axisbelow":       True,
        "grid.color":           COLOR_GRID,
        "grid.linewidth":       0.5,
        "grid.linestyle":       "-",
        "xtick.color":          COLOR_AXIS,
        "ytick.color":          COLOR_AXIS,
        "xtick.labelsize":      8.0,
        "ytick.labelsize":      8.0,
        "xtick.major.width":    0.6,
        "ytick.major.width":    0.6,
        "xtick.major.size":     2.5,
        "ytick.major.size":     2.5,
        "legend.fontsize":      8.0,
        "legend.frameon":       False,
        "figure.dpi":           120,
        "savefig.dpi":          300,
        "savefig.bbox":         "tight",
        "savefig.pad_inches":   0.05,
        "pdf.fonttype":         42,
        "ps.fonttype":          42,
    })


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def find_jsonl(run_dir: str, prefix: str) -> str | None:
    hits = sorted(glob.glob(os.path.join(run_dir, f"{prefix}_*.jsonl")))
    return hits[0] if hits else None


def load_jsonl(path: str) -> list[dict]:
    events = []
    with open(path) as fh:
        for line in fh:
            line = line.strip()
            if line:
                try:
                    events.append(json.loads(line))
                except json.JSONDecodeError:
                    pass
    return events


def find_summary(run_dir: str) -> str | None:
    hits = sorted(glob.glob(os.path.join(run_dir, "*_summary.json")))
    return hits[0] if hits else None


def _tint_panel(ax):
    ax.set_facecolor(COLOR_PANEL_BG)


def _style_twin_ax(ax2):
    """Right y-axis for the queue depth overlay — minimal, paper-style."""
    ax2.tick_params(axis="y", colors=COLOR_AXIS, labelsize=7)
    ax2.yaxis.label.set_color(COLOR_AXIS)
    ax2.spines["right"].set_edgecolor(COLOR_AXIS)
    ax2.spines["right"].set_linewidth(0.5)
    for side in ("left", "top", "bottom"):
        ax2.spines[side].set_visible(False)
    ax2.grid(False)


def downsample(pairs: list, window: int) -> tuple[list, list]:
    """Average a list of [t, v] pairs into non-overlapping windows."""
    if not pairs:
        return [], []
    ts_out, v_out = [], []
    bucket_ts, bucket_v = [], []
    base_t = pairs[0][0]
    step = window
    cutoff = base_t + step
    for t, v in pairs:
        if t < cutoff:
            bucket_ts.append(t)
            bucket_v.append(v)
        else:
            if bucket_ts:
                ts_out.append(sum(bucket_ts) / len(bucket_ts))
                v_out.append(sum(bucket_v) / len(bucket_v))
            bucket_ts, bucket_v = [t], [v]
            cutoff = t + step
    if bucket_ts:
        ts_out.append(sum(bucket_ts) / len(bucket_ts))
        v_out.append(sum(bucket_v) / len(bucket_v))
    return ts_out, v_out


# ---------------------------------------------------------------------------
# Summary-JSON mode (wave / autoscale runs)
# ---------------------------------------------------------------------------

def build_replica_ts_from_summary(
    stage_scaling: dict,  # per_stage dict from summary['scaling']
) -> dict[str, tuple[list, list]]:
    """
    Build (ts, counts) step series per stage from summary scaling events.
    Ignores 'init_jit' events (don't change replica count).
    """
    result = {}
    for stage, data in stage_scaling.items():
        ts, counts = [0.0], [0]
        cur = 0
        for ev in data["events"]:
            if ev["action"] == "init_jit":
                continue
            cur = ev["new_count"]
            ts.append(ev["t_s"])
            counts.append(cur)
        result[stage] = (ts, counts)
    return result


def build_queue_ts_from_summary(
    queue_raw: dict,  # summary['queue_depth']  {stage: [[t_s, qd], ...]}
    window_s: float = 0.5,
) -> dict[str, tuple[list, list]]:
    """Downsample the dense queue-depth samples to window_s resolution."""
    out = {}
    for stage, samples in queue_raw.items():
        ts_d, v_d = downsample(samples, window_s)
        out[stage] = (ts_d, v_d)
    return out


def parse_autoscaler_params(run_dir: str) -> dict[str, dict]:
    """Read queue_up, queue_down, min_rep, max_rep from 'autoscaler ready' JSONL events."""
    master_path = find_jsonl(run_dir, "master")
    params: dict[str, dict] = {}
    if not master_path:
        return params
    for ev in load_jsonl(master_path):
        if ev.get("msg") == "autoscaler ready":
            stage = ev.get("component", "").replace("autoscaler/", "")
            params[stage] = {
                "queue_up":   float(ev.get("queue_up", 50)),
                "queue_down": float(ev.get("queue_down", 0)),
                "min_rep":    int(ev.get("min_rep", 1)),
                "max_rep":    int(ev.get("max_rep", 8)),
            }
    return params


def _sent_per_second(cumul_pts: list[tuple[float, int]], n_seconds: int) -> list[float]:
    """
    Resample sparse cumulative-sent progress points to per-second event rate.
    cumul_pts: sorted list of (t_rel_s, total_sent_so_far).
    Returns a list of length n_seconds where each value is events/s in that bin.
    """
    if not cumul_pts:
        return [0.0] * n_seconds

    def interp(t: float) -> float:
        if t <= cumul_pts[0][0]:
            return float(cumul_pts[0][1])
        if t >= cumul_pts[-1][0]:
            return float(cumul_pts[-1][1])
        for i in range(1, len(cumul_pts)):
            if cumul_pts[i][0] >= t:
                t0, v0 = cumul_pts[i - 1]
                t1, v1 = cumul_pts[i]
                alpha = (t - t0) / (t1 - t0)
                return v0 + alpha * (v1 - v0)
        return float(cumul_pts[-1][1])

    return [interp(s + 1) - interp(s) for s in range(n_seconds)]


def plot_wave_run(run_dir: str, summary_path: str, out_path: str) -> None:
    setup_style()
    summary = json.load(open(summary_path))
    t_start = summary.get("test_start_wall", 0.0)
    stages  = summary.get("stage_names",
                          list(summary["scaling"]["per_stage"].keys()))

    # -- data -----------------------------------------------------------------
    recv_ps    = summary.get("recv_per_second", [])
    n_sec      = len(recv_ps)
    t_arr      = np.arange(n_sec)
    asc_params = parse_autoscaler_params(run_dir)

    # loadgen sent rate — resample cumulative progress to 1-second bins
    lg_path = find_jsonl(run_dir, "loadgen")
    sent_ps: list[float] = []
    if lg_path:
        lg_events = load_jsonl(lg_path)
        cumul_pts = sorted(
            (float(e["ts"]) - t_start, int(e["sent"]))
            for e in lg_events if e.get("msg") == "progress"
        )
        sent_ps = _sent_per_second(cumul_pts, n_sec)

    # per-stage replica step series and event lists
    replica_ts = build_replica_ts_from_summary(summary["scaling"]["per_stage"])
    scale_up:   dict[str, list[tuple[float, int]]] = {s: [] for s in stages}
    scale_down: dict[str, list[tuple[float, int]]] = {s: [] for s in stages}
    for stage, data in summary["scaling"]["per_stage"].items():
        for ev in data["events"]:
            if ev["action"] == "init_jit":
                continue
            pt = (ev["t_s"], int(ev["new_count"]))
            if ev["action"] in ("spawn", "cold_start"):
                scale_up[stage].append(pt)
            elif ev["action"] == "drain":
                scale_down[stage].append(pt)

    # queue depth: downsample dense samples to 0.5 s resolution
    queue_ts = build_queue_ts_from_summary(summary.get("queue_depth", {}))

    x_end = float(n_sec)

    # -- figure: 1 + N rows, shared x ----------------------------------------
    # Width matches LNCS full text block (4.8 in); height scales with row count.
    n_rows = 1 + len(stages)
    height_ratios = [1.6] + [1.0] * len(stages)
    fig, axes = plt.subplots(
        n_rows, 1,
        figsize=(7.5, 0.9 + 0.85 * n_rows),
        gridspec_kw={"height_ratios": height_ratios, "hspace": 0.18},
        sharex=True,
    )
    fig.patch.set_facecolor("white")
    for ax in axes:
        _tint_panel(ax)

    ax_tp      = axes[0]
    stage_axes = axes[1:]

    # =========================================================================
    # ROW 0 — throughput lines
    # =========================================================================
    if recv_ps:
        ax_tp.plot(t_arr, recv_ps,
                   color=COLOR_TP_RECV, linewidth=1.8,
                   label="service rate", zorder=3)
    if sent_ps:
        ax_tp.plot(t_arr, sent_ps,
                   color=COLOR_TP_SENT, linewidth=1.3, linestyle="--", alpha=0.8,
                   label="arrival rate", zorder=3)

    y_tp_max = max(
        max(recv_ps) if recv_ps else 0,
        max(sent_ps) if sent_ps else 0,
    ) * 1.15
    ax_tp.set_ylim(0, y_tp_max)
    ax_tp.set_ylabel("ev/s")
    ax_tp.yaxis.set_major_formatter(
        ticker.FuncFormatter(lambda x, _: f"{x/1000:.0f}k" if x >= 1000 else str(int(x)))
    )
    ax_tp.set_title(
        f"Epico — per-stage autoscaling  "
        f"({os.path.basename(run_dir)}, ZeroMQ, wave load)",
    )
    ax_tp.legend(loc="upper center", bbox_to_anchor=(0.5, 1.0),
                 ncol=2, borderaxespad=0.3)

    # =========================================================================
    # ROWS 1..N — left y: queue depth (continuous), right y: replica count
    # =========================================================================
    REPLICA_COLOR = "#555555"

    for i, (stage, ax) in enumerate(zip(stages, stage_axes)):
        color = stage_color(stage, i)
        ts, counts = replica_ts.get(stage, ([0.0], [0]))
        step_ts = list(ts) + [x_end]
        step_c  = list(counts) + [counts[-1]]

        # ── LEFT y-axis: continuous queue depth ──────────────────────────────
        qd_t, qd_v = queue_ts.get(stage, ([], []))
        if qd_t:
            p      = asc_params.get(stage, {})
            qu     = float(p.get("queue_up", 50))
            qd_arr = np.array(qd_t, dtype=float)
            qd_val = np.array(qd_v, dtype=float)
            ax.fill_between(qd_arr, qd_val, alpha=0.18, color=color, zorder=0)
            ax.plot(qd_arr, qd_val, color=color, linewidth=0.8, zorder=1)
            ax.axhline(qu, color=color, linewidth=1.0, linestyle=":", alpha=0.8, zorder=2)
            ax.text(25, qu * 1.04, f"q↑={qu:.0f}", color=color,
                    fontsize=7, va="bottom", ha="left")
            y_max = max(float(np.nanmax(qd_val)), qu) * 1.15
            ax.set_ylim(0, y_max)

        ax.set_ylabel(stage, color=color, fontweight="bold", labelpad=4)

        # ── RIGHT y-axis: replica count (thin, same neutral color) ───────────
        ax2 = ax.twinx()
        _style_twin_ax(ax2)
        ax2.step(step_ts, step_c, where="post",
                 color=REPLICA_COLOR, linewidth=1.0, zorder=3)
        ax2.set_ylim(0, 9)
        ax2.set_yticks([0, 2, 4, 6, 8])
        ax2.set_ylabel("replicas", labelpad=4)

    # shared x label on last panel only
    stage_axes[-1].set_xlabel("Time from pipeline start (s)", fontsize=10)

    plt.xlim(-1, x_end + 1)
    axes[-1].xaxis.set_major_locator(ticker.MultipleLocator(10))
    axes[-1].xaxis.set_minor_locator(ticker.MultipleLocator(5))

    # shared figure legend
    legend_handles = [
        mpatches.Patch(facecolor=COLOR_AXIS, alpha=0.25, edgecolor="none",
                       label="dispatcher queue depth"),
        Line2D([0], [0], color=COLOR_AXIS, linewidth=1.2, linestyle=":",
               label="scale-up threshold  (q↑)"),
        Line2D([0], [0], color=REPLICA_COLOR, linewidth=1.0,
               label="replica count"),
    ]
    fig.legend(handles=legend_handles, loc="lower center",
               bbox_to_anchor=(0.5, -0.04), ncol=3,
               frameon=False, fontsize=8.0,
               handletextpad=0.5, columnspacing=1.4)

    fig.subplots_adjust(bottom=0.11)

    # save PNG and PDF
    import pathlib
    stem = pathlib.Path(out_path).with_suffix("")
    for ext in (".png", ".pdf"):
        p = str(stem) + ext
        fig.savefig(p, facecolor=fig.get_facecolor())
        print(f"Saved → {p}")


# ---------------------------------------------------------------------------
# JSONL-only mode (fixed-replica bench_N16 runs)
# ---------------------------------------------------------------------------

def parse_replicas_jsonl(master_events):
    counts: dict[str, int] = defaultdict(int)
    series: dict[str, list] = defaultdict(list)
    for ev in master_events:
        if ev.get("msg") == "worker booted":
            comp = ev.get("component", "")
            if comp.startswith("worker/"):
                stage = comp.split("/", 1)[1]
                counts[stage] += 1
                series[stage].append((float(ev["ts"]), counts[stage]))
    return dict(series)


def parse_coll_throughput(master_events):
    pts = [(float(ev["ts"]), int(ev["received"]))
           for ev in master_events if ev.get("msg") == "collector progress"]
    if len(pts) < 2:
        return [], []
    pts.sort()
    ts_out, eps_out = [], []
    for i in range(1, len(pts)):
        dt = pts[i][0] - pts[i-1][0]
        if dt > 0:
            ts_out.append((pts[i][0] + pts[i-1][0]) / 2)
            eps_out.append((pts[i][1] - pts[i-1][1]) / dt)
    return ts_out, eps_out


def parse_lg_throughput(lg_events):
    pts = [(float(ev["ts"]), int(ev["sent"]))
           for ev in lg_events if ev.get("msg") == "progress"]
    if len(pts) < 2:
        return [], []
    pts.sort()
    ts_out, eps_out = [], []
    for i in range(1, len(pts)):
        dt = pts[i][0] - pts[i-1][0]
        if dt > 0:
            ts_out.append((pts[i][0] + pts[i-1][0]) / 2)
            eps_out.append((pts[i][1] - pts[i-1][1]) / dt)
    return ts_out, eps_out


def _draw_replicas_jsonl(ax, stages, replica_series, t0, x_end):
    patches = []
    for i, stage in enumerate(stages):
        color = stage_color(stage, i)
        pts = replica_series[stage]
        ts_rel = [p[0] - t0 + i * 0.002 for p in pts]
        counts  = [p[1] for p in pts]
        step_ts = [-0.001] + ts_rel + [x_end]
        step_c  = [0] + counts + [counts[-1]]
        ax.step(step_ts, step_c, where="post", color=color, linewidth=2.2)
        ax.scatter(ts_rel, counts, color=color, s=45, zorder=5)
        patches.append(mpatches.Patch(color=color, label=f"stage: {stage}"))
    return patches


def plot_jsonl_run(run_dir: str, out_path: str) -> None:
    master_path = find_jsonl(run_dir, "master")
    lg_path     = find_jsonl(run_dir, "loadgen")
    if not master_path:
        sys.exit(f"ERROR: no master_*.jsonl in {run_dir}")

    master_events = load_jsonl(master_path)
    lg_events     = load_jsonl(lg_path) if lg_path else []

    replica_series = parse_replicas_jsonl(master_events)
    if not replica_series:
        sys.exit("ERROR: no 'worker booted' events found")

    coll_ts, coll_eps = parse_coll_throughput(master_events)
    lg_ts, lg_eps     = parse_lg_throughput(lg_events)

    all_ts = [float(ev["ts"]) for ev in master_events + lg_events if "ts" in ev]
    t0 = min(all_ts)

    def rel(ts_list): return [t - t0 for t in ts_list]

    stages = sorted(replica_series.keys())
    all_boot_rel = sorted(p[0] - t0 for s in replica_series.values() for p in s)
    x_end = max(
        max(rel(coll_ts), default=0),
        max(rel(lg_ts), default=0),
        all_boot_rel[-1] if all_boot_rel else 1,
    ) + 1.0
    cs_start = max(all_boot_rel[0] - 0.05, -0.01) if all_boot_rel else -0.01
    cs_end   = all_boot_rel[-1] + 0.05 if all_boot_rel else 1.0

    fig = plt.figure(figsize=(14, 8))
    fig.patch.set_facecolor("white")
    gs = fig.add_gridspec(2, 2, height_ratios=[1, 1.4], width_ratios=[2, 1],
                          hspace=0.48, wspace=0.28,
                          left=0.07, right=0.97, top=0.88, bottom=0.08)
    ax_rep  = fig.add_subplot(gs[0, 0])
    ax_zoom = fig.add_subplot(gs[0, 1])
    ax_tp   = fig.add_subplot(gs[1, :])
    for ax in (ax_rep, ax_zoom, ax_tp): _tint_panel(ax)

    patches = _draw_replicas_jsonl(ax_rep, stages, replica_series, t0, x_end)
    ax_rep.set_xlim(-0.5, x_end)
    max_c = max(p[1] for s in replica_series.values() for p in s)
    ax_rep.set_ylim(-0.3, max_c + 0.5)
    ax_rep.yaxis.set_major_locator(plt.MaxNLocator(integer=True))
    ax_rep.set_xlabel("Time from start (s)")
    ax_rep.set_ylabel("Active replicas")
    ax_rep.set_title(f"Replica count — full run\n{os.path.basename(run_dir)}")
    ax_rep.legend(handles=patches, loc="lower right")

    _draw_replicas_jsonl(ax_zoom, stages, replica_series, t0, cs_end + 0.01)
    ax_zoom.set_xlim(cs_start, cs_end)
    ax_zoom.set_ylim(-0.3, max_c + 0.5)
    ax_zoom.yaxis.set_major_locator(plt.MaxNLocator(integer=True))
    ax_zoom.set_xlabel("Time from start (s)")
    ax_zoom.set_ylabel("Active replicas")
    ax_zoom.set_title("Cold-start zoom\n(each stage scales independently)")
    for spine in ["left", "right"]:
        ax_zoom.spines[spine].set_edgecolor("#e08214")
        ax_zoom.spines[spine].set_linewidth(1.4)
    ax_rep.axvspan(cs_start, cs_end, color="#e08214", alpha=0.08, zorder=0)
    ax_rep.axvline(cs_start, color="#e08214", linewidth=0.8, linestyle=":")
    ax_rep.axvline(cs_end,   color="#e08214", linewidth=0.8, linestyle=":")

    if coll_eps:
        ax_tp.plot(rel(coll_ts), [e/1000 for e in coll_eps],
                   color=COLOR_TP_RECV, linewidth=1.6, label="pipeline received (k ev/s)")
    if lg_eps:
        ax_tp.plot(rel(lg_ts), [e/1000 for e in lg_eps],
                   color=COLOR_TP_SENT, linewidth=1.6, linestyle="--",
                   label="loadgen sent (k ev/s)")
    ax_tp.set_xlim(-0.5, x_end)
    ax_tp.set_ylim(bottom=0)
    ax_tp.set_xlabel("Time from pipeline start (s)")
    ax_tp.set_ylabel("Throughput (k events/s)")
    ax_tp.set_title("Pipeline throughput over time")
    ax_tp.legend(loc="lower right")

    fig.suptitle("Epico — independent per-stage autoscaling",
                 fontsize=13, fontweight="bold", color=COLOR_AXIS, y=0.96)
    fig.savefig(out_path, facecolor=fig.get_facecolor())
    print(f"Saved → {out_path}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def find_default_run() -> str:
    for base in [
        os.path.join(os.path.dirname(__file__), "..", "bench_wave", "results", "wave_run_1"),
        os.path.join(os.path.dirname(__file__), "..", "bench_N16", "results", "lat_mpmc_json_1"),
    ]:
        base = os.path.normpath(base)
        if os.path.isdir(base):
            return base
    hits = sorted(
        d for d in glob.glob(os.path.join(
            os.path.dirname(__file__), "..", "bench_N16", "results", "*"
        )) if os.path.isdir(d)
    )
    return hits[0] if hits else sys.exit("ERROR: no run directories found")


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("run_dir", nargs="?", default=None)
    parser.add_argument("--out", default="scaling_plot.png")
    args = parser.parse_args()

    run_dir = args.run_dir or find_default_run()
    if not os.path.isdir(run_dir):
        sys.exit(f"ERROR: {run_dir!r} is not a directory")

    summary = find_summary(run_dir)
    if summary and "scaling" in json.load(open(summary)):
        print(f"Using summary-JSON mode  ({os.path.basename(summary)})")
        plot_wave_run(run_dir, summary, args.out)
    else:
        print("Using JSONL-only mode")
        plot_jsonl_run(run_dir, args.out)


if __name__ == "__main__":
    main()
