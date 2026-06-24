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
import numpy as np


STAGE_COLORS = {
    "relay":   "#2196F3",   # blue
    "forward": "#FF5722",   # orange
}
DEFAULT_COLORS = ["#4CAF50", "#9C27B0", "#F44336", "#FFC107"]


def stage_color(name: str, idx: int) -> str:
    return STAGE_COLORS.get(name, DEFAULT_COLORS[idx % len(DEFAULT_COLORS)])


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


def _style_ax(ax):
    ax.set_facecolor("#161B22")
    ax.tick_params(colors="#C9D1D9", labelsize=9)
    ax.xaxis.label.set_color("#C9D1D9")
    ax.yaxis.label.set_color("#C9D1D9")
    ax.title.set_color("#E6EDF3")
    for spine in ax.spines.values():
        spine.set_edgecolor("#30363D")


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


def _avg_per_second(raw_samples: list, n_seconds: int) -> list[float]:
    """Average dense [[t_s, v], ...] samples into 1-second integer bins."""
    bins: list[list] = [[] for _ in range(n_seconds + 1)]
    for t, v in raw_samples:
        idx = int(t)
        if 0 <= idx < len(bins):
            bins[idx].append(v)
    return [sum(b) / len(b) if b else 0.0 for b in bins]


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
    summary = json.load(open(summary_path))
    t_start = summary.get("test_start_wall", 0.0)
    stages  = summary.get("stage_names",
                          list(summary["scaling"]["per_stage"].keys()))
    asc_params = parse_autoscaler_params(run_dir)

    # -- data -----------------------------------------------------------------
    recv_ps = summary.get("recv_per_second", [])
    n_sec   = len(recv_ps)

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

    # scaling events: list of (t_s, action, stage) skipping init_jit
    scale_events: list[tuple[float, str, str]] = []
    for stage, data in summary["scaling"]["per_stage"].items():
        for ev in data["events"]:
            if ev["action"] == "init_jit":
                continue
            scale_events.append((ev["t_s"], ev["action"], stage))
    scale_events.sort()

    # queue depth averaged to 1-second bins
    qd_per_s: dict[str, list[float]] = {}
    for stage, raw in summary.get("queue_depth", {}).items():
        qd_per_s[stage] = _avg_per_second(raw, n_sec)

    x_end = n_sec + 1.0

    # -- figure ---------------------------------------------------------------
    fig, (ax_tp, ax_qd) = plt.subplots(
        1, 2,
        figsize=(16, 6),
        gridspec_kw={"width_ratios": [1, 1]},
    )
    fig.patch.set_facecolor("#0D1117")
    _style_ax(ax_tp)
    _style_ax(ax_qd)

    # =========================================================================
    # LEFT — throughput (1 s bars) + scale-event indicators
    # =========================================================================
    x_bars_tp = np.arange(n_sec)
    bar_w_tp  = 0.46

    y_tp_max = max(
        max(recv_ps) / 1000 if recv_ps else 0,
        max(sent_ps) / 1000 if sent_ps else 0,
    ) * 1.12

    if recv_ps:
        ax_tp.bar(x_bars_tp - bar_w_tp / 2,
                  [v / 1000 for v in recv_ps], width=bar_w_tp,
                  color="#4CAF50", alpha=0.85,
                  label="pipeline received (k ev/s)", zorder=3)
    if sent_ps:
        ax_tp.bar(x_bars_tp + bar_w_tp / 2,
                  [v / 1000 for v in sent_ps], width=bar_w_tp,
                  color="#E0E0E0", alpha=0.75,
                  label="loadgen sent (k ev/s)", zorder=3)

    # scale-event vertical markers
    # solid line + ▲ = scale up;  dashed line + ▼ = scale down
    # y positions for triangles: near top for ↑, near bottom for ↓
    y_up   = y_tp_max * 0.96
    y_down = y_tp_max * 0.04
    marker_added: set[str] = set()   # avoid duplicate legend entries
    for t_ev, action, stage in scale_events:
        color = stage_color(stage, stages.index(stage) if stage in stages else 0)
        is_up = action in ("spawn", "cold_start")
        ls  = "-"   if is_up else "--"
        mrk = "^"   if is_up else "v"
        y_m = y_up  if is_up else y_down
        ax_tp.axvline(t_ev, color=color, linewidth=1.0, alpha=0.45,
                      linestyle=ls, zorder=2)
        ax_tp.scatter([t_ev], [y_m], marker=mrk, color=color,
                      s=70, zorder=5)
        # legend key (one entry per stage × direction)
        key = f"{stage}_{'up' if is_up else 'down'}"
        if key not in marker_added:
            dir_label = "scale ↑" if is_up else "scale ↓"
            ax_tp.scatter([], [], marker=mrk, color=color, s=55,
                          label=f"{stage} {dir_label}")
            marker_added.add(key)

    ax_tp.set_xlim(-1, x_end)
    ax_tp.set_ylim(0, y_tp_max)
    ax_tp.set_xlabel("Time from pipeline start (s)", fontsize=10)
    ax_tp.set_ylabel("Throughput (k events/s)", fontsize=10)
    ax_tp.set_title(
        "Throughput  +  scaling events per stage",
        fontsize=11, fontweight="bold", pad=7,
    )
    ax_tp.grid(color="#30363D", linestyle="--", linewidth=0.6, zorder=0)
    ax_tp.legend(loc="upper right", framealpha=0.35, fontsize=8.5,
                 labelcolor="#C9D1D9", facecolor="#161B22", edgecolor="#30363D",
                 ncol=2)

    # =========================================================================
    # RIGHT — avg queue depth per second (grouped bar chart)
    # =========================================================================
    x_bars = np.arange(n_sec)
    bar_w  = 0.46      # each stage bar is half the 1-second slot

    # y cap: 8× the largest queue_up so the threshold line is visible
    max_qu  = max((p.get("queue_up", 50) for p in asc_params.values()), default=50)
    all_raw_max = max(
        (max(qd_per_s[s]) for s in stages if s in qd_per_s and qd_per_s[s]),
        default=max_qu,
    )
    y_qd_cap = max_qu * 8
    clipped  = all_raw_max > y_qd_cap

    for i, stage in enumerate(stages):
        color = stage_color(stage, i)
        avgs  = qd_per_s.get(stage, [0.0] * n_sec)[:n_sec]
        # clip to y_qd_cap for display
        avgs_clip = [min(v, y_qd_cap) for v in avgs]
        offset = (i - (len(stages) - 1) / 2) * bar_w
        ax_qd.bar(x_bars + offset, avgs_clip, width=bar_w,
                  color=color, alpha=0.80, label=f"stage: {stage}")

        # queue_up threshold line + label on the left
        p  = asc_params.get(stage, {})
        qu = p.get("queue_up")
        if qu is not None:
            ax_qd.axhline(qu, color=color, linewidth=1.3,
                          linestyle=":", alpha=0.9)
            ax_qd.text(0.5, qu + y_qd_cap * 0.012,
                       f"queue_up={qu:.0f} ({stage})",
                       color=color, fontsize=7.5, va="bottom", ha="left")

    ax_qd.set_xlim(-1, n_sec)
    ax_qd.set_ylim(0, y_qd_cap * 1.08)
    ax_qd.set_xlabel("Time from pipeline start (s)", fontsize=10)
    ax_qd.set_ylabel("Avg queue depth (events / s)", fontsize=10)
    cap_note = f"  [capped at {y_qd_cap:.0f}, peaks up to {all_raw_max:.0f}]" if clipped else ""
    ax_qd.set_title(
        f"Avg dispatcher queue depth — 1 s bins{cap_note}",
        fontsize=11, fontweight="bold", pad=7,
    )
    ax_qd.grid(axis="y", color="#30363D", linestyle="--", linewidth=0.6, zorder=0)
    ax_qd.legend(loc="upper right", framealpha=0.35, fontsize=8.5,
                 labelcolor="#C9D1D9", facecolor="#161B22", edgecolor="#30363D")

    # =========================================================================
    fig.suptitle(
        f"Epico — independent per-stage autoscaling  "
        f"({os.path.basename(run_dir)}, ZeroMQ, wave load)",
        fontsize=12, fontweight="bold", color="#E6EDF3",
    )
    fig.tight_layout(rect=[0, 0, 1, 0.95], pad=1.6)
    fig.savefig(out_path, dpi=150, bbox_inches="tight",
                facecolor=fig.get_facecolor())
    print(f"Saved → {out_path}")


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
    fig.patch.set_facecolor("#0D1117")
    gs = fig.add_gridspec(2, 2, height_ratios=[1, 1.4], width_ratios=[2, 1],
                          hspace=0.48, wspace=0.28,
                          left=0.07, right=0.97, top=0.88, bottom=0.08)
    ax_rep  = fig.add_subplot(gs[0, 0])
    ax_zoom = fig.add_subplot(gs[0, 1])
    ax_tp   = fig.add_subplot(gs[1, :])
    for ax in (ax_rep, ax_zoom, ax_tp): _style_ax(ax)

    patches = _draw_replicas_jsonl(ax_rep, stages, replica_series, t0, x_end)
    ax_rep.set_xlim(-0.5, x_end)
    max_c = max(p[1] for s in replica_series.values() for p in s)
    ax_rep.set_ylim(-0.3, max_c + 0.5)
    ax_rep.yaxis.set_major_locator(plt.MaxNLocator(integer=True))
    ax_rep.grid(axis="y", color="#30363D", linestyle="--", linewidth=0.6)
    ax_rep.set_xlabel("Time from start (s)", fontsize=9)
    ax_rep.set_ylabel("Active replicas", fontsize=9)
    ax_rep.set_title(f"Replica count — full run\n{os.path.basename(run_dir)}",
                     fontsize=10, fontweight="bold", pad=5)
    ax_rep.legend(handles=patches, loc="lower right", framealpha=0.3, fontsize=8,
                  labelcolor="#C9D1D9", facecolor="#161B22", edgecolor="#30363D")

    _draw_replicas_jsonl(ax_zoom, stages, replica_series, t0, cs_end + 0.01)
    ax_zoom.set_xlim(cs_start, cs_end)
    ax_zoom.set_ylim(-0.3, max_c + 0.5)
    ax_zoom.yaxis.set_major_locator(plt.MaxNLocator(integer=True))
    ax_zoom.grid(color="#30363D", linestyle="--", linewidth=0.6)
    ax_zoom.set_xlabel("Time from start (s)", fontsize=9)
    ax_zoom.set_ylabel("Active replicas", fontsize=9)
    ax_zoom.set_title("Cold-start zoom\n(each stage scales independently)",
                      fontsize=10, fontweight="bold", pad=5)
    for spine in ["left", "right"]:
        ax_zoom.spines[spine].set_edgecolor("#FFC107")
        ax_zoom.spines[spine].set_linewidth(1.4)
    ax_rep.axvspan(cs_start, cs_end, color="#FFC107", alpha=0.07, zorder=0)
    ax_rep.axvline(cs_start, color="#FFC107", linewidth=0.8, linestyle=":")
    ax_rep.axvline(cs_end,   color="#FFC107", linewidth=0.8, linestyle=":")

    if coll_eps:
        ax_tp.plot(rel(coll_ts), [e/1000 for e in coll_eps],
                   color="#4CAF50", linewidth=1.6, label="pipeline received (k ev/s)")
    if lg_eps:
        ax_tp.plot(rel(lg_ts), [e/1000 for e in lg_eps],
                   color="#FFC107", linewidth=1.6, linestyle="--",
                   label="loadgen sent (k ev/s)")
    ax_tp.set_xlim(-0.5, x_end)
    ax_tp.set_ylim(bottom=0)
    ax_tp.set_xlabel("Time from pipeline start (s)", fontsize=10)
    ax_tp.set_ylabel("Throughput (k events/s)", fontsize=10)
    ax_tp.set_title("Pipeline throughput over time", fontsize=11, fontweight="bold", pad=6)
    ax_tp.grid(color="#30363D", linestyle="--", linewidth=0.6)
    ax_tp.legend(loc="lower right", framealpha=0.3, fontsize=9,
                 labelcolor="#C9D1D9", facecolor="#161B22", edgecolor="#30363D")

    fig.suptitle("Epico — independent per-stage autoscaling",
                 fontsize=13, fontweight="bold", color="#E6EDF3", y=0.96)
    fig.savefig(out_path, dpi=150, bbox_inches="tight",
                facecolor=fig.get_facecolor())
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
