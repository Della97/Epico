#!/usr/bin/env python3
"""
plot_run.py — produce paper-grade figures from a Metapod run summary.

Usage:
    python plot_run.py path/to/agent_20260422_190827_summary.json
    python plot_run.py summary.json --out ./figures --format pdf
    python plot_run.py summary.json --stage normalize   # filter scaling panel

Produces figures:

  Latency
    1.  e2e_latency_cdf.pdf       E2E latency CDF (log-x), p50/p95/p99 annotated
    2.  latency_timeseries.pdf    Per-second p50/p99/p99.9 over time
    3.  per_stage_latency.pdf     Per-stage WASM execution time distributions

  Throughput & scaling
    4.  throughput.pdf            Events/sec over time with mean line
    5.  scaling.pdf               Replica count + throughput (two-axis)

  Worker breakdown (new)
    6.  worker_breakdown.pdf      Stacked bar: wasm vs serde vs overhead per stage
    7.  worker_cdf.pdf            CDFs of total_us / wasm_us / serde_us per stage
    8.  serde_timeseries.pdf      Serde us p50/p99 over time

  Communication (new)
    9.  inter_stage.pdf           Per-edge transport latency histograms
   10.  queue_depth.pdf           Dispatcher queue depth over time, per stage

  Infrastructure
   11.  cold_start.pdf            Cold-start latency scatter + marginal histogram
   12.  resources.pdf             CPU % and RSS MB over time

  Text
   13.  run_card.txt              Headline metrics, paste-ready

Depends on: matplotlib, seaborn, numpy (no other deps).
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns


# ── Style ─────────────────────────────────────────────────────────────────────

sns.set_theme(
    context="paper",
    style="whitegrid",
    palette="deep",
    font_scale=1.1,
)
plt.rcParams.update({
    "figure.figsize":   (5.2, 3.2),
    "figure.dpi":       140,
    "savefig.dpi":      300,
    "savefig.bbox":     "tight",
    "axes.grid":        True,
    "grid.linewidth":   0.4,
    "grid.alpha":       0.5,
    "lines.linewidth":  1.6,
    "legend.frameon":   False,
})


# ── Loader ────────────────────────────────────────────────────────────────────

def load_summary(path: Path) -> dict[str, Any]:
    with path.open() as f:
        data = json.load(f)
    if "latency_ms" not in data or "resources" not in data:
        raise SystemExit(
            f"{path} doesn't look like a valid summary.\n"
            "Expected keys: environment, counters, throughput, latency_ms, scaling, resources.\n"
            "Re-run with the patched agent."
        )
    return data


# ── Helpers ───────────────────────────────────────────────────────────────────

def _save(fig, out_dir: Path, filename: str) -> Path:
    path = out_dir / filename
    fig.savefig(path)
    plt.close(fig)
    return path


# ─────────────────────────────────────────────────────────────────────────────
# 1. E2E latency CDF
# ─────────────────────────────────────────────────────────────────────────────

def plot_latency_cdf(summary: dict, out_dir: Path, fmt: str) -> Path | None:
    samples = np.asarray(summary["latency_ms"]["e2e"]["samples"], dtype=float)
    if samples.size == 0:
        print("warning: no latency samples; skipping CDF")
        return None

    samples = np.sort(samples)
    pcts    = np.linspace(0, 100, samples.size)

    fig, ax = plt.subplots()
    ax.plot(samples, pcts, linewidth=2)
    ax.set_xscale("log")
    ax.set_xlabel("End-to-end latency (ms)")
    ax.set_ylabel("Cumulative share (%)")

    e2e = summary["latency_ms"]["e2e"]
    for p_key, label in [("p50", "p50"), ("p95", "p95"), ("p99", "p99")]:
        v = e2e.get(p_key)
        if v and v > 0:
            ax.axvline(v, color="grey", linestyle="--", linewidth=0.8, alpha=0.7)
            ax.text(v, 5, f" {label}={v:.2f} ms",
                    rotation=90, va="bottom", ha="left", fontsize=8, color="#444")

    ax.set_ylim(0, 100)
    ax.set_title(f"End-to-end latency CDF  (n = {e2e['count']:,})")
    return _save(fig, out_dir, f"e2e_latency_cdf.{fmt}")


# ─────────────────────────────────────────────────────────────────────────────
# 2. Latency time series
# ─────────────────────────────────────────────────────────────────────────────

def plot_latency_timeseries(summary: dict, out_dir: Path, fmt: str) -> Path | None:
    rows = (summary.get("events") or {}).get("rows") or []
    if not rows:
        print("warning: no events.rows; skipping latency timeseries")
        return None

    ts  = np.fromiter((r[0] for r in rows), dtype=float, count=len(rows))
    lat = np.fromiter((r[1] for r in rows), dtype=float, count=len(rows))

    duration = summary.get("duration", float(ts.max()) if ts.size else 1.0)
    n_bins   = max(1, int(round(duration)))
    edges    = np.arange(0, n_bins + 1, dtype=float)
    bin_ix   = np.clip(np.searchsorted(edges, ts, side="right") - 1, 0, n_bins - 1)

    p50  = np.full(n_bins, np.nan)
    p99  = np.full(n_bins, np.nan)
    p999 = np.full(n_bins, np.nan)
    for b in range(n_bins):
        mask = bin_ix == b
        if mask.any():
            vals    = lat[mask]
            p50[b]  = np.percentile(vals, 50)
            p99[b]  = np.percentile(vals, 99)
            p999[b] = np.percentile(vals, 99.9)

    x = edges[:-1] + 0.5
    fig, ax = plt.subplots(figsize=(6.4, 3.4))
    ax.plot(x, p50,  label="p50",   color="C0", linewidth=1.4)
    ax.plot(x, p99,  label="p99",   color="C1", linewidth=1.4)
    ax.plot(x, p999, label="p99.9", color="C3", linewidth=1.2, alpha=0.85)
    ax.set_yscale("log")
    ax.set_xlabel("Time since test start (s)")
    ax.set_ylabel("E2E latency (ms, log)")
    ax.set_title(f"E2E latency over time  (n = {lat.size:,})")

    for stage_sc in summary.get("scaling", {}).get("per_stage", {}).values():
        for e in stage_sc.get("events", []):
            if e.get("action") == "cold_start" and e.get("t_s") is not None:
                ax.axvline(e["t_s"], color="grey", linestyle=":", linewidth=0.8, alpha=0.6)

    ax.legend(loc="upper right")
    return _save(fig, out_dir, f"latency_timeseries.{fmt}")

# ─────────────────────────────────────────────────────────────────────────────
# 2b. Latency scatter time series (Raw points, linear scale)
# ─────────────────────────────────────────────────────────────────────────────

def plot_e2e_scatter(summary: dict, out_dir: Path, fmt: str) -> Path | None:
    rows = (summary.get("events") or {}).get("rows") or []
    if not rows:
        print("warning: no events.rows; skipping E2E scatter")
        return None

    # r[0] is timestamp, r[1] is e2e latency
    ts  = np.fromiter((r[0] for r in rows), dtype=float, count=len(rows))
    lat = np.fromiter((r[1] for r in rows), dtype=float, count=len(rows))

    fig, ax = plt.subplots(figsize=(6.4, 3.4))
    
    # Use a small point size and alpha (transparency) to handle dense clusters
    ax.scatter(ts, lat, s=2, alpha=0.3, color="C0", edgecolors="none")
    
    ax.set_xlabel("Time since test start (s)")
    ax.set_ylabel("E2E latency (ms)")
    ax.set_ylim(bottom=0)  # Pin the bottom of the graph to 0 ms
    ax.set_title(f"Raw E2E latency over time  (n = {lat.size:,})")

    # Add cold start markers if available
    for stage_sc in summary.get("scaling", {}).get("per_stage", {}).values():
        for e in stage_sc.get("events", []):
            if e.get("action") == "cold_start" and e.get("t_s") is not None:
                ax.axvline(e["t_s"], color="grey", linestyle=":", linewidth=0.8, alpha=0.6)

    return _save(fig, out_dir, f"e2e_latency_scatter.{fmt}")

# ─────────────────────────────────────────────────────────────────────────────
# 3. Per-stage WASM latency distributions
# ─────────────────────────────────────────────────────────────────────────────

def plot_per_stage_latency(summary: dict, out_dir: Path, fmt: str) -> Path | None:
    """Violin plots of WASM execution time per stage, from bench_hops in events.rows.
    Falls back to summary percentile bars if raw hop data is unavailable."""
    rows = (summary.get("events") or {}).get("rows") or []

    stage_samples: dict[str, list[float]] = {}
    for row in rows:
        if len(row) < 3:
            continue
        for hop in row[2]:
            if len(hop) < 3:
                continue
            name, enter, exit_ = hop[0], float(hop[1]), float(hop[2])
            if exit_ > enter:
                stage_samples.setdefault(name, []).append((exit_ - enter) * 1000.0)

    if not stage_samples:
        per_stage = summary.get("latency_ms", {}).get("per_stage", {})
        if not per_stage:
            print("warning: no per-stage latency data; skipping")
            return None
        fig, ax = plt.subplots()
        names = list(per_stage.keys())
        x = np.arange(len(names))
        ax.bar(x - 0.2, [per_stage[n]["p50"] for n in names], 0.35, label="p50")
        ax.bar(x + 0.2, [per_stage[n]["p99"] for n in names], 0.35, label="p99")
        ax.set_xticks(x)
        ax.set_xticklabels(names, rotation=20, ha="right")
        ax.set_ylabel("WASM execution time (ms)")
        ax.set_title("Per-stage WASM latency")
        ax.legend()
        return _save(fig, out_dir, f"per_stage_latency.{fmt}")

    stages = sorted(stage_samples.keys())
    n = len(stages)
    fig, axes = plt.subplots(1, n, figsize=(3.2 * n, 3.2), squeeze=False)

    for i, name in enumerate(stages):
        ax  = axes[0][i]
        arr = np.asarray(stage_samples[name], dtype=float)
        ax.violinplot(arr, positions=[0], showmedians=True, showextrema=True)
        p99 = np.percentile(arr, 99)
        ax.axhline(p99, color="C3", linestyle="--", linewidth=0.9, alpha=0.8,
                   label=f"p99 = {p99:.2f} ms")
        ax.set_xticks([])
        ax.set_title(f"{name}\n(n={arr.size:,})", fontsize=10)
        if i == 0:
            ax.set_ylabel("WASM execution time (ms)")
        ax.legend(fontsize=8)

    fig.suptitle("Per-stage WASM execution time", y=1.02)
    fig.tight_layout()
    return _save(fig, out_dir, f"per_stage_latency.{fmt}")


# ─────────────────────────────────────────────────────────────────────────────
# 4. Throughput
# ─────────────────────────────────────────────────────────────────────────────

def plot_throughput(summary: dict, out_dir: Path, fmt: str) -> Path | None:
    per_sec = summary.get("recv_per_second") or []
    if not per_sec:
        print("warning: no recv_per_second; skipping throughput")
        return None

    xs = np.asarray(summary["lat_ts_labels"], dtype=float)
    ys = np.asarray(per_sec, dtype=float)

    fig, ax = plt.subplots()
    ax.plot(xs, ys, linewidth=1.4, alpha=0.85)
    ax.set_xlabel("Time since test start (s)")
    ax.set_ylabel("Events / sec")

    mean_eps = summary["throughput"]["sustained_eps_warmup_trimmed"]
    ax.axhline(mean_eps, color="C3", linestyle="--", linewidth=1.0,
               label=f"mean = {mean_eps:,.0f} ev/s")
    ax.legend(loc="lower right")
    ax.set_ylim(bottom=0)
    total = summary["counters"]["events_received"]
    ax.set_title(f"Sustained throughput  (total: {total:,})")
    return _save(fig, out_dir, f"throughput.{fmt}")


# ─────────────────────────────────────────────────────────────────────────────
# 5. Scaling (replicas + throughput)
# ─────────────────────────────────────────────────────────────────────────────

def plot_scaling(summary: dict, out_dir: Path, fmt: str, stage_filter: str | None) -> Path | None:
    stages = summary.get("scaling", {}).get("per_stage", {})
    if not stages:
        print("warning: no scaling data; skipping")
        return None

    if stage_filter and stage_filter not in stages:
        print(f"warning: stage {stage_filter!r} not in summary; available: {list(stages)}")
        return None

    stage_names = [stage_filter] if stage_filter else list(stages.keys())

    fig, ax_left = plt.subplots()
    ax_right = ax_left.twinx()
    ax_right.grid(False)

    for i, name in enumerate(stage_names):
        points = stages[name].get("replicas_over_time", [])
        if not points:
            continue
        ts = [p[0] for p in points]
        rs = [p[1] for p in points]
        ax_left.step(ts, rs, where="post", label=f"replicas: {name}",
                     color=f"C{i}", linewidth=1.8)

    xs = np.asarray(summary.get("lat_ts_labels", []), dtype=float)
    ys = np.asarray(summary.get("recv_per_second", []), dtype=float)
    if xs.size:
        ax_right.plot(xs, ys, color="C3", linewidth=1.0, alpha=0.5, label="throughput (ev/s)")

    ax_left.set_xlabel("Time since test start (s)")
    ax_left.set_ylabel("Replica count")
    ax_right.set_ylabel("Events / sec")
    ax_left.set_ylim(bottom=0)
    ax_right.set_ylim(bottom=0)

    lines_l, labels_l = ax_left.get_legend_handles_labels()
    lines_r, labels_r = ax_right.get_legend_handles_labels()
    ax_left.legend(lines_l + lines_r, labels_l + labels_r, loc="lower right")
    ax_left.set_title("Autoscaling behavior")

    suffix = f"_{stage_filter}" if stage_filter else ""
    return _save(fig, out_dir, f"scaling{suffix}.{fmt}")


# ─────────────────────────────────────────────────────────────────────────────
# 6. Worker timing stacked breakdown (wasm / serde / overhead)
# ─────────────────────────────────────────────────────────────────────────────

def plot_worker_breakdown(summary: dict, out_dir: Path, fmt: str) -> Path | None:
    """Grouped stacked bars: solid = p50, hatched = p99.
    Segments: WASM execution / serialization / other overhead."""
    wt = summary.get("worker_timing") or {}
    if not wt:
        print("warning: no worker_timing data; skipping worker breakdown")
        print("  (agent may be pre-patch — rebuild after updating master.rs + autoscaler.rs)")
        return None

    stages = sorted(wt.keys())
    if not stages:
        return None

    def ms(stage, key, pct):
        return wt[stage].get(key, {}).get(pct, 0) / 1000.0

    wasm_p50     = [ms(s, "wasm_us",     "p50") for s in stages]
    serde_p50    = [ms(s, "serde_us",    "p50") for s in stages]
    overhead_p50 = [ms(s, "overhead_us", "p50") for s in stages]
    wasm_p99     = [ms(s, "wasm_us",     "p99") for s in stages]
    serde_p99    = [ms(s, "serde_us",    "p99") for s in stages]
    overhead_p99 = [ms(s, "overhead_us", "p99") for s in stages]

    x = np.arange(len(stages))
    w = 0.35

    fig, ax = plt.subplots(figsize=(max(5.2, 2.6 * len(stages)), 3.8))

    # p50 solid bars
    ax.bar(x - w/2, wasm_p50, w, label="WASM (p50)", color="C0")
    ax.bar(x - w/2, serde_p50, w,
           bottom=wasm_p50, label="Serialization (p50)", color="C2")
    bot50 = [a + b for a, b in zip(wasm_p50, serde_p50)]
    ax.bar(x - w/2, overhead_p50, w,
           bottom=bot50, label="Other overhead (p50)", color="C7", alpha=0.6)

    # p99 hatched bars
    ax.bar(x + w/2, wasm_p99, w,
           label="WASM (p99)", color="C0", alpha=0.5, hatch="//")
    ax.bar(x + w/2, serde_p99, w, bottom=wasm_p99,
           label="Serialization (p99)", color="C2", alpha=0.5, hatch="//")
    bot99 = [a + b for a, b in zip(wasm_p99, serde_p99)]
    ax.bar(x + w/2, overhead_p99, w, bottom=bot99,
           label="Other overhead (p99)", color="C7", alpha=0.3, hatch="//")

    # Annotate totals
    for i, s in enumerate(stages):
        t50 = wasm_p50[i] + serde_p50[i] + overhead_p50[i]
        t99 = wasm_p99[i] + serde_p99[i] + overhead_p99[i]
        pad = max(t99, t50) * 0.02
        if t50 > 0:
            ax.text(x[i] - w/2, t50 + pad, f"{t50:.2f}",
                    ha="center", va="bottom", fontsize=7)
        if t99 > 0:
            ax.text(x[i] + w/2, t99 + pad, f"{t99:.2f}",
                    ha="center", va="bottom", fontsize=7)

    ax.set_xticks(x)
    ax.set_xticklabels(stages, rotation=20, ha="right")
    ax.set_ylabel("Time per invocation (ms)")
    ax.set_title("Worker invocation time breakdown  (solid = p50 · hatched = p99)")
    ax.legend(ncol=2, fontsize=8)
    fig.tight_layout()
    return _save(fig, out_dir, f"worker_breakdown.{fmt}")


# ─────────────────────────────────────────────────────────────────────────────
# 7. Worker CDFs (total / wasm / serde)
# ─────────────────────────────────────────────────────────────────────────────

def plot_worker_cdf(summary: dict, out_dir: Path, fmt: str) -> Path | None:
    """One CDF subplot per stage, overlaying total / WASM / serde."""
    wt = summary.get("worker_timing") or {}
    if not wt:
        print("warning: no worker_timing; skipping worker CDFs")
        return None

    stages = sorted(wt.keys())
    n = len(stages)
    if n == 0:
        return None

    fig, axes = plt.subplots(1, n, figsize=(4.0 * n, 3.4), squeeze=False)

    for i, stage in enumerate(stages):
        ax   = axes[0][i]
        data = wt[stage]
        for key, label, color in [
            ("total_us", "total", "C0"),
            ("wasm_us",  "WASM",  "C4"),
            ("serde_us", "serde", "C2"),
        ]:
            samples = (data.get(key) or {}).get("samples") or []
            if not samples:
                continue
            arr  = np.sort(np.asarray(samples, dtype=float)) / 1000.0  # us → ms
            pcts = np.linspace(0, 100, arr.size)
            ax.plot(arr, pcts, label=label, color=color, linewidth=1.4)

        ax.set_xscale("log")
        ax.set_xlabel("Time (ms, log)")
        if i == 0:
            ax.set_ylabel("Cumulative (%)")
        ax.set_ylim(0, 100)
        ax.set_title(f"{stage}\n(n={data.get('n', '?'):,})", fontsize=10)
        ax.legend(fontsize=8)

    fig.suptitle("Worker timing CDFs: total / WASM / serialization", y=1.02)
    fig.tight_layout()
    return _save(fig, out_dir, f"worker_cdf.{fmt}")


# ─────────────────────────────────────────────────────────────────────────────
# 8. Serde time-series
# ─────────────────────────────────────────────────────────────────────────────

def plot_serde_timeseries(summary: dict, out_dir: Path, fmt: str) -> Path | None:
    """Serialization cost over time, clocked by queue-depth polling timestamps.
    Shows whether serde is stable or inflates under load / after GC pauses."""
    wt = summary.get("worker_timing") or {}
    qd = summary.get("queue_depth") or {}

    if not wt:
        print("warning: no worker_timing; skipping serde timeseries")
        return None

    stages   = sorted(wt.keys())
    fig, ax  = plt.subplots()
    has_data = False

    for i, stage in enumerate(stages):
        data          = wt[stage]
        serde_samples = (data.get("serde_us") or {}).get("samples") or []
        if not serde_samples:
            continue
        has_data = True

        arr      = np.asarray(serde_samples, dtype=float) / 1000.0  # us → ms
        qd_stage = qd.get(stage) or []

        if len(qd_stage) >= 4:
            ts_arr = np.asarray([pt[0] for pt in qd_stage], dtype=float)
            n_bins = len(ts_arr)
            win    = max(1, len(arr) // n_bins)
            p50s = [float(np.percentile(arr[max(0, j * win):(j + 1) * win], 50))
                    for j in range(n_bins)]
            p99s = [float(np.percentile(arr[max(0, j * win):(j + 1) * win], 99))
                    for j in range(n_bins)]
            ax.plot(ts_arr, p50s, label=f"{stage} p50",
                    color=f"C{i}", linewidth=1.4)
            ax.plot(ts_arr, p99s, label=f"{stage} p99",
                    color=f"C{i}", linewidth=0.9, linestyle="--", alpha=0.7)
        else:
            # No time axis — fall back to flat reference lines.
            p50 = data["serde_us"]["p50"] / 1000.0
            p99 = data["serde_us"]["p99"] / 1000.0
            ax.axhline(p50, color=f"C{i}", linewidth=1.4,
                       label=f"{stage} p50 = {p50:.3f} ms")
            ax.axhline(p99, color=f"C{i}", linewidth=0.9, linestyle="--", alpha=0.7,
                       label=f"{stage} p99 = {p99:.3f} ms")

    if not has_data:
        plt.close(fig)
        print("warning: no serde samples; skipping serde timeseries")
        return None

    ax.set_xlabel("Time since test start (s)")
    ax.set_ylabel("Serialization time (ms)")
    ax.set_title("Serialization overhead over time")
    ax.legend()
    return _save(fig, out_dir, f"serde_timeseries.{fmt}")


# ─────────────────────────────────────────────────────────────────────────────
# 9. Inter-stage transport latency
# ─────────────────────────────────────────────────────────────────────────────

def plot_inter_stage(summary: dict, out_dir: Path, fmt: str) -> Path | None:
    inter = summary.get("inter_stage") or {}
    edges = inter.get("edges") or []
    if not edges:
        print(f"note: skipping inter-stage — {inter.get('note', 'no data')}")
        return None

    n    = len(edges)
    cols = min(n, 3)
    rows = (n + cols - 1) // cols
    fig, axes = plt.subplots(rows, cols,
                             figsize=(3.2 * cols, 2.8 * rows),
                             squeeze=False)
    axes_flat = [a for row in axes for a in row]

    for i, edge in enumerate(edges):
        ax      = axes_flat[i]
        samples = np.asarray(edge.get("samples") or [], dtype=float)
        if samples.size == 0:
            ax.text(0.5, 0.5, "(no samples)", transform=ax.transAxes,
                    ha="center", va="center", color="grey")
            continue
        ax.hist(samples, bins=50, color=f"C{i % 10}",
                edgecolor="white", linewidth=0.3)
        ax.set_xlabel(f"{edge['from']} → {edge['to']}  (ms)")
        ax.set_ylabel("Count")
        ax.set_title(
            f"p50={edge['p50']:.2f}  p99={edge['p99']:.2f}  "
            f"max={edge['max']:.2f}  (n={edge['count']:,})",
            fontsize=9,
        )

    for j in range(n, len(axes_flat)):
        axes_flat[j].axis("off")

    fig.suptitle("Inter-stage transport latency  (exit[i] → enter[i+1])", y=1.02)
    fig.tight_layout()
    return _save(fig, out_dir, f"inter_stage.{fmt}")


# ─────────────────────────────────────────────────────────────────────────────
# 10. Queue depth time-series
# ─────────────────────────────────────────────────────────────────────────────

def plot_queue_depth(summary: dict, out_dir: Path, fmt: str) -> Path | None:
    qd = summary.get("queue_depth") or {}
    if not qd:
        print("warning: no queue_depth data; skipping")
        print("  (agent may be pre-patch — rebuild after updating autoscaler.rs)")
        return None

    stages   = sorted(qd.keys())
    fig, ax  = plt.subplots()
    has_data = False

    for i, stage in enumerate(stages):
        pts = qd[stage]
        if not pts:
            continue
        has_data = True
        ts  = [p[0] for p in pts]
        dep = [p[1] for p in pts]
        ax.plot(ts, dep, label=stage, color=f"C{i}", linewidth=1.3, alpha=0.85)

    if not has_data:
        plt.close(fig)
        return None

    ax.set_xlabel("Time since test start (s)")
    ax.set_ylabel("Dispatcher queue depth (events)")
    ax.set_title("Dispatcher queue depth over time")
    ax.set_ylim(bottom=0)
    if len(stages) > 1:
        ax.legend()
    return _save(fig, out_dir, f"queue_depth.{fmt}")


# ─────────────────────────────────────────────────────────────────────────────
# 11a. Compile + instantiate_pre times (new: cold-start-opt comparison)
# ─────────────────────────────────────────────────────────────────────────────

def plot_compile_times(summary: dict, out_dir: Path, fmt: str) -> Path | None:
    """Bar chart of Component::from_file and linker.instantiate_pre times
    per stage, extracted from 'init' events in scaling.per_stage.
    These are the operations most affected by cold-start-opt."""
    per_stage = summary.get("scaling", {}).get("per_stage", {})
    if not per_stage:
        print("warning: no scaling data; skipping compile times")
        return None

    stages: list[str] = []
    compile_ms: list[float] = []
    pre_ms: list[float] = []

    for name, sc in per_stage.items():
        events = sc.get("events") or []
        init_events = [e for e in events
                       if e.get("action") in ("init", "init_aot", "init_jit")]
        if not init_events:
            continue
        e = init_events[0]
        c = e.get("compile_ms")
        p = e.get("instantiate_pre_ms")
        if c is None and p is None:
            continue
        # Track mode (aot/jit) for plot title
        mode = e.get("action", "init").replace("init_", "") if e.get("action") != "init" else "?"
        stages.append(name)
        compile_ms.append(c or 0.0)
        pre_ms.append(p or 0.0)

    if not stages:
        print("warning: no init events with compile_ms/instantiate_pre_ms; "
              "skipping compile times (rebuild agent after patching autoscaler.rs)")
        return None

    x = np.arange(len(stages))
    w = 0.35
    fig, ax = plt.subplots(figsize=(max(5.2, 2.6 * len(stages)), 3.4))

    bars_c = ax.bar(x - w / 2, compile_ms, w, label="compile  (Component::from_file)", color="C0")
    bars_p = ax.bar(x + w / 2, pre_ms,     w, label="instantiate_pre",                 color="C2")

    # Annotate values
    for bar in bars_c:
        h = bar.get_height()
        if h > 0:
            ax.text(bar.get_x() + bar.get_width() / 2, h + max(compile_ms + pre_ms) * 0.01,
                    f"{h:.1f}", ha="center", va="bottom", fontsize=8)
    for bar in bars_p:
        h = bar.get_height()
        if h > 0:
            ax.text(bar.get_x() + bar.get_width() / 2, h + max(compile_ms + pre_ms) * 0.01,
                    f"{h:.1f}", ha="center", va="bottom", fontsize=8)

    ax.set_xticks(x)
    ax.set_xticklabels(stages, rotation=20, ha="right")
    ax.set_ylabel("Time (ms)")
    ax.set_ylim(bottom=0)
    title = "Wasm component load + pre-instantiation time per stage"
    if mode in ("aot", "jit"):
        title += f"  [mode: {mode.upper()}]"
    ax.set_title(title)
    ax.legend()
    fig.tight_layout()
    return _save(fig, out_dir, f"compile_times.{fmt}")


# ─────────────────────────────────────────────────────────────────────────────
# 11. Cold start
# ─────────────────────────────────────────────────────────────────────────────

def _load_cold_starts(summary: dict, summary_path: Path) -> list[dict]:
    records: list[dict] = []

    # Per-stage summary list is used only as a fallback when no JSONL is
    # available. It only contains one entry per stage (the first back-filled
    # cold_start event), so it misses all warm scale-up spawns. Prefer JSONL.
    fallback_records: list[dict] = []
    for stage_name, sc in summary.get("scaling", {}).get("per_stage", {}).items():
        for cs_ms in sc.get("cold_start_ms", []):
            fallback_records.append({"stage": stage_name, "t_s": None,
                                     "boot_ms": None, "cold_start_ms": cs_ms})

    jsonl_path = summary.get("_jsonl_path")
    if jsonl_path:
        jsonl_path = Path(jsonl_path)
    if not jsonl_path or not jsonl_path.exists():
        guess = summary_path.with_suffix("").name.replace("_summary", "") + ".jsonl"
        jsonl_path = summary_path.parent / guess
    if not jsonl_path.exists():
        return fallback_records or []

    test_start = float(summary.get("test_start_wall") or 0.0)
    with jsonl_path.open() as f:
        for line in f:
            if '"worker booted"' not in line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue
            if obj.get("msg") != "worker booted":
                continue
            comp  = obj.get("component", "")
            stage = comp.split("/", 1)[1] if "/" in comp else comp

            def _f(k):
                v = obj.get(k)
                if v is None: return None
                try: return float(v)
                except: return None

            cs_ms = _f("cold_start_ms")
            if cs_ms is None:
                continue
            ts  = float(obj.get("ts", 0.0))
            t_s = ts - test_start if test_start > 0 else None
            records.append({"stage": stage, "t_s": t_s,
                            "boot_ms": _f("boot_ms"), "cold_start_ms": cs_ms})
    return records


def plot_cold_start(summary: dict, summary_path: Path, out_dir: Path, fmt: str) -> Path | None:
    records = _load_cold_starts(summary, summary_path)
    if not records:
        print("warning: no cold-start records; skipping")
        return None

    stages_all = [r["stage"] for r in records]
    ts_all     = [r["t_s"] for r in records]
    cs_all     = [r["cold_start_ms"] for r in records]
    has_time   = all(t is not None for t in ts_all)
    n = len(records)

    unique_stages = sorted(set(stages_all))
    stage_colour  = {s: f"C{i}" for i, s in enumerate(unique_stages)}

    if n >= 20:
        fig = plt.figure(figsize=(6.4, 3.2))
        gs  = fig.add_gridspec(1, 4, wspace=0.05)
        ax_time = fig.add_subplot(gs[0, 0:3])
        ax_hist = fig.add_subplot(gs[0, 3], sharey=ax_time)
        ax_hist.tick_params(left=False, labelleft=False)
    else:
        fig, ax_time = plt.subplots()
        ax_hist = None

    for s in unique_stages:
        xs = ([r["t_s"] for r in records if r["stage"] == s] if has_time
              else list(range(sum(1 for r in records if r["stage"] == s))))
        ys = [r["cold_start_ms"] for r in records if r["stage"] == s]
        ax_time.scatter(xs, ys, color=stage_colour[s], s=40,
                        label=f"{s}  (n={len(xs)})",
                        edgecolor="white", linewidth=0.6, zorder=3)

    ax_time.set_xlabel("Time since test start (s)" if has_time else "Spawn index")
    ax_time.set_ylabel("Cold-start latency (ms)")
    ax_time.set_ylim(bottom=0)
    if len(unique_stages) > 1:
        ax_time.legend(loc="upper right")

    cs_arr = np.asarray(cs_all)
    ax_time.set_title(
        f"Cold-start latency  (n={n};  median={np.median(cs_arr):.2f} ms;  "
        f"max={cs_arr.max():.2f} ms)"
    )

    if ax_hist is not None:
        for s in unique_stages:
            vals = [r["cold_start_ms"] for r in records if r["stage"] == s]
            ax_hist.hist(vals, bins=20, orientation="horizontal",
                         color=stage_colour[s], alpha=0.7,
                         edgecolor="white", linewidth=0.4)
        ax_hist.set_xlabel("Count")

    return _save(fig, out_dir, f"cold_start.{fmt}")


# ─────────────────────────────────────────────────────────────────────────────
# 12. Resources
# ─────────────────────────────────────────────────────────────────────────────

def plot_resources(summary: dict, out_dir: Path, fmt: str) -> Path | None:
    r   = summary.get("resources") or {}
    cpu = r.get("cpu_pct_samples") or []
    rss = r.get("rss_mb_samples")  or []
    if not cpu and not rss:
        print("warning: no resource samples; skipping")
        return None

    fig, (ax_cpu, ax_rss) = plt.subplots(2, 1, sharex=True, figsize=(5.2, 4.2))
    if cpu:
        ts, vs = zip(*cpu)
        ax_cpu.plot(ts, vs, color="C0", linewidth=1.3)
        ax_cpu.set_ylabel("CPU (%)")
        ax_cpu.set_ylim(bottom=0)
        peak  = max(vs)
        n_cpu = summary.get("environment", {}).get("cpu_cores_logical", 0)
        ax_cpu.set_title(
            f"Resource usage  (peak {peak:.0f}% ≈ {peak/100:.1f} of {n_cpu} cores)"
        )
    if rss:
        ts, vs = zip(*rss)
        ax_rss.plot(ts, vs, color="C2", linewidth=1.3)
        ax_rss.set_ylabel("RSS (MB)")
        ax_rss.set_xlabel("Time since test start (s)")
        ax_rss.set_ylim(bottom=0)

    return _save(fig, out_dir, f"resources.{fmt}")


# ─────────────────────────────────────────────────────────────────────────────
# 13. Run card
# ─────────────────────────────────────────────────────────────────────────────

def write_run_card(summary: dict, out_dir: Path) -> Path:
    env = summary["environment"]
    thr = summary["throughput"]
    e2e = summary["latency_ms"]["e2e"]
    cnt = summary["counters"]
    wt  = summary.get("worker_timing") or {}

    lines = [
        "Metapod run card",
        "=" * 56,
        "",
        "Environment",
        f"  host                : {env['host']}",
        f"  os                  : {env['os_name']} {env['os_version']} (kernel {env['kernel']})",
        f"  cpu                 : {env['cpu_model']}",
        f"  cores (phys/logical): {env['cpu_cores_physical']} / {env['cpu_cores_logical']}",
        f"  ram                 : {env['ram_total_mb']} MB",
        f"  rustc               : {env['rustc']}",
        f"  wasmtime            : {env['wasmtime']}",
        f"  git commit          : {env['git_commit'][:12]}"
            + ("  [DIRTY]" if env.get("git_dirty") else ""),
        "",
        "Run",
        f"  duration            : {summary['duration']} s",
        f"  events received     : {cnt['events_received']:,}",
        f"  stages              : {', '.join(summary['stage_names'])}",
        "",
        "Throughput",
        f"  sustained           : {thr['sustained_eps']:,.1f} ev/s",
        f"  sustained (trimmed) : {thr['sustained_eps_warmup_trimmed']:,.1f} ev/s"
            + f"  (skip first {thr['warmup_trim_s']:.0f}s)",
        "",
        "End-to-end latency (ms)",
        f"  p50                 : {e2e['p50']:.3f}",
        f"  p95                 : {e2e['p95']:.3f}",
        f"  p99                 : {e2e['p99']:.3f}",
        f"  p999                : {e2e['p999']:.3f}",
        f"  max                 : {e2e['max']:.3f}",
        f"  mean                : {e2e['mean']:.3f}",
        "",
        "Per-stage WASM latency (ms)",
    ]

    for name, stats in summary["latency_ms"]["per_stage"].items():
        lines.append(
            f"  {name:20s}: p50={stats['p50']:.3f}  p99={stats['p99']:.3f}  "
            f"max={stats['max']:.3f}  (n={stats['count']:,})"
        )

    if wt:
        lines += ["", "Worker invocation breakdown (ms, p50 / p99)"]
        for stage, data in sorted(wt.items()):
            def _ms(key, pct, _d=data):
                return (_d.get(key) or {}).get(pct, 0) / 1000.0
            lines.append(
                f"  {stage:20s}: "
                f"total={_ms('total_us','p50'):.3f}/{_ms('total_us','p99'):.3f}  "
                f"wasm={_ms('wasm_us','p50'):.3f}/{_ms('wasm_us','p99'):.3f}  "
                f"serde={_ms('serde_us','p50'):.3f}/{_ms('serde_us','p99'):.3f}  "
                f"serde%={data.get('serde_frac_p50', 0) * 100:.1f}%"
            )

    inter = summary.get("inter_stage") or {}
    edges = inter.get("edges") or []
    if edges:
        lines += ["", "Inter-stage transport latency (ms)"]
        for edge in edges:
            lines.append(
                f"  {edge['from']:>12s} → {edge['to']:<12s}: "
                f"p50={edge['p50']:.3f}  p99={edge['p99']:.3f}  "
                f"max={edge['max']:.3f}  (n={edge['count']:,})"
            )

    # Ingress wait: time spent in the first dispatcher's frontend queue
    # before the entry stage's worker picked the event up. Hidden inside
    # e2e but separate from any inter-stage transport number — surfacing
    # it makes "queue-bound latency" obvious when the pipeline is over-
    # subscribed (p50 here will dominate e2e in that regime; in steady
    # state it should be a few hundred microseconds at most).
    ingress = summary.get("ingress_wait") or {}
    iw_stages = ingress.get("stages") or []
    if iw_stages:
        lines += ["", "Ingress wait (ms, time in entry-dispatcher queue)"]
        for s in iw_stages:
            lines.append(
                f"  {s['stage']:20s}: "
                f"p50={s['p50']:.3f}  p99={s['p99']:.3f}  "
                f"max={s['max']:.3f}  (n={s['count']:,})"
            )

    lines += ["", "Scaling"]
    for name, sc in summary["scaling"]["per_stage"].items():
        lines.append(
            f"  {name:20s}: scale-up={sc['scale_up_count']}  "
            f"scale-down={sc['scale_down_count']}  "
            f"cold-starts={sc['cold_start_count']}"
        )
        # compile + instantiate_pre from init event
        compile_vals = sc.get("compile_ms") or []
        pre_vals     = sc.get("instantiate_pre_ms") or []
        if compile_vals:
            lines.append(f"    compile_ms          : {compile_vals[0]:.2f}")
        if pre_vals:
            lines.append(f"    instantiate_pre_ms  : {pre_vals[0]:.2f}")

    cpu = (summary.get("resources") or {}).get("cpu_pct_samples") or []
    rss = (summary.get("resources") or {}).get("rss_mb_samples")  or []
    if cpu or rss:
        lines.append("")
    if cpu:
        peak_cpu = max(v for _, v in cpu)
        lines.append(f"Peak CPU              : {peak_cpu:.1f}%  (≈ {peak_cpu/100:.2f} cores)")
    if rss:
        peak_rss = max(v for _, v in rss)
        lines.append(f"Peak RSS              : {peak_rss} MB")

    path = out_dir / "run_card.txt"
    path.write_text("\n".join(lines) + "\n")
    return path


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument("summary", type=Path, help="Path to agent_<ts>_summary.json")
    ap.add_argument("--out",    type=Path, default=Path("./figures"),
                    help="Output directory (default: ./figures)")
    ap.add_argument("--format", choices=["pdf", "png", "svg"], default="pdf",
                    help="Figure format (default: pdf; use png for quick view)")
    ap.add_argument("--stage", type=str, default=None,
                    help="Only plot this stage's scaling panel")
    args = ap.parse_args()

    if not args.summary.exists():
        print(f"error: {args.summary} does not exist", file=sys.stderr)
        return 2

    summary = load_summary(args.summary)
    args.out.mkdir(parents=True, exist_ok=True)

    fmt = args.format
    produced: list[Path] = []

    plot_fns = [
        # Latency
        lambda: plot_latency_cdf(summary, args.out, fmt),
        lambda: plot_latency_timeseries(summary, args.out, fmt),
        lambda: plot_e2e_scatter(summary, args.out, fmt),
        lambda: plot_per_stage_latency(summary, args.out, fmt),
        # Throughput & scaling
        lambda: plot_throughput(summary, args.out, fmt),
        lambda: plot_scaling(summary, args.out, fmt, args.stage),
        # Worker breakdown
        lambda: plot_worker_breakdown(summary, args.out, fmt),
        lambda: plot_worker_cdf(summary, args.out, fmt),
        lambda: plot_serde_timeseries(summary, args.out, fmt),
        # Communication
        lambda: plot_inter_stage(summary, args.out, fmt),
        lambda: plot_queue_depth(summary, args.out, fmt),
        # Infrastructure
        lambda: plot_compile_times(summary, args.out, fmt),
        lambda: plot_cold_start(summary, args.summary, args.out, fmt),
        lambda: plot_resources(summary, args.out, fmt),
    ]

    for fn in plot_fns:
        try:
            p = fn()
            if p is not None:
                produced.append(p)
        except Exception as exc:
            print(f"warning: plot failed — {exc}")

    card = write_run_card(summary, args.out)
    produced.append(card)

    print(f"\nWrote {len(produced)} file(s) to {args.out}:")
    for p in produced:
        print(f"  {p.name}")
    return 0


if __name__ == "__main__":
    sys.exit(main())