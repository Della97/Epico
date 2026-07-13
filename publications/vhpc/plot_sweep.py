#!/usr/bin/env python3
"""Plot a latency-and-throughput sweep over offered rate.

Takes one or more summary.json files (each from a separate run at a
different offered rate) and produces two figures:

  sweep_latency.pdf
    e2e p50 / p95 / p99 / p999 vs offered rate, log-y. Throughput
    overlay (sustained vs offered) on a twin axis. The canonical
    "hockey stick" plot for stream processor characterization.

  sweep_decomposition.pdf
    Stacked bars showing how end-to-end latency time is spent at each
    offered rate: ingress queue wait, inter-stage transport, per-stage
    WASM compute. Demonstrates that runtime overhead is rate-invariant
    while queue wait grows past the saturation knee.

Offered-rate-per-run is taken from one of three sources, in order:

  1. --rates flag: comma-separated list, one per input file in the
     order they're passed. e.g. --rates 20000,40000,60000,80000.
  2. Filename pattern: any input matching `*_<digits>k_summary.json`
     or `*_<digits>_summary.json` has its rate parsed from the name.
     e.g. `run_60k_summary.json` → 60000.
  3. The `source.rate` field from a sidecar pipeline.yaml, looked up
     in the same directory as the summary.json. Slowest fallback;
     mainly there so a freshly captured run "just works".

Usage:
    plot_sweep.py run_20k_summary.json run_40k_summary.json ... \
        --out figures/

    plot_sweep.py --rates 20000,40000,60000,75000,80000 \
        run1.json run2.json run3.json run4.json run5.json \
        --out figures/
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Optional

import matplotlib.pyplot as plt
import numpy as np


# ─────────────────────────────────────────────────────────────────────────────
# Data extraction
# ─────────────────────────────────────────────────────────────────────────────

# Filename pattern: anything ending with `_<digits>[k]_summary.json`. Picks up
# the LAST such number in the name so we're robust to dates and other digits.
_RATE_RE = re.compile(r"_(\d+)(k?)(?:_summary)?\.json$", re.IGNORECASE)


def offered_rate_from_filename(path: Path) -> Optional[int]:
    """Parse offered rate from a filename, or return None if the pattern
    doesn't match. Accepts both `_60k_` (= 60000) and `_60000_`."""
    m = _RATE_RE.search(path.name)
    if not m:
        return None
    n = int(m.group(1))
    return n * 1000 if m.group(2) else n


def offered_rate_from_sidecar(summary_path: Path) -> Optional[int]:
    """Look for `source.rate:` in a pipeline.yaml next to the summary.
    This is the cheapest fallback that lets a fresh run be plotted with
    no manual annotation. Doesn't import a YAML library — does a one-line
    regex search since the field is always a top-level integer."""
    candidates = [
        summary_path.parent / "pipeline.yaml",
        summary_path.parent.parent / "pipeline.yaml",
    ]
    for yaml_path in candidates:
        if not yaml_path.exists():
            continue
        text = yaml_path.read_text()
        # Match indented `rate: <int>` under a `source:` key. Loose
        # enough to handle 2- or 4-space indents. We don't try to
        # validate that we're under `source:` — if pipeline.yaml has
        # multiple `rate:` keys this will pick the first, which in
        # practice is always the loadgen rate.
        m = re.search(r"^\s*rate:\s*(\d+)\s*$", text, re.MULTILINE)
        if m:
            return int(m.group(1))
    return None


def load_run(path: Path, override_rate: Optional[int]) -> dict:
    """Load a summary.json and extract the metrics we need for the plot."""
    with path.open() as f:
        s = json.load(f)

    e2e = (s.get("latency_ms") or {}).get("e2e") or {}
    thr = s.get("throughput") or {}
    ing = (s.get("ingress_wait") or {}).get("stages") or [{}]
    inter = (s.get("inter_stage") or {}).get("edges") or []
    wt = s.get("worker_timing") or {}

    # Total per-stage WASM time (sum of all stages, p50). Same for p99.
    # Used by the decomposition figure to show "actual compute time"
    # vs queue/transport time.
    wasm_p50_total_ms = 0.0
    wasm_p99_total_ms = 0.0
    for stage_data in wt.values():
        wasm_p50_total_ms += (stage_data.get("wasm_us") or {}).get("p50", 0) / 1000.0
        wasm_p99_total_ms += (stage_data.get("wasm_us") or {}).get("p99", 0) / 1000.0

    # Total inter-stage transport (sum of all edges, p50/p99).
    transport_p50_ms = sum(e.get("p50", 0) for e in inter)
    transport_p99_ms = sum(e.get("p99", 0) for e in inter)

    # Resolve offered rate: explicit > filename > sidecar.
    if override_rate is not None:
        rate = override_rate
    else:
        rate = offered_rate_from_filename(path)
        if rate is None:
            rate = offered_rate_from_sidecar(path)
    if rate is None:
        raise SystemExit(
            f"Could not determine offered rate for {path}. "
            "Pass --rates explicitly, or rename the file to include "
            "`_<rate>k_summary.json`, or place a pipeline.yaml next to it."
        )

    return {
        "path":            path,
        "offered_eps":     rate,
        "sustained_eps":   thr.get("sustained_eps_warmup_trimmed") or thr.get("sustained_eps", 0),
        "e2e_p50":         e2e.get("p50", 0),
        "e2e_p95":         e2e.get("p95", 0),
        "e2e_p99":         e2e.get("p99", 0),
        "e2e_p999":        e2e.get("p999", 0),
        "ingress_p50":     (ing[0] or {}).get("p50", 0),
        "ingress_p99":     (ing[0] or {}).get("p99", 0),
        "transport_p50":   transport_p50_ms,
        "transport_p99":   transport_p99_ms,
        "wasm_p50":        wasm_p50_total_ms,
        "wasm_p99":        wasm_p99_total_ms,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Plot 1: latency + throughput vs offered rate
# ─────────────────────────────────────────────────────────────────────────────

def plot_sweep_latency(runs: list[dict], out_path: Path) -> None:
    """The headline figure. Latency percentiles vs offered rate (log y),
    with sustained-vs-offered throughput on a twin axis. The point where
    sustained-eps falls below offered-eps marks the saturation knee — we
    annotate it with a vertical line if it's identifiable in the data."""

    runs = sorted(runs, key=lambda r: r["offered_eps"])
    offered = np.array([r["offered_eps"] for r in runs], dtype=float)
    sustained = np.array([r["sustained_eps"] for r in runs], dtype=float)

    fig, ax_lat = plt.subplots(figsize=(8, 5))
    ax_thr = ax_lat.twinx()

    # ── Latency curves on the left axis ────────────────────────────────────
    # Log y because percentiles span microseconds (idle) to seconds (over-
    # subscribed), so a linear axis flattens the entire body of the curve.
    series = [
        ("p50",  "e2e_p50",  "C0", "-",  "o"),
        ("p95",  "e2e_p95",  "C1", "-",  "s"),
        ("p99",  "e2e_p99",  "C3", "-",  "^"),
        ("p999", "e2e_p999", "C4", "--", "v"),
    ]
    for label, key, color, ls, marker in series:
        ys = [r[key] for r in runs]
        ax_lat.plot(offered / 1000.0, ys, color=color, linestyle=ls,
                    marker=marker, markersize=6, linewidth=1.6,
                    label=f"e2e {label}")

    ax_lat.set_yscale("log")
    ax_lat.set_xlabel("Offered rate (kev/s)")
    ax_lat.set_ylabel("End-to-end latency (ms, log)")
    ax_lat.grid(True, which="both", linestyle=":", alpha=0.4)

    # ── Throughput on the right axis ───────────────────────────────────────
    # Two lines: actual sustained, and a y=x reference. Where they
    # diverge is the knee.
    ax_thr.plot(offered / 1000.0, sustained / 1000.0, color="C2",
                linestyle="-", marker="D", markersize=5, linewidth=1.4,
                label="sustained (actual)")
    ax_thr.plot(offered / 1000.0, offered / 1000.0, color="C7",
                linestyle=":", linewidth=1.0, label="sustained = offered")
    ax_thr.set_ylabel("Throughput (kev/s)", color="C2")
    ax_thr.tick_params(axis="y", labelcolor="C2")

    # ── Knee detection ─────────────────────────────────────────────────────
    # Define the knee as the lowest offered rate at which sustained falls
    # below 90% of offered. This is a heuristic — could be 95% or 85% —
    # but 90% is the convention most stream-processor papers use.
    knee_idx = next((i for i, r in enumerate(runs)
                     if r["sustained_eps"] < 0.9 * r["offered_eps"]), None)
    if knee_idx is not None and knee_idx > 0:
        knee_x = offered[knee_idx] / 1000.0
        ax_lat.axvline(knee_x, color="gray", linestyle="--", alpha=0.5)
        # Place annotation in axes-relative y so it sits at top of the
        # latency axis regardless of the data's actual y range.
        ax_lat.annotate(
            f"knee ≈ {knee_x:.0f} kev/s",
            xy=(knee_x, 1.0), xycoords=("data", "axes fraction"),
            xytext=(5, -20), textcoords="offset points",
            fontsize=9, color="gray",
        )

    # Combined legend: pull handles from both axes onto one. Otherwise the
    # twin axes produce two legends in two corners and it looks messy.
    h_lat, l_lat = ax_lat.get_legend_handles_labels()
    h_thr, l_thr = ax_thr.get_legend_handles_labels()
    ax_lat.legend(h_lat + h_thr, l_lat + l_thr,
                  loc="upper left", fontsize=9, framealpha=0.92)

    fig.suptitle(
        "Latency and throughput vs offered rate\n"
        f"({len(runs)} runs, {runs[0]['offered_eps']/1000:.0f}–"
        f"{runs[-1]['offered_eps']/1000:.0f} kev/s offered)",
        y=1.02, fontsize=11,
    )
    fig.tight_layout()
    fig.savefig(out_path, bbox_inches="tight")
    plt.close(fig)
    print(f"wrote {out_path}")


# ─────────────────────────────────────────────────────────────────────────────
# Plot 2: latency decomposition stacked bars
# ─────────────────────────────────────────────────────────────────────────────

def plot_sweep_decomposition(runs: list[dict], out_path: Path) -> None:
    """Stacked bars showing how each run's e2e p50 (and p99) is partitioned
    between ingress queue wait, inter-stage transport, and WASM compute.

    The story this plot tells: the WASM and transport components are
    rate-invariant (flat bars), while ingress wait is the only piece that
    grows past the knee. Visually proves that runtime overhead is
    independent of load and queue wait is the only saturation symptom.

    Two subplots side by side: p50 (left) and p99 (right) — bars look
    very different at p99 because that's where the burstiness shows up.
    """
    runs = sorted(runs, key=lambda r: r["offered_eps"])
    n = len(runs)
    x = np.arange(n)
    labels = [f"{r['offered_eps']/1000:.0f}k" for r in runs]

    fig, (ax50, ax99) = plt.subplots(1, 2, figsize=(11, 4.6), sharey=False)

    # --- p50 panel ---------------------------------------------------------
    # Stacking order (bottom-up) is wasm → transport → ingress, which
    # mirrors the temporal order an event experiences: queued first, then
    # transit between stages, then actual compute. Using the SAME order
    # bottom-up means the visually largest segment (queue wait at high
    # offered) sits on top, where it's easiest to compare between bars.
    wasm_p50      = np.array([r["wasm_p50"]      for r in runs])
    transport_p50 = np.array([r["transport_p50"] for r in runs])
    ingress_p50   = np.array([r["ingress_p50"]   for r in runs])

    ax50.bar(x, wasm_p50,      label="WASM compute", color="C0")
    ax50.bar(x, transport_p50, bottom=wasm_p50, label="Inter-stage transport", color="C2")
    ax50.bar(x, ingress_p50,   bottom=wasm_p50 + transport_p50,
             label="Ingress queue wait", color="C3")
    ax50.set_yscale("log")
    ax50.set_xticks(x)
    ax50.set_xticklabels(labels)
    ax50.set_xlabel("Offered rate")
    ax50.set_ylabel("Latency component (ms, log)")
    ax50.set_title("e2e p50 decomposition")
    ax50.grid(True, which="both", axis="y", linestyle=":", alpha=0.4)
    ax50.legend(loc="upper left", fontsize=8)

    # --- p99 panel ---------------------------------------------------------
    wasm_p99      = np.array([r["wasm_p99"]      for r in runs])
    transport_p99 = np.array([r["transport_p99"] for r in runs])
    ingress_p99   = np.array([r["ingress_p99"]   for r in runs])

    ax99.bar(x, wasm_p99,      color="C0")
    ax99.bar(x, transport_p99, bottom=wasm_p99, color="C2")
    ax99.bar(x, ingress_p99,   bottom=wasm_p99 + transport_p99, color="C3")
    ax99.set_yscale("log")
    ax99.set_xticks(x)
    ax99.set_xticklabels(labels)
    ax99.set_xlabel("Offered rate")
    ax99.set_title("e2e p99 decomposition")
    ax99.grid(True, which="both", axis="y", linestyle=":", alpha=0.4)

    fig.suptitle("Where the time goes: latency component breakdown across offered rate",
                 y=1.00, fontsize=11)
    fig.tight_layout()
    fig.savefig(out_path, bbox_inches="tight")
    plt.close(fig)
    print(f"wrote {out_path}")


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument("summaries", type=Path, nargs="+",
                    help="One or more summary.json files, one per offered rate")
    ap.add_argument("--rates", type=str, default=None,
                    help="Comma-separated offered rates (one per summary, "
                         "in the same order). Overrides filename/sidecar lookup.")
    ap.add_argument("--out", type=Path, default=Path("./figures"),
                    help="Output directory (default: ./figures)")
    args = ap.parse_args()

    args.out.mkdir(parents=True, exist_ok=True)

    if args.rates:
        rates = [int(x.strip()) for x in args.rates.split(",")]
        if len(rates) != len(args.summaries):
            print(f"error: --rates has {len(rates)} values but "
                  f"{len(args.summaries)} summaries given", file=sys.stderr)
            return 2
    else:
        rates = [None] * len(args.summaries)

    runs = []
    for path, rate in zip(args.summaries, rates):
        if not path.exists():
            print(f"error: {path} does not exist", file=sys.stderr)
            return 2
        runs.append(load_run(path, rate))

    print(f"loaded {len(runs)} runs:")
    for r in runs:
        print(f"  {r['path'].name}: offered={r['offered_eps']:>7,} ev/s, "
              f"sustained={r['sustained_eps']:>7,.0f} ev/s, "
              f"e2e p50={r['e2e_p50']:7.2f} ms, p99={r['e2e_p99']:7.2f} ms")

    plot_sweep_latency(runs, args.out / "sweep_latency.pdf")
    plot_sweep_decomposition(runs, args.out / "sweep_decomposition.pdf")
    return 0


if __name__ == "__main__":
    sys.exit(main())