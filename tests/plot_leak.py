#!/usr/bin/env python3
"""
plot_leak.py — visualize event conservation across scale-up/down cycles.

Three time-aligned panels from one leak-test run (tests/no_leak_scaling.sh):

  1. Cumulative events: sent (loadgen) vs received (collector). The vertical
     gap between the curves is in-flight events; a RESIDUAL gap at the end of
     the run is leakage. The final leak count is annotated.
  2. Replica count per stage (step), with scale-down (drain) events marked —
     leaks correlate with drains, so this is where the gap should open.
  3. Dispatcher/edge queue depth per stage (the autoscaler's scaling signal).

Usage:
    python3 tests/plot_leak.py [--logs-dir examples/two-stage-min/logs]
                               [--summary path] [--loadgen path]
                               [--label spsc] [--out leak_spsc.png]

Defaults: newest master_*_summary.json and newest loadgen_*.jsonl in logs-dir.
"""

import argparse
import glob
import json
import os
import sys

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt


def newest(pattern):
    files = sorted(glob.glob(pattern), key=os.path.getmtime)
    return files[-1] if files else None


def load_loadgen_sent_series(path, t0_wall):
    """Cumulative (t_rel_s, sent) step points from the loadgen's JSONL log.

    Uses producer burst-completion entries (pulse/tp) and the periodic
    progress entries (any profile), whichever exist. Both carry an absolute
    wall-clock `ts` and a cumulative `sent`/`loadgen_sent` counter.
    """
    pts = []
    with open(path) as f:
        for line in f:
            try:
                d = json.loads(line)
            except json.JSONDecodeError:
                continue
            ts = d.get("ts")
            if ts is None:
                continue
            msg = d.get("msg", "")
            sent = None
            if msg in ("pulse burst complete", "progress"):
                sent = d.get("sent")
            elif msg == "EOS sent":
                sent = d.get("loadgen_sent")
            if sent is None:
                continue
            try:
                pts.append((float(ts) - t0_wall, int(sent)))
            except (TypeError, ValueError):
                continue
    pts.sort()
    # Drop non-monotonic duplicates (progress thread vs producer thread races).
    out, hi = [], -1
    for t, s in pts:
        if s >= hi:
            out.append((t, s))
            hi = s
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--logs-dir", default="examples/two-stage-min/logs")
    ap.add_argument("--summary")
    ap.add_argument("--loadgen")
    ap.add_argument("--label", default="", help="transport label for the title")
    ap.add_argument("--out", default="leak_plot.png")
    args = ap.parse_args()

    summary_path = args.summary or newest(os.path.join(args.logs_dir, "master_*_summary.json"))
    loadgen_path = args.loadgen or newest(os.path.join(args.logs_dir, "loadgen_*.jsonl"))
    if not summary_path or not loadgen_path:
        sys.exit(f"missing inputs (summary={summary_path}, loadgen={loadgen_path})")

    with open(summary_path) as f:
        summary = json.load(f)

    t0_wall  = summary["test_start_wall"]
    received = summary["counters"]["events_received"]

    # ── Cumulative received: recv_per_second is bucketed by absolute second;
    # lat_ts_labels is the same series relative to test start.
    rel_s   = summary.get("lat_ts_labels", [])
    per_s   = summary.get("recv_per_second", [])
    recv_t, recv_cum, acc = [0.0], [0], 0
    for t, n in zip(rel_s, per_s):
        acc += n
        recv_t.append(float(t) + 1.0)  # bucket t counts arrivals within [t, t+1)
        recv_cum.append(acc)

    sent_pts = load_loadgen_sent_series(loadgen_path, t0_wall)
    if not sent_pts:
        sys.exit(f"no cumulative-sent entries found in {loadgen_path}")
    sent_t   = [0.0] + [p[0] for p in sent_pts]
    sent_cum = [0]   + [p[1] for p in sent_pts]
    total_sent = sent_cum[-1]
    leaked     = total_sent - received

    scaling = summary.get("scaling", {}).get("per_stage", {})
    qdepth  = summary.get("queue_depth", {})
    t_end   = max(recv_t[-1], sent_t[-1],
                  max((e[0] for s in scaling.values()
                       for e in s.get("replicas_over_time", [(0, 0)])), default=0))

    # ── Figure ───────────────────────────────────────────────────────────────
    fig, (ax1, ax2, ax3) = plt.subplots(
        3, 1, figsize=(11, 9), sharex=True,
        gridspec_kw={"height_ratios": [2.2, 1.2, 1.2]})
    ok = leaked == 0
    label = f" [{args.label}]" if args.label else ""
    fig.suptitle(
        f"Event conservation under autoscaling{label} — "
        f"sent={total_sent:,} received={received:,} "
        f"{'PASS (no leakage)' if ok else f'LEAKED {leaked:,} events'}",
        fontsize=12, fontweight="bold",
        color="darkgreen" if ok else "darkred")

    # Panel 1 — cumulative sent vs received.
    ax1.step(sent_t, sent_cum, where="post", lw=1.8, color="tab:blue",
             label="cumulative sent (loadgen)")
    ax1.step(recv_t, recv_cum, where="post", lw=1.8, color="tab:orange",
             label="cumulative received (collector)")
    ax1.fill_between(recv_t, recv_cum,
                     [min(total_sent, v) for v in
                      [next((s for t2, s in reversed(list(zip(sent_t, sent_cum))) if t2 <= t), 0)
                       for t in recv_t]],
                     step="post", alpha=0.15, color="tab:red",
                     label="in-flight / leaked gap")
    ax1.axhline(total_sent, color="tab:blue", ls=":", lw=0.8)
    if not ok:
        ax1.annotate(f"residual gap = {leaked:,} leaked",
                     xy=(recv_t[-1], received),
                     xytext=(recv_t[-1] * 0.62, total_sent * 0.75),
                     arrowprops=dict(arrowstyle="->", color="darkred"),
                     color="darkred", fontweight="bold")
    ax1.set_ylabel("events (cumulative)")
    ax1.legend(loc="lower right", fontsize=9)
    ax1.grid(alpha=0.3)

    # Panel 2 — replicas per stage + drain markers.
    colors = plt.cm.tab10.colors
    for i, (stage, s) in enumerate(sorted(scaling.items())):
        rot = s.get("replicas_over_time", [])
        if not rot:
            continue
        t = [p[0] for p in rot] + [t_end]
        r = [p[1] for p in rot] + [rot[-1][1]]
        ax2.step(t, r, where="post", lw=1.5, color=colors[i % 10], label=stage)
        drains = [e["t_s"] for e in s.get("events", []) if e.get("action") == "drain"]
        if drains:
            ax2.scatter(drains,
                        [next(rr for tt, rr in reversed(rot) if tt <= d) for d in drains],
                        marker="v", s=45, color=colors[i % 10], zorder=5,
                        edgecolors="black", linewidths=0.5,
                        label=f"{stage} drains")
    ax2.set_ylabel("replicas")
    ax2.legend(loc="upper right", fontsize=8, ncol=2)
    ax2.grid(alpha=0.3)

    # Panel 3 — queue depth per stage.
    if any(qdepth.values()):
        for i, (stage, samples) in enumerate(sorted(qdepth.items())):
            if not samples:
                continue
            ax3.plot([p[0] for p in samples], [p[1] for p in samples],
                     lw=1.0, color=colors[i % 10], label=stage)
        ax3.set_yscale("symlog")
        ax3.legend(loc="upper right", fontsize=8)
    else:
        ax3.text(0.5, 0.5, "no queue-depth samples in this summary",
                 transform=ax3.transAxes, ha="center", color="gray")
    ax3.set_ylabel("queue depth")
    ax3.set_xlabel("seconds since test start")
    ax3.grid(alpha=0.3)

    fig.tight_layout(rect=[0, 0, 1, 0.96])
    fig.savefig(args.out, dpi=130)
    print(f"wrote {args.out}  (sent={total_sent} received={received} leaked={leaked})")


if __name__ == "__main__":
    main()
