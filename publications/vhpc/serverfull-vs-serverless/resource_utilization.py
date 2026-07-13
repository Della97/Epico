#!/usr/bin/env python3
"""
resource_utilization.py — compare replica-second usage across run types.

Usage:
    python3 resource_utilization.py [serverfull-vs-serverless/data]

Prints per-stage and total replica-seconds for each run type (serverfull,
serverless_aot, serverless_jit) averaged over all runs found, then shows
the absolute and percentage savings of each serverless variant vs serverfull.
"""

import glob
import json
import os
import sys
from collections import defaultdict


RUN_TYPES = ["serverfull", "serverless_aot", "serverless_jit"]


def replica_seconds(replicas_over_time: list, duration: float) -> float:
    """Area under the replica-count step function over [0, duration]."""
    if not replicas_over_time:
        return 0.0
    area = 0.0
    for i in range(len(replicas_over_time) - 1):
        t0, count = replicas_over_time[i]
        t1        = replicas_over_time[i + 1][0]
        area += count * (t1 - t0)
    # last step runs until end of run
    t_last, count_last = replicas_over_time[-1]
    area += count_last * max(duration - t_last, 0.0)
    return area


def load_runs(data_dir: str, run_type: str) -> list[dict]:
    pattern = os.path.join(data_dir, run_type, "*_summary.json")
    summaries = []
    for path in sorted(glob.glob(pattern)):
        try:
            summaries.append(json.load(open(path)))
        except Exception as e:
            print(f"  warning: could not load {path}: {e}", file=sys.stderr)
    return summaries


def compute_stats(summaries: list[dict]) -> dict:
    """
    Returns {stage: [replica_seconds per run]} and per-run totals.
    """
    per_stage: dict[str, list[float]] = defaultdict(list)
    totals: list[float] = []

    for s in summaries:
        duration = float(s.get("duration", 360))
        sc = s.get("scaling", {}).get("per_stage", {})
        run_total = 0.0
        for stage, data in sc.items():
            rs = replica_seconds(data.get("replicas_over_time", []), duration)
            per_stage[stage].append(rs)
            run_total += rs
        totals.append(run_total)

    return {"per_stage": dict(per_stage), "totals": totals}


def mean(values: list[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def fmt(v: float) -> str:
    return f"{v:>10.1f}"


def main():
    data_dir = sys.argv[1] if len(sys.argv) > 1 else "serverfull-vs-serverless/data"
    if not os.path.isdir(data_dir):
        sys.exit(f"ERROR: {data_dir!r} not found")

    # load & compute
    stats: dict[str, dict] = {}
    for rt in RUN_TYPES:
        runs = load_runs(data_dir, rt)
        if not runs:
            print(f"  warning: no runs found for {rt}", file=sys.stderr)
            continue
        stats[rt] = compute_stats(runs)
        stats[rt]["n"] = len(runs)

    if not stats:
        sys.exit("ERROR: no data found")

    # collect stage order from first available type
    stages = []
    for rt in RUN_TYPES:
        if rt in stats:
            stages = sorted(stats[rt]["per_stage"].keys())
            break

    # ── header ──────────────────────────────────────────────────────────────
    col_w = 18
    types_present = [rt for rt in RUN_TYPES if rt in stats]

    print()
    print("=" * 78)
    print("  RESOURCE UTILIZATION  (replica-seconds, mean over N runs)")
    print("=" * 78)

    header = f"  {'stage':<14}"
    for rt in types_present:
        n = stats[rt]["n"]
        label = f"{rt} (N={n})"
        header += f"  {label:>{col_w}}"
    print(header)
    print("  " + "-" * 74)

    # per-stage rows
    stage_means: dict[str, dict[str, float]] = {}
    for stage in stages:
        row = f"  {stage:<14}"
        stage_means[stage] = {}
        for rt in types_present:
            vals = stats[rt]["per_stage"].get(stage, [])
            m = mean(vals)
            stage_means[stage][rt] = m
            row += f"  {m:>{col_w}.1f}"
        print(row)

    # total row
    print("  " + "-" * 74)
    total_means: dict[str, float] = {}
    row = f"  {'TOTAL':<14}"
    for rt in types_present:
        m = mean(stats[rt]["totals"])
        total_means[rt] = m
        row += f"  {m:>{col_w}.1f}"
    print(row)

    # ── savings vs serverfull ────────────────────────────────────────────────
    if "serverfull" not in stats:
        print("\n  (no serverfull baseline — cannot compute savings)")
        return

    serverless_types = [rt for rt in types_present if rt != "serverfull"]
    if not serverless_types:
        print("\n  (no serverless variants found)")
        return

    print()
    print("=" * 78)
    print("  SAVINGS vs SERVERFULL  (replica-seconds saved  /  % reduction)")
    print("=" * 78)

    header2 = f"  {'stage':<14}"
    for rt in serverless_types:
        label = f"{rt}"
        header2 += f"  {label:>{col_w}}"
    print(header2)
    print("  " + "-" * 74)

    def savings_str(baseline: float, variant: float) -> str:
        saved = baseline - variant
        pct   = saved / baseline * 100 if baseline else 0.0
        return f"{saved:>+8.1f}  ({pct:>5.1f}%)"

    for stage in stages:
        baseline = stage_means[stage].get("serverfull", 0.0)
        row = f"  {stage:<14}"
        for rt in serverless_types:
            variant = stage_means[stage].get(rt, 0.0)
            row += f"  {savings_str(baseline, variant):>{col_w}}"
        print(row)

    print("  " + "-" * 74)
    row = f"  {'TOTAL':<14}"
    for rt in serverless_types:
        row += f"  {savings_str(total_means['serverfull'], total_means[rt]):>{col_w}}"
    print(row)
    print()


if __name__ == "__main__":
    main()
