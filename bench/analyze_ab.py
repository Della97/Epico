#!/usr/bin/env python3
"""Analyze Epico dispatcher-vs-edge A/B results.

Usage:
    python3 analyze_ab.py <results_dir>

Reads every <run>/summary.json + <run>/meta.json under the results tree and reports:
  * throughput: max sustained ev/s per arm at each replica count (the saturation
    sweep). The plateau across replicas is the transport's sustained ceiling;
    where two arms diverge is the transport effect. Also prints per-stage ev/s
    balance and loadgen drop count so you can tell a real ceiling from a
    bottlenecked-source artifact.
  * latency:   e2e p50/p99/p999 per arm at each offered rate (the curve).
  * inter-stage hop latency per arm (direct transport-cost evidence).
Writes ab_tidy.csv (long format) and, if matplotlib is present, PNGs:
  throughput_vs_replicas.png (saturation curves) and latency_vs_load.png.
"""
import csv
import json
import statistics as st
import sys
from collections import defaultdict
from pathlib import Path


def load_runs(root: Path):
    runs = []
    for meta_path in root.rglob("meta.json"):
        summ_path = meta_path.with_name("summary.json")
        if not summ_path.exists():
            continue
        try:
            meta = json.loads(meta_path.read_text())
            summ = json.loads(summ_path.read_text())
        except json.JSONDecodeError:
            continue
        runs.append((meta, summ))
    return runs


def warm_eps(s):
    return s.get("throughput", {}).get("sustained_eps_warmup_trimmed")


def per_stage_eps(s):
    return s.get("throughput", {}).get("per_stage_eps", {}) or {}


def e2e(s, k):
    return s.get("latency_ms", {}).get("e2e", {}).get(k)


def hop_rows(s):
    return s.get("inter_stage", {}).get("edges", []) or []


def dropped(s):
    # loadgen drop count, if surfaced in the summary; tolerate absence.
    return s.get("counters", {}).get("loadgen_dropped")


def agg(values):
    vals = [v for v in values if v is not None]
    if not vals:
        return (float("nan"), float("nan"), 0)
    return (st.fmean(vals), st.stdev(vals) if len(vals) > 1 else 0.0, len(vals))


def main():
    if len(sys.argv) != 2:
        print(__doc__)
        sys.exit(1)
    root = Path(sys.argv[1])
    runs = load_runs(root)
    if not runs:
        print(f"No (summary.json, meta.json) pairs found under {root}")
        sys.exit(1)

    tidy = []

    # ---- throughput: (arm, replicas) -> [warm eps] ------------------------
    tp = defaultdict(list)
    tp_stage = defaultdict(lambda: defaultdict(list))   # (arm,reps) -> stage -> [eps]
    tp_hops = defaultdict(lambda: defaultdict(list))     # (arm,reps) -> hop -> [p50]
    for meta, summ in runs:
        if meta.get("profile") != "tp":
            continue
        arm = meta["arm"]
        reps = meta.get("replicas", 1)
        eps = warm_eps(summ)
        tp[(arm, reps)].append(eps)
        for stage, v in per_stage_eps(summ).items():
            tp_stage[(arm, reps)][stage].append(v)
        for h in hop_rows(summ):
            tp_hops[(arm, reps)][f"{h['from']}->{h['to']}"].append(h.get("p50"))
        tidy.append(dict(experiment="throughput", arm=arm, replicas=reps, rate="",
                         repeat=meta["repeat"], metric="warm_eps", value=eps))

    if tp:
        print("\n=== MAX SUSTAINED THROUGHPUT (warm-trimmed ev/s) ===")
        print(f"  {'arm':<11} {'replicas':>8} {'ev/s':>14} {'stdev':>9}  n   per-stage balance (ev/s)")
        # per (arm,reps)
        means = {}
        for (arm, reps) in sorted(tp.keys(), key=lambda x: (x[0], x[1])):
            m, sd, n = agg(tp[(arm, reps)])
            means[(arm, reps)] = m
            bal = []
            for stage, vs in sorted(tp_stage[(arm, reps)].items()):
                bm = agg(vs)[0]
                bal.append(f"{stage}={bm:.0f}")
            print(f"  {arm:<11} {reps:>8} {m:>14.1f} {sd:>9.1f}  {n}   {'  '.join(bal)}")

        # plateau (best replica count) per arm + speedup
        def best(arm):
            cand = [(reps, means[(a, reps)]) for (a, reps) in means if a == arm
                    and means[(a, reps)] == means[(a, reps)]]  # drop NaN
            return max(cand, key=lambda x: x[1]) if cand else None
        be, bd = best("edge"), best("dispatcher")
        if be:
            print(f"\n  edge       plateau: {be[1]:.0f} ev/s at {be[0]} replicas")
        if bd:
            print(f"  dispatcher plateau: {bd[1]:.0f} ev/s at {bd[0]} replicas")
        if be and bd and bd[1]:
            print(f"  edge / dispatcher (max): {be[1] / bd[1]:.2f}x")

        print("\n  per-hop transport latency p50 (ms):")
        for (arm, reps), hops in sorted(tp_hops.items()):
            for hop, p50s in sorted(hops.items()):
                m, sd, n = agg(p50s)
                print(f"    {arm:<11} reps={reps} {hop:<22} {m:7.4f} ± {sd:.4f}")
        print("\n  NOTE: per-stage ev/s should be ~equal when truly saturated.")
        print("  If throughput stops rising with replicas, you've hit the transport")
        print("  ceiling OR the core count. Check CPU during the run to tell which.")

    # ---- latency: (arm, rate) -> {p50/p99/p999: [...]} --------------------
    lat = defaultdict(lambda: defaultdict(list))
    for meta, summ in runs:
        if meta.get("profile") != "steady":
            continue
        arm, rate = meta["arm"], meta["rate"]
        for k in ("p50", "p99", "p999"):
            v = e2e(summ, k)
            lat[(arm, rate)][k].append(v)
            tidy.append(dict(experiment="latency", arm=arm, replicas=meta.get("replicas", 1),
                             rate=rate, repeat=meta["repeat"], metric=f"e2e_{k}", value=v))

    if lat:
        print("\n=== LATENCY vs OFFERED LOAD (e2e ms) ===")
        print(f"  {'arm':<11} {'rate':>8}   {'p50':>10}   {'p99':>10}   {'p999':>10}")
        for (arm, rate) in sorted(lat.keys(), key=lambda x: (x[0], x[1])):
            row = lat[(arm, rate)]
            print(f"  {arm:<11} {rate:>8}   {agg(row['p50'])[0]:>10.3f}   "
                  f"{agg(row['p99'])[0]:>10.3f}   {agg(row['p999'])[0]:>10.3f}")

    # ---- tidy CSV ----------------------------------------------------------
    csv_path = root / "ab_tidy.csv"
    with csv_path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["experiment", "arm", "replicas", "rate",
                                          "repeat", "metric", "value"])
        w.writeheader()
        for r in tidy:
            w.writerow(r)
    print(f"\nTidy CSV -> {csv_path}")

    # ---- plots -------------------------------------------------------------
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        print("(matplotlib not installed -- skipping PNGs; CSV is ready for your plot scripts)")
        return

    if tp:
        fig, ax = plt.subplots(figsize=(6, 4))
        by_arm = defaultdict(list)
        for (arm, reps) in tp:
            by_arm[arm].append((reps, agg(tp[(arm, reps)])[0], agg(tp[(arm, reps)])[1]))
        for arm, pts in by_arm.items():
            pts.sort()
            xs = [p[0] for p in pts]; ys = [p[1] for p in pts]; es = [p[2] for p in pts]
            ax.errorbar(xs, ys, yerr=es, marker="o", capsize=4, label=arm)
        ax.set_xlabel("replicas per stage (min=max)")
        ax.set_ylabel("max sustained ev/s")
        ax.set_title("Throughput saturation: dispatcher vs in-process edge")
        ax.legend()
        fig.tight_layout(); fig.savefig(root / "throughput_vs_replicas.png", dpi=150)
        print(f"Plot -> {root / 'throughput_vs_replicas.png'}")

    if lat:
        fig, ax = plt.subplots(figsize=(6, 4))
        by_arm = defaultdict(list)
        for (arm, rate) in lat:
            by_arm[arm].append((rate, agg(lat[(arm, rate)]["p99"])[0]))
        for arm, pts in by_arm.items():
            pts.sort()
            ax.plot([p[0] for p in pts], [p[1] for p in pts], "-o", label=arm)
        ax.set_xlabel("offered load (ev/s)"); ax.set_ylabel("e2e p99 (ms)")
        ax.set_title("Latency vs load (p99)"); ax.legend()
        fig.tight_layout(); fig.savefig(root / "latency_vs_load.png", dpi=150)
        print(f"Plot -> {root / 'latency_vs_load.png'}")


if __name__ == "__main__":
    main()