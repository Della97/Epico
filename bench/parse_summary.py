#!/usr/bin/env python3
"""Aggregate master_*_summary.json into a transport x codec comparison.

Usage: parse_summary.py <results_dir> [offered_lat_rate]

Reads files named  tp_<transport>_<codec>_<rep>.json  and
                   lat_<transport>_<codec>_<rep>.json,
takes the median across reps, prints tables + writes results.csv.
"""
import glob
import json
import os
import re
import sys
from statistics import median

TP_RE  = re.compile(r"tp_([a-z0-9]+)_([a-z0-9]+)_\d+\.json$")
LAT_RE = re.compile(r"lat_([a-z0-9]+)_([a-z0-9]+)_\d+\.json$")


def get(d, *path, default=None):
    for k in path:
        if not isinstance(d, dict) or k not in d:
            return default
        d = d[k]
    return d


def med(vals):
    vals = [v for v in vals if v is not None]
    return median(vals) if vals else None


def discover(results_dir, rx):
    """{(transport, codec): [summary dicts]} plus ordered transport/codec lists."""
    cells, transports, codecs = {}, [], []
    for f in sorted(glob.glob(os.path.join(results_dir, "*.json"))):
        m = rx.search(os.path.basename(f))
        if not m:
            continue
        t, c = m.group(1), m.group(2)
        if t not in transports:
            transports.append(t)
        if c not in codecs:
            codecs.append(c)
        try:
            with open(f) as fh:
                cells.setdefault((t, c), []).append(json.load(fh))
        except Exception as e:
            print(f"  (warn) {f}: {e}", file=sys.stderr)
    # stable, intuitive ordering
    order = ["zeromq", "mpmc", "spsc"]
    transports.sort(key=lambda x: (order.index(x) if x in order else 99, x))
    codecs.sort(key=lambda x: (0 if x == "json" else 1, x))
    return cells, transports, codecs


def fmt(v, p=3):
    return "n/a" if v is None else f"{v:.{p}f}"


def pct(new, base, lower_better):
    if new is None or base is None or base == 0:
        return ""
    d = (new - base) / base * 100.0
    tag = ("better" if (d < 0) == lower_better else "worse")
    return f"{d:+.1f}% {tag}"


def main():
    if len(sys.argv) < 2:
        print("usage: parse_summary.py <results_dir> [offered_lat_rate]"); sys.exit(1)
    rd = sys.argv[1]
    offered = float(sys.argv[2]) if len(sys.argv) > 2 else None

    tp_cells, tps, codecs = discover(rd, TP_RE)
    lat_cells, ltps, lcodecs = discover(rd, LAT_RE)

    # ── throughput: sustained eps (warmup-trimmed) ────────────────────────────
    def eps(t, c):
        return med([get(s, "throughput", "sustained_eps_warmup_trimmed")
                    for s in tp_cells.get((t, c), [])])

    W = 84
    print("=" * W)
    print(" two-stage-min  —  transport x codec")
    print("=" * W)

    if tp_cells:
        nrep = max((len(v) for v in tp_cells.values()), default=0)
        print(f"\nTHROUGHPUT   profile=tp + blast   (median of {nrep})   sustained ev/s (trimmed)")
        head = f"  {'transport':<10}" + "".join(f"{c:>14}" for c in codecs)
        if "json" in codecs and "binary" in codecs:
            head += "   binary vs json"
        print(head)
        base_zero = {}
        for t in tps:
            row = f"  {t:<10}" + "".join(f"{fmt(eps(t,c),0):>14}" for c in codecs)
            if "json" in codecs and "binary" in codecs:
                row += "   " + pct(eps(t, "binary"), eps(t, "json"), lower_better=False)
            print(row)
            base_zero[t] = eps(t, codecs[0])
        # transport speedup vs zeromq (per codec)
        if "zeromq" in tps and len(tps) > 1:
            print(f"\n  speedup vs zeromq (same codec):")
            for c in codecs:
                z = eps("zeromq", c)
                parts = []
                for t in tps:
                    if t == "zeromq":
                        continue
                    parts.append(f"{t} {pct(eps(t,c), z, lower_better=False)}")
                print(f"    {c:<8} " + "   ".join(parts))

    # ── latency: e2e + serde ──────────────────────────────────────────────────
    if lat_cells:
        nrepl = max((len(v) for v in lat_cells.values()), default=0)
        off = f" @ {offered:.0f} ev/s" if offered else ""
        print(f"\nLATENCY   profile=steady{off}   (median of {nrepl})   e2e ms  |  serde µs")
        print(f"  {'transport':<10}{'codec':<8}{'p50':>9}{'p99':>9}{'p999':>9}{'mean':>9}"
              f"{'relay_sd':>10}{'fwd_sd':>9}{'achieved':>10}")

        def serde_disp(t, val):
            # serde telemetry rides the dispatcher credit channel; the in-process
            # spine skips it, so suppress the misleading 0.000.
            if t in ("mpmc", "spsc") and (val is None or val == 0):
                return "—"
            return fmt(val)

        any_inproc_serde = False
        loadgen_limited, saturating = [], []
        for t in ltps:
            for c in lcodecs:
                runs = lat_cells.get((t, c), [])
                e = lambda k: med([get(s, "latency_ms", "e2e", k) for s in runs])
                def serde(stage):
                    return med([get(s, "worker_timing", stage, "serde_us", "p50") for s in runs])
                ach = med([get(s, "throughput", "sustained_eps") for s in runs])
                rs, fs = serde_disp(t, serde("relay")), serde_disp(t, serde("forward"))
                if rs == "—" or fs == "—":
                    any_inproc_serde = True
                print(f"  {t:<10}{c:<8}{fmt(e('p50')):>9}{fmt(e('p99')):>9}{fmt(e('p999')):>9}"
                      f"{fmt(e('mean')):>9}{rs:>10}{fs:>9}{fmt(ach,0):>10}")
                # classify: offered-rate-limited (loadgen) vs pipeline-saturated,
                # using the throughput-arm ceiling for the same cell.
                if offered and ach is not None and ach < 0.9 * offered:
                    ceil = eps(t, c)
                    if ceil and ach < 0.6 * ceil:
                        loadgen_limited.append(f"{t}/{c}")
                    else:
                        saturating.append(f"{t}/{c} ({ach:.0f})")

        if any_inproc_serde:
            print("\n  '—' serde: per-stage serde timing rides the dispatcher credit channel,")
            print("      which the in-process spine skips. e2e above is unaffected/valid.")
        if loadgen_limited:
            print(f"\n  (note) achieved ≈ {offered:.0f}-offered but well under each transport's")
            print(f"         throughput ceiling for {len(loadgen_limited)} cells → OFFERED-RATE-LIMITED by the")
            print(f"         single-thread loadgen (~25k steady), NOT pipeline saturation. These")
            print(f"         latencies are clean below-knee. To probe nearer the knee, raise the")
            print(f"         offered rate (multiple loadgen producers).")
        if saturating:
            print(f"\n  (note) pipeline SATURATING (achieved ≪ offered, near ceiling) for: "
                  + ", ".join(saturating) + " → lower LAT_RATE.")

    # ── csv ───────────────────────────────────────────────────────────────────
    csv = os.path.join(rd, "results.csv")
    with open(csv, "w") as f:
        f.write("arm,transport,codec,metric,value\n")
        for t in tps:
            for c in codecs:
                f.write(f"throughput,{t},{c},sustained_eps_trimmed,{eps(t,c)}\n")
        for t in ltps:
            for c in lcodecs:
                runs = lat_cells.get((t, c), [])
                for k in ("p50", "p99", "p999", "mean"):
                    f.write(f"latency,{t},{c},e2e_{k}_ms,{med([get(s,'latency_ms','e2e',k) for s in runs])}\n")
                for st in ("relay", "forward"):
                    f.write(f"latency,{t},{c},serde_p50_us_{st},{med([get(s,'worker_timing',st,'serde_us','p50') for s in runs])}\n")
    print(f"\n  wrote {csv}")


if __name__ == "__main__":
    main()
