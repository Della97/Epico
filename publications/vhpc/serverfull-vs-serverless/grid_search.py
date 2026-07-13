#!/usr/bin/env python3
"""
grid_search.py — Find replica splits and autoscaling params that yield similar P50
between serverfull and serverless, while proving lower resource utilisation for serverless.

STRATEGY
--------
Phase 1 — Serverfull sweep
  Enumerate (n_norm, n_detect, n_finalize) splits where sum == total_cores.
  Run each once, record P50 and peak replica count.

Phase 2 — Serverless sweep
  For each serverfull split, set serverless max = same split (equal peak capacity).
  Vary queue_up_base and cooldown_down_factor to find configs where:
    - |P50_sl - P50_sf| / P50_sf  < p50_tolerance   (similar latency)
    - avg_replicas_sl / total_fixed  < resource_threshold  (lower avg utilisation)

KEY METRICS
-----------
- p50              : 50th-percentile end-to-end latency (ms)
- avg_replicas     : area-under-replica-count-curve / duration  (per stage, then sum)
- resource_savings : 1 - avg_replicas_sl / total_fixed_sf

OUTPUT
------
grid_results.csv   : one row per (config, mode) with all metrics
grid_summary.txt   : ranked table of promising pairs

USAGE
-----
  python3 grid_search.py [--dry-run] [--total-cores N] [--runs N]
"""

from __future__ import annotations

import argparse
import csv
import itertools
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import NamedTuple

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

SCRIPT_DIR  = Path(__file__).parent.resolve()
REPO_ROOT   = (SCRIPT_DIR / "../../..").resolve()
PIPELINE_DIR = REPO_ROOT / "examples" / "three-stage-test"
PIPELINE_YAML = PIPELINE_DIR / "pipeline.yaml"
LOGS_DIR    = PIPELINE_DIR / "logs"
RESULTS_DIR = SCRIPT_DIR / "grid_results"

# ---------------------------------------------------------------------------
# Parameter space
# ---------------------------------------------------------------------------

# Total cores shared across the 3 stages (hard limit = 16 on this machine).
TOTAL_CORES = 16

# Curated serverfull splits (normalize, detect, finalize) summing to
# TOTAL_CORES.  Each split is physically meaningful:
#   normalize is first stage and sees the full input rate → give it most cores.
#   detect is CPU-heaviest (anomaly scoring) → second most.
#   finalize is lightweight formatting → fewest.
# Change to None to enumerate ALL valid triples instead.
CURATED_SF_SPLITS: list[tuple[int, int, int]] | None = [
    # Only splits where all stages have ≥ 4 replicas — anything with a
    # stage at 3 replicas was a bottleneck in the first grid search.
    (8, 4, 4),   # normalize-heavy
    (6, 5, 5),   # balanced
    (5, 6, 5),   # detect-centric
    (5, 5, 6),   # finalize-centric (best P99 in first search)
]

# Fallback bounds when enumerating (used only if CURATED_SF_SPLITS is None).
MIN_PER_STAGE   = 2
MAX_PER_STAGE   = 12

# Serverless autoscaling knobs to sweep.
# queue_up_base   → triggers scale-up for normalize; detect gets 2×, finalize 4×.
# cooldown_down_s → scale-down delay for normalize; detect gets 2×, finalize 3×.
# cooldown_up_s   → kept small so scale-up is fast; not swept.
QUEUE_UP_BASE_VALUES    = [50, 150]        # aggressive vs moderate scale-up

# True trough duration (load < mean) ≈ 25s for wave_period=120s.
# With UNIFORM cooldown_down (multiplier=1 for all stages), max scale-down
# steps per trough = 25 // cd.  Need ≥ 2 steps to see meaningful drain.
#   cd5  → 5 steps → drains 5→0 replicas in one trough  ✓
#   cd10 → 2 steps → partial drain (5→3 or similar)     ✓
#   cd20 → 1 step  → barely any drain                   (borderline)
COOLDOWN_DOWN_S_VALUES  = [5, 10, 20]

# 0 = no inter-spawn delay → all replicas come up near-instantly from AOT.
# This eliminates the cascade startup lag that was blowing up P50.
COOLDOWN_UP_S           = 0

# Acceptance thresholds for "similar P50 + lower utilisation".
P50_TOLERANCE        = 0.30   # serverless P50 within ±30 % of serverfull
RESOURCE_THRESHOLD   = 0.80   # serverless avg-replicas < 80 % of serverfull

# Minimum scale-down events for serverless to be considered "elastic".
MIN_SCALE_DOWN_EVENTS = 3     # across all stages combined

# ---------------------------------------------------------------------------
# Config generation
# ---------------------------------------------------------------------------

def make_serverfull_yaml(src: Path, dst: Path, replicas: dict[str, int]) -> None:
    """Patch min/max for each stage to replicas[stage] (disables autoscaling)."""
    lines = src.read_text().splitlines(keepends=True)
    current_stage = None
    in_scaling    = False
    result        = []

    for line in lines:
        stripped = line.strip()
        m = re.match(r'^\s*-\s+name:\s+(\w+)', line)
        if m:
            current_stage = m.group(1)
            in_scaling    = False
            result.append(line)
            continue

        if stripped == "scaling:":
            in_scaling = True
            result.append(line)
            continue

        if in_scaling and current_stage in replicas:
            r = replicas[current_stage]
            m_min = re.match(r'^(\s+min:\s*)\d+', line)
            m_max = re.match(r'^(\s+max:\s*)\d+', line)
            if m_min:
                line = m_min.group(1) + str(r) + "\n"
            elif m_max:
                line = m_max.group(1) + str(r) + "\n"

        if in_scaling and stripped and not stripped.startswith('#'):
            indent = len(line) - len(line.lstrip())
            if indent <= 4 and stripped not in ("scaling:",):
                in_scaling = False

        result.append(line)

    dst.write_text("".join(result))


def make_serverless_yaml(
    src: Path,
    dst: Path,
    max_replicas: dict[str, int],
    queue_up_base: int,
    cooldown_down_s: float,
    cooldown_up_s: float = COOLDOWN_UP_S,
) -> None:
    """
    Patch each stage's scaling block:
      min=0, max=max_replicas[stage]
      queue_up    = queue_up_base (uniform — all stages see the same threshold)
      queue_down  = max(1, queue_up_base // 10)
      cooldown_up_s   = cooldown_up_s  (0 = immediate, no cascade lag)
      cooldown_down_s = cooldown_down_s (uniform — so all stages drain at same rate)

    Uniform cooldown_down is critical: with the old 1×/2×/4× multipliers,
    detect and finalize could never complete a scale-down step within the
    ~25s trough window (e.g. finalize got 40s or 120s cooldown).
    """
    multipliers = {"normalize": 1, "detect": 1, "finalize": 1}  # uniform

    lines = src.read_text().splitlines(keepends=True)
    current_stage = None
    in_scaling    = False
    result        = []

    for line in lines:
        stripped = line.strip()
        m = re.match(r'^\s*-\s+name:\s+(\w+)', line)
        if m:
            current_stage = m.group(1)
            in_scaling    = False
            result.append(line)
            continue

        if stripped == "scaling:":
            in_scaling = True
            result.append(line)
            continue

        if in_scaling and current_stage in max_replicas:
            mult = multipliers.get(current_stage, 1)  # now always 1
            r    = max_replicas[current_stage]
            qup  = queue_up_base                       # uniform across stages
            qdn  = max(1, qup // 10)
            cdup = cooldown_up_s                       # 0 = no lag between scale-up steps
            cddn = cooldown_down_s                     # uniform — all stages drain together

            m_min  = re.match(r'^(\s+min:\s*)\d+', line)
            m_max  = re.match(r'^(\s+max:\s*)\d+', line)
            m_qup  = re.match(r'^(\s+queue_up:\s*)\d+', line)
            m_qdn  = re.match(r'^(\s+queue_down:\s*)\d+', line)
            m_cdup = re.match(r'^(\s+cooldown_up_s:\s*)[\d.]+', line)
            m_cddn = re.match(r'^(\s+cooldown_down_s:\s*)[\d.]+', line)

            if m_min:
                line = m_min.group(1) + "0\n"
            elif m_max:
                line = m_max.group(1) + str(r) + "\n"
            elif m_qup:
                line = m_qup.group(1) + str(qup) + "\n"
            elif m_qdn:
                line = m_qdn.group(1) + str(qdn) + "\n"
            elif m_cdup:
                line = m_cdup.group(1) + str(cdup) + "\n"
            elif m_cddn:
                line = m_cddn.group(1) + str(cddn) + "\n"

        if in_scaling and stripped and not stripped.startswith('#'):
            indent = len(line) - len(line.lstrip())
            if indent <= 4 and stripped not in ("scaling:",):
                in_scaling = False

        result.append(line)

    dst.write_text("".join(result))

# ---------------------------------------------------------------------------
# Metric extraction
# ---------------------------------------------------------------------------

def replica_seconds(rot: list[list], duration: float) -> float:
    """Integrate replicas_over_time (list of [t, count]) over [0, duration]."""
    area = 0.0
    for i in range(len(rot) - 1):
        t0, c0 = rot[i]
        t1, _  = rot[i + 1]
        area  += c0 * (t1 - t0)
    area += rot[-1][1] * (duration - rot[-1][0])
    return area


def extract_metrics(summary_path: Path) -> dict:
    with open(summary_path) as f:
        d = json.load(f)

    dur = float(d["duration"])
    p50 = float(d["p50"])
    p99 = float(d["p99"])

    per_stage = d["scaling"]["per_stage"]
    total_rep_sec = 0.0
    total_scale_down = 0
    stage_avg = {}

    for stage, info in per_stage.items():
        rot = info["replicas_over_time"]
        rs  = replica_seconds(rot, dur)
        avg = rs / dur
        stage_avg[stage] = avg
        total_rep_sec   += rs
        total_scale_down += info.get("scale_down_count", 0)

    avg_total_replicas = total_rep_sec / dur

    return {
        "duration":            dur,
        "p50":                 p50,
        "p99":                 p99,
        "avg_total_replicas":  avg_total_replicas,
        "avg_normalize":       stage_avg.get("normalize", 0),
        "avg_detect":          stage_avg.get("detect", 0),
        "avg_finalize":        stage_avg.get("finalize", 0),
        "total_scale_down":    total_scale_down,
    }

# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

def snapshot_summaries() -> list[str]:
    return sorted(str(p) for p in LOGS_DIR.glob("master_*_summary.json"))


def run_config(config_path: Path, label: str, dry_run: bool) -> dict | None:
    """Run `epico run --aot` and return extracted metrics (or None on failure)."""
    if dry_run:
        print(f"  [DRY-RUN] would run: epico run --aot --config {config_path.name}")
        return None

    before = set(snapshot_summaries())

    result = subprocess.run(
        ["epico", "run", "--aot", "--config", config_path.name],
        cwd=PIPELINE_DIR,
        capture_output=False,
    )
    if result.returncode != 0:
        print(f"  ERROR: epico run failed for {label}")
        return None

    after = set(snapshot_summaries())
    new   = sorted(after - before)
    if not new:
        print(f"  WARN: no new summary for {label}")
        return None

    summary_path = Path(new[-1])
    metrics = extract_metrics(summary_path)

    # Copy summary to results directory
    tag = label.replace(" ", "_").replace("/", "-").replace("=", "")
    ts  = time.strftime("%Y%m%d_%H%M%S")
    dst = RESULTS_DIR / f"{tag}_{ts}_summary.json"
    shutil.copy(summary_path, dst)

    return metrics

# ---------------------------------------------------------------------------
# Grid enumeration
# ---------------------------------------------------------------------------

class SFConfig(NamedTuple):
    normalize: int
    detect:    int
    finalize:  int

    @property
    def total(self) -> int:
        return self.normalize + self.detect + self.finalize

    def label(self) -> str:
        return f"sf_n{self.normalize}_d{self.detect}_f{self.finalize}"

    def as_dict(self) -> dict[str, int]:
        return {"normalize": self.normalize, "detect": self.detect, "finalize": self.finalize}


def enumerate_sf_configs(total: int) -> list[SFConfig]:
    if CURATED_SF_SPLITS is not None:
        configs = []
        for n, d, f in CURATED_SF_SPLITS:
            if n + d + f != total:
                print(f"  SKIP curated split ({n},{d},{f}): sum={n+d+f} ≠ {total}")
                continue
            configs.append(SFConfig(n, d, f))
        return configs
    # Full enumeration fallback
    configs = []
    for n in range(MIN_PER_STAGE, min(MAX_PER_STAGE, total - 2 * MIN_PER_STAGE) + 1):
        for d in range(MIN_PER_STAGE, min(MAX_PER_STAGE, total - n - MIN_PER_STAGE) + 1):
            f = total - n - d
            if MIN_PER_STAGE <= f <= MAX_PER_STAGE:
                configs.append(SFConfig(n, d, f))
    return configs


class SLConfig(NamedTuple):
    normalize:      int
    detect:         int
    finalize:       int
    queue_up_base:  int
    cooldown_down_s: float

    def label(self) -> str:
        return (
            f"sl_n{self.normalize}_d{self.detect}_f{self.finalize}"
            f"_qu{self.queue_up_base}_cd{self.cooldown_down_s}"
        )

    def max_dict(self) -> dict[str, int]:
        return {"normalize": self.normalize, "detect": self.detect, "finalize": self.finalize}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dry-run",      action="store_true",
                    help="Print configs that would be run without executing them")
    ap.add_argument("--total-cores",  type=int, default=TOTAL_CORES,
                    help=f"Max total replicas across all stages (default {TOTAL_CORES})")
    ap.add_argument("--runs",         type=int, default=1,
                    help="Repetitions per config (default 1; use 3+ for final configs)")
    ap.add_argument("--sf-only",      action="store_true",
                    help="Only run the serverfull phase (useful for finding good splits first)")
    ap.add_argument("--sl-only",      action="store_true",
                    help="Only run the serverless phase (skip serverfull re-runs)")
    ap.add_argument("--p50-tol",      type=float, default=P50_TOLERANCE,
                    help=f"P50 tolerance for 'similar latency' (default {P50_TOLERANCE})")
    ap.add_argument("--res-thr",      type=float, default=RESOURCE_THRESHOLD,
                    help=f"Resource threshold ratio (default {RESOURCE_THRESHOLD})")
    args = ap.parse_args()

    total = args.total_cores
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)

    sf_configs = enumerate_sf_configs(total)
    print(f"Serverfull configs (sum={total}): {len(sf_configs)}")
    for c in sf_configs:
        print(f"  {c.label()}")

    sl_configs_per_sf = []
    for sf in sf_configs:
        for qup in QUEUE_UP_BASE_VALUES:
            for cdn in COOLDOWN_DOWN_S_VALUES:
                sl_configs_per_sf.append(SLConfig(
                    normalize=sf.normalize,
                    detect=sf.detect,
                    finalize=sf.finalize,
                    queue_up_base=qup,
                    cooldown_down_s=cdn,
                ))

    print(f"Serverless configs: {len(sl_configs_per_sf)}")
    print(f"Total runs (×{args.runs}): "
          f"{(len(sf_configs) + len(sl_configs_per_sf)) * args.runs}")
    print()

    if args.dry_run:
        print("--- DRY RUN ---")
        if not args.sl_only:
            for sf in sf_configs:
                print(f"  [SF] {sf.label()}")
        if not args.sf_only:
            for sl in sl_configs_per_sf:
                print(f"  [SL] {sl.label()}")
        return

    csv_path = RESULTS_DIR / "grid_results.csv"
    csv_fields = [
        "mode", "label",
        "n_normalize", "n_detect", "n_finalize", "total_fixed",
        "queue_up_base", "cooldown_down_s",
        "run", "p50", "p99",
        "avg_total_replicas", "avg_normalize", "avg_detect", "avg_finalize",
        "total_scale_down",
    ]

    write_header = not csv_path.exists()
    csv_fh = open(csv_path, "a", newline="")
    writer = csv.DictWriter(csv_fh, fieldnames=csv_fields)
    if write_header:
        writer.writeheader()

    tmp_yaml = PIPELINE_DIR / "_grid_search_tmp.yaml"

    # ---- Phase 1: Serverfull -----------------------------------------------
    sf_results: dict[str, list[dict]] = {}  # label → list of metric dicts

    if not args.sl_only:
        print("=" * 60)
        print("PHASE 1 — Serverfull sweep")
        print("=" * 60)

        for sf in sf_configs:
            make_serverfull_yaml(PIPELINE_YAML, tmp_yaml, sf.as_dict())
            sf_results[sf.label()] = []

            for run_i in range(1, args.runs + 1):
                lbl = f"{sf.label()} run{run_i}"
                print(f"  Running {lbl} …")
                m = run_config(tmp_yaml, lbl, dry_run=False)
                if m is None:
                    continue
                sf_results[sf.label()].append(m)
                row = {
                    "mode": "serverfull", "label": sf.label(),
                    "n_normalize": sf.normalize, "n_detect": sf.detect,
                    "n_finalize": sf.finalize, "total_fixed": sf.total,
                    "queue_up_base": "", "cooldown_down_s": "",
                    "run": run_i,
                    **{k: m[k] for k in
                       ["p50", "p99", "avg_total_replicas",
                        "avg_normalize", "avg_detect", "avg_finalize",
                        "total_scale_down"]},
                }
                writer.writerow(row)
                csv_fh.flush()
                print(f"    p50={m['p50']:.2f} ms  avg_replicas={m['avg_total_replicas']:.1f}")

    # ---- Phase 2: Serverless -----------------------------------------------
    if not args.sf_only:
        print()
        print("=" * 60)
        print("PHASE 2 — Serverless sweep")
        print("=" * 60)

        sl_results: list[dict] = []

        for sl in sl_configs_per_sf:
            make_serverless_yaml(
                PIPELINE_YAML, tmp_yaml, sl.max_dict(),
                queue_up_base=sl.queue_up_base,
                cooldown_down_s=sl.cooldown_down_s,
            )
            for run_i in range(1, args.runs + 1):
                lbl = f"{sl.label()} run{run_i}"
                print(f"  Running {lbl} …")
                m = run_config(tmp_yaml, lbl, dry_run=False)
                if m is None:
                    continue
                sl_results.append({
                    "sl_config": sl, "metrics": m,
                })
                row = {
                    "mode": "serverless", "label": sl.label(),
                    "n_normalize": sl.normalize, "n_detect": sl.detect,
                    "n_finalize": sl.finalize, "total_fixed": sl.normalize + sl.detect + sl.finalize,
                    "queue_up_base": sl.queue_up_base, "cooldown_down_s": sl.cooldown_down_s,
                    "run": run_i,
                    **{k: m[k] for k in
                       ["p50", "p99", "avg_total_replicas",
                        "avg_normalize", "avg_detect", "avg_finalize",
                        "total_scale_down"]},
                }
                writer.writerow(row)
                csv_fh.flush()
                print(f"    p50={m['p50']:.2f} ms  avg_replicas={m['avg_total_replicas']:.1f}"
                      f"  scale_downs={m['total_scale_down']}")

    csv_fh.close()
    tmp_yaml.unlink(missing_ok=True)

    # ---- Summary -----------------------------------------------------------
    print()
    print("=" * 70)
    print("RESULTS SUMMARY")
    print("=" * 70)

    # Load everything from CSV for analysis
    rows = []
    with open(csv_path) as f:
        for row in csv.DictReader(f):
            rows.append(row)

    sf_rows = [r for r in rows if r["mode"] == "serverfull"]
    sl_rows = [r for r in rows if r["mode"] == "serverless"]

    if not sf_rows or not sl_rows:
        print("Not enough data for comparison yet.")
        return

    # Average metrics per config label
    def avg_by_label(row_list: list[dict], key: str) -> dict[str, float]:
        by_label: dict[str, list[float]] = {}
        for r in row_list:
            lbl = r["label"]
            val = float(r[key])
            by_label.setdefault(lbl, []).append(val)
        return {lbl: sum(v) / len(v) for lbl, v in by_label.items()}

    sf_p50     = avg_by_label(sf_rows, "p50")
    sf_avg_rep = avg_by_label(sf_rows, "avg_total_replicas")
    sl_p50     = avg_by_label(sl_rows, "p50")
    sl_avg_rep = avg_by_label(sl_rows, "avg_total_replicas")
    sl_sdn     = avg_by_label(sl_rows, "total_scale_down")

    # Build a mapping from serverless label → matching serverfull label
    # (based on equal n/d/f split)
    def sf_label_for(sl_lbl: str, sl_row_list: list[dict]) -> str | None:
        r = next((x for x in sl_row_list if x["label"] == sl_lbl), None)
        if r is None:
            return None
        return f"sf_n{r['n_normalize']}_d{r['n_detect']}_f{r['n_finalize']}"

    print(f"\n{'Serverless config':<55} {'SF P50':>8} {'SL P50':>8} "
          f"{'ΔP50%':>7} {'SF avg R':>8} {'SL avg R':>8} "
          f"{'Savings%':>8} {'ScaleDn':>7}  {'OK?':>4}")
    print("-" * 120)

    candidates = []
    for sl_lbl in sorted(sl_p50):
        sf_lbl = sf_label_for(sl_lbl, sl_rows)
        if sf_lbl not in sf_p50:
            continue
        sf_p = sf_p50[sf_lbl]
        sl_p = sl_p50[sl_lbl]
        rel_diff = (sl_p - sf_p) / sf_p
        sf_r = sf_avg_rep.get(sf_lbl, 0)
        sl_r = sl_avg_rep.get(sl_lbl, 0)
        savings = 1.0 - (sl_r / sf_r) if sf_r > 0 else 0.0
        sdn     = sl_sdn.get(sl_lbl, 0)

        ok_p50  = abs(rel_diff) <= args.p50_tol
        ok_res  = (sl_r / sf_r) <= args.res_thr if sf_r > 0 else False
        ok_sdn  = sdn >= MIN_SCALE_DOWN_EVENTS
        ok      = ok_p50 and ok_res and ok_sdn

        flag = "YES" if ok else ("~p50" if not ok_p50 else ("~res" if not ok_res else "~sdn"))

        print(f"{sl_lbl:<55} {sf_p:>8.1f} {sl_p:>8.1f} "
              f"{rel_diff*100:>+7.1f}% {sf_r:>8.1f} {sl_r:>8.1f} "
              f"{savings*100:>8.1f}% {sdn:>7.0f}  {flag:>4}")

        if ok:
            candidates.append({
                "sl_label": sl_lbl, "sf_label": sf_lbl,
                "sf_p50": sf_p, "sl_p50": sl_p, "rel_diff": rel_diff,
                "sf_avg_replicas": sf_r, "sl_avg_replicas": sl_r,
                "savings_pct": savings * 100, "scale_down_events": sdn,
            })

    print()
    if candidates:
        candidates.sort(key=lambda x: (abs(x["rel_diff"]), -x["savings_pct"]))
        print(f"BEST CANDIDATES (|ΔP50| ≤ {args.p50_tol*100:.0f}%, "
              f"savings ≥ {(1-args.res_thr)*100:.0f}%, scale-downs ≥ {MIN_SCALE_DOWN_EVENTS}):")
        for c in candidates[:5]:
            print(f"  {c['sl_label']}")
            print(f"    SF P50={c['sf_p50']:.1f} ms  SL P50={c['sl_p50']:.1f} ms  "
                  f"ΔP50={c['rel_diff']*100:+.1f}%")
            print(f"    SF avg_R={c['sf_avg_replicas']:.1f}  SL avg_R={c['sl_avg_replicas']:.1f}  "
                  f"savings={c['savings_pct']:.1f}%  scale_downs={c['scale_down_events']:.0f}")
            print()
        print(f"Full results: {csv_path}")
    else:
        print("No candidates met all criteria. Check grid_results.csv and consider:")
        print("  - Wider QUEUE_UP_BASE_VALUES / COOLDOWN_DOWN_S_VALUES")
        print("  - Increasing wave_period in pipeline.yaml (longer troughs → easier scale-down)")
        print("  - Loosening --p50-tol or --res-thr")
        print(f"Full results: {csv_path}")

    # Write text summary
    summary_path = RESULTS_DIR / "grid_summary.txt"
    with open(summary_path, "w") as f:
        f.write(f"Grid search results — {time.strftime('%Y-%m-%d %H:%M')}\n")
        f.write(f"total_cores={total}  runs_per_config={args.runs}\n\n")
        f.write(f"Candidates: {len(candidates)}\n")
        for c in candidates:
            f.write(f"\n  {c['sl_label']}\n")
            f.write(f"    SF P50={c['sf_p50']:.1f} ms  SL P50={c['sl_p50']:.1f} ms  ΔP50={c['rel_diff']*100:+.1f}%\n")
            f.write(f"    savings={c['savings_pct']:.1f}%  scale_downs={c['scale_down_events']:.0f}\n")
    print(f"Summary written: {summary_path}")


if __name__ == "__main__":
    main()
