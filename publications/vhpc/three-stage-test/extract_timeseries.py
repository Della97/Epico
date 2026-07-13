#!/usr/bin/env python3
"""Extract run timeseries from Metapod outputs.

Inputs:
- agent summary JSON (for CPU/RSS samples)
- loadgen JSONL (for progress snapshots)

Outputs:
- cpu_memory_timeseries.csv
- loadgen_timeseries.csv
- run_overview.json
"""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path


def read_summary(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def write_cpu_mem_csv(summary: dict, out_path: Path) -> int:
    resources = summary.get("resources") or {}
    cpu_samples = resources.get("cpu_pct_samples") or []
    rss_samples = resources.get("rss_mb_samples") or []

    rows = max(len(cpu_samples), len(rss_samples))
    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["t_s", "cpu_pct", "rss_mb"])
        for i in range(rows):
            t_s = ""
            cpu = ""
            rss = ""

            if i < len(cpu_samples):
                t_s = cpu_samples[i][0]
                cpu = cpu_samples[i][1]
            if i < len(rss_samples):
                if t_s == "":
                    t_s = rss_samples[i][0]
                rss = rss_samples[i][1]

            w.writerow([t_s, cpu, rss])

    return rows


def write_loadgen_csv(loadgen_jsonl: Path, out_path: Path) -> tuple[int, dict]:
    rows = 0
    final = {}

    with loadgen_jsonl.open("r", encoding="utf-8") as src, out_path.open(
        "w", newline="", encoding="utf-8"
    ) as dst:
        w = csv.writer(dst)
        w.writerow(["ts", "elapsed_s", "sent", "send_rate_ev_s", "dropped", "anomalies"])

        for line in src:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            msg = obj.get("msg", "")

            if msg == "progress":
                w.writerow(
                    [
                        obj.get("ts", ""),
                        obj.get("elapsed_s", ""),
                        obj.get("sent", ""),
                        obj.get("send_rate", ""),
                        obj.get("dropped", ""),
                        obj.get("anomalies", ""),
                    ]
                )
                rows += 1
            elif msg == "run complete":
                final = {
                    "total_sent": obj.get("total_sent"),
                    "total_dropped": obj.get("total_dropped"),
                    "duration_s": obj.get("duration_s"),
                    "actual_rate": obj.get("actual_rate"),
                    "anomalies": obj.get("anomalies"),
                }

    return rows, final


def main() -> int:
    ap = argparse.ArgumentParser(description="Extract Metapod CPU/memory and loadgen timeseries")
    ap.add_argument("--summary", required=True, type=Path, help="Path to agent_*_summary.json")
    ap.add_argument("--loadgen-jsonl", required=True, type=Path, help="Path to loadgen_*.jsonl")
    ap.add_argument("--out-dir", required=True, type=Path, help="Output directory for CSV/JSON")
    args = ap.parse_args()

    args.out_dir.mkdir(parents=True, exist_ok=True)

    summary = read_summary(args.summary)
    cpu_csv = args.out_dir / "cpu_memory_timeseries.csv"
    lg_csv = args.out_dir / "loadgen_timeseries.csv"
    overview_json = args.out_dir / "run_overview.json"

    cpu_rows = write_cpu_mem_csv(summary, cpu_csv)
    lg_rows, lg_final = write_loadgen_csv(args.loadgen_jsonl, lg_csv)

    overview = {
        "summary": str(args.summary),
        "loadgen_jsonl": str(args.loadgen_jsonl),
        "cpu_memory_rows": cpu_rows,
        "loadgen_rows": lg_rows,
        "loadgen_final": lg_final,
    }

    with overview_json.open("w", encoding="utf-8") as f:
        json.dump(overview, f, indent=2)

    print(f"wrote {cpu_csv}")
    print(f"wrote {lg_csv}")
    print(f"wrote {overview_json}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
