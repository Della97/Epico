#!/usr/bin/env python3
"""Plot CPU/memory and loadgen time series.

Outputs:
- cpu_normalized.png
- memory_rss.png
- loadgen_timeseries.png
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import matplotlib.pyplot as plt


def _latest_match(base: Path, pattern: str) -> Path:
    matches = sorted(base.glob(pattern))
    if not matches:
        # Also support nested layouts such as logs/<run-tag>/...
        matches = sorted(base.rglob(pattern))
    if not matches:
        raise FileNotFoundError(f"No files matching {pattern} in {base}")
    return matches[-1]


def _normalize_logs_path(path: Path) -> Path:
    """Accept small CLI typos like './logs.' and recover gracefully."""
    if path.exists():
        return path

    s = str(path)
    stripped = s.rstrip(".,;:")
    if stripped and stripped != s:
        p2 = Path(stripped)
        if p2.exists():
            return p2

    # Final fallback: if caller gave a bad relative path, prefer local ./logs.
    fallback = Path("./logs")
    if fallback.exists():
        return fallback

    return path


def _read_json(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _plot_cpu_normalized(summary: dict, out_dir: Path) -> Path:
    resources = summary.get("resources") or {}
    cpu_samples = resources.get("cpu_pct_samples") or []
    cpu_unit = resources.get("cpu_pct_unit")
    logical_cores = max(1, int((summary.get("environment") or {}).get("cpu_cores_logical", 1)))

    fig, ax1 = plt.subplots(figsize=(10, 4.5))
    if cpu_samples:
        t_cpu = [float(t) for t, _ in cpu_samples]
        raw = [float(v) for _, v in cpu_samples]

        # Backward compatibility:
        # - New summaries: already normalized in master (percent_of_available_cpus)
        # - Old summaries: core-percent (100 == 1 full core)
        if cpu_unit == "percent_of_available_cpus":
            y_cpu = raw
            y_label = "CPU utilization (%)"
            title = "CPU Usage (Percent of Available CPUs)"
        else:
            # Old runs had core-percent; convert to host-wide percentage.
            y_cpu = [v / logical_cores for v in raw]
            y_label = "CPU utilization (%)"
            title = "CPU Usage (Legacy core-% normalized to host %)"

        ax1.plot(t_cpu, y_cpu, color="#0b7285", linewidth=2.0, label=y_label)
    ax1.set_xlabel("Time (s)")
    ax1.set_ylabel("CPU utilization (%)", color="#0b7285")
    ax1.tick_params(axis="y", labelcolor="#0b7285")
    ax1.grid(True, alpha=0.25)

    fig.suptitle(title if cpu_samples else "CPU Usage")
    out = out_dir / "cpu_normalized.png"
    fig.tight_layout()
    fig.savefig(out, dpi=180)
    plt.close(fig)
    return out


def _plot_mem(summary: dict, out_dir: Path) -> Path:
    resources = summary.get("resources") or {}
    rss_samples = resources.get("rss_mb_samples") or []

    fig, ax1 = plt.subplots(figsize=(10, 4.5))
    if rss_samples:
        t_mem = [float(t) for t, _ in rss_samples]
        y_mem = [float(v) for _, v in rss_samples]
        ax1.plot(t_mem, y_mem, color="#2b8a3e", linewidth=2.0, label="RSS (MB)")
    ax1.set_xlabel("Time (s)")
    ax1.set_ylabel("RSS (MB)", color="#2b8a3e")
    ax1.tick_params(axis="y", labelcolor="#2b8a3e")
    ax1.grid(True, alpha=0.25)

    fig.suptitle("Memory Usage (RSS)")
    out = out_dir / "memory_rss.png"
    fig.tight_layout()
    fig.savefig(out, dpi=180)
    plt.close(fig)
    return out


def _plot_loadgen(loadgen_jsonl: Path, out_dir: Path) -> Path:
    t: list[float] = []
    rate: list[float] = []

    with loadgen_jsonl.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            if obj.get("msg") != "progress":
                continue
            t.append(float(obj.get("elapsed_s", 0)))
            rate.append(float(obj.get("send_rate", 0)))

    fig, ax1 = plt.subplots(figsize=(10, 4.5))
    if t:
        ax1.plot(t, rate, color="#1c7ed6", linewidth=2.0, label="Send rate (ev/s)")
    ax1.set_xlabel("Time (s)")
    ax1.set_ylabel("Send rate (ev/s)", color="#1c7ed6")
    ax1.tick_params(axis="y", labelcolor="#1c7ed6")
    ax1.grid(True, alpha=0.25)

    fig.suptitle("Loadgen Send Rate Over Time")
    out = out_dir / "loadgen_timeseries.png"
    fig.tight_layout()
    fig.savefig(out, dpi=180)
    plt.close(fig)
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description="Plot CPU/memory and loadgen trace")
    ap.add_argument("--logs", type=Path, default=Path("./logs"), help="Directory with run logs")
    ap.add_argument("--summary", type=Path, default=None, help="Path to agent_*_summary.json")
    ap.add_argument("--loadgen", type=Path, default=None, help="Path to loadgen_*.jsonl")
    ap.add_argument("--out", type=Path, default=None, help="Output directory for figures")
    args = ap.parse_args()

    logs = _normalize_logs_path(args.logs)
    summary = args.summary or _latest_match(logs, "agent_*_summary.json")
    loadgen = args.loadgen or _latest_match(logs, "loadgen_*.jsonl")

    out_dir = args.out or (logs / "figures")
    out_dir.mkdir(parents=True, exist_ok=True)

    summary_obj = _read_json(summary)

    out_cpu = _plot_cpu_normalized(summary_obj, out_dir)
    out_mem = _plot_mem(summary_obj, out_dir)
    out_lg = _plot_loadgen(loadgen, out_dir)

    print(f"summary: {summary}")
    print(f"loadgen: {loadgen}")
    print(f"wrote: {out_cpu}")
    print(f"wrote: {out_mem}")
    print(f"wrote: {out_lg}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
