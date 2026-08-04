"""Finding things on disk: pipelines, their runs, and a run's files.

Two log layouts exist and both are read here. Runs made after the logging
rework live in `logs/run_<ts>/{master.jsonl,master_summary.json,...}`; older
ones are flat, `logs/master_<ts>_summary.json` with the timestamp on each
file. A run is identified by its summary, and everything else (master log,
loadgen log) is located relative to that.
"""

from __future__ import annotations

import glob
import json
import os
import re
from dataclasses import dataclass, field


# ── repo / pipeline discovery ────────────────────────────────────────────────

def find_repo_root(start: str) -> str:
    """Walk up from `start` looking for the Epico checkout."""
    cur = os.path.abspath(start)
    while True:
        if os.path.isdir(os.path.join(cur, "examples")) and \
           os.path.isfile(os.path.join(cur, "Cargo.toml")):
            return cur
        parent = os.path.dirname(cur)
        if parent == cur:
            raise SystemExit(
                f"not inside an Epico checkout (no examples/ + Cargo.toml above {start})")
        cur = parent


@dataclass
class Pipeline:
    name: str
    path: str          # the example directory
    yaml_path: str
    logs_dir: str
    run_count: int = 0
    stage_count: int = 0
    edge_count: int = 0


def list_pipelines(repo_root: str) -> list[Pipeline]:
    out = []
    for d in sorted(glob.glob(os.path.join(repo_root, "examples", "*"))):
        yaml_path = os.path.join(d, "pipeline.yaml")
        if not os.path.isfile(yaml_path):
            continue
        p = Pipeline(
            name=os.path.basename(d),
            path=d,
            yaml_path=yaml_path,
            logs_dir=os.path.join(d, "logs"),
        )
        stages, edges = parse_pipeline_yaml(yaml_path)
        p.stage_count = len(stages)
        p.edge_count = len(edges)
        p.run_count = len(list_runs(p))
        out.append(p)
    return out


# ── run discovery ────────────────────────────────────────────────────────────

@dataclass
class Run:
    run_id: str            # folder name, or the timestamp for legacy layout
    summary_path: str
    master_log: str | None
    loadgen_log: str | None
    dispatcher_logs: list[str] = field(default_factory=list)
    layout: str = "run-dir"          # or "flat"
    mtime: float = 0.0
    # cheap preview fields, read from the summary so the picker can show them
    events: int = 0
    duration_s: float = 0.0
    p50_ms: float = 0.0
    p99_ms: float = 0.0
    stages: int = 0
    broken: str | None = None        # why the summary could not be read


def list_runs(pipeline: Pipeline) -> list[Run]:
    """Every run of this pipeline, newest first."""
    runs: list[Run] = []
    logs = pipeline.logs_dir
    if not os.path.isdir(logs):
        return runs

    # New layout: one folder per run.
    for d in sorted(glob.glob(os.path.join(logs, "*", ""))):
        d = d.rstrip(os.sep)
        summary = os.path.join(d, "master_summary.json")
        if not os.path.isfile(summary):
            continue
        runs.append(_make_run(
            run_id=os.path.basename(d),
            summary_path=summary,
            master_log=_first_existing([os.path.join(d, "master.jsonl")]),
            loadgen_log=_first_existing([os.path.join(d, "loadgen.jsonl")]),
            dispatcher_logs=sorted(glob.glob(os.path.join(d, "dispatcher-*.jsonl"))),
            layout="run-dir",
        ))

    # Legacy flat layout: timestamp in each filename.
    for summary in sorted(glob.glob(os.path.join(logs, "master_*_summary.json"))):
        m = re.search(r"master_(\d{8}_\d{6})_summary\.json$", os.path.basename(summary))
        if not m:
            continue
        ts = m.group(1)
        runs.append(_make_run(
            run_id=ts,
            summary_path=summary,
            master_log=_first_existing([os.path.join(logs, f"master_{ts}.jsonl")]),
            # The loadgen starts a beat after the agent, so its stamp rarely
            # matches; take the closest one at or after the master's.
            loadgen_log=_nearest_loadgen(logs, ts),
            dispatcher_logs=sorted(glob.glob(os.path.join(logs, f"dispatcher-*_{ts}.jsonl"))),
            layout="flat",
        ))

    runs.sort(key=lambda r: r.mtime, reverse=True)
    return runs


def _make_run(**kw) -> Run:
    run = Run(**kw)
    run.mtime = os.path.getmtime(run.summary_path)
    try:
        with open(run.summary_path) as f:
            d = json.load(f)
        run.events = int(d.get("counters", {}).get("events_received", 0) or 0)
        run.duration_s = float(d.get("duration", 0) or 0)
        run.p50_ms = float(d.get("p50", 0) or 0)
        run.p99_ms = float(d.get("p99", 0) or 0)
        run.stages = len(d.get("stage_names", []) or [])
    except Exception as e:                       # noqa: BLE001 — preview only
        run.broken = str(e)
    return run


def _first_existing(paths: list[str]) -> str | None:
    for p in paths:
        if os.path.isfile(p):
            return p
    return None


def _nearest_loadgen(logs: str, master_ts: str) -> str | None:
    """Loadgen log whose stamp is closest to (and usually just after) the master's."""
    cands = []
    for p in glob.glob(os.path.join(logs, "loadgen_*.jsonl")):
        m = re.search(r"loadgen_(\d{8}_\d{6})\.jsonl$", os.path.basename(p))
        if m:
            cands.append((abs(_stamp_key(m.group(1)) - _stamp_key(master_ts)), p))
    if not cands:
        return None
    delta, path = min(cands)
    # More than a minute apart is a different run, not a slow start.
    return path if delta <= 60 else None


def _stamp_key(stamp: str) -> int:
    """`YYYYMMDD_HHMMSS` → seconds-ish key. Good enough for nearest-match."""
    d, t = stamp.split("_")
    import calendar, time  # noqa: PLC0415 — tiny, local use
    return calendar.timegm(time.strptime(d + t, "%Y%m%d%H%M%S"))


# ── pipeline.yaml (topology only; no PyYAML dependency) ──────────────────────

def parse_pipeline_yaml(path: str) -> tuple[list[str], list[tuple[str, str]]]:
    """Stage names and edges.

    Only the two blocks the visualisation needs are parsed, using the same
    shapes the CLI accepts: `- name: x` inside `stages:`, and the
    `- a -> b` shorthand inside `edges:`. An empty `edges:` block means the
    linear chain implied by stage order, which is what the runtime does too.
    """
    try:
        text = open(path).read()
    except OSError:
        return [], []

    stages: list[str] = []
    edges: list[tuple[str, str]] = []
    block = None

    for raw in text.splitlines():
        line = raw.split("#", 1)[0].rstrip()
        if not line:
            continue
        if re.match(r"^[A-Za-z_][\w-]*:", line):        # a top-level key
            block = line.split(":", 1)[0]
            continue
        if block == "stages":
            m = re.match(r"^\s*-\s*name:\s*(\S+)", line)
            if m:
                stages.append(m.group(1).strip("'\""))
        elif block == "edges":
            m = re.match(r"^\s*-\s*(\S+)\s*->\s*(\S+)\s*$", line)
            if m:
                edges.append((m.group(1), m.group(2)))

    if not edges and len(stages) > 1:
        edges = list(zip(stages, stages[1:]))
    return stages, edges
