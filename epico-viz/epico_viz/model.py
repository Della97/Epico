"""Turn a run's files into the single JSON blob the page renders from.

Everything the page shows is computed here, so the HTML/JS layer stays a
renderer with no knowledge of Epico's log schema. Three sources feed in:

  * `master_summary.json` — distributions, scaling events, queue depth,
    counters, throughput, environment. The bulk of it.
  * `master.jsonl`        — the run's *configuration*, which the summary does
    not carry: transport, compile mode actually used, per-stage autoscaler
    thresholds, per-replica cold-start phases, and the flags.
  * `loadgen.jsonl`       — what was offered, and when each burst landed.

Anything missing degrades to `None`/empty rather than raising: old runs
predate some of these fields, and a page that renders 90% of a run beats one
that refuses to open.
"""

from __future__ import annotations

import json
import os
import re
from collections import defaultdict

from .discover import Pipeline, Run, parse_pipeline_yaml

# Rows in `events` are one per sampled event; a scatter plot cannot show
# 200k of them and the page would carry megabytes of JSON for no gain.
SCATTER_POINTS = 4000


# ── small helpers ────────────────────────────────────────────────────────────

def _f(v, default=0.0) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def _i(v, default=0) -> int:
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return default


def _read_jsonl(path: str | None) -> list[dict]:
    if not path or not os.path.isfile(path):
        return []
    out = []
    with open(path, errors="replace") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                out.append(json.loads(line))
            except json.JSONDecodeError:
                continue        # a torn last line on a killed run
    return out


def _stage_of(component: str) -> str | None:
    """`worker/joinleft` → `joinleft`; `master` → None."""
    if "/" in component:
        return component.split("/", 1)[1]
    return None


# ── main entry point ─────────────────────────────────────────────────────────

def build_model(pipeline: Pipeline, run: Run) -> dict:
    summary = json.load(open(run.summary_path))
    mlog = _read_jsonl(run.master_log)
    llog = _read_jsonl(run.loadgen_log)

    t0 = _f(summary.get("test_start_wall"))
    # Fall back to the first log line so relative times still work on a run
    # whose summary lacks the wall clock.
    if not t0 and mlog:
        t0 = _f(mlog[0].get("ts"))

    stage_names = list(summary.get("stage_names") or [])
    topology = _topology(pipeline, mlog, stage_names)
    flags = _flags(summary, mlog, llog, topology)
    replicas, scaling_events = _replicas(summary, topology["nodes"])

    model = {
        "meta": _meta(pipeline, run, summary),
        "flags": flags,
        "topology": topology,
        "replicas": replicas,
        "scaling_events": scaling_events,
        "queue": _queue(summary),
        "load": _load(llog, t0),
        "latency": _latency(summary),
        "coldstart": _coldstart(summary, mlog, t0),
        "throughput": _throughput(summary),
        "scatter": _scatter(summary),
        "counters": _counters(summary, topology, llog),
        "worker_timing": _worker_timing(summary),
        "resources": _resources(summary),
        "markers": _markers(mlog, llog, t0),
        "environment": summary.get("environment") or {},
        "warnings": [],
    }
    model["warnings"] = _warnings(model, run)
    return model


# ── meta ─────────────────────────────────────────────────────────────────────

def _meta(pipeline: Pipeline, run: Run, summary: dict) -> dict:
    import datetime as dt
    started = _f(summary.get("test_start_wall"))
    return {
        "pipeline": pipeline.name,
        "pipeline_dir": pipeline.path,
        "run_id": run.run_id,
        "layout": run.layout,
        "summary_path": run.summary_path,
        "master_log": run.master_log,
        "loadgen_log": run.loadgen_log,
        "started_iso": (dt.datetime.fromtimestamp(started).isoformat(" ", "seconds")
                        if started else None),
        "duration_s": _f(summary.get("duration")),
        "events_received": _i(summary.get("counters", {}).get("events_received")),
        "stage_count": len(summary.get("stage_names") or []),
    }


# ── topology ─────────────────────────────────────────────────────────────────

def _topology(pipeline: Pipeline, mlog: list[dict], stage_names: list[str]) -> dict:
    """Edges as they actually ran, falling back to the declared pipeline.

    The agent logs one line per in-process edge, which is the ground truth for
    a run (it reflects env overrides the YAML never saw). The ZMQ spine logs
    no such line, so the YAML is the fallback.
    """
    edges: list[dict] = []
    seen = set()
    for rec in mlog:
        msg = rec.get("msg", "")
        m = re.match(r"^in-process edge \((\w+)\)$", msg)
        if not m:
            continue
        key = (rec.get("from"), rec.get("to"))
        if key in seen or not all(key):
            continue
        seen.add(key)
        edges.append({
            "from": rec["from"], "to": rec["to"], "transport": m.group(1),
            "cap": _i(rec.get("cap")) or None,
            "producers": _i(rec.get("producers")) or None,
            "base": _i(rec.get("base")) if rec.get("base") is not None else None,
        })

    yaml_stages, yaml_edges = parse_pipeline_yaml(pipeline.yaml_path)
    source = "master log"
    if not edges:
        source = "pipeline.yaml"
        edges = [{"from": a, "to": b, "transport": None, "cap": None,
                  "producers": None, "base": None} for a, b in yaml_edges]

    names = stage_names or yaml_stages
    if not names:
        names = sorted({e["from"] for e in edges} | {e["to"] for e in edges})

    # Keep only edges whose endpoints are stages we know about — a stale YAML
    # next to a newer run would otherwise inject phantom nodes.
    known = set(names)
    edges = [e for e in edges if e["from"] in known and e["to"] in known]

    succ = defaultdict(list)
    pred = defaultdict(list)
    for e in edges:
        succ[e["from"]].append(e["to"])
        pred[e["to"]].append(e["from"])

    layer = _layers(names, pred, succ)
    nodes = [{
        "name": n,
        "layer": layer.get(n, 0),
        "in_degree": len(pred[n]),
        "out_degree": len(succ[n]),
        "is_source": len(pred[n]) == 0,
        "is_sink": len(succ[n]) == 0,
    } for n in names]

    paths_in = _path_counts(names, pred)
    paths_out = _path_counts(names, succ)
    for n in nodes:
        n["paths_in"] = paths_in.get(n["name"], 1)
        n["paths_out"] = paths_out.get(n["name"], 1)

    return {
        "nodes": nodes,
        "edges": edges,
        "edge_source": source,
        "sources": [n["name"] for n in nodes if n["is_source"]],
        "sinks": [n["name"] for n in nodes if n["is_sink"]],
        "total_paths": sum(paths_in[n["name"]] for n in nodes if n["is_sink"]) or 1,
    }


def _layers(names: list[str], pred: dict, succ: dict) -> dict[str, int]:
    """Longest-path layering: a node sits one level below its deepest parent."""
    layer: dict[str, int] = {}

    def depth(n: str, seen: frozenset = frozenset()) -> int:
        if n in layer:
            return layer[n]
        if n in seen:                       # cycle guard; DAGs only, but be safe
            return 0
        parents = pred.get(n, [])
        d = 0 if not parents else 1 + max(depth(p, seen | {n}) for p in parents)
        layer[n] = d
        return d

    for n in names:
        depth(n)
    return layer


def _path_counts(names: list[str], adj: dict) -> dict[str, int]:
    """Distinct paths from each node to the far end of `adj` (memoised)."""
    memo: dict[str, int] = {}

    def count(n: str, seen: frozenset = frozenset()) -> int:
        if n in memo:
            return memo[n]
        if n in seen:
            return 1
        nxt = adj.get(n, [])
        v = 1 if not nxt else sum(count(x, seen | {n}) for x in nxt)
        memo[n] = v
        return v

    for n in names:
        count(n)
    return memo


# ── flags / configuration ────────────────────────────────────────────────────

def _flags(summary: dict, mlog: list[dict], llog: list[dict], topology: dict) -> dict:
    requested: dict[str, str] = {}
    effective: dict[str, str] = {}
    fallbacks: list[dict] = []
    compile_ms: dict[str, float] = {}
    warmup_ms: dict[str, float] = {}
    autoscaler: dict[str, dict] = {}
    typed_dispatch: list[str] = []
    cold_start_opt = None
    ingress = {"mode": "socket", "cap": None}
    egress = {"mode": "socket"}
    dispatchers: list[str] = []
    resource_sampling = None
    engine = {}

    for rec in mlog:
        msg = rec.get("msg", "")
        comp = rec.get("component", "")
        stage = rec.get("stage") or _stage_of(comp)

        if msg == "build features":
            v = rec.get("cold_start_opt")
            cold_start_opt = (str(v).lower() == "true")
        elif msg == "loading component" and stage:
            requested[stage] = rec.get("compile_mode") or ""
        elif msg == "instance_pre ready" and stage:
            effective[stage] = rec.get("mode") or ""
        elif msg.startswith("AOT deserialize failed"):
            fallbacks.append({"path": rec.get("path"), "err": rec.get("err")})
        elif msg == "JIT component compiled":
            st = _stage_from_wasm(rec.get("path", ""))
            if st:
                compile_ms[st] = _f(rec.get("compile_ms"))
        elif msg == "cranelift stage warmup complete" and stage:
            warmup_ms[stage] = _f(rec.get("warmup_ms"))
        elif msg == "autoscaler ready" and stage:
            autoscaler[stage] = {
                "min": _i(rec.get("min_rep")), "max": _i(rec.get("max_rep")),
                "queue_up": _f(rec.get("queue_up")), "queue_down": _f(rec.get("queue_down")),
                "mode": rec.get("mode"),
            }
        elif msg.startswith("TYPED DISPATCH ACTIVE") and stage:
            typed_dispatch.append(stage)
        elif msg == "in-process ingress (source pump)":
            ingress = {"mode": "inprocess", "cap": _i(rec.get("cap")) or None}
        elif msg == "in-process egress (sink drain)":
            egress = {"mode": "inprocess"}
        elif msg == "starting dispatcher":
            dispatchers.append(rec.get("name", "?"))
        elif msg.startswith("resource sampler disabled"):
            resource_sampling = False
        elif msg == "engine ready":
            engine = {"max_replicas_total": _i(rec.get("max_replicas_total"))}

    if resource_sampling is None:
        resource_sampling = bool(summary.get("resources", {}).get("sample_count"))

    # One label for the whole run when every stage agrees, which is the norm.
    def _one(d: dict[str, str]) -> str | None:
        vals = {v for v in d.values() if v}
        if not vals:
            return None
        return vals.pop() if len(vals) == 1 else "mixed"

    transports = {e["transport"] for e in topology["edges"] if e["transport"]}
    edge_impl = transports.pop() if len(transports) == 1 else (
        "mixed" if transports else ("zmq" if dispatchers else None))
    caps = {e["cap"] for e in topology["edges"] if e["cap"]}

    run_config = summary.get("run_config") or {}
    per_stage_cfg = {s.get("stage"): s for s in (run_config.get("stages") or [])}

    return {
        "compile_mode_requested": _one(requested),
        "compile_mode_effective": _one(effective),
        "compile_mode_per_stage": {s: {"requested": requested.get(s),
                                       "effective": effective.get(s)}
                                   for s in sorted(set(requested) | set(effective))},
        "aot_fallbacks": fallbacks,
        "cold_start_opt": cold_start_opt,
        "edge_impl": edge_impl,
        "edge_cap": (caps.pop() if len(caps) == 1 else None),
        "ingress": ingress,
        "egress": egress,
        "dispatchers": dispatchers,
        "credit_window": _i(run_config.get("credit_window")) or None,
        "batch_events": _first_of(per_stage_cfg, "batch_events"),
        "resource_sampling": resource_sampling,
        "typed_dispatch": sorted(set(typed_dispatch)),
        "engine": engine,
        "compile_ms": compile_ms,
        "warmup_ms": warmup_ms,
        "autoscaler": autoscaler,
        "per_stage_cfg": per_stage_cfg,
        "source": _source_flags(llog),
    }


def _first_of(per_stage_cfg: dict, key: str):
    for cfg in per_stage_cfg.values():
        if cfg.get(key) is not None:
            return cfg[key]
    return None


def _stage_from_wasm(path: str) -> str | None:
    m = re.search(r"stage_([A-Za-z0-9_]+)\.(?:cwasm|wasm)$", path)
    return m.group(1) if m else None


def _source_flags(llog: list[dict]) -> dict:
    out = {"kind": None, "profile": None, "count": None, "rate": None,
           "format": None, "entry": None, "sent": None, "dropped": None,
           "anomalies": None}
    for rec in llog:
        msg = rec.get("msg", "")
        if msg == "configuration":
            out.update({
                "kind": "loadgen",
                "profile": rec.get("profile"),
                "count": _i(rec.get("count")) or None,
                "rate": _i(rec.get("rate")) or None,
                "format": rec.get("format"),
                "entry": rec.get("entry_addr"),
            })
        elif msg == "exiting":
            out["sent"] = _i(rec.get("total_sent"))
            out["dropped"] = _i(rec.get("total_dropped"))
            out["anomalies"] = _i(rec.get("total_anomalies"))
        elif msg == "EOS sent" and out["sent"] is None:
            out["sent"] = _i(rec.get("loadgen_sent"))

    # `--rate` is the loadgen's default whatever the profile, but only the
    # rate-paced profiles act on it. Reporting "1000 ev/s" for a tp or pulse
    # run would describe something that never happened.
    prof = (out["profile"] or "")
    if prof.startswith(("pulse", "throughput-max")):
        out["rate"] = None
    return out


# ── replicas over time ───────────────────────────────────────────────────────

def _replicas(summary: dict, nodes: list[dict]) -> tuple[dict, list[dict]]:
    """Step series of live replica count per stage, plus the raw events.

    The autoscaler records `new_count` with every action, so the series is a
    direct step function — no need to replay spawn/drain arithmetic.
    """
    per_stage = (summary.get("scaling") or {}).get("per_stage") or {}
    series: dict[str, list[list[float]]] = {}
    events: list[dict] = []

    for node in nodes:
        name = node["name"]
        raw = (per_stage.get(name) or {}).get("events") or []
        pts: list[list[float]] = [[0.0, 0]]
        for ev in sorted(raw, key=lambda e: _f(e.get("t_s"))):
            t = _f(ev.get("t_s"))
            action = ev.get("action") or ""
            # `init_jit` is a compile milestone, not a replica change; it
            # carries new_count 0 and would stamp a bogus step at t=0.
            if action != "init_jit":
                pts.append([round(t, 4), _i(ev.get("new_count"))])
            events.append({
                "t": round(t, 4), "stage": name, "action": action,
                "new_count": _i(ev.get("new_count")),
                "cold_start_ms": _f(ev.get("cold_start_ms"), None) if ev.get("cold_start_ms") is not None else None,
                "compile_ms": _f(ev.get("compile_ms"), None) if ev.get("compile_ms") is not None else None,
                "instantiate_pre_ms": (_f(ev.get("instantiate_pre_ms"))
                                       if ev.get("instantiate_pre_ms") is not None else None),
            })
        series[name] = pts

    events.sort(key=lambda e: e["t"])
    return series, events


def _load(llog: list[dict], t0: float) -> dict:
    """Offered load over time, from the loadgen's own cumulative counter.

    Resolution is whatever the loadgen logged, and that differs per profile:
    `pulse` and `tp` report once per burst (the producer keeps a local count),
    while the rate-paced profiles also tick a periodic `progress` line every
    2 s. So the cumulative curve is exact at each sample, but the *rate*
    between two samples is only their average — a pulse burst's instantaneous
    rate is far higher than the average across the burst plus its idle gap.
    The page labels it as such rather than passing it off as instantaneous.
    """
    pts: list[tuple[float, int]] = []
    bursts: list[list[float]] = []
    for rec in llog:
        ts = rec.get("ts")
        if ts is None or not t0:
            continue
        msg = rec.get("msg", "")
        t = round(_f(ts) - t0, 4)
        if msg in ("pulse burst complete", "progress"):
            sent = rec.get("sent")
        elif msg == "EOS sent":
            sent = rec.get("loadgen_sent")
        elif msg == "exiting":
            sent = rec.get("total_sent")
        else:
            continue
        if sent is None:
            continue
        if msg == "pulse burst complete":
            bursts.append([t, _i(sent)])
        pts.append((t, _i(sent)))

    # Keep it monotonic: `progress` reports a counter the pulse/tp producer
    # never touches, so it arrives as a stale 0 and would saw-tooth the curve.
    pts.sort()
    cum: list[list[float]] = []
    hi = -1
    for t, s in pts:
        if s > hi:
            cum.append([t, s])
            hi = s
    if not cum:
        return {"cumulative": [], "rate": [], "bursts": [], "samples": 0, "total": None}
    if cum[0][0] > 0:
        cum.insert(0, [0.0, 0])

    # Rate points are stamped at the START of each interval so a step render
    # holds the value across the interval it was measured over.
    rate: list[list[float]] = []
    for (ta, sa), (tb, sb) in zip(cum, cum[1:]):
        dt = tb - ta
        if dt > 0:
            rate.append([ta, round((sb - sa) / dt, 1)])
    if rate:
        rate.append([cum[-1][0], rate[-1][1]])

    # Only show a rate curve when the samples are evenly spaced. On `pulse` the
    # first interval is a burst with no idle after it (~71k ev/s) while the rest
    # span burst+idle (~2.5k ev/s) — same workload, wildly different numbers,
    # purely an artefact of where the sample boundaries fell. Uniform intervals
    # (the periodic `progress` tick) don't have that problem.
    # Judged against the MEDIAN interval rather than min/max, because the last
    # sample is a flush (`exiting`) landing a fraction of a tick after the final
    # periodic one — one short interval that a min/max ratio would fail on even
    # though the cadence is otherwise perfectly regular. A pulse run fails this
    # on its merits: its first interval is a whole burst with no idle, which is
    # a real outlier and a big fraction of only four intervals.
    spans = [round(b[0] - a[0], 4) for a, b in zip(cum, cum[1:])]
    uniform = False
    if len(spans) >= 4 and min(spans) > 0:
        mid = sorted(spans)[len(spans) // 2]
        close = sum(1 for s in spans if 0.75 * mid <= s <= 1.25 * mid)
        uniform = close / len(spans) >= 0.9

    return {
        "cumulative": cum,
        "rate": rate,
        "rate_uniform": uniform,
        "bursts": bursts,
        "samples": len(cum),
        "total": cum[-1][1],
    }


def _queue(summary: dict) -> dict:
    out = {}
    for stage, samples in (summary.get("queue_depth") or {}).items():
        out[stage] = [[round(_f(t), 4), _i(d)] for t, d in samples]
    return out


# ── latency ──────────────────────────────────────────────────────────────────

def _latency(summary: dict) -> dict:
    lat = summary.get("latency_ms") or {}
    e2e = lat.get("e2e") or {}
    per_stage = {}
    for name, st in (lat.get("per_stage") or {}).items():
        per_stage[name] = {k: _f(st.get(k)) for k in
                           ("count", "mean", "p50", "p95", "p99", "p999", "max")}

    inter = []
    for e in (summary.get("inter_stage") or {}).get("edges", []):
        inter.append({"from": e.get("from"), "to": e.get("to"),
                      "count": _i(e.get("count")),
                      **{k: _f(e.get(k)) for k in ("mean", "p50", "p95", "p99", "p999", "max")}})

    ingress = []
    for s in (summary.get("ingress_wait") or {}).get("stages", []):
        ingress.append({"stage": s.get("stage"), "count": _i(s.get("count")),
                        **{k: _f(s.get(k)) for k in ("mean", "p50", "p95", "p99", "p999", "max")}})

    ts_labels = summary.get("lat_ts_labels") or []
    return {
        "e2e": {
            "count": _i(e2e.get("count") or summary.get("e2e_count")),
            **{k: _f(e2e.get(k, summary.get(k)))
               for k in ("min", "mean", "p50", "p90", "p95", "p99", "p999", "max")},
        },
        "cdf": {"x": summary.get("cdf_x") or [], "y": summary.get("cdf_y") or []},
        "hist": {"labels": summary.get("hist_labels") or [],
                 "counts": summary.get("hist_counts") or []},
        "series": {
            "labels": ts_labels,
            "p50": summary.get("lat_ts_p50") or [],
            "p99": summary.get("lat_ts_p99") or [],
        },
        "per_stage": per_stage,
        "inter_stage": inter,
        "ingress_wait": ingress,
        "per_replica": _per_replica(summary),
    }


def _per_replica(summary: dict) -> list[dict]:
    out = []
    for stage, reps in (summary.get("per_replica") or {}).items():
        for rid, st in (reps or {}).items():
            out.append({
                "stage": stage, "replica": rid, "count": _i(st.get("count")),
                **{k: _f(st.get(k)) for k in ("mean", "p50", "p95", "p99", "max")},
            })
    out.sort(key=lambda r: (r["stage"], r["replica"]))
    return out


# ── cold start ───────────────────────────────────────────────────────────────

def _coldstart(summary: dict, mlog: list[dict], t0: float) -> dict:
    """Per-replica boots from the master log, which is richer than the summary.

    The summary's `cold_start_list` is often empty (it only fills on certain
    paths) while every boot always logs `worker booted` with its phase
    breakdown, so the log is the primary source here.
    """
    boots = []
    phases_by_rid: dict[str, dict] = {}

    for rec in mlog:
        if rec.get("msg") == "worker boot phases":
            rid = rec.get("rid")
            if rid:
                phases_by_rid[rid] = {
                    k[3:]: _f(v) for k, v in rec.items() if k.startswith("ph_")
                }

    for rec in mlog:
        if rec.get("msg") != "worker booted":
            continue
        rid = rec.get("rid")
        boots.append({
            "t": round(_f(rec.get("ts")) - t0, 4) if t0 else None,
            "stage": _stage_of(rec.get("component", "")) or rec.get("stage"),
            "replica": _i(rec.get("replica")),
            "rid": rid,
            "cold_start_ms": _f(rec.get("cold_start_ms")),
            "spawn_ms": _f(rec.get("spawn_ms")),
            "instantiate_ms": _f(rec.get("instantiate_ms")),
            "export_ms": _f(rec.get("export_ms")),
            "sockets_ms": _f(rec.get("sockets_ms")),
            "phases": phases_by_rid.get(rid, {}),
        })
    boots.sort(key=lambda b: (b["t"] is None, b["t"]))

    vals = sorted(b["cold_start_ms"] for b in boots if b["cold_start_ms"])
    def pct(p):
        if not vals:
            return 0.0
        return vals[min(len(vals) - 1, int(round(p * (len(vals) - 1))))]

    return {
        "boots": boots,
        "count": len(boots),
        "p50": pct(0.5), "p99": pct(0.99),
        "max": vals[-1] if vals else 0.0,
        "mean": (sum(vals) / len(vals)) if vals else 0.0,
        "first_e2e_ms": _f(summary.get("cold_start_e2e_ms")),
        "milestones": summary.get("cold_start_milestones") or {},
    }


# ── throughput / scatter / counters ──────────────────────────────────────────

def _throughput(summary: dict) -> dict:
    tp = summary.get("throughput") or {}
    return {
        "recv_per_second": [_i(v) for v in (summary.get("recv_per_second") or [])],
        "per_stage_eps": {k: _f(v) for k, v in (tp.get("per_stage_eps") or {}).items()},
        "sustained_eps": _f(tp.get("sustained_eps")),
        "sustained_eps_trimmed": _f(tp.get("sustained_eps_warmup_trimmed")),
        "warmup_trim_s": _f(tp.get("warmup_trim_s")),
    }


def _scatter(summary: dict) -> dict:
    rows = (summary.get("events") or {}).get("rows") or []
    n = len(rows)
    step = max(1, n // SCATTER_POINTS)
    pts = [[round(_f(r[0]), 4), round(_f(r[1]), 4)] for r in rows[::step] if len(r) >= 2]
    return {"points": pts, "total": n, "shown": len(pts),
            "subsample_rate": _i((summary.get("events") or {}).get("subsample_rate"))}


def _counters(summary: dict, topology: dict, llog: list[dict]) -> dict:
    counters = summary.get("counters") or {}
    per_stage = {k: _i(v) for k, v in (counters.get("per_stage_count") or {}).items()}
    received = _i(counters.get("events_received"))

    sent = None
    for rec in llog:
        if rec.get("msg") == "exiting":
            sent = _i(rec.get("total_sent"))
        elif rec.get("msg") == "EOS sent" and sent is None:
            sent = _i(rec.get("loadgen_sent"))

    # Conservation: under broadcast fan-out a stage appears once per path
    # through it, so the expected count is paths-in x paths-out x sent. Only
    # meaningful when we know how many events were offered.
    rows = []
    by_name = {n["name"]: n for n in topology["nodes"]}
    for name in sorted(set(per_stage) | set(by_name)):
        node = by_name.get(name, {})
        expected = (node.get("paths_in", 1) * node.get("paths_out", 1) * sent
                    if sent else None)
        actual = per_stage.get(name)
        rows.append({
            "stage": name,
            "paths_in": node.get("paths_in"),
            "paths_out": node.get("paths_out"),
            "expected": expected,
            "actual": actual,
            "delta": (actual - expected) if (expected is not None and actual is not None) else None,
        })

    expected_recv = topology["total_paths"] * sent if sent else None
    return {
        "sent": sent,
        "received": received,
        "expected_received": expected_recv,
        "conserved": (expected_recv == received) if expected_recv is not None else None,
        "paths": topology["total_paths"],
        "rows": rows,
    }


def _worker_timing(summary: dict) -> dict:
    """Per-stage wasm/serde timing, percentiles only.

    Each block also carries a raw `samples` array — several MB across a
    14-stage run, and nothing on the page plots them. Keeping the scalars
    takes the embedded JSON from ~5.5 MB to a few hundred KB.
    """
    out = {}
    for stage, block in (summary.get("worker_timing") or {}).items():
        entry = {"n": _i(block.get("n"))}
        for key in ("wasm_us", "serde_us", "total_us", "overhead_us"):
            src = block.get(key) or {}
            entry[key] = {k: _f(v) for k, v in src.items() if k != "samples"}
        entry["serde_frac_p50"] = _f(block.get("serde_frac_p50"))
        out[stage] = entry
    return out


def _resources(summary: dict) -> dict:
    res = summary.get("resources") or {}

    def pairs(raw):
        """Samples are `[t_s, value]`; tolerate a bare value list as index-timed."""
        out = []
        for i, s in enumerate(raw or []):
            if isinstance(s, (list, tuple)) and len(s) >= 2:
                out.append([round(_f(s[0]), 3), _f(s[1])])
            else:
                out.append([float(i), _f(s)])
        return out

    return {
        "cpu": pairs(res.get("cpu_pct_samples")),
        "rss": pairs(res.get("rss_mb_samples")),
        "cpu_unit": res.get("cpu_pct_unit"),
        "count": _i(res.get("sample_count")),
    }


# ── timeline markers ─────────────────────────────────────────────────────────

def _markers(mlog: list[dict], llog: list[dict], t0: float) -> list[dict]:
    """Points of interest to pin on the time axis."""
    out = []
    if not t0:
        return out

    for rec in llog:
        msg = rec.get("msg", "")
        t = round(_f(rec.get("ts")) - t0, 4)
        if msg == "pulse burst complete":
            out.append({"t": t, "kind": "burst",
                        "label": f"burst {rec.get('burst')} ({rec.get('sent')} sent)"})
        elif msg == "EOS sent":
            out.append({"t": t, "kind": "eos", "label": "loadgen EOS"})

    for rec in mlog:
        msg = rec.get("msg", "")
        t = round(_f(rec.get("ts")) - t0, 4)
        if msg == "EOS received at collector":
            out.append({"t": t, "kind": "eos", "label": "EOS at collector"})
        elif msg.startswith("SLO breach"):
            stage = _stage_of(rec.get("component", "")) or "?"
            out.append({"t": t, "kind": "slo",
                        "label": f"SLO breach {stage} (qd {rec.get('qd')})"})

    out.sort(key=lambda m: m["t"])
    return out


# ── sanity warnings surfaced at the top of the page ──────────────────────────

def _warnings(model: dict, run: Run) -> list[str]:
    w = []
    f = model["flags"]

    if f["aot_fallbacks"]:
        w.append(
            f"{len(f['aot_fallbacks'])} stage(s) fell back from AOT to JIT at load — "
            "cold-start numbers include Cranelift compile time for those.")
    # `startup` requesting a `jit` load is the normal startup-JIT path, not a
    # mismatch — only an AOT request that did not end up running AOT is one,
    # and if it was a logged fallback the warning above already said so.
    if f["compile_mode_requested"] == "aot" and \
       f["compile_mode_effective"] not in (None, "aot") and not f["aot_fallbacks"]:
        w.append(f"AOT was requested but {f['compile_mode_effective']!r} was used, "
                 "with no fallback logged.")
    if f["cold_start_opt"] is None:
        w.append("cold-start-opt state unknown: this run predates the agent logging "
                 "its build features (it was stderr-only).")
    if model["counters"]["conserved"] is False:
        c = model["counters"]
        w.append(f"event conservation FAILED: received {c['received']}, "
                 f"expected {c['expected_received']} ({c['paths']} paths x {c['sent']} sent).")
    if not model["coldstart"]["boots"] and run.master_log is None:
        w.append("no master.jsonl next to this summary — configuration flags, "
                 "cold-start phases and markers are unavailable.")
    if not model["resources"]["count"]:
        w.append("resource sampling was off for this run (resource_sample_interval_ms: 0).")
    return w
