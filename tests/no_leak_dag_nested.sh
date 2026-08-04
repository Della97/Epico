#!/usr/bin/env bash
# no_leak_dag_nested.sh — event conservation on a NESTED fan-out / fan-in DAG.
#
# The diamond (tests/no_leak_dag.sh) covers one fan-out and one fan-in. This
# runs examples/dag-nested, where they are nested and interleaved: fan-outs
# inside fan-out branches, a fan-in of fan-ins, branches of unequal depth
# meeting at one barrier, one stage feeding two different joins, and a stage
# that is both fan-in and fan-out. See the topology drawing in the example's
# pipeline.yaml.
#
# Under broadcast semantics each stage forwards a copy on EVERY out-edge, so
# the exact number of events the collector must see is the number of distinct
# source→sink PATHS, and the number of times a stage appears across those
# paths is P(v)·D(v) — paths into it times paths out of it. Both are derived
# here from `edges:` in the YAML, so the assertions follow the topology
# instead of a hardcoded table that rots the moment someone adds an edge.
#
# Every count is checked for equality in both directions: short means a branch
# leaked, long means something was duplicated on a path that should not have
# carried it.
#
# Runs examples/dag-nested/.leak_test.yaml — the same topology as that
# example's pipeline.yaml, but sourced by `pulse` instead of `wave`. Only the
# `pulse` and `tp` profiles send an exactly known number of events and then emit
# EOS; a rate-paced profile never emits one, so there would be no moment at
# which every event is known to have flowed through, which is exactly what this
# test has to know.
#
# Usage:  tests/no_leak_dag_nested.sh              # spsc + mpmc
#         TRANSPORTS=mpmc tests/no_leak_dag_nested.sh
set -u

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
EXAMPLE="$ROOT/examples/dag-nested"
YAML="$EXAMPLE/.leak_test.yaml"      # pulse-sourced twin of pipeline.yaml
TRANSPORTS="${TRANSPORTS:-spsc mpmc}"
TIMEOUT_S="${TIMEOUT_S:-480}"

echo "==> building release binaries"
(cd "$ROOT" && cargo build --release --workspace) || exit 1
EPICO="$ROOT/target/release/epico"

run_with_timeout() {
    local secs=$1; shift
    "$@" &
    local pid=$! t=0
    while kill -0 "$pid" 2>/dev/null; do
        sleep 1; t=$((t + 1))
        if [ "$t" -ge "$secs" ]; then
            echo "    TIMEOUT after ${secs}s — killing run"
            kill -INT "$pid" 2>/dev/null; sleep 3
            kill -9 "$pid" 2>/dev/null
            return 124
        fi
    done
    wait "$pid"
}

overall=0
for impl in $TRANSPORTS; do
    echo
    echo "=== transport: $impl ==="
    stamp="$(mktemp)"
    (
        cd "$EXAMPLE" || exit 1
        export EPICO_EDGE_IMPL="$impl"
        run_with_timeout "$TIMEOUT_S" "$EPICO" run -c .leak_test.yaml
    )
    rc=$?
    [ "$rc" -ne 0 ] && echo "    (epico run exited rc=$rc; checking summary anyway)"

    summary="$(find "$EXAMPLE/logs" -name 'master*summary.json' -newer "$stamp" 2>/dev/null | sort | tail -1)"
    rm -f "$stamp"
    if [ -z "$summary" ]; then
        echo "    FAIL: no summary produced by this run"
        overall=1
        continue
    fi
    echo "    summary: $summary"

    python3 - "$summary" "$YAML" <<'PYEOF'
import json, re, sys
from collections import defaultdict

summary_path, yaml_path = sys.argv[1], sys.argv[2]
text = open(yaml_path).read()

# ── topology + load, straight out of the YAML (no PyYAML dependency) ────────
# `edges:` entries are the "- a -> b" shorthand the CLI parses; comments and
# blank lines are ignored, and the block ends at the next top-level key.
sent = int(re.search(r'^\s*count:\s*(\d+)', text, re.M).group(1))
edges, in_block = [], False
for line in text.splitlines():
    stripped = line.split('#', 1)[0].rstrip()
    if not stripped:
        continue
    if re.match(r'^edges:\s*$', stripped):
        in_block = True
        continue
    if in_block:
        m = re.match(r'^\s*-\s*(\S+)\s*->\s*(\S+)\s*$', stripped)
        if m:
            edges.append((m.group(1), m.group(2)))
        elif not stripped.startswith((' ', '\t', '-')):
            break                                   # next top-level key
if not edges:
    print("    FAIL: no edges parsed from pipeline.yaml")
    sys.exit(1)

succ, pred = defaultdict(list), defaultdict(list)
for a, b in edges:
    succ[a].append(b)
    pred[b].append(a)
nodes = set(succ) | set(pred)
sources = [n for n in nodes if not pred[n]]
sinks   = [n for n in nodes if not succ[n]]

# Path counts, memoised over the DAG. P(v) = distinct paths from the source
# into v (how many copies of each sent event v processes); D(v) = distinct
# paths from v to a sink (how many collector events carry v in their hop
# chain, per event v processes). per_stage_count is the product.
def count(node, adj, seed, memo):
    if node in memo:
        return memo[node]
    memo[node] = seed(node) if not adj[node] else sum(
        count(n, adj, seed, memo) for n in adj[node])
    return memo[node]

P = {n: count(n, pred, lambda _: 1, {}) for n in nodes}
D = {n: count(n, succ, lambda _: 1, {}) for n in nodes}
expected_recv = sum(P[s] for s in sinks) * sent

d = json.load(open(summary_path))
received  = d["counters"]["events_received"]
per_stage = d["counters"].get("per_stage_count", {})
scaling   = d.get("scaling", {}).get("per_stage", {})

print(f"    sources={sorted(sources)}  sinks={sorted(sinks)}  stages={len(nodes)}")
print(f"    sent={sent}  paths={expected_recv // sent}  "
      f"expected={expected_recv}  received={received}")

fail = False
if len(sources) != 1:
    print(f"    FAIL: expected exactly 1 source stage, found {sorted(sources)}")
    fail = True
# One sink keeps EOS unambiguous: the collector shuts the run down on the
# first marker it sees, so a second terminal stage would end the run early.
if len(sinks) != 1:
    print(f"    FAIL: expected exactly 1 sink stage, found {sorted(sinks)}")
    fail = True

print(f"    {'stage':<11} {'P':>3} {'D':>3} {'expected':>9} {'actual':>9}  ups/downs")
for name in sorted(nodes):
    want = P[name] * D[name] * sent
    got  = per_stage.get(name)
    sc   = scaling.get(name, {})
    mark = ""
    if got is None:
        mark, fail = "  MISSING", True
    elif got != want:
        mark, fail = f"  FAIL ({got - want:+d})", True
    print(f"    {name:<11} {P[name]:>3} {D[name]:>3} {want:>9} "
          f"{got if got is not None else '-':>9}  "
          f"{sc.get('scale_up_count', 0)}/{sc.get('scale_down_count', 0)}{mark}")

if received != expected_recv:
    delta = expected_recv - received
    verb = "leaked" if delta > 0 else "duplicated"
    print(f"    FAIL: {abs(delta)} events {verb} "
          f"(received {received}, expected {expected_recv})")
    fail = True

sys.exit(1 if fail else 0)
PYEOF
    [ $? -ne 0 ] && overall=1 || echo "    PASS: nested DAG conserved exactly on every path"
done

echo
[ "$overall" -eq 0 ] && echo "ALL TRANSPORTS PASS" || echo "FAILURES DETECTED"
exit "$overall"
