#!/usr/bin/env bash
# controller_finds_the_fuse.sh — the cost-model controller deciding for itself
# (roadmap M2, phase 5d).
#
# Every M2 test before this one drove the actuator from a scripted `morphs:`
# block at KNOWN instants, because that is what makes switch cost and break-even
# measurable. This is the opposite arm: the schedule is REMOVED and the
# controller enabled, so nothing tells the runtime what to do or when. It has to
# read its own per-edge statistics, conclude that `normalize -> detect` is
# edge-dominated and under pressure, and issue the fuse itself.
#
# Four things are asserted, and each fails differently:
#
#   1. the controller armed at all                  (config plumbed through)
#   2. it proposed exactly one fuse, of the right   (the decision rule works)
#      pair, with an auditable reason
#   3. the actuator applied it                      (the request is the same
#                                                    one a schedule emits)
#   4. conservation still holds exactly             (a controller-driven morph
#                                                    is not a special case)
#
# Plus a sanity cross-check that the live per-edge statistic the decision was
# made on agrees, in order of magnitude, with the summary's whole-run
# `inter_stage` for the same edge — which is what catches a units or derivation
# error in `telemetry::live` that would otherwise silently drive every decision.
#
# Usage:  tests/controller_finds_the_fuse.sh
#         TRANSPORTS=mpmc tests/controller_finds_the_fuse.sh
set -u

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
EXAMPLE="$ROOT/examples/fusion-chain"
YAML=".controller_test.yaml"
EXPECTED=3000000              # must match `count:` in .controller_test.yaml
TRANSPORTS="${TRANSPORTS:-spsc}"
TIMEOUT_S="${TIMEOUT_S:-300}"

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
    runlog="$(mktemp)"
    sleep 1
    (
        cd "$EXAMPLE" || exit 1
        export EPICO_EDGE_IMPL="$impl"
        export EPICO_DYNAMIC_DISPATCH=1
        run_with_timeout "$TIMEOUT_S" "$EPICO" run -c "$YAML"
    ) 2>&1 | tee "$runlog"
    rc=${PIPESTATUS[0]}
    [ "$rc" -ne 0 ] && echo "    (epico run exited rc=$rc; checking anyway)"

    summary="$(find "$EXAMPLE/logs" -name 'master*summary.json' -newer "$stamp" 2>/dev/null | sort | tail -1)"
    rm -f "$stamp"
    if [ -z "$summary" ]; then
        echo "    FAIL: no summary produced by this run"
        rm -f "$runlog"
        overall=1
        continue
    fi
    echo "    summary: $summary"

    python3 - "$summary" "$runlog" "$EXPECTED" <<'PYEOF'
import json, re, sys

summary_path, runlog_path, expected = sys.argv[1], sys.argv[2], int(sys.argv[3])
d = json.load(open(summary_path))
log = open(runlog_path, errors="replace").read()

fail = False

# ── 1. The controller armed ──────────────────────────────────────────────
armed = re.search(r"cost-model controller armed(.*)", log)
if not armed:
    print("    FAIL: the controller never armed — the `controller:` block did not "
          "reach the agent (check the CLI's runtime.yaml emission)")
    fail = True
else:
    print(f"    armed: {armed.group(1).strip()}")

# ── 2. It proposed a fuse, on its own, with a reason ─────────────────────
proposals = re.findall(r"controller proposing a morph\s+decision=(.*)", log)
print(f"    proposals ({len(proposals)}):")
for p in proposals:
    print(f"      {p.strip()}")

fuses = [p for p in proposals if p.strip().startswith("fuse ")]
if len(fuses) != 1:
    print(f"    FAIL: expected exactly 1 fuse proposal, got {len(fuses)} — "
          f"0 means the rule never fired on an edge that plainly dominates; "
          f">1 means hysteresis is not holding")
    fail = True
elif "normalize->detect" not in fuses[0]:
    print(f"    FAIL: fused the wrong pair: {fuses[0].strip()}")
    fail = True

# The reason has to carry the evidence, or a decision cannot be audited after
# the fact — which is the whole point of separating the verdict from the rule.
if fuses:
    for token in ("edge p50", "compute p50", "p99s", "gain", "ev/s", "queue", "pool"):
        if token not in fuses[0]:
            print(f"    FAIL: the proposal's reason omits {token!r}: {fuses[0].strip()}")
            fail = True

# ── 3. The actuator applied it ───────────────────────────────────────────
scaling = d.get("scaling", {}).get("per_stage", {})
applied = sorted(
    (e["t_s"], stage, e["action"])
    for stage, blk in scaling.items()
    for e in blk.get("events", [])
    if e["action"] in ("fuse", "split", "identity")
)
print(f"    morphs applied ({len(applied)}):")
for t_s, stage, action in applied:
    print(f"      t={t_s:7.3f}s  {action:<9} -> {stage}")

if [a for _, _, a in applied] != ["fuse"]:
    print(f"    FAIL: expected exactly one applied `fuse`, got "
          f"{[a for _, _, a in applied]}")
    fail = True

if any(e["action"] == "morph_failed"
       for blk in scaling.values() for e in blk.get("events", [])):
    print("    FAIL: a morph was refused or aborted")
    fail = True

# ── 4. Conservation across a controller-driven morph ─────────────────────
received = d["counters"]["events_received"]
print(f"    sent={expected}  received={received}")
if received != expected:
    delta = expected - received
    verb = "lost" if delta > 0 else "duplicated"
    print(f"    FAIL: {abs(delta)} events {verb} across the switch boundary")
    fail = True

# ── 5. Live stats vs the summary, in order of magnitude ──────────────────
# The point of this check is to catch a units or derivation error in
# telemetry::live — a live figure reported in the wrong unit, or stuck at zero,
# would drive every decision downstream of it while looking perfectly plausible
# in isolation.
#
# It is done at p99, NOT at p50, and the reason is worth stating: the fuse lands
# about two seconds into a twelve-second run, so the overwhelming majority of
# events cross a CONTRACTED edge and the summary's whole-run median for this
# edge is the post-fusion ~1 us. The controller measured the two seconds BEFORE
# that. Only the tail still holds a comparable sample of the unfused regime — so
# p99 is the one quantile these two windows can honestly be compared at, and a
# p50 comparison here would be measuring the morph rather than the telemetry.
m50 = re.search(r"edge p50 (\d+(?:\.\d+)?)us", fuses[0]) if fuses else None
m99 = re.search(r"p99s (\d+(?:\.\d+)?)us", fuses[0]) if fuses else None
edges = {(e["from"], e["to"]): e for e in d.get("inter_stage", {}).get("edges", [])}
summary_edge = edges.get(("normalize", "detect"))
if m99 and summary_edge:
    live_us = float(m99.group(1))
    summary_us = summary_edge["p99"] * 1000.0
    ratio = live_us / summary_us if summary_us > 0 else float("inf")
    if m50:
        print(f"    live edge p50 {float(m50.group(1)):.0f}us "
              f"(summary whole-run p50 {summary_edge['p50'] * 1000.0:.0f}us — "
              f"post-fusion, not comparable)")
    print(f"    live edge p99 {live_us:.0f}us vs summary inter_stage p99 "
          f"{summary_us:.0f}us (ratio {ratio:.2f})")
    if not (0.2 < ratio < 5.0):
        print(f"    FAIL: live per-edge statistic disagrees with the summary by "
              f"{ratio:.1f}x — suspect the fold or the unit conversion in "
              f"telemetry::live, not the controller")
        fail = True
elif fuses:
    print("    (no normalize->detect edge in the summary to cross-check against)")

sys.exit(1 if fail else 0)
PYEOF
    if [ $? -ne 0 ]; then
        overall=1
    else
        echo "    PASS: the controller found the fuse on its own"
    fi
    rm -f "$runlog"
done

echo
if [ "$overall" -eq 0 ]; then
    echo "ALL TRANSPORTS PASS"
else
    echo "FAILURES — see above"
fi
exit "$overall"
