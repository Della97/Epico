#!/usr/bin/env python3
"""Calibrate the cost-model controller's significance floor (roadmap M2, phase 5e).

    bench/calibrate_controller.py --edge normalize detect \
        examples/fusion-chain/logs/run_*/master_summary.json

Prints the `significance_floor_us` to put in a pipeline's `controller:` block.

Why this exists
---------------
The controller refuses to act when the per-event gain a fusion would deliver is
below a floor. That floor cannot be a constant, for a reason RESULTS.md records
as C7: break-even goes as `S^2 / (2 * delta)`, so a near-zero `delta` drives the
estimate toward zero and produces confident-looking nonsense. The identity
control — a stage redeployed UNCHANGED, which by construction gains exactly
nothing — produced *finite* break-even in 13 of 20 morphs, at a p50 of 0.022 s.
An order of magnitude "better" than a real fuse's 0.204 s, from a morph that did
nothing at all.

So the floor is a noise measurement, and noise scales with the workload: the
phase-4 arms differed roughly fivefold between the uniform and skewed
configurations. It has to be measured on the deployed build, under the workload
that build will actually see.

What it measures
----------------
The identity arm is the whole trick. An `identity` morph tears a stage down and
brings it back with the SAME topology, so any apparent change in the contracted
edge's per-event gap across it is measurement noise and nothing else. This
script takes the per-event `inter_stage` gap in the window before each identity
morph and in the window after, and treats the difference between them as one
noise sample. The floor is the upper tail of those samples: a gain smaller than
the largest thing an identity morph "achieved" is not evidence.

Runs containing no identity morph are skipped with a warning — a floor derived
from fuse morphs would fold the real gain into the noise estimate and produce a
floor that suppresses every genuine decision.

How to run the calibration
--------------------------
    # 10 identity morphs under the workload the deployment will see
    bench/morph_bench.sh                 # or any config whose `morphs:` block
                                         # is identity-only
    bench/calibrate_controller.py --edge <a> <b> logs/run_*/master_summary.json

Then put the printed value in the pipeline's `controller:` block. The agent logs
the floor in force at boot and repeats it in every refusal it causes, so a
decision can always be traced back to the number that produced it.
"""

from __future__ import annotations

import argparse
import statistics
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from analyze_morph import (  # noqa: E402
    WINDOW_S,
    edge_gaps_ms,
    find_morphs,
    load_rows,
    pct,
    window,
)

import json  # noqa: E402


#: Events needed on EACH side of a morph before its window is worth a median.
MIN_PER_SIDE = 20


def noise_samples(path: Path, edge: tuple[str, str]) -> tuple[list[float], int, int]:
    """Per-identity-morph |post - pre| median-gap deltas, in microseconds.

    Returns `(samples, n_identity, n_too_sparse)`. The delta is taken between
    MEDIANS rather than between tails: the floor is about the typical gain a
    decision would be sized on, and the controller's significance test reads p50
    for the same reason — sizing a decision off the tail is how a controller
    talks itself into acting on an outlier.

    A morph whose surrounding windows are too sparse is counted separately
    rather than lumped in with "no identity morph found". The two call for
    opposite fixes and are easy to confuse: the first means the config's
    `morphs:` block is wrong, the second means the morph landed in an idle
    stretch of a bursty workload and the source needs to be sustained.
    """
    summary = json.loads(path.read_text())
    rows = load_rows(summary)
    if not rows:
        return ([], 0, 0)

    frm, to = edge
    out: list[float] = []
    n_identity = 0
    n_sparse = 0
    for m in find_morphs(summary):
        if m["verb"] != "identity":
            continue
        n_identity += 1
        t_begin, t_end = m["t_begin"], m["t_end"]
        pre = edge_gaps_ms(window(rows, t_begin - WINDOW_S, t_begin), frm, to)
        post = edge_gaps_ms(window(rows, t_end, t_end + WINDOW_S), frm, to)
        if len(pre) < MIN_PER_SIDE or len(post) < MIN_PER_SIDE:
            n_sparse += 1
            continue
        delta_us = abs(pct(post, 0.50) - pct(pre, 0.50)) * 1000.0
        out.append(delta_us)
    return (out, n_identity, n_sparse)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("summaries", nargs="+", type=Path)
    ap.add_argument("--edge", nargs=2, metavar=("FROM", "TO"), required=True,
                    help="the fusible edge to calibrate against, e.g. "
                         "--edge normalize detect")
    ap.add_argument("--quantile", type=float, default=0.95,
                    help="upper tail of the noise distribution to take as the "
                         "floor (default 0.95: a gain smaller than 19 of 20 "
                         "identity morphs' apparent gains is not evidence)")
    ap.add_argument("--margin", type=float, default=2.0,
                    help="multiplier applied to that quantile (default 2.0). "
                         "The cost of a floor that is too high is a missed "
                         "optimisation; the cost of one that is too low is a "
                         "controller that morphs on noise, which is strictly "
                         "worse than no controller")
    args = ap.parse_args()

    all_samples: list[float] = []
    total_identity = 0
    total_sparse = 0
    skipped: list[Path] = []
    for p in args.summaries:
        try:
            s, n, sparse = noise_samples(p, tuple(args.edge))
        except Exception as e:                                  # noqa: BLE001
            print(f"  ! {p}: {e}", file=sys.stderr)
            continue
        total_identity += n
        total_sparse += sparse
        if n == 0:
            skipped.append(p)
        all_samples.extend(s)

    if skipped:
        print(f"note: {len(skipped)} summary/summaries contained no identity morph "
              f"and were skipped. A floor derived from fuse morphs would fold the "
              f"real gain into the noise estimate.", file=sys.stderr)

    if not all_samples:
        if total_sparse:
            print(f"found {total_sparse} identity morph(s), but every one had fewer "
                  f"than {MIN_PER_SIDE} events on the {args.edge[0]} -> "
                  f"{args.edge[1]} edge within {WINDOW_S:g}s on one side.\n"
                  f"The morphs landed in idle stretches: calibrate under a "
                  f"SUSTAINED source (`profile: steady`), not a bursty one, so "
                  f"traffic is flowing on both sides of every transition.",
                  file=sys.stderr)
        else:
            print("no identity morphs found.\n"
                  "Run a config whose `morphs:` block is identity-only, under the "
                  "workload the deployment will see, then re-run this.",
                  file=sys.stderr)
        return 1

    edge = f"{args.edge[0]} -> {args.edge[1]}"
    q = pct(all_samples, args.quantile)
    floor = q * args.margin

    print(f"edge:                {edge}")
    print(f"identity morphs:     {total_identity} found, {len(all_samples)} usable")
    print(f"apparent gain (us):  "
          f"min {min(all_samples):.1f}  "
          f"p50 {pct(all_samples, 0.50):.1f}  "
          f"p95 {pct(all_samples, 0.95):.1f}  "
          f"max {max(all_samples):.1f}")
    if len(all_samples) > 1:
        print(f"                     stdev {statistics.stdev(all_samples):.1f}")
    print()
    print(f"  q{args.quantile:.2f} = {q:.1f} us, x{args.margin:g} margin")
    print()
    print("Put this in the pipeline's `controller:` block:")
    print()
    print("controller:")
    print("  enabled: true")
    print(f"  significance_floor_us: {floor:.0f}")
    print()
    if len(all_samples) < 10:
        print(f"warning: only {len(all_samples)} samples. The plan asks for N >= 10 "
              f"identity morphs; a floor from fewer is a guess with a decimal point.",
              file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
