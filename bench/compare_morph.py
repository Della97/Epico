#!/usr/bin/env python3
"""Compare two morph result sets — e.g. cold-start-opt off vs on.

    bench/compare_morph.py OLD.json NEW.json [--labels off,on]

`bench/analyze_morph.py` answers "what did this configuration do". This answers
"did changing the configuration move anything", which is a different question
and needs the control to stay in frame: a shift that appears in `identity` too
is a shift in the harness or the machine, not in the mechanism under test.

Reads the `--json` output of analyze_morph.py, so it inherits that script's
definitions of S and break-even verbatim.
"""

from __future__ import annotations

import argparse
import json
import math
import statistics as st
import sys
from pathlib import Path

METRICS = [
    ("s_control_ms", "S_control ms", "lower"),
    ("s_gap_ms",     "S_gap ms",     "lower"),
    ("s_e2e_ms",     "S_e2e ms",     "lower"),
    ("breakeven_s",  "break-even s", "lower"),
]


def by_verb(path: Path) -> dict[str, list[dict]]:
    out: dict[str, list[dict]] = {}
    for run in json.load(open(path)):
        for m in run["morphs"]:
            if m.get("ok"):
                out.setdefault(m["verb"], []).append(m)
    return out


def finite(ms: list[dict], key: str) -> list[float]:
    return [m[key] for m in ms
            if isinstance(m.get(key), (int, float))
            and not math.isnan(m[key]) and not math.isinf(m[key])]


def med(xs: list[float]) -> float:
    return st.median(xs) if xs else float("nan")


def f(x: float, nd: int = 2) -> str:
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return "n/a"
    return f"{x:.{nd}f}"


def delta_str(a: float, b: float) -> str:
    """Percent change b vs a, with the sign that means 'got worse' spelled out."""
    if math.isnan(a) or math.isnan(b) or a == 0:
        return "     n/a"
    pct = 100.0 * (b - a) / abs(a)
    return f"{pct:+7.1f}%"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("old", type=Path)
    ap.add_argument("new", type=Path)
    ap.add_argument("--labels", default="old,new")
    args = ap.parse_args()

    la, lb = (args.labels.split(",") + ["new"])[:2]
    for p in (args.old, args.new):
        if not p.exists():
            print(f"missing {p}", file=sys.stderr)
            return 1

    A, B = by_verb(args.old), by_verb(args.new)

    print(f"\n{'':<14} {la:>12} {lb:>12}   {'change':>8}   n")
    for verb in ("identity", "fuse", "split"):
        if verb not in A and verb not in B:
            continue
        na, nb = len(A.get(verb, [])), len(B.get(verb, []))
        tag = "  (CONTROL — a shift here is the harness, not the mechanism)" \
              if verb == "identity" else ""
        print(f"\n  {verb.upper()}{tag}")
        for key, label, _ in METRICS:
            xa, xb = finite(A.get(verb, []), key), finite(B.get(verb, []), key)
            ma, mb = med(xa), med(xb)
            print(f"    {label:<14} {f(ma):>10} {f(mb):>12}   {delta_str(ma, mb):>8}"
                  f"   {na}->{nb}")

        # Throughput gain is the mechanism's own claim; keep it visible.
        for src, lbl in ((A.get(verb, []), la), (B.get(verb, []), lb)):
            g = [100.0 * (m["lambda_post_eps"] - m["lambda_pre_eps"]) / m["lambda_pre_eps"]
                 for m in src if m.get("lambda_pre_eps")]
            if g:
                up = sum(1 for x in g if x > 0)
                print(f"    {'d throughput':<14} {lbl:>10}: {st.mean(g):+6.1f}%"
                      f"  ({up}/{len(g)} positive)")

    # The contracted edge is the mechanism evidence — if it did not move, any
    # apparent break-even change came from somewhere else.
    print("\n  contracted edge p50 (ms), fuse only")
    for src, lbl in ((A.get("fuse", []), la), (B.get("fuse", []), lb)):
        pre = [m["edge_gap_pre_p50_ms"] for m in src
               if isinstance(m.get("edge_gap_pre_p50_ms"), float)
               and not math.isnan(m["edge_gap_pre_p50_ms"])]
        post = [m["edge_gap_post_p50_ms"] for m in src
                if isinstance(m.get("edge_gap_post_p50_ms"), float)
                and not math.isnan(m["edge_gap_post_p50_ms"])]
        if pre and post:
            print(f"    {lbl:<10} {med(pre):.3f} -> {med(post):.3f}")
    print()
    return 0


if __name__ == "__main__":
    sys.exit(main())
