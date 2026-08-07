# epico-viz — fusion support, config identity, and a visual pass

**Date:** 2026-08-07
**Status:** approved, implementing

## Problem

`epico-viz` renders a run as one self-contained HTML page. M2 gave the runtime
the ability to contract an edge at runtime, and the page has not kept up. Four
concrete failures:

1. **Event conservation is computed wrong on any run that declares a fusible
   pair.** Reproduced on `fusion-chain/logs/run_20260807_081135` (the phase-5
   negative control): `sent 39998, received 39998`, but the page reports
   `expected_received 79996` and `conserved: false`.
2. **The page does not know which config a run executed.** `discover.py`
   hardcodes `pipeline.yaml`, while every M2 run executed a sibling
   (`.morph_test.yaml`, `.controller_test.yaml`, …). So the topology and
   `fusible:` data being parsed can come from the wrong file entirely.
3. **Fusion is nearly invisible.** A morph appears only as a table row and a
   tick on the time charts. Nothing shows which physical topology was in force
   at a given instant, and the Execution DAG always draws the *logical*
   topology.
4. **The visual design has drifted flat** — little type hierarchy, uniform
   section rhythm, dense untreated tables.

## Root cause of (1)

`_topology()` excludes fused stages via `morphs["fused_names"]`, which
`_morphs()` populates only from morphs that **actually fired**. The agent adds
every *declared* fusible product to the summary's `stage_names` so its scaling
block survives a morph. So on a run that declares `fusible: [normalize, detect]`
and never contracts it, `normalize_detect` stays in `stage_names`, enters the
topology as a node with no edges — simultaneously a source and a sink — and
invents a second path. `total_paths` becomes 2 and every expected count doubles.

The existing README section "A fused stage is not a DAG node" describes the
intent correctly; the implementation only realises it for observed morphs.

## Design

### 1. Conservation — exclude on declaration, not observation

The agent already logs, at boot, one line per declared pair:

```json
{"msg": "fusible pair declared", "a": "normalize", "b": "detect", "max_ab": "8"}
```

So the exclusion set is available without the yaml and without a runtime change,
**and this repairs existing runs**.

- New `_fusible_declared(mlog) -> list[{a, b, fused_name, max_ab}]`, where
  `fused_name` is `f"{a}_{b}"` (matching `config::fused_stage_name`).
- `_topology()` excludes `observed_fused_names | declared_fused_names`.
- **Structural backstop**, for runs predating that log line: on a pipeline that
  has edges at all, any stage in `stage_names` with in-degree 0 *and* out-degree
  0 is not a node. This is the general invariant; the declaration is the precise
  signal.

Conservation arithmetic itself (`paths_in × paths_out × sent`) is unchanged and
correct — it was fed a wrong topology, not computing wrongly.

### 2. Config identity — archive it

**Rust.** `epico-cli/src/main.rs` already creates the run directory
(`resolve_run_dir` → `create_dir_all`, ~line 282). Copy in:

- `config.yaml` — the source config verbatim, as passed to `-c`
- `runtime.yaml` — the generated one the agent actually read

Both beside `master.jsonl`, so a run is self-describing forever. `runtime.yaml`
currently lives at `target/epico/runtime.yaml` and is overwritten by the next
run, which is why the identity is unrecoverable today.

**Page.** A `CONFIG` row in the Run section: filename, `package:`, and a
collapsible verbatim source.

**Degradation.** A run without `config.yaml` says *"config not archived (run
predates it)"* and falls back to `pipeline.yaml` **with that stated**. No
inference from ingress URIs or package names — a wrong guess is worse than an
absent answer, and the user explicitly chose archival over matching.

### 3. Topology lane + contracting DAG

**Epochs.** The summary's scaling events bracket every transition
(`morph_begin` … verb … `morph_end`). Derive an ordered list of topology
epochs: `{t_from, t_to, fused: [[a, b], …], label}` where the label renders as
`A>B>C>D` / `A>[BC]>D`.

**Lane.** One row appended to the run tape, sharing its clock, playhead and
scrub handling — no new interaction model. Segment boundaries at `morph_end`;
each segment labelled with its topology.

**DAG.** The Execution DAG becomes scrub-aware: at an instant inside a fused
epoch, the pair renders as one module with an internal divider, and the
contracted edge renders collapsed carrying its live gap.

Rationale for tying this to the existing scrubber rather than a standalone
view: every other time series on the page already shares that clock, so a fuse
lines up by eye with the throughput step it caused. That alignment is the whole
argument.

### 4. Fuse behaviour box

Framed as **declared vs actual**, because that is the question the yaml poses.

| Group | Contents |
|---|---|
| Declared | pairs, `max_ab`, and either the `morphs:` schedule or the `controller:` block |
| Actual | every transition: verb, `t_s`, switch ms, `R_ab`, ok/failed |
| Effect | contracted-edge gap before → after; per-half attribution while fused |
| Verdict | *declared, never contracted* / *contracted N×* / *refused* |

Renders whenever the run **declared** a pair — including when nothing fired,
which is exactly the negative-control case that today renders as a silent
phantom node.

### 5. Visual pass

Within the existing tokens and bench-instrument identity:

- a real type scale (currently near-flat)
- section rhythm and vertical spacing scale
- table density, zebra, aligned numerics
- plate/card elevation
- a denser primary readout cluster
- honest empty and degraded states

`TOKENS` in `page.py` remains the single source generating both CSS custom
properties and the JS the SVG draws with, so chrome and chart colours cannot
drift. **Out of scope:** palette change, light mode, chart-mark rewrites — the
charts are what would regress.

## Explicitly out of scope

- Best-effort matching of historical runs to their source yaml (archival was
  chosen instead; old runs get an honest "not recorded")
- Any change to how charts are drawn

## Testing

- **Conservation:** assert `conserved` and `paths == 1` across every existing
  `fusion-chain` run, including the four negative-control runs that fail today,
  and assert the nine currently-correct runs are unchanged.
- **Regression:** build the model for every run of every pipeline (`dag-nested`,
  `dag-diamond`, `two-stage-min`, `three-stage-test`, `fusion-chain`) and assert
  no exception and no change in `expected_received` where it was already right.
- **Degradation:** a run with no archived config still renders, with the note.
- **Archival:** after the Rust change, a fresh run writes both files and the page
  shows the real filename.

Rendering is checked by generating pages and inspecting them, since output is
HTML — no assertion framework exists for this tool and adding one is out of
scope.
