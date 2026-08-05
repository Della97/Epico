# epico-viz

Pick a pipeline, pick a run, get a page. Reads what a run already wrote to
`logs/` and renders the whole execution as one self-contained HTML page served
on localhost.

Not tracked by git (`/epico-viz/` is in `.gitignore`) and not part of the cargo
workspace — it is a local tool, Python 3 standard library only, no install step.

```bash
./epico-viz                     # interactive: choose a pipeline, then a run
./epico-viz -p dag-nested       # skip the first prompt
./epico-viz -p dag-nested -r 0  # newest run of it, no prompts at all
./epico-viz --list              # show every pipeline and its runs, then exit
```

Useful flags: `--port` (default 8777), `--no-open` (don't launch a browser),
`--no-serve` (write the file and exit), `--out DIR` (default `epico-viz/out/`).

## What's on the page

- **The run tape** — the hero, and the scrubber. One row per stage for the whole
  run: fill intensity is replica occupancy, the hairline over it is cumulative
  offered load, and dashed rules mark loadgen bursts, EOS and SLO breaches.
  Click or drag anywhere on it to move the run clock. Scroll past it and it
  reappears as a **sticky transport rail** at the top of the window, so the
  clock is reachable from any section. Space plays, `←`/`→` step, `Home`
  rewinds.
- **Run** — four primary readouts (events at the collector, duration, e2e
  p50/p99) over a spec plate with the totals and the machine, toolchain and git
  commit the run came from.
- **Configuration** — compile mode (AOT / startup-JIT / cold-start JIT, *and*
  what actually ran if an AOT artifact fell back), cold-start-opt, edge
  transport and ring capacity, ingress/egress mode, dispatchers, credit window,
  batch size, typed dispatch, resource sampling, and the source profile.
- **Execution** — the DAG at the scrubbed instant. Each node is a stage module:
  a segment meter counting live replicas against its ceiling, and a bar for
  queue depth against this run's peak. Edges carry their transport and ring
  geometry on hover. Under it, **offered load** and, separately, **offered
  rate**. Every chart on the run clock shares one plot area and one playhead, so
  a burst lines up by eye with the scale-up it triggered.

  The offered-load curve is cumulative, because that is what the loadgen
  actually records — a counter, logged once per burst on `pulse`/`tp` and every
  2 s on the rate-paced profiles. Its slope is the offered rate. The rate panel
  appears **only when those samples are evenly spaced**: on `pulse` the first
  interval is a burst with no idle after it (~71k ev/s) while the rest span
  burst+idle (~2.5k ev/s), so an average between samples would describe where
  the sample boundaries fell rather than how fast the burst ran.
- **Backpressure** — queue depth as small multiples, one panel per stage on a
  shared scale, all sharing the run clock and the playhead.
- **Latency** (CDF, histogram, per-event scatter over time, per-stage p50→p99
  residency, inter-stage edges, per-replica, ingress wait), **cold start**
  (every worker boot over time plus its spawn/instantiate/export/sockets
  phases), **throughput** (collector rate, agent CPU and resident memory as
  three separate panels), **scaling events**, **event conservation**, and
  **worker timing**.

Append `#t=2.5` to the URL to open at a specific instant; scrubbing keeps the
hash in sync, so a particular moment can be linked. Without a hash the page
opens at the first instant the pipeline is carrying as many replicas as it ever
will — at `t=0` the autoscaler has not acted yet and every stage would read 0.

## How it's drawn

A bench-instrument readout rather than a dashboard: a warm graphite housing, one
cool readout hue for data, and reserved status lamps that always ship with a
word next to them. Stage identity is carried by position and label — the gutter
every time chart shares — which frees colour to encode magnitude instead of
naming things. Series that genuinely overlap are faceted into small multiples
rather than given a hue each, and no chart carries two y-scales.

The palette lives in one place, `TOKENS` in `page.py`, and generates both the
CSS custom properties and the JS the SVG draws with, so a colour cannot drift
between the page chrome and the charts.

## Where the data comes from

| Source | What it contributes |
|---|---|
| `master_summary.json` | distributions, scaling events, queue depth, counters, throughput, environment |
| `master.jsonl` | the run's *configuration* — transport, compile mode actually used, per-stage autoscaler thresholds, per-replica cold-start phases, flags |
| `loadgen.jsonl` | offered count and profile, burst timings |
| `pipeline.yaml` | topology, when the run used the ZMQ spine and logged no in-process edges |

Both log layouts are read: the current `logs/run_<ts>/` folders and the older
flat `logs/master_<ts>_summary.json`. Anything missing degrades to "—" or an
explicit note at the top of the page rather than failing — old runs predate some
of these fields.

**Event conservation** is computed, not read: under broadcast fan-out a stage is
traversed once per path through it, so the expected per-stage count is
`paths_in × paths_out × offered`, exactly the identity `tests/no_leak_dag*.sh`
assert. It only appears when the loadgen log says how many events were offered.

## Layout

```
epico-viz/
├── epico-viz            # the CLI (executable)
├── epico_viz/
│   ├── discover.py      # find pipelines and their runs, both log layouts
│   ├── model.py         # summary + logs -> one JSON model
│   └── page.py          # model -> self-contained HTML (inline CSS/SVG/JS)
└── out/<pipeline>__<run>/index.html
```

The generated page has no external references, so `out/.../index.html` also
opens straight off disk.

## Note on `cold-start-opt`

It is a cargo feature baked into the agent at `epico bootstrap --cold-start-opt`
time, and it used to be announced only on stderr — which no log file keeps. The
agent now logs it (`build features` in `master.jsonl`), so it shows on the page
for runs made after that change; older runs show `unknown` and say so in the
notes.
