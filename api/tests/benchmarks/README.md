# EDGIS route-plotter benchmarks

Two complementary benchmarks.

## 1. Quality benchmark (jump counts vs spansh): `run_plotter_bench.py`

Re-plots a curated set of routes (`route_cases.json`, mined from production
`route_trace.jsonl`) and compares each to:
- **our stored baseline** (`our`): catches regressions/improvements in jump count or solvability.
- **spansh** (`spansh`): a static number from the same trace, so this benchmark **never calls spansh** (respects the go-easy-on-spansh rule).

The cases span the route classes we care about: flagship wins (Sol->Colonia 143 vs 166),
close wins/ties, the known losses (Beagle->Colonia +49, low-range Lave->Maia@20 +8),
obstacle routing (Graea pair), a bidirectional pair (asymmetry check), and the
void-stuck / not-found cases.

```bash
# against prod (fast cases only; long routes re-solve slowly if not cached)
python run_plotter_bench.py --base-url https://edgis.elitedangereuse.fr --key <KEY> \
       --only win-close,tie,loss,obstacle,validation,bidir

# after an engine change, bust the cache so you measure the NEW engine
python run_plotter_bench.py --base-url http://127.0.0.1:8383 --key <KEY> --fresh

# recalibrate baselines to the current engine (on a known-good build)
python run_plotter_bench.py --base-url <URL> --key <KEY> --fresh --update
```

Exit code is non-zero on any regression or unexpected failure (CI-friendly).
NOTE: the route cache busts only on neutron-index rebuild, not on code changes,
so always use `--fresh` when validating an engine change or you get stale jumps.

## 2. Load benchmark (throughput/latency): `../k6/`

- `edgis_endpoint_mix.js`: coords / neighbors / bodies / nearest-neutron / stats mix.
- `edgis_route_load.js`: the `/route` plotter. Default is cache-dominant (a small pool
  of popular routes, realistic traffic). `-e ROUTE_FRESH=1 -e VUS=2` stresses the cold
  solver behind the concurrency semaphore.

```bash
k6 run -e ROUTE_KEY=<key> ../k6/edgis_route_load.js
k6 run -e ROUTE_KEY=<key> -e ROUTE_FRESH=1 -e VUS=2 ../k6/edgis_route_load.js
```

## Refreshing the dataset
`route_cases.json` is a snapshot. To add routes or refresh the spansh column from
new traces, pull `route_trace*.jsonl` and re-mine it (the join is `kind=route` vs
`kind=spansh` on `(frm, to, range)`).
