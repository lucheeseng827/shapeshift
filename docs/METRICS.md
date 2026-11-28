# Monitoring shapeshift

shapeshift keeps no metrics unless asked, and never sends any anywhere. There are three ways
to run it:

1. **Nothing.** With no flag, no counter is kept, no file is written and no port opens.
2. **A file per run** (`--metrics-file <path>` on `shape`, `compact`, `expire` and `relocate`).
   On exit, the run's numbers are written in the Prometheus text format, replacing the file
   whole. This suits node_exporter's textfile collector, or any file scraper. A run lives
   seconds, so there is nothing to scrape while it runs. The metrics are listed in the README
   section "Monitor it — `--metrics-file`".
3. **An endpoint** (`serve --metrics-addr <addr>`). The console serves `GET /metrics` on a
   listener of its own, which serves nothing else. Its metrics are listed in
   `shapeshift-serve/README.md` under "Metrics".

## What every metric has in common

- **Labels.** They are names you chose (a dataset, a connector) and fixed words (an outcome, a
  kind). They are never a path, a URL, a run id or a value from the data. A run posted to the
  console's `/api/shape` is labelled `dataset="(adhoc)"`. A metric holds at most 500 label sets;
  past that, new ones are folded into `other` and counted in
  `shapeshift_metrics_dropped_labels_total`.
- **Counters** start again when the console restarts, as every Prometheus counter does. A
  run's file holds the last run's numbers (gauges), so it has nothing to reset.
- **`shapeshift_build_info`** says which build wrote the metrics.

## Ready-made pieces (`docs/monitoring/`)

| File | What it is |
|---|---|
| `grafana-dashboard.json` | A Grafana dashboard: runs and their freshness, the console's runs and Overview, connectors and source lag. Import it and choose your Prometheus data source. |
| `shapeshift.rules.yml` | Prometheus alert rules: a run failing, a dataset with no success in its window, the Overview listing something, a connector backed up, a source falling behind. |
| `prometheusrule.yaml` | The same rules as a `PrometheusRule`, for the Prometheus Operator. |

The rules' thresholds are starting points. `ShapeshiftDatasetStale`, for one, assumes six
hours: set each dataset's own window. A dataset that has never succeeded has no success time,
so `time() - x` returns no data for it. To catch that case, alert on
`absent(shapeshift_run_last_success_timestamp_seconds{dataset="…"})` too.

Both files pass `promtool check rules`.
