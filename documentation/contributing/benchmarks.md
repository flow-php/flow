# Benchmarks

[TOC]

Flow ships a local benchmark suite built on [phpbench](https://phpbench.readthedocs.io). It runs real DataFrame
pipelines over pregenerated datasets so you can measure the performance impact of a change and compare branches or
extension on/off on identical data.

## Requirements

Benchmarks **must be run inside `nix-shell`** so opcache, the profiler and (optionally) the `flow_php` extension are
available and the environment is reproducible.

```bash
nix-shell
```

The phpbench binary lives in its own tool directory (installed by `composer install`):

```
tools/phpbench/vendor/bin/phpbench
```

## Running

`just benchmark` forwards every argument to `phpbench run` (like `just test` does for phpunit) and defaults to
`--report=flow-report` (pass your own `--report=...` to override). Bare `just benchmark` runs every suite.

```bash
# run every suite and print the custom Flow report
just benchmark
```

The `flow-report` columns are: `benchmark`, `subject`, `set`, `mem_peak`, `mode`, `total_time`, `rstdev`.
The `set` column is the parameter set label - the row count and, for the parquet format benchmarks, the
engine variant (e.g. `100,000,php` vs `100,000,arrow`).

Defaults (see `phpbench.json.dist`): remote executor (per-iteration process isolation, the only executor that
measures `mem_peak`), `revs=1` (one e2e pipeline is one measurement), `iterations=3`, `warmup=1`, `opcache.enable_cli=1`,
`pcov.enabled=0`, unlimited `memory_limit`.

## Row-count

Each suite runs a single row-count tier, `100,000` by default. Set `FLOW_BENCH_ROWS` to an integer to run at that count
instead (datasets are generated on demand and cached, so any count works):

```bash
FLOW_BENCH_ROWS=1000000 just benchmark
```

## Tags and baseline comparison

Store a run under a tag, then compare a later run against it:

```bash
# store a baseline
just benchmark --store --tag=before

# ... make your change ...

# run the current code and compare it against the stored baseline
just benchmark --ref=before
```

To report a stored run without re-running (e.g. `phpbench report`, `phpbench log`), call the binary directly:
`tools/phpbench/vendor/bin/phpbench report --ref=before --report=flow-report`.

Stored runs live in `var/phpbench/` (gitignored).

Compare only runs of shells with the same `php -m`. The Blackfire probe alone (`nix-shell --arg with-blackfire true`)
moves call-heavy subjects by up to 50 %: time without it, profile with `blackfire run`.

## Profiling a scenario without phpbench

Every benchmark is a thin phpbench wrapper (`benchmarks/suites/`) around a plain, phpbench-free scenario class
(`benchmarks/src/`, namespace `Flow\Benchmarks\`). A scenario is invokable directly, so you can profile it with
Blackfire without any phpbench overhead. Bootstrapping through `benchmarks/bootstrap.php` mirrors the phpbench
environment (wipes `var/`, exports the service defaults) so the profile matches the benchmark run:

```bash
blackfire run php -r 'require "benchmarks/bootstrap.php"; (new Flow\Benchmarks\Joining\JoinOrdersWithSellersScenario(Flow\ETL\Values\Join\JoinType::left, 100000))->run();'
```

## Reference engines

`benchmarks/reference/` holds one script per engine. Each script runs the same four shapes over one CSV or Parquet file:

| shape            | work                                                                  |
|------------------|-----------------------------------------------------------------------|
| `scan`           | count the rows that pass a predicate touching every column            |
| `to_parquet`     | write the rows as Parquet                                             |
| `group_by`       | group by `country` with `sum(amount)`, `count(id)` and `avg(amount)`  |
| `sort`           | sort by `amount` descending, write as Parquet                         |

Every script takes `<shape> <input.csv|input.parquet> <output-dir>`. The thread count is the knob of each engine. The engines come
from the nix shell flags `with-duckdb`, `with-polars` and `with-spark`, see the
[Nix page](/documentation/contributing/nix.md#reference-engines-duckdb-polars-spark).

```bash
benchmarks/reference/duckdb/bench.sh group_by orders.csv var/reference 8
POLARS_MAX_THREADS=8 python benchmarks/reference/polars/bench.py group_by orders.csv var/reference
spark-submit --master 'local[8]' benchmarks/reference/spark/bench.py group_by orders.csv var/reference
```

Rules for a comparable number:

- Time with `hyperfine` and an explicit run count. Its default run count targets three seconds in total.

```bash
hyperfine --runs 3 --warmup 1 --export-json var/reference/group_by.json 'benchmarks/reference/duckdb/bench.sh group_by orders.csv var/reference 8'
```

- Hot page cache is the default. Record one cold run per shape with a prepare command that drops the page cache.

```bash
# macOS
hyperfine --runs 1 --prepare 'sudo purge' '...'
# Linux
hyperfine --runs 1 --prepare 'sync && echo 3 | sudo tee /proc/sys/vm/drop_caches' '...'
```

- Give every engine the same N. DuckDB takes `SET threads TO N`, Polars `POLARS_MAX_THREADS=N`, Spark `local[N]`
  and Flow N workers. The Flow coordinator process is busy on top of its N workers. State that next to every table.
- Time Flow in a `--arg with-php-benchmark true` shell, without pcov or blackfire.
- Spark pays a JVM start on every submission. The warmup run absorbs it.
- DuckDB and Polars infer column types from the CSV. Spark runs with `inferSchema=True`, which costs a second pass
  over the file.
