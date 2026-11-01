# Benchmarks

[TOC]

Flow has a local benchmark suite that uses [phpbench](https://phpbench.readthedocs.io). The suite runs real DataFrame
pipelines over datasets that it generates before the run. With it, you can measure how a change affects performance.
You can also compare two branches, or a run with the extension and a run without it, on identical data.

## Requirements

Run the benchmarks inside `nix-shell`. The shell makes opcache, the profiler and the optional `flow_php` extension
available, and it makes the environment reproducible.

```bash
nix-shell
```

The phpbench binary is in its own tool directory. `composer install` installs it:

```
tools/phpbench/vendor/bin/phpbench
```

## Running

`just benchmark` sends all its arguments to `phpbench run`, as `just test` does for phpunit. The default report is
`--report=flow-report`. To use a different report, give your own `--report=...`. Without arguments, `just benchmark`
runs all suites.

```bash
# run every suite and print the custom Flow report
just benchmark
```

The `flow-report` columns are: `benchmark`, `subject`, `set`, `mem_peak`, `mode`, `total_time`, `rstdev`.
The `set` column is the label of the parameter set. It contains the row count. For the parquet format benchmarks, it
also contains the engine variant, for example `100,000,php` or `100,000,arrow`.

The defaults are in `phpbench.json.dist`:

- The remote executor. It isolates each iteration in its own process, and it is the only executor that measures
  `mem_peak`.
- `revs=1`, because one end-to-end pipeline is one measurement.
- `iterations=3`, `warmup=1`, `opcache.enable_cli=1`, `pcov.enabled=0` and no `memory_limit`.

## Row-count

Each suite runs one row count, `100,000` by default. To run a different count, set `FLOW_BENCH_ROWS` to an integer.
Any count works. The suite generates each dataset at its first use and caches it:

```bash
FLOW_BENCH_ROWS=1000000 just benchmark
```

## Tags and baseline comparison

Store a run under a tag. Then compare a later run with it:

```bash
# store a baseline
just benchmark --store --tag=before

# ... make your change ...

# run the current code and compare it against the stored baseline
just benchmark --ref=before
```

To show a stored run without a new run, call the binary directly, for example with `phpbench report` or
`phpbench log`: `tools/phpbench/vendor/bin/phpbench report --ref=before --report=flow-report`.

The stored runs are in `var/phpbench/`. Git ignores this directory.

Compare only runs from shells that have the same `php -m` output. The Blackfire probe alone
(`nix-shell --arg with-blackfire true`) changes the time of subjects with many calls by up to 50 %. Time without the
probe, and profile with `blackfire run`.

## Profiling a scenario without phpbench

Each benchmark is a thin phpbench wrapper (`benchmarks/suites/`) around a scenario class that does not use phpbench
(`benchmarks/src/`, namespace `Flow\Benchmarks\`). You can call a scenario directly, so you can profile it with
Blackfire without phpbench overhead. `benchmarks/bootstrap.php` makes the same environment as phpbench: it deletes the
contents of `var/` and exports the service defaults. The profile then matches the benchmark run:

```bash
blackfire run php -r 'require "benchmarks/bootstrap.php"; (new Flow\Benchmarks\Joining\JoinOrdersWithSellersScenario(Flow\ETL\Values\Join\JoinType::left, 100000))->run();'
```

## Reference engines

`benchmarks/reference/` has one script for each engine. Each script runs the same four shapes over one CSV or Parquet
file:

| shape            | work                                                                  |
|------------------|-----------------------------------------------------------------------|
| `scan`           | count the rows that pass a predicate on every column                  |
| `to_parquet`     | write the rows as Parquet                                             |
| `group_by`       | group by `country` with `sum(amount)`, `count(id)` and `avg(amount)`  |
| `sort`           | sort by `amount` descending, write as Parquet                         |

Each script takes `<shape> <input.csv|input.parquet> <output-dir>`. Each engine has its own setting for the thread
count. The engines come from the nix shell flags `with-duckdb`, `with-polars` and `with-spark`. For more information,
read the [Nix page](/documentation/contributing/nix.md#reference-engines-duckdb-polars-spark).

```bash
benchmarks/reference/duckdb/bench.sh group_by orders.csv var/reference 8
POLARS_MAX_THREADS=8 python benchmarks/reference/polars/bench.py group_by orders.csv var/reference
spark-submit --master 'local[8]' benchmarks/reference/spark/bench.py group_by orders.csv var/reference
```

Rules for numbers that you can compare:

- Time with `hyperfine` and an explicit run count. Without it, hyperfine picks the run count to fill three seconds.

```bash
hyperfine --runs 3 --warmup 1 --export-json var/reference/group_by.json 'benchmarks/reference/duckdb/bench.sh group_by orders.csv var/reference 8'
```

- The default is a hot page cache. Record one cold run for each shape, with a prepare command that clears the page
  cache.

```bash
# macOS
hyperfine --runs 1 --prepare 'sudo purge' '...'
# Linux
hyperfine --runs 1 --prepare 'sync && echo 3 | sudo tee /proc/sys/vm/drop_caches' '...'
```

- Give each engine the same N. DuckDB takes `SET threads TO N`, Polars `POLARS_MAX_THREADS=N`, Spark `local[N]`
  and Flow N workers. The Flow coordinator process also uses CPU in addition to its N workers. Write this next to
  each table.
- Time Flow in a `--arg with-php-benchmark true` shell, without pcov or blackfire.
- Spark starts a JVM for each submission. The warmup run takes this cost.
- DuckDB and Polars infer the column types from the CSV. Spark runs with `inferSchema=True`, which reads the file a
  second time.
