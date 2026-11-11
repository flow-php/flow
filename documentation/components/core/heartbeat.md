# Heartbeat

[DOC_LINK:/documentation/components/core/core.md]

[TOC]

## Register a heartbeat

A heartbeat is a callback. Flow calls it every N seconds while a run is busy. This includes a sort, group by, pivot,
join or window function, and the end of a file write. A completed run beats once more, with every stage ended. A
failed run does not.

```php
<?php

use Flow\ETL\Runtime\Heartbeat\HeartbeatFailure;
use function Flow\ETL\Adapter\CSV\{from_csv, to_csv};
use function Flow\ETL\DSL\{config_builder, df, ref};

df(config_builder()->heartbeat(new ExtendLease($lease), everySeconds: 120))
    ->read(from_csv($path))
    ->sortBy(ref('id'))
    ->write(to_csv($out))
    ->run();

// a failing heartbeat is logged and the run continues
config_builder()->heartbeat(new ExtendLease($lease), everySeconds: 120, onFailure: HeartbeatFailure::log);
```

`everySeconds` is a float. The minimum is 0.1.

## Implement a heartbeat

```php
<?php

use Flow\ETL\Runtime\Heartbeat\{Heartbeat, Pulse};

final readonly class ExtendLease implements Heartbeat
{
    public function __construct(private Lease $lease)
    {
    }

    public function beat(Pulse $pulse): void
    {
        $this->lease->extend(seconds: 300);
    }
}
```

A heartbeat that logs every open stage:

```php
<?php

use Flow\ETL\Runtime\Heartbeat\{Heartbeat, Pulse};
use Psr\Log\LoggerInterface;

final readonly class LogOpenStages implements Heartbeat
{
    public function __construct(private LoggerInterface $logger)
    {
    }

    public function beat(Pulse $pulse): void
    {
        foreach ($pulse->stages as $stage) {
            if ($stage->endedAtSeconds === null) {
                $this->logger->info(sprintf('%s %s: %d', $stage->name->value, $stage->subject ?? '', $stage->count));
            }
        }
    }
}
```

## Several heartbeats

Each `heartbeat()` call adds one more heartbeat with its own interval, failure mode and beat count. Heartbeats that
are due together are called in the order they were registered.

```php
<?php

use Flow\ETL\Runtime\Heartbeat\HeartbeatFailure;
use function Flow\ETL\Adapter\CSV\{from_csv, to_csv};
use function Flow\ETL\DSL\{config_builder, df, ref};

df(config_builder()
    ->heartbeat(new ExtendLease($lease), everySeconds: 120)
    ->heartbeat(new LogOpenStages($logger), everySeconds: 1, onFailure: HeartbeatFailure::log))
    ->read(from_csv($path))
    ->sortBy(ref('id'))
    ->write(to_csv($out))
    ->run();
```

The [Symfony Console Bridge](/documentation/components/bridges/symfony-console-bridge.md) draws a run's progress in a
terminal.

## Pulse

| Field             | Value                                                                                                               |
|-------------------|---------------------------------------------------------------------------------------------------------------------|
| `beat`            | 1, 2, 3... counted per heartbeat                                                                                    |
| `elapsedSeconds`  | seconds since the run started, with a fraction                                                                      |
| `rowsRead`        | rows read from the source                                                                                           |
| `stages`          | one entry per stage, in the order of the pipeline's steps, two entries can have the same name                       |
| `memoryBytes`     | the whole process, not the run alone: `memory_get_usage(true)` plus native backend bytes, workers added             |
| `peakMemoryBytes` | the highest memory of this run, in the measure of `memoryBytes`, workers included                                   |
| `idleSeconds`     | seconds since the run last made progress, with a fraction, growing only while the run keeps checking                |

`memoryBytes` is allocated memory. A memory limit compares live memory, so `memoryBytes` can stay above it after a
spill.

`peakMemoryBytes` is a lower bound of the run's peak. It is the highest of these values:

- the highest `memoryBytes` sampled at a beat or a worker report
- the PHP peak (`memory_get_peak_usage(true)`) that this run raised above the peak at its start
- the highest native backend memory since the run started
- the peak that a worker reported

An earlier run in the same process does not raise it. Only a sample catches two kinds of spike: a PHP heap spike below
an earlier run's peak, and a spike of Arrow buffers imported from another extension.

Each entry of `stages` is a `StageProgress`:

| Field              | Value                                                                                                              |
|--------------------|--------------------------------------------------------------------------------------------------------------------|
| `id`               | the same in every beat, use it to follow one stage over beats, null for a stage that a worker builds itself        |
| `name`             | a `StageName` case from the table under Stages, for example `StageName::sort`                                      |
| `unit`             | `rows`, or `files` for `commit`. `group_by` starts in `rows`, then counts `groups`                                 |
| `subject`          | a read's file or glob, a write's directory, else null, never a user, password, port, query or fragment             |
| `count`            | what the stage processed, in its unit                                                                              |
| `total`            | exact only for a read that nothing pushed narrows (`limit()`, `offset()`, a filter, a partition filter), else null |
| `startedAtSeconds` | seconds since the run started, null for a stage that is planned and has not started yet                            |
| `endedAtSeconds`   | seconds since the run started, null while the stage is open                                                        |

Every stage of the plan is in the list from the first beat after planning, with `startedAtSeconds` null until it
starts. A stage that never starts, for example a sort after a filter that drops every row, ends with the run: its
`endedAtSeconds` is set and its `startedAtSeconds` stays null. A join's `subject` is its condition, for example
`inner on id` or `left on order_id = id, region`.

`DataFrame::schema()` beats too. A long schema inference reports to the `infer` stage, and `schema()` ends with a last
beat. A heartbeat that fails with `HeartbeatFailure::stop` throws out of `schema()`.

`count` is not a share of `rowsRead`:

| Stage                         | `count` after N rows          |
|-------------------------------|-------------------------------|
| `sort`, in memory or spilling | N, merges and output add none |
| `sort` followed by `limit()`  | N                             |
| `group_by`                    | N rows, then the groups       |
| `join`                        | the rows the join streams     |
| `window` with `partitionBy()` | at least 2 x N                |
| `commit`                      | the files written             |

## Stages

Each stage is a case of the enum `Flow\ETL\Foundation\StageName`. Its value is the name in the table.

| Stage         | Reported by                                                            |
|---------------|------------------------------------------------------------------------|
| `infer`       | schema inference before the first batch, `everyRow()` or a sample      |
| `read`        | reading from the source, one entry per source                          |
| `sort`        | `sortBy()`, and `sortBy()` followed by `limit()`                       |
| `group_by`    | `groupBy()->aggregate()`                                               |
| `pivot`       | `groupBy()->pivot()->aggregate()`                                      |
| `join`        | `join()`                                                               |
| `window`      | `withColumn()` with a window function                                  |
| `repartition` | `repartition()`                                                        |
| `commit`      | one entry per write to files, counting the files it makes visible      |
| `parallel`    | no entry, the workers' stages add to the entries of the steps they run |

A `join` counts the rows of the side it streams: the left side, or the larger side of each bucket pair after it
spills. The side it holds counts in its own `read` entry. Two `sortBy()` are two `sort` entries. The right read of a
join comes after the left read.

A count only grows while its unit stays, except the `read` count under Accuracy. A unit changes at most once.

## Report progress from a loader

A loader that also implements `Closure` reports through the `Progress` that `closure()` takes:

```php
<?php

use Flow\ETL\Foundation\Progress;
use Flow\ETL\IO\Loader;
use Flow\ETL\IO\Loader\Closure;

final readonly class ClientLoader implements Closure, Loader
{
    // the constructor, load(), describe() and fromDescription() of the loader

    public function closure(Progress $progress): void
    {
        $this->client->flush();
        $progress->busy();
    }
}
```

- `advanced(1)` counts a file that became visible at its target.
- `busy()` reports any other blocking call.
- The runtime calls `done()`.

## Stop a lease when the run stalls

```php
<?php

use Flow\ETL\Runtime\Heartbeat\{Heartbeat, Pulse};

final readonly class ExtendLeaseWhileProgressing implements Heartbeat
{
    public function __construct(private Lease $lease)
    {
    }

    public function beat(Pulse $pulse): void
    {
        if ($pulse->idleSeconds < 600) {
            $this->lease->extend(seconds: 300);
        }
    }
}
```

The threshold must be well above the late-beat bound under Accuracy.

`idleSeconds` grows only through checks that see no progress. A single process blocked in one call sends no beat at
all, so its lease expires by itself. The example matters most for a parallel run: the parent keeps checking while a
worker hangs. It also matters for a loop that keeps checking without producing rows.

## Failures

| `onFailure`                        | When `beat()` throws                                                   |
|------------------------------------|------------------------------------------------------------------------|
| `HeartbeatFailure::stop` (default) | the exception stops the run unchanged, later heartbeats are not called |
| `HeartbeatFailure::log`            | the exception is logged as `Heartbeat failed.`, the run continues      |

A `HeartbeatFailure::stop` throw while the writes of a run end stops all of them. The write that was ending keeps the
files that it already moved. With `overwrite()`, the files that it replaces also stay. Every write that did not end
yet is discarded, a write inside `to_branch()` included. The heartbeat is not called again in that run.

`HeartbeatFailure::log` writes to the telemetry logger. The default telemetry records nothing, so configure a logger
to see the failures:

```php
<?php

use Flow\ETL\Foundation\Clock\SystemClock;
use Flow\ETL\Runtime\Heartbeat\HeartbeatFailure;
use function Flow\ETL\DSL\config_builder;
use function Flow\Telemetry\DSL\{batching_log_processor, console_exporter, logger_provider, memory_context_storage, resource, telemetry};

$telemetry = telemetry(
    resource(['service.name' => 'my-etl']),
    loggerProvider: logger_provider(batching_log_processor(console_exporter()), SystemClock::utc(), memory_context_storage()),
);
$telemetry->registerShutdownFunction();

config_builder()
    ->withTelemetry($telemetry)
    ->heartbeat(new ExtendLease($lease), everySeconds: 120, onFailure: HeartbeatFailure::log);
```

## Accuracy

A beat can be late by the interval plus the longest call that cannot report progress:

- one call of an extractor that fetches a batch, or of a transformer or loader on a batch
- one file operation while a write ends: closing one file (a format's footer or the end of an upload), moving one file,
  listing one directory, or removing one file
- the in-memory sort of the rows a sort holds, measured on an M1 Max:

| Rows | PHP backend | Rust backend |
|------|-------------|--------------|
| 1M   | 0.9 s       | 0.2 s        |
| 5M   | 5.7 s       | 1.5 s        |

In a parallel run the callback runs in the parent process. The pulse adds the rows and memory that the workers
report, at most once per second. The workers' stages add to the entries of the steps they run. A worker reports
through `heartbeatToParent()`: its stages carry only the ids the parent gave them, and a DataFrame that a worker step
runs adds to what the worker counted. A parallel read is the read entry the parent planned, with its subject and its
exact total. The workers add their counts to it. A stage that a worker builds itself does not show. `idleSeconds`
stays low while any worker works, also between its counts.

`rowsRead` and the `count` of that `read` entry can go down between two pulses in a parallel join. If the right side
of a broadcast join grows past `broadcastLimit`, the rows read so far are read again, so they leave the count. If the
parallel launch is refused after the right side was read, the same happens. A progress bar on `rowsRead` can step back
once.
