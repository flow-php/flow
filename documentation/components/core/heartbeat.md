# Heartbeat

[DOC_LINK:/documentation/components/core/core.md]

[TOC]

## Register a heartbeat

A heartbeat is a callback. Flow calls it every N seconds while a run is busy, also during a sort, group by, pivot,
join or window function.

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

`everySeconds` must be at least 1.

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

## Several heartbeats

Each `heartbeat()` call adds one more heartbeat with its own interval, failure mode and beat count. Heartbeats that
are due together are called in the order they were registered.

```php
<?php

use Flow\ETL\Runtime\Heartbeat\HeartbeatFailure;
use function Flow\Bridge\Symfony\Console\DSL\console_heartbeat;
use function Flow\ETL\Adapter\CSV\{from_csv, to_csv};
use function Flow\ETL\DSL\{config_builder, df, ref};

df(config_builder()
    ->heartbeat(new ExtendLease($lease), everySeconds: 120)
    ->heartbeat(console_heartbeat($output), everySeconds: 1, onFailure: HeartbeatFailure::log))
    ->read(from_csv($path))
    ->sortBy(ref('id'))
    ->write(to_csv($out))
    ->run();
```

`console_heartbeat()` comes from the [Symfony Console Bridge](/documentation/components/bridges/symfony-console-bridge.md).

## Pulse

| Field             | Value                                                                                                   |
|-------------------|---------------------------------------------------------------------------------------------------------|
| `beat`            | 1, 2, 3... counted per heartbeat                                                                        |
| `elapsedSeconds`  | seconds since the run started, rounded down                                                             |
| `rowsRead`        | rows read from the source                                                                               |
| `stage`           | the stage that reported progress last                                                                   |
| `stageRows`       | rows that stage processed, counted once in every pass                                                   |
| `memoryBytes`     | the whole process, not the run alone: `memory_get_usage(true)` plus native backend bytes, workers added |
| `peakMemoryBytes` | peak memory of the process, in a parallel run the highest of parent and workers                         |
| `idleSeconds`     | seconds since the run last made progress, growing only while the run keeps checking                     |

`stageRows` is not a share of `rowsRead`. A stage counts a row in every pass over it:

| Stage                          | `stageRows` after N rows        |
|--------------------------------|---------------------------------|
| `sort` in memory               | 2 x N (in and out)              |
| `sort` that spills             | more than 2 x N (every merge)   |
| `sort` followed by `limit()`   | N + the limit                   |
| `group_by`                     | N + the number of groups        |
| `window` with `partitionBy()`  | at least 2 x N                  |

## Stages

| Stage         | Reported by                                                       |
|---------------|-------------------------------------------------------------------|
| `infer`       | schema inference before the first batch, `everyRow()` or a sample |
| `read`        | reading from the source, while no other stage takes each batch    |
| `sort`        | `sortBy()`, and `sortBy()` followed by `limit()`                  |
| `group_by`    | `groupBy()->aggregate()`                                          |
| `pivot`       | `groupBy()->pivot()->aggregate()`                                 |
| `join`        | `join()`                                                          |
| `window`      | `withColumn()` with a window function                             |
| `repartition` | `repartition()`                                                   |
| `parallel`    | the parent process until the first worker report                  |

While `sortBy()` or `groupBy()` takes every batch read, the stage is theirs; `rowsRead` shows the source.

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
all, so its lease expires by itself. The example matters most for a parallel run, where the parent keeps checking
while a worker hangs, and for a loop that keeps checking without producing rows.

## Failures

| `onFailure`                        | When `beat()` throws                                                   |
|------------------------------------|------------------------------------------------------------------------|
| `HeartbeatFailure::stop` (default) | the exception stops the run unchanged, later heartbeats are not called |
| `HeartbeatFailure::log`            | the exception is logged as `Heartbeat failed.`, the run continues      |

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
    ->heartbeat(new ConsoleProgress(), everySeconds: 1, onFailure: HeartbeatFailure::log);
```

## Accuracy

A beat can be late by the interval plus the longest call that cannot report progress:

- one call of an extractor that fetches a batch, or of a transformer or loader on a batch
- the in-memory sort of the rows a sort holds, measured on an M1 Max:

| Rows | PHP backend | Rust backend |
|------|-------------|--------------|
| 1M   | 0.9 s       | 0.2 s        |
| 5M   | 5.7 s       | 1.5 s        |

In a parallel run the callback runs in the parent process. The pulse adds the rows and memory that the workers
report, at most once per second. The stage and stage rows of the workers' latest report come through too.

`rowsRead` can go down between two pulses in a parallel join. When the right side of a broadcast join grows past
`broadcastLimit`, the attempt is dropped, and its rows leave `rowsRead` because they are read again. A progress bar on
`rowsRead` can step back once. The final value equals the rows read from the sources.
