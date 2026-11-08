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

use Flow\ETL\Runtime\Heartbeat\{Heartbeat, HeartbeatFailure, Pulse};
use function Flow\ETL\Adapter\CSV\{from_csv, to_csv};
use function Flow\ETL\DSL\{config_builder, df, ref};

final readonly class ConsoleProgress implements Heartbeat
{
    public function beat(Pulse $pulse): void
    {
        printf("#%d %ds %s: %d rows, %d read\n", $pulse->beat, $pulse->elapsedSeconds, $pulse->stage, $pulse->stageRows, $pulse->rowsRead);
    }
}

df(config_builder()
    ->heartbeat(new ExtendLease($lease), everySeconds: 120)
    ->heartbeat(new ConsoleProgress(), everySeconds: 1, onFailure: HeartbeatFailure::log))
    ->read(from_csv($path))
    ->sortBy(ref('id'))
    ->write(to_csv($out))
    ->run();
```

## Pulse

| Field             | Value                                                                           |
|-------------------|---------------------------------------------------------------------------------|
| `beat`            | 1, 2, 3... counted per heartbeat                                                |
| `elapsedSeconds`  | seconds since the run started                                                   |
| `rowsRead`        | rows read from the source                                                       |
| `stage`           | the stage that reported progress last                                           |
| `stageRows`       | rows that stage processed, counted once in every pass                           |
| `memoryBytes`     | current memory of the run, workers included                                     |
| `peakMemoryBytes` | peak memory of the process, in a parallel run the highest of parent and workers |

`stageRows` is not a share of `rowsRead`. A stage counts a row in every pass over it:

| Stage                          | `stageRows` after N rows        |
|--------------------------------|---------------------------------|
| `sort` in memory               | 2 x N (in and out)              |
| `sort` that spills             | more than 2 x N (every merge)   |
| `sort` followed by `limit()`   | N + the limit                   |
| `group_by`                     | N + the number of groups        |
| `window` with `partitionBy()`  | at least 2 x N                  |

## Stages

| Stage         | Reported by                                         |
|---------------|-----------------------------------------------------|
| `read`        | reading from the source                             |
| `sort`        | `sortBy()`, and `sortBy()` followed by `limit()`    |
| `group_by`    | `groupBy()->aggregate()`                            |
| `pivot`       | `groupBy()->pivot()->aggregate()`                   |
| `join`        | `join()`                                            |
| `window`      | `withColumn()` with a window function               |
| `repartition` | `repartition()`                                     |
| `parallel`    | the parent process while it waits for its workers   |

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
- `from_array()` with a generator and no schema: it reads and spills the whole generator before its first batch
- the in-memory sort of the rows a sort holds, measured on an M1 Max:

| Rows | PHP backend | Rust backend |
|------|-------------|--------------|
| 1M   | 0.9 s       | 0.2 s        |
| 5M   | 5.7 s       | 1.5 s        |

In a parallel run the callback runs in the parent process. The pulse adds the rows and memory that the workers
report, at most once per second.
