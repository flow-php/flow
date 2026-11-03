# Upgrade Guide 0.41.x - 0.43.x

[TOC]

This document provides guidelines for upgrading between versions of Flow PHP. Please follow the instructions for your
specific version to ensure a smooth upgrade process.

Upgrading from older versions: [0.35.x to 0.41.x](/documentation/upgrading-0.35-0.41.md)

---

## Upgrading from 0.42.x to 0.43.x

### 1) `flow-php/etl` - `to_transformation()` expands a `Transformation` once per loader, not once per batch

| Before                                                                            | After                   |
|-----------------------------------------------------------------------------------|-------------------------|
| `to_transformation(limit(3), $loader)`, 6 batches → 6 rows loaded                 | 3 rows loaded           |
| `to_transformation(add_row_index('n'), $loader)`, 6 batches → `n = [1,1,1,1,1,1]` | `n = [1,2,3,4,5,6]`     |
| nested `DataFrame` span per batch                                                 | one nested span per run |

Unchanged: any pipeline using `->collect()`, `drop()`, `select()`, `mask_columns()`, `batch_size()`, `batch_by()`.

### 2) `flow-php/telemetry` - `Tracer::span()` no longer activates the span

| Before                                             | After                                                             |
|----------------------------------------------------|-------------------------------------------------------------------|
| `$tracer->span('x')` makes the span current        | does not; `$tracer->activate($span): Scope` does                  |
| `$tracer->complete($span)` also detaches the scope | ends the span only                                                |
| `span(..., SpanContext $parentContext)`            | `span(..., Context $parent)`                                      |
| `trace(..., SpanContext $parentContext)`           | `trace(..., Context $parent)`                                     |
| `Scope::detach()` always returns `0`               | returns `Scope::DETACHED`, `Scope::INACTIVE` or `Scope::MISMATCH` |

Call sites relying on implicit nesting still compile and silently produce siblings. Rewrite each one that needs
children:

Before:

```php
$span = $tracer->span('parent');

try {
    // ...
} finally {
    $tracer->complete($span);
}
```

After:

```php
$span = $tracer->span('parent');
$scope = $tracer->activate($span);

try {
    // ...
} finally {
    $scope->detach();
    $tracer->complete($span);
}
```

### 3) `flow-php/etl` - window aggregates use the SQL default frame

On `d = 2024-01-01, 2024-01-02, 2024-01-03, 2024-01-04` and `s = 100, 200, 300, 400`:

| Before                                                                        | After                                         |
|-------------------------------------------------------------------------------|-----------------------------------------------|
| `sum(ref('s'))->over(window()->orderBy(ref('d')))` → `1000, 1000, 1000, 1000` | `100, 300, 600, 1000`                         |
| `average()`, `count()` over an ordered window - whole partition               | rows up to the current row's peers            |
| `window()->partitionBy(ref('dept'))` - whole partition                        | unchanged                                     |
| empty frame                                                                   | `sum()`/`average()` → `null`, `count()` → `0` |

Restore the previous result:

```php
sum(ref('s'))->over(window()->orderBy(ref('d'))->rowsBetween(unbounded_preceding(), unbounded_following()));
```

### 4) `flow-php/etl` - `count()` over a window is SQL `COUNT`

On `s = 100, 100, 300` ordered by a distinct column:

| Before                                                                    | After                                           |
|---------------------------------------------------------------------------|-------------------------------------------------|
| `count(ref('s'))` counts rows sharing the current row's value → `2, 2, 1` | counts non-null values in the frame → `1, 2, 3` |
| `count()` threw `Count WindowFunction function requires a reference.`     | counts every row in the frame (`COUNT(*)`)      |

### 5) `flow-php/etl` - `partitionBy()` no longer sets `orderBy()`

| Before                                                                                                   | After                                             |
|----------------------------------------------------------------------------------------------------------|---------------------------------------------------|
| `window()->orderBy(ref('date'))->partitionBy(ref('dept'))->order()` → `['dept']`                         | `['date']`                                        |
| `window()->partitionBy(ref('dept'))->order()` → `['dept']`                                               | `[]`                                              |
| `rank()`/`dense_rank()`/`row_number()` over a `partitionBy()`-only window ranked by the partition column | throws `... requires to be ordered by one column` |

Add the ordering explicitly:

```php
rank()->over(window()->partitionBy(ref('dept'))->orderBy(ref('salary')->desc()));
```

### 6) `flow-php/etl` - `WindowFunction::apply()` receives a `WindowContext`

| Before                                                   | After                                                    |
|----------------------------------------------------------|----------------------------------------------------------|
| `apply(Row $row, Rows $partition, FlowContext $context)` | `apply(WindowContext $window)`                           |
| `$row`                                                   | `$window->row()`                                         |
| `$partition`                                             | `$window->partition()`                                   |
| `$context`                                               | `$window->flowContext()`                                 |
| -                                                        | `$window->frame()` - rows within the current row's frame |
| -                                                        | `$window->index()` - position in the ordered partition   |
| `row_number()` on duplicate rows → `1, 1, 3`             | `1, 2, 3`                                                |

Implementations must no longer sort; `$window->partition()` and `$window->frame()` are already ordered.

### 7) `flow-php/postgresql` - query recording moved to `flow-php/symfony-postgresql-bundle`

| Before                                           | After                                                                |
|--------------------------------------------------|----------------------------------------------------------------------|
| `Flow\PostgreSql\Client\Debug\RecordingClient`   | `Flow\Bridge\Symfony\PostgreSqlBundle\Profiler\ProfilerClient`       |
| `Flow\PostgreSql\Client\Debug\QueryLog`          | `Flow\Bridge\Symfony\PostgreSqlBundle\Profiler\QueryRecorder`        |
| `Flow\PostgreSql\Client\Debug\QueryLogOptions`   | `Flow\Bridge\Symfony\PostgreSqlBundle\Profiler\QueryRecorderOptions` |
| `Flow\PostgreSql\Client\Debug\RecordedQuery`     | `Flow\Bridge\Symfony\PostgreSqlBundle\Profiler\RecordedQuery`        |
| `QueryLogOptions::$maxParameters`                | `QueryRecorderOptions::$maxRetainedParameters`                       |
| `QueryLogOptions::maxParameters()`               | `QueryRecorderOptions::maxRetainedParameters()`                      |
| service `flow.postgresql.profiler.query_log`     | `flow.postgresql.profiler.query_recorder`                            |
| config `flow_postgresql.profiler.max_parameters` | `flow_postgresql.profiler.max_retained_parameters`                   |

Applies to `flow-php/postgresql` users only through the bundle; `Client\Telemetry` is unchanged.

### 8) `flow-php/symfony-telemetry-bundle` - cache pools and PSR-18 clients that were silently skipped are now traced

| Before                                                                                                                                               | After                                                               |
|------------------------------------------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------|
| `cache.system`, `cache.validator`, `cache.serializer`, `cache.property_info`, `cache.app`, `cache.doctrine.*`, `cache.http_client.pool` - not traced | traced: `cache.*` spans and `flow.cache.hits` / `flow.cache.misses` |
| pool or client whose class is a `%parameter%` - not traced                                                                                           | traced                                                              |
| tag-aware pool whose class is a `%parameter%` - got the non-tag-aware decorator                                                                      | gets `TagAwareTraceableCacheAdapter`                                |
| PSR-18 client behind an autoconfigured or abstract parent definition - container build failed with *"has a reference to an abstract definition"*     | compiles; the client is traced                                      |
| `instrumentation.cache.exclude_pools` entries for framework pools - had no effect                                                                    | take effect                                                         |

To keep the previous set of traced pools, exclude the framework's own:

```yaml
flow_telemetry:
  instrumentation:
    cache:
      exclude_pools:
        - 'cache.system'
        - 'cache.validator'
        - 'cache.serializer'
        - 'cache.property_info'
        - '/^cache\.doctrine\..*/'
        - 'cache.http_client.pool'
```

### 9) `flow-php/types` - a `Type` implementation's generic parameter is the value it represents

| Before                           | After                                    |
|----------------------------------|------------------------------------------|
| `ListType<string>`               | `ListType<list<string>>`                 |
| `MapType<string, int>`           | `MapType<array<string, int>>`            |
| `StructureType<mixed>`           | `StructureType<array<array-key, mixed>>` |
| `ClassStringType<Foo>`           | `ClassStringType<class-string<Foo>>`     |
| `ListType::element(): Type<T>`   | `Type<value-of<T>>`                      |
| `MapType::key(): Type<TKey>`     | `Type<key-of<T>>`                        |
| `MapType::value(): Type<TValue>` | `Type<value-of<T>>`                      |
| `type_string(): Type`            | `type_string(): StringType`              |

Update `ListType`, `MapType`, `StructureType` and `ClassStringType` parameters in your own docblocks.

---

## Upgrading from 0.41.x to 0.42.x

### 1) `flow-php/symfony-telemetry-bundle` - messenger tracing simplified

| Before (0.41)                                                        | After (0.42)                                                       |
|----------------------------------------------------------------------|--------------------------------------------------------------------|
| `instrumentation.messenger.trace`: `worker`/`handlers`/`both`/`none` | `instrumentation.messenger.trace`: `true`/`false` (default `true`) |
| `instrumentation.messenger.link`: `dispatcher`/`worker`/`both`       | removed                                                            |
| `messenger.receive` worker-cycle span                                | removed                                                            |

The worker-cycle span and the `trace: worker`/`both` modes are gone; a consumed message is a `process` span and a
produced one a `send` span. The `messenger:consume` worker loop is suppressed via
`instrumentation.console.exclude_commands` - which now **fully suppresses** matching commands (previously it only
skipped the console span) and defaults to `['messenger:consume']` - so transport-poll/idle-tick work does not surface
and the long-lived `messenger:consume` console span is dropped. Per-message handler traces are still recorded. Set
`console.exclude_commands: []` to trace the worker loop.

### 2) `flow-php/symfony-telemetry-bundle` - cache span names unified to dotted lowercase

| Before                                                                | After                                                    |
|-----------------------------------------------------------------------|----------------------------------------------------------|
| `Cache Commit {pool}`                                                 | `cache.commit`                                           |
| `Cache Save {key} {pool}`                                             | `cache.save`                                             |
| `Cache SaveDeferred {key} {pool}`                                     | `cache.save_deferred`                                    |
| `Cache Delete {key} {pool}`                                           | `cache.delete`                                           |
| `Cache DeleteItem {key} {pool}`                                       | `cache.delete_item`                                      |
| `Cache DeleteItems {pool}`                                            | `cache.delete_items`                                     |
| `Cache Clear {pool}`                                                  | `cache.clear`                                            |
| `Cache Prune {pool}`                                                  | `cache.prune`                                            |
| `Cache Reset {pool}`                                                  | `cache.reset`                                            |
| `Cache InvalidateTags {pool}`                                         | `cache.invalidate_tags`                                  |
| `cache.operation: saveDeferred/deleteItem/deleteItems/invalidateTags` | `save_deferred/delete_item/delete_items/invalidate_tags` |

Cache spans now match the DBAL/messenger convention (dotted lowercase, low cardinality). The `{key}` and `{pool}`
that were baked into the span name move out of it - they were already available as the `cache.key` and `cache.pool`
attributes. Rename these series in dashboards and alerts, and update any filters on the camelCase `cache.operation`
values.

### 3) `flow-php/telemetry` - `Sampler::shouldSample()` receives the parent `Context`

| Before                                     | After                                                              |
|--------------------------------------------|--------------------------------------------------------------------|
| `shouldSample(Span $span): SamplingResult` | `shouldSample(Context $parentContext, Span $span): SamplingResult` |
| `$sampler->shouldSample($span)`            | `$sampler->shouldSample($context, $span)`                          |

Custom `Sampler` implementations (including a `sampler: { type: service }` service in
`flow-php/symfony-telemetry-bundle`) must update the signature and forward `$parentContext` to any delegated sampler.

### 4) `flow-php/telemetry` - `ResettableContextStorage` removed; `MemoryContextStorage::reset()` removed

| Before                                            | After   |
|---------------------------------------------------|---------|
| `Flow\Telemetry\Context\ResettableContextStorage` | removed |
| `MemoryContextStorage::reset()`                   | removed |

The context storage is no longer tagged `kernel.reset` in `flow-php/symfony-telemetry-bundle`; scope balance is
maintained by attach/detach alone. A custom `context_storage` service no longer needs a `reset()` method.

### 5) `flow-php/symfony-telemetry-bundle` - messenger tracing middleware auto-injected into all buses

The tracing middleware is now injected into every message bus automatically. If you previously added
`flow.telemetry.messenger.middleware` to a bus's `framework.messenger.buses.*.middleware` list by hand, remove it to
avoid duplicate spans.

### 6) `flow-php/symfony-telemetry-bundle` - `http_kernel.exclude_paths` now suppresses the whole request

| Before                                            | After                                                   |
|---------------------------------------------------|---------------------------------------------------------|
| Excluded path only skips its own request span     | Excluded path suppresses tracing for the entire request |
| DBAL/cache/`kernel.terminate` work still recorded | DBAL/cache/`kernel.terminate` work produces no spans    |

An excluded path now attaches the OpenTelemetry suppression key for the request's duration (through
`kernel.terminate`), so lower-level auto-instrumentation and terminate-phase database writes no longer emit orphan root
spans (e.g. the `/_wdt` toolbar fetch writing an audit row after its response). If you relied on those child spans being
recorded for an excluded path, remove the path from `exclude_paths`.

### 7) `flow-php/postgresql`, `flow-php/symfony-postgresql-bundle` - `traceTransactions` bool replaced by

`transactionSpans` mode

| Before                                                         | After                                                                          |
|----------------------------------------------------------------|--------------------------------------------------------------------------------|
| `postgresql_telemetry_options(traceTransactions: true)`        | `postgresql_telemetry_options(transactionSpans: TransactionSpanMode::GROUPED)` |
| `postgresql_telemetry_options(traceTransactions: false)`       | `postgresql_telemetry_options(transactionSpans: TransactionSpanMode::OFF)`     |
| `PostgreSqlTelemetryOptions` 2nd arg `bool $traceTransactions` | `TransactionSpanMode $transactionSpans`                                        |
| `$options->traceTransactions(false)`                           | `$options->transactionSpans(TransactionSpanMode::OFF)`                         |
| `$options->traceTransactions` (property)                       | `$options->transactionSpans` (`TransactionSpanMode`)                           |
| config `telemetry.trace_transactions: true`                    | config `telemetry.transaction_spans: grouped`                                  |
| config `telemetry.trace_transactions: false`                   | config `telemetry.transaction_spans: off`                                      |

`TransactionSpanMode::PER_OPERATION` emits a short span per `BEGIN`/`COMMIT`/`ROLLBACK`; `GROUPED` (default) keeps the
single long-lived transaction span.

The `db.client.operation.duration` metric now uses only low-cardinality dimensions: queries are tagged with
`db.system.name`, `db.namespace`, `db.operation.name`, `db.collection.name`; transactions with `db.system.name`,
`db.namespace`, `db.operation.name` (`begin`/`commit`/`rollback`), `db.transaction.nesting_level`. `db.query.text`,
`db.query.parameter.*`, `server.address` and `db.transaction.savepoint` are no longer metric dimensions.

### 8) `flow-php/symfony-telemetry-bundle` - DBAL spans, metrics and config aligned with the PostgreSQL client

| Before                                                           | After                                                                                      |
|------------------------------------------------------------------|--------------------------------------------------------------------------------------------|
| `doctrine.dbal.transaction.begin`/`.commit`/`.rollback` (always) | one `BEGIN TRANSACTION` span per transaction (`grouped`)                                   |
| `doctrine.dbal.connection.exec`/`.query` span names              | semconv `{db.operation.name} {db.collection.name}` (e.g. `SELECT users`)                   |
| `doctrine.dbal.statement.execute`/`.prepare` span names          | semconv `{db.operation.name} {db.collection.name}`                                         |
| config `instrumentation.dbal.log_sql`                            | removed - `db.query.text` is always recorded (bounded by `max_sql_length`)                 |
| -                                                                | config `instrumentation.dbal.transaction_spans`: `grouped` (default)/`per_operation`/`off` |
| -                                                                | config `instrumentation.dbal.collect_metrics` (default `true`)                             |
| -                                                                | config `instrumentation.dbal.include_parameters`/`max_parameters`/`max_parameter_length`   |

Query spans now carry `db.system.name`, `db.namespace`, `server.address`, `server.port`, `db.operation.name`,
`db.collection.name`, `db.response.returned_rows` and (on error) `db.response.status_code`, and the instrumentation
emits `db.client.operation.duration` and `db.client.response.returned_rows` metrics. If you matched DBAL spans by their
`doctrine.dbal.*` names, switch to the semantic names above.

### 9) `flow-php/symfony-telemetry-bundle` - custom attribute keys moved out of reserved OTel namespaces

| Before                             | After                                  |
|------------------------------------|----------------------------------------|
| `controller`                       | `flow.symfony.controller`              |
| `controller.argument`              | `flow.symfony.controller.argument`     |
| `code.namespace` + `code.function` | `code.function.name` (`Class::method`) |
| `command.name`                     | `flow.symfony.command.name`            |
| `command.class`                    | `flow.symfony.command.class`           |
| `process.signal`                   | `flow.symfony.command.signal`          |
| `process.exit_code`                | `process.exit.code`                    |
| `db.connection.name`               | `flow.db.connection.name`              |
| `db.transaction.nesting_level`     | `flow.db.transaction.nesting_level`    |
| `http.client.name`                 | `flow.http.client.name`                |
| `log.channel`                      | `flow.log.channel`                     |

### 10) `flow-php/symfony-telemetry-bundle` - HTTP spans aligned with the stable HTTP semconv

| Before                                          | After                                        |
|-------------------------------------------------|----------------------------------------------|
| server span `url.full`                          | removed                                      |
| server span `url.path` = URI incl. query        | path only; query moves to `url.query`        |
| -                                               | server span `user_agent.original`            |
| client span name `{method} {host}`              | `{method}`                                   |
| client span `server.port` only when non-default | always set (scheme default 80/443 filled in) |

Applies to the HttpKernel server span and to the `http_client`/`psr18_client` client spans.

### 11) `flow-php/symfony-telemetry-bundle` - messenger destination is the transport; `metrics_duration_unit` removed

| Before                                                             | After                                                    |
|--------------------------------------------------------------------|----------------------------------------------------------|
| `messaging.destination.name` = short message class                 | transport name (consume side); absent on dispatch        |
| `messaging.transport`                                              | removed (folded into `messaging.destination.name`)       |
| `messaging.consumer.group.name` (metric attribute)                 | removed                                                  |
| `messaging.message.class`                                          | `flow.messenger.message.class`                           |
| `messaging.symfony.bus`                                            | `flow.messenger.bus`                                     |
| consume span `process {ShortClass}`                                | `process {transport}`                                    |
| dispatch span `send {ShortClass}`                                  | `send`                                                   |
| config `instrumentation.messenger.metrics_duration_unit`: `s`/`ms` | removed - `messaging.process.duration` is always seconds |
| `Instrumentation\Messenger\MessengerMetricDurationUnit`            | removed                                                  |

### 12) `flow-php/phpunit-telemetry-bridge` - `test.*` keys aligned with the OTel test registry; durations in seconds

| Before                                            | After                                                  |
|---------------------------------------------------|--------------------------------------------------------|
| `test.name`                                       | `test.case.name`                                       |
| `test.status`                                     | `test.case.result.status`                              |
| `test.suite`                                      | `test.suite.name`                                      |
| -                                                 | `test.suite.run.status`: `success`/`failure`/`skipped` |
| `test.id`                                         | `flow.phpunit.test.id`                                 |
| `test.class`                                      | `flow.phpunit.test.class`                              |
| `test.method`                                     | `flow.phpunit.test.method`                             |
| `test.suite.test_count`                           | `flow.phpunit.suite.test_count`                        |
| `test.suite.is_root`                              | `flow.phpunit.suite.is_root`                           |
| `test.memory.peak_bytes`                          | `flow.phpunit.test.memory.peak`                        |
| `test.memory.delta_bytes`                         | `flow.phpunit.test.memory.delta`                       |
| `test.duration_ms` span attribute                 | removed (use the span duration)                        |
| `exception.message` span attribute                | removed (message stays in the span status description) |
| `flow.phpunit.test.duration` unit `ms`            | `s` (values rescaled)                                  |
| `flow.phpunit.suite.duration` unit `ms`           | `s` (values rescaled)                                  |
| `flow.phpunit.test.memory.*` unit `bytes`         | `By`                                                   |
| `flow.phpunit.test.count`/`suite.test_count` unit | `{test}`                                               |
| -                                                 | resource attribute `telemetry.sdk.version`             |

### 13) `flow-php/filesystem`, `flow-php/etl`, `flow-php/postgresql` - flow-custom keys under `flow.*`; UCUM units

| Before                                                     | After                                                |
|------------------------------------------------------------|------------------------------------------------------|
| `path.uri`/`path.to`                                       | `flow.filesystem.path.uri`/`.path.to`                |
| `stream.type`                                              | `flow.filesystem.stream.type`                        |
| `bytes.total_read`/`bytes.total_written`                   | `flow.filesystem.bytes.total_read`/`.total_written`  |
| `filesystem.operation`/`filesystem.protocol`               | `flow.filesystem.operation`/`.protocol`              |
| spans `Read {file}`/`Write {file}`                         | `filesystem.read`/`filesystem.write`                 |
| `flow.filesystem.*.size` unit `bytes`                      | `By`                                                 |
| `flow.filesystem.*.operations` unit `operations`           | `{operation}`                                        |
| `dataframe.id`/`dataframe.name`                            | `flow.etl.dataframe.id`/`.dataframe.name`            |
| `rows.total`/`rows.throughput.per_second`                  | `flow.etl.rows.total`/`.rows.throughput.per_second`  |
| `memory.min.mb`/`memory.max.mb`                            | `flow.etl.memory.min`/`.memory.max` (values stay MB) |
| `loader.class`/`transformer.class`                         | `flow.etl.loader.class`/`.transformer.class`         |
| `destination.uri`/`loading.rows`                           | `flow.etl.destination.uri`/`.loading.rows`           |
| `transformation.input_rows`/`.output_rows`                 | `flow.etl.transformation.input_rows`/`.output_rows`  |
| `join.type`/`scalar.function`                              | `flow.etl.join.type`/`.scalar.function`              |
| spans `Cache Set {key}`/`Cache Delete {key}`/`Cache Clear` | `cache.set`/`cache.delete`/`cache.clear`             |
| `flow.cache.hits`/`.misses` unit `operations`              | `{operation}`                                        |
| `flow.etl.rows.processed` unit `rows`                      | `{row}`                                              |
| `flow.etl.rows.throughput` unit `rows/s/sec`               | `{row}/s`                                            |
| `db.transaction.savepoint`                                 | `flow.db.transaction.savepoint`                      |
| `db.transaction.nesting_level`                             | `flow.db.transaction.nesting_level`                  |

### 14) `flow-php/telemetry`, `flow-php/psr18-telemetry-bridge` - shared semconv constants; UCUM time units

| Before                                                                                                                          | After                                                 |
|---------------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------|
| official keys duplicated per package (`DbAttributes`, `PostgreSqlTelemetryAttributes`, inline strings)                          | `Flow\Telemetry\SemConvAttributes` / `SemConvMetrics` |
| `TimeUnit::SECONDS->value` = `'sec'`                                                                                            | `'s'`                                                 |
| `TimeUnit::MICROSECONDS->value` = `'µs'`                                                                                        | `'us'`                                                |
| PSR-18 client span name `{method} {host}`                                                                                       | `{method}`                                            |
| PSR-18 `server.port` only when non-default                                                                                      | always set (scheme default 80/443 filled in)          |
| `FilesystemTelemetryAttributes::ATTR_BYTES_READ`/`ATTR_BYTES_WRITTEN`/`ATTR_PATH_FROM`/`ATTR_PATH_IS_PATTERN`/`ATTR_ERROR_TYPE` | removed                                               |
| `PostgreSqlTelemetryAttributes::DB_QUERY_SUMMARY` + official-key constants                                                      | removed (officials via `SemConvAttributes`)           |

### 15) `flow-php/symfony-telemetry-bundle` - `runtime_mode` removed; terminate flushes, process end shuts down

| Before                                                                      | After                                       |
|-----------------------------------------------------------------------------|---------------------------------------------|
| `flow_telemetry.runtime_mode: auto`/`classic`/`worker`                      | removed                                     |
| `Flow\Bridge\Symfony\TelemetryBundle\Runtime\RuntimeModeResolver`           | removed                                     |
| `Flow\Bridge\Symfony\TelemetryBundle\Runtime\RuntimeMode`                   | removed                                     |
| `Flow\Bridge\Symfony\TelemetryBundle\Runtime\WorkerModeDetector`            | removed                                     |
| `Flow\Bridge\Symfony\TelemetryBundle\Runtime\EnvironmentWorkerModeDetector` | removed                                     |
| shutdown on `kernel.terminate`/`console.terminate` (classic mode)           | flush on terminate; shutdown at process end |

Drop the `runtime_mode` key from `flow_telemetry` config and remove any `WorkerModeDetector` service overrides.

### 16) `flow-php/telemetry` - `Telemetry::registerShutdownFunction()` holds a weak reference

| Before                                           | After                                                         |
|--------------------------------------------------|---------------------------------------------------------------|
| strong reference; instance kept alive until exit | weak reference; garbage-collected instances are not shut down |

Keep the registered `Telemetry` instance referenced for as long as it should be shut down at process end.

### 17) `flow-php/etl` - `SchemaValidator::isValid()` replaced by `validate(): ValidationContext`

| Before                                        | After                                                            |
|-----------------------------------------------|------------------------------------------------------------------|
| `SchemaValidator::isValid(...): bool`         | `SchemaValidator::validate(...): ValidationContext`              |
| `$validator->isValid($expected, $given)`      | `$validator->validate($expected, $given)->isValid()`             |
| `schema_validate(...): bool`                  | `schema_validate(...): ValidationContext`                        |
| `new SchemaValidationException($exp, $given)` | `new SchemaValidationException($exp, $given, ValidationContext)` |
| -                                             | `SchemaValidationException::context(): ValidationContext`        |

Custom `SchemaValidator` implementations must return a `Flow\ETL\Schema\Validator\ValidationContext`
built from the missing, mismatched (`MismatchedDefinition`), and unexpected definitions they reject.

### 18) `flow-php/filesystem` - `Partition` name and value forbid `{` and `}`

| Before                           | After                             |
|----------------------------------|-----------------------------------|
| `new Partition('na{me', 'a}b')`  | throws `InvalidArgumentException` |
| `partitionBy()` values with `{}` | throws `InvalidArgumentException` |

`{name}` in a path is now a partition placeholder resolved from `partitionBy()` columns; strip braces from partition
values before partitioning.

### 19) `flow-php/etl` - `Cache` stores only `Rows`; cache indexes are stored as `Rows`

| Before                                            | After                                         |
|---------------------------------------------------|-----------------------------------------------|
| `Cache::get(string): Row\|Rows\|CacheIndex`       | `Cache::get(string): Rows`                    |
| `Cache::set(string, Row\|Rows\|CacheIndex): void` | `Cache::set(string, Rows): void`              |
| `$cache->set($id, $row)`                          | `$cache->set($id, rows($row))`                |
| `$cache->set($id, $cacheIndex)`                   | `$cache->set($id, $cacheIndex->toRows())`     |
| `$cache->get($id)` returning `CacheIndex`         | `CacheIndex::fromRows($id, $cache->get($id))` |

Custom `Cache` implementations must adopt the `Rows`-only signatures.

### 20) `flow-php/etl` - cache and serialization use the Floe (`.floe`) binary format; datetime subclasses rejected

| Before                                                                                 | After                                                                  |
|----------------------------------------------------------------------------------------|------------------------------------------------------------------------|
| `config_builder()->serializer()` default `Base64Serializer(new NativePHPSerializer())` | `Flow\Floe\FloeSerializer`                                             |
| `filesystem_cache(..., Serializer $serializer = new NativePHPSerializer())`            | `filesystem_cache(..., Serializer $serializer = new FloeSerializer())` |
| `new FilesystemCache($filesystem, $serializer, $cacheDir)`                             | `new FilesystemCache($filesystem, $cacheDir, $serializer)`             |
| caching or serializing a `DateTime`/`DateTimeImmutable` subclass (e.g. Carbon)         | throws `Flow\Floe\Exception\FloeException`                             |

Delete cache directories written by 0.41 - the old serialized payloads are unreadable. Convert
`DateTime`/`DateTimeImmutable` subclasses to `DateTime`/`DateTimeImmutable` before caching or serializing.

A Floe file carries exactly one schema, and sort / join / group-by spill batches through it - a pipeline with a drifting
or schemaless source must declare column types at the source so nullable columns carry a concrete type
(`DataFrame::match($schema)` validates but does not retype values):

```php
->read(from_array($data, schema(str_schema('email'), float_schema('discount', nullable: true))))
```

### 21) `flow-php/symfony-telemetry-bundle` - `HttpKernelSpanSubscriber` takes a

`RouteNamePathMap` instead of the router

| Before                                                    | After                                                                   |
|-----------------------------------------------------------|-------------------------------------------------------------------------|
| `new HttpKernelSpanSubscriber(..., router: $router, ...)` | `new HttpKernelSpanSubscriber(..., routePaths: $routeNamePathMap, ...)` |
| `?RouterInterface $router = null`                         | `?RouteNamePathMap $routePaths = null`                                  |

Applies only to direct construction; services wired by the bundle need no change.

### 22) `flow-php/etl` - sort algorithm is an explicit choice; external sort is the default

| Before                                                          | After                                                                     |
|-----------------------------------------------------------------|---------------------------------------------------------------------------|
| `SortAlgorithms` enum                                           | removed; `ConfigBuilder::sort(memory_sort()` / `external_sort())`         |
| `SortAlgorithms::MEMORY_FALLBACK_EXTERNAL_SORT` (default)       | removed; default is `external_sort()`                                     |
| `SortAlgorithms::SQLITE_SORT`                                   | removed                                                                   |
| `SortAlgorithms::useMemory()`                                   | removed                                                                   |
| `ConfigBuilder::sortMemoryLimit(Unit $unit)`                    | removed                                                                   |
| `SortConfigBuilder`                                             | removed; `external_sort()` builder                                        |
| `SortConfigBuilder::sortMemoryLimit(Unit $unit)`                | removed                                                                   |
| `SortConfig`                                                    | `MemorySortConfig` \| `ExternalSortConfig` (`Config::$sort`)              |
| `SortConfig::$memoryLimit`                                      | removed                                                                   |
| `SortConfig::SORT_MAX_MEMORY_ENV` / `FLOW_SORT_MAX_MEMORY` env  | removed                                                                   |
| `new MemorySort(Unit $maximumMemory)`                           | removed; `sort(memory_sort())`                                            |
| `MemorySort` throwing `Flow\ETL\Exception\OutOfMemoryException` | removed                                                                   |
| `new ExternalSort($cache, $bucketsCount)`                       | removed; `sortBy()` pipelines `BucketingProcessor` + `MergeSortProcessor` |
| `ConfigBuilder::externalSortFilesystem(string)`                 | `external_sort()->filesystemProtocol(string)`                             |
| -                                                               | `external_sort()->runSize(int)` (default `10000`)                         |
| -                                                               | `external_sort()->bucketsCount(int)` (default `100`)                      |
| -                                                               | `external_sort()->batchSize(int)` (default `1000`)                        |
| -                                                               | `external_sort()->storage(BucketsStorage)` (default `FilesystemBuckets`)  |

To sort in memory, opt in explicitly:

```php
data_frame(config_builder()->sort(memory_sort()))->read(...)->sortBy(ref('id'))->run();
```

### 23) `flow-php/etl` - joins emit every matching right row and follow SQL semantics

| Before                                                                | After                                                                  |
|-----------------------------------------------------------------------|------------------------------------------------------------------------|
| inner/left/right join: first matching row per probe row               | one output row per matching pair                                       |
| left/left_anti join: left row dropped on hash collision without match | left row kept                                                          |
| duplicated right side rows collapsed into one                         | preserved                                                              |
| `DataFrame::join(..., Join::inner)`: duplicated join columns kept     | duplicated join columns dropped (empty prefix)                         |
| `Equal` on object values (Uuid, etc.): always `false`                 | compared with `==`                                                     |
| `Flow\ETL\Processor\HashJoin\HashTable`                               | `Flow\ETL\Join\HashJoin\HashTable`                                     |
| `Flow\ETL\Processor\HashJoin\Bucket`                                  | removed                                                                |
| -                                                                     | `Expression::comparison()`, `All::comparisons()`, `Any::comparisons()` |

`Rows::joinRight()` emits matched rows in left-probe order and unmatched right rows last.

### 24) `flow-php/etl` - join buckets storage is configurable, spills to disk by default

| Before                                                    | After                                                                   |
|-----------------------------------------------------------|-------------------------------------------------------------------------|
| `join()` holds right side in memory, keeps left row order | join buckets spilled to disk (Floe files), left row order not preserved |
| -                                                         | `ConfigBuilder::join(hash_join())`                                      |
| -                                                         | `hash_join()->storage(BucketsStorage)` (default `FilesystemBuckets`)    |
| -                                                         | `hash_join()->bucketsCount(int)` (default `64`)                         |
| -                                                         | `hash_join()->batchSize(int)` (default `1000`)                          |
| -                                                         | `Flow\ETL\Bucketing\ResidentBucketsStorage`                             |
| -                                                         | `Flow\ETL\Bucketing\Storage\MemoryBuckets`                              |

To keep the right side in memory and preserve left row order:

```php
data_frame(config_builder()->join(hash_join()->storage(new MemoryBuckets())))
    ->read(...)
    ->join(...)
    ->run();
```

### 25) `flow-php/etl` - `Serializer` works on `Rows` and filesystem streams

| Before                                                                                    | After                                                                     |
|-------------------------------------------------------------------------------------------|---------------------------------------------------------------------------|
| `Serializer::serialize(object $serializable): string`                                     | `Serializer::serialize(Rows $rows, DestinationStream $destination): void` |
| `Serializer::unserialize(string $serialized, array $classes): object`                     | `Serializer::unserialize(SourceStream $source): Rows`                     |
| `CompressingSerializer`/`NativePHPSerializer` throw `Flow\ETL\Exception\RuntimeException` | throw `Flow\Serializer\Exception\SerializationException`                  |
| -                                                                                         | `Flow\Floe\FloeSerializer` (the default)                                  |

### 26) `flow-php/etl` - `EntryFactory` API reshaped

| Before                                                                                            | After                                                                                              |
|---------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------|
| `EntryFactory::create(string $entryName, mixed $value, Schema\|Definition\|null $schema = null)`  | `EntryFactory::create(string $name, mixed $value, ?Type $type = null, ?Metadata $metadata = null)` |
| `EntryFactory::createAs(string $entryName, mixed $value, Type $type, ?Metadata $metadata = null)` | `EntryFactory::cast(string $name, mixed $value, Type $type, ?Metadata $metadata = null)`           |
| -                                                                                                 | `EntryFactory::fromDefinition(Definition $definition, mixed $value): Entry`                        |
| `to_entry($name, $data, EntryFactory $entryFactory)`                                              | `to_entry($name, $data, EntryFactory $entryFactory = new EntryFactory())`                          |
| `ConfigBuilder::build(EntryFactory $entryFactory = new EntryFactory())`                           | `ConfigBuilder::build()`                                                                           |

### 27) `flow-php/etl` - `Encoder`/`Hydrator` contracts; hydrator configured on the context

| Before                                                  | After                                                                                                                                                     |
|---------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------|
| -                                                       | `Flow\ETL\Row\Encoder<TPhysical>`: `encode(list<TypedRowValues>): list<TPhysical>`, `decode(list<TPhysical>): list<RawRowValues>`                         |
| -                                                       | `Flow\ETL\Row\Hydrator`: `cast(list<RawRowValues>, ?Schema): Rows`, `hydrate(list<RawRowValues>, ?Schema): Rows`, `dehydrate(Rows): list<TypedRowValues>` |
| -                                                       | `Flow\ETL\Row\RawRowValues`, `Flow\ETL\Row\TypedRowValues`                                                                                                |
| -                                                       | `Flow\ETL\Row\PhpRowHydrator`, `NativeRowHydrator`, `AdaptiveRowHydrator` (the default)                                                                   |
| -                                                       | `ConfigBuilder::hydrator(Hydrator)`, `Config::hydrator()`, `FlowContext::hydrator()`                                                                      |
| `array_to_row($data, EntryFactory $entryFactory, ...)`  | `array_to_row($data, Hydrator $hydrator = new AdaptiveRowHydrator(), ...)`                                                                                |
| `array_to_rows($data, EntryFactory $entryFactory, ...)` | `array_to_rows($data, Hydrator $hydrator = new AdaptiveRowHydrator(), ...)`                                                                               |

`cast()` coerces untrusted values through the schema (`null` schema infers types); `hydrate()` instantiates trusted,
already-typed values and requires a schema. A custom extractor reading raw values (CSV, JSON, XML, Excel, Text, Google
Sheet, PostgreSQL, Doctrine) calls `$context->hydrator()->cast(...)`; a self-describing source (Parquet, Floe)
calls `$context->hydrator()->hydrate(...)`.

Under a schema, entries emit in schema-definition order (was data-key order with missing columns appended); a schema
column missing from the source hydrates as a typed null (was a `StringEntry` null); a column absent from the schema is
dropped. When the `flow_php` extension is loaded, `AdaptiveRowHydrator` runs hydration natively - pin the pure-PHP
engine with `config_builder()->hydrator(new PhpRowHydrator())`.

### 28) `flow-php/etl-adapter-csv`, `-json`, `-parquet`, `-xml`, `-excel`, `-doctrine`, `-postgresql`, `-seal`, `-text`,

`-google-sheet` - per-format `Encoder`s replace normalizers

| Before                                                                                                                       | After                                                                                                                                            |
|------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------|
| `Flow\ETL\Adapter\CSV\RowsNormalizer`, `RowsNormalizer\EntryNormalizer`, `RowsNormalizer\ScalarCast`                         | `Flow\ETL\Adapter\CSV\CSVEncoder`                                                                                                                |
| `Flow\ETL\Adapter\JSON\RowsNormalizer`, `RowsNormalizer\EntryNormalizer`                                                     | `Flow\ETL\Adapter\JSON\JSONEncoder`                                                                                                              |
| `Flow\ETL\Adapter\Parquet\RowsNormalizer`                                                                                    | `Flow\ETL\Adapter\Parquet\ParquetEncoder`                                                                                                        |
| `Flow\ETL\Adapter\XML\RowsNormalizer`, `RowsNormalizer\EntryNormalizer`, `RowsNormalizer\EntryNormalizer\PHPValueNormalizer` | `Flow\ETL\Adapter\XML\XMLEncoder`                                                                                                                |
| `Flow\ETL\Adapter\Excel\RowsNormalizer\ExcelRowsNormalizer`                                                                  | `Flow\ETL\Adapter\Excel\ExcelEncoder`                                                                                                            |
| `Flow\ETL\Adapter\Doctrine\RowsNormalizer`                                                                                   | `Flow\ETL\Adapter\Doctrine\DbalEncoder`                                                                                                          |
| `Flow\ETL\Adapter\Seal\RowsNormalizer`, `RowsNormalizer\EntryNormalizer`                                                     | `Flow\ETL\Adapter\Seal\SealEncoder`                                                                                                              |
| -                                                                                                                            | `Flow\ETL\Adapter\Text\TextEncoder`, `Flow\ETL\Adapter\PostgreSql\PostgreSqlEncoder`, `Flow\ETL\Adapter\GoogleSheet\GoogleSheetEncoder`          |
| `Flow\ETL\Adapter\Doctrine\TypesMap::flowRowTypes(Row)`                                                                      | `TypesMap::flowSchemaTypes(Schema)`                                                                                                              |
| `Flow\ETL\Adapter\PostgreSql\EntryTypesMap::mapEntry(Entry)`                                                                 | `EntryTypesMap::map(string $column, Type $type, mixed $value)`                                                                                   |
| `InsertQueryBuilder::build(Rows, ...)`                                                                                       | `InsertQueryBuilder::build(array $values, Schema, ...)`                                                                                          |
| `UpdateQueryBuilder::build(Row, ...)`, `DeleteQueryBuilder::build(Row, ...)`                                                 | `build(array $value, Schema, ...)`                                                                                                               |
| CSV/JSON/XML/Excel/Seal writers (no `dateFormat`)                                                                            | `dateFormat = 'Y-m-d'` encoder constructor param                                                                                                 |
| Excel `timeFormat = 'H:i:s'`                                                                                                 | `timeFormat = '%H:%I:%S'`                                                                                                                        |
| -                                                                                                                            | `withDateFormat()` on `CSVLoader`, `JsonLoader`, `JsonLinesLoader`, `XMLLoader`, `ExcelLoader`, `SealLoader`; `SealLoader::withDateTimeFormat()` |

Behavioural changes: CSV/JSON/XML/Excel/Seal `date` columns render with `dateFormat` (`Y-m-d`, was `dateTimeFormat`);
XML `date`/`time` columns render (was `InvalidArgumentException`); Seal `time` columns render as microseconds (was
`null`); Google Sheet `_spread_sheet_id`/`_sheet_name` are appended to the schema under `putInputIntoRows()` (were
dropped when a schema was set); Excel now appends `_input_file_uri` under `putInputIntoRows()`.

### 29) `flow-php/etl` - null is a first-class `NullEntry`/`NullDefinition`; `from_null` metadata removed

| Before                                                                    | After                                                                                |
|---------------------------------------------------------------------------|--------------------------------------------------------------------------------------|
| `null_entry($name)` → `StringEntry` (type `string`, `from_null` metadata) | `null_entry($name)` → `Flow\ETL\Row\Entry\NullEntry` (type `null`)                   |
| `null_schema($name)` → `StringDefinition` + `from_null` metadata          | `null_schema($name)` → `Flow\ETL\Schema\Definition\NullDefinition` (always nullable) |
| `Flow\ETL\Row\Entry\StringEntry::fromNull($name, $metadata)`              | `null_entry($name, $metadata)`                                                       |
| `new StringEntry($name, $value, $metadata, fromNull: true)`               | `$fromNull` constructor flag removed                                                 |
| `Flow\ETL\Schema\Metadata::FROM_NULL`                                     | removed                                                                              |

### 30) `flow-php/etl` - group by runs on bucketing storage; only referenced columns are spilled

| Before                                                                     | After                                                                                |
|----------------------------------------------------------------------------|--------------------------------------------------------------------------------------|
| grouping state held in memory                                              | rows partitioned into buckets through `BucketsStorage` (default `FilesystemBuckets`) |
| rows carry all columns into grouping                                       | only `groupBy` refs + aggregator refs are spilled                                    |
| STRICT schema mode throws for a missing aggregate column under `groupBy()` | row gets a `null` entry for it; aggregators skip `null`                              |
| group output follows input order (incidental)                              | follows hash-bucket order                                                            |
| -                                                                          | `AggregatingFunction::references(): ?array`                                          |
| -                                                                          | `ConfigBuilder::groupBy(hash_group_by())`                                            |
| -                                                                          | `hash_group_by()->storage(BucketsStorage)` (default `FilesystemBuckets`)             |
| -                                                                          | `hash_group_by()->bucketsCount(int)` (default `64`)                                  |
| -                                                                          | `hash_group_by()->batchSize(int)` (default `1000`)                                   |

Custom aggregators must implement `references()` - return the references the aggregator reads, or
`null` when they cannot be statically enumerated (disables spill column pruning).

### 31) `flow-php/etl` - `DataFrame::pivot()` removed; pivot is declared between `groupBy()` and `aggregate()`

| Before                                            | After                                             |
|---------------------------------------------------|---------------------------------------------------|
| `->groupBy(...)->aggregate(...)->pivot(ref('x'))` | `->groupBy(...)->pivot(ref('x'))->aggregate(...)` |
| `DataFrame::pivot()`                              | removed; `GroupedDataFrame::pivot()` only         |

### 32) `flow-php/etl-adapter-csv`, `-excel`, `-json`,

`-xml` - explicit schema no longer projects partition columns away

| Before                                                           | After                                                |
|------------------------------------------------------------------|------------------------------------------------------|
| partition columns undeclared in the schema are dropped from rows | force-added to rows as non-nullable `string` columns |

Declare the partition column in the schema to control its type.

### 33) `flow-php/etl` - `Schema` and `Schema\Definition` mutators return a new instance

| Before                                  | After                                                |
|-----------------------------------------|------------------------------------------------------|
| `$schema->add(str_schema('x'));`        | `$schema = $schema->add(str_schema('x'));`           |
| `$schema->addMetadata('id', 'k', 'v');` | `$schema = $schema->addMetadata('id', 'k', 'v');`    |
| `$definition->setMetadata($metadata);`  | `$definition = $definition->setMetadata($metadata);` |

Assign the result of every `Schema` mutator - `add`, `addAfter`, `addBefore`, `addMetadata`, `gracefulRemove`,
`insertAt`, `keep`, `makeNullable`, `merge`, `moveAfter`, `moveBefore`, `moveTo`, `prepend`, `remove`, `rename`,
`reorder`, `replace`, `setMetadata`, `sort` - and of `Definition::addMetadata()` / `Definition::setMetadata()`.
Discarding it is a silent no-op.
