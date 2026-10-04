# Upgrade Guide 0.35.x - 0.40.x

[TOC]

This document provides guidelines for upgrading between versions of Flow PHP. Please follow the instructions for your
specific version to ensure a smooth upgrade process.

Upgrading from older versions: [0.16.x to 0.35.x](/documentation/upgrading-0.16-0.35.md)

---

## Upgrading from 0.39.x to 0.40.x

### 1) `flow-php/postgresql` - column and domain defaults are modeled as `ColumnDefault`

| Before                                                        | After                                                                       |
|---------------------------------------------------------------|-----------------------------------------------------------------------------|
| `Column::$default` / `Domain::$default` type `?string`        | `?Flow\PostgreSql\Schema\ColumnDefault`                                     |
| `new Column('c', $type, true, "'0'")`                         | `new Column('c', $type, true, ColumnDefault::fromExpression("'0'", $type))` |
| `$column->default` (string)                                   | `$column->default?->literal` / `$column->default?->applicableSql()`         |
| `ColumnShape['default']` / `DomainShape['default']` `?string` | `?array{literal: string, type: ?ColumnTypeShape, kind: string}`             |

`Column::create()` / `Domain::create()` still accept `bool|float|int|string|Expression|null`. Schema arrays serialized
by `Column::normalize()` / `Domain::normalize()` before 0.40 must be regenerated - `fromArray()` reads the nested
`default` shape only.

### 2) `flow-php/telemetry` - Log severity filtering moved to a pipeline middleware

| Before                                                          | After                                                                                   |
|-----------------------------------------------------------------|-----------------------------------------------------------------------------------------|
| `Flow\Telemetry\Logger\Processor\SeverityFilteringLogProcessor` | `Flow\Telemetry\Logger\Middleware\SeverityFilteringLogMiddleware`                       |
| `new SeverityFilteringLogProcessor($inner, $minSeverity)`       | `new PipelineLogProcessor([new SeverityFilteringLogMiddleware($minSeverity)], $inner)`  |
| `severity_filtering_log_processor($processor, $minSeverity)`    | `pipeline_log_processor([severity_filtering_log_middleware($minSeverity)], $processor)` |

The previously wrapped processor is now the pipeline's **sink** and must implement
`Flow\Telemetry\Logger\LogSink` (the built-in `batching`, `pass_through`, `memory`, `void` and `composite`
log processors already do).

Before:

```php
$processor = severity_filtering_log_processor(
    batching_log_processor($exporter),
    Severity::WARN,
);
```

After:

```php
$processor = pipeline_log_processor(
    [severity_filtering_log_middleware(Severity::WARN)],
    batching_log_processor($exporter),
);
```

### 2) `flow-php/symfony-telemetry-bundle` - `severity_filtering` log processor type replaced by `pipeline`

The `severity_filtering` processor type (with its `inner_processor`) is no longer a `logger_provider`
processor type; it is a middleware inside a `pipeline`.

Before:

```yaml
flow_telemetry:
  logger_provider:
    processor:
      type: severity_filtering
      minimum_severity: warn
      inner_processor:
        type: batching
        exporter: otlp
```

After:

```yaml
flow_telemetry:
  logger_provider:
    processor:
      type: pipeline
      middleware:
        - { type: severity_filtering, minimum_severity: warn }
      sink:
        type: batching
        exporter: otlp
```

### 3) `flow-php/symfony-telemetry-bundle` - named scope `attributes` split into `scope` and `signal`

Applies to `tracers`, `meters`, and `loggers`. The `attributes` map is no longer a flat list of scope attributes; scope
attributes move under `attributes.scope`.

Before:

```yaml
flow_telemetry:
  loggers:
    audit:
      attributes:
        team: checkout
```

After:

```yaml
flow_telemetry:
  loggers:
    audit:
      attributes:
        scope:
          team: checkout
```

### 4) `flow-php/postgresql` - `DateTimeConverter` split into `TimestampConverter` and `TimestampTzConverter`

| Before                                                     | After                                                                         |
|------------------------------------------------------------|-------------------------------------------------------------------------------|
| `Flow\PostgreSql\Client\Types\Converter\DateTimeConverter` | `TimestampConverter` (`TIMESTAMP`) and `TimestampTzConverter` (`TIMESTAMPTZ`) |
| `typed($value, ValueType::TIMESTAMP)` keeps the offset     | `typed($value, ValueType::TIMESTAMP)` normalizes the value to UTC             |
| `timestamp` column read as `2024-01-15 10:30:00`           | `timestamp` column read as `2024-01-15 10:30:00+00:00`                        |

### 5) `flow-php/etl-adapter-postgresql` - `DateTimeEntry` maps to `timestamp` instead of `timestamptz`

| Flow type                 | Before        | After       |
|---------------------------|---------------|-------------|
| `DateTimeEntry` (binding) | `TIMESTAMPTZ` | `TIMESTAMP` |
| `DateTimeType` (DDL)      | `timestamptz` | `timestamp` |

To keep the previous behavior, pass overrides to `EntryTypesMap`:

```php
to_pgsql_table($client, 'users')->withTypesMap(new EntryTypesMap(
    [DateTimeEntry::class => ValueType::TIMESTAMPTZ],
    [DateTimeType::class => ColumnType::timestamptz()],
));
```

### 6) `flow-php/symfony-postgresql-messenger` - `messenger_messages` time columns use `timestamp` instead of

`timestamptz`

| Column type for `created_at`, `available_at`, `delivered_at` | Before        | After       |
|--------------------------------------------------------------|---------------|-------------|
| `MessengerCatalogProvider` (DDL)                             | `timestamptz` | `timestamp` |
| `Connection` bindings                                        | `TIMESTAMPTZ` | `TIMESTAMP` |

Existing tables, realign the column type (UTC instants preserved):

```sql
ALTER TABLE messenger_messages
    ALTER COLUMN created_at TYPE TIMESTAMP USING created_at AT TIME ZONE 'UTC',
    ALTER COLUMN available_at TYPE TIMESTAMP USING available_at AT TIME ZONE 'UTC',
    ALTER COLUMN delivered_at TYPE TIMESTAMP USING delivered_at AT TIME ZONE 'UTC';
```

### 7) `flow-php/telemetry` - `OTEL_RESOURCE_ATTRIBUTES` keys and values are percent-decoded, not backslash-escaped

| Escaping a `,` or `=` in `OTEL_RESOURCE_ATTRIBUTES` | Before                    | After                       |
|-----------------------------------------------------|---------------------------|-----------------------------|
| literal comma in a value                            | `key=value\,with\,commas` | `key=value%2Cwith%2Ccommas` |
| literal `=` in a value                              | not supported             | `key=a%3Db`                 |

Re-encode any `OTEL_RESOURCE_ATTRIBUTES` that relied on backslash escaping; both keys and values are now
percent-decoded.

---

## Upgrading from 0.37.x to 0.38.x

### 1) `flow-php/types` - PHPStan extension extracted to `flow-php/phpstan-types-bridge`

The `StructureTypeReturnTypeExtension` - which narrows the return type of `type_structure()` for PHPStan - has been
moved out of `flow-php/types` into a dedicated package, `flow-php/phpstan-types-bridge`.
`flow-php/types` no longer ships any PHPStan code.

| Before                                                | After                                                        |
|-------------------------------------------------------|--------------------------------------------------------------|
| `Flow\Types\PHPStan\StructureTypeReturnTypeExtension` | `Flow\Bridge\PHPStan\Types\StructureTypeReturnTypeExtension` |
| shipped inside `flow-php/types`                       | shipped inside `flow-php/phpstan-types-bridge`               |

If you used `type_structure()` together with PHPStan, install the new package:

```
composer require --dev flow-php/phpstan-types-bridge
```

With [phpstan/extension-installer](https://github.com/phpstan/extension-installer) the extension is registered
automatically. If you registered it manually, update your `phpstan.neon`:

Before:

```neon
services:
    -
        class: Flow\Types\PHPStan\StructureTypeReturnTypeExtension
        tags:
            - phpstan.broker.dynamicFunctionReturnTypeExtension
```

After:

```neon
includes:
    - vendor/flow-php/phpstan-types-bridge/extension.neon
```

---

## Upgrading from 0.36.x to 0.37.x

### 1) `flow-php/telemetry` - Per-signal exporter contracts merged into `Exporter`

| Before                                                                             | After                                                            |
|------------------------------------------------------------------------------------|------------------------------------------------------------------|
| `Flow\Telemetry\Tracer\SpanExporter` (interface)                                   | `Flow\Telemetry\Exporter\Exporter`                               |
| `Flow\Telemetry\Meter\MetricExporter` (interface)                                  | `Flow\Telemetry\Exporter\Exporter`                               |
| `Flow\Telemetry\Logger\LogExporter` (interface)                                    | `Flow\Telemetry\Exporter\Exporter`                               |
| `VoidSpanExporter`, `VoidMetricExporter`, `VoidLogExporter`                        | `Flow\Telemetry\Provider\Void\VoidExporter`                      |
| `MemorySpanExporter`, `MemoryMetricExporter`, `MemoryLogExporter`                  | `Flow\Telemetry\Provider\Memory\MemoryExporter`                  |
| `ConsoleSpanExporter`, `ConsoleMetricExporter`, `ConsoleLogExporter`               | `Flow\Telemetry\Provider\Console\ConsoleExporter`                |
| `void_span_exporter()` / `void_metric_exporter()` / `void_log_exporter()`          | `void_exporter()`                                                |
| `memory_span_exporter()` / `memory_metric_exporter()` / `memory_log_exporter()`    | `memory_exporter()`                                              |
| `console_span_exporter()` / `console_metric_exporter()` / `console_log_exporter()` | `console_exporter()`                                             |
| `MemoryLogExporter::entries()`                                                     | `MemoryExporter::logs()`                                         |
| `MemorySpanExporter::spans()`                                                      | `MemoryExporter::spans()`                                        |
| `MemoryMetricExporter::metrics()`                                                  | `MemoryExporter::metrics()`                                      |
| `Exporter::transports()`                                                           | removed                                                          |
| `(Span\|Metric\|Log)Exporter::export(array $items) : bool`                         | `Exporter::export(Flow\Telemetry\Signal\Signals $signal) : bool` |
| -                                                                                  | `Exporter::shutdown() : void` (added)                            |

### 2) `flow-php/telemetry` - `Transport` contract relocated to OTLP bridge

| Before                                                    | After                                                     |
|-----------------------------------------------------------|-----------------------------------------------------------|
| `Flow\Telemetry\Transport\Transport`                      | `Flow\Bridge\Telemetry\OTLP\Transport\Transport`          |
| `Flow\Telemetry\Transport\TransportException`             | `Flow\Bridge\Telemetry\OTLP\Transport\TransportException` |
| `Flow\Telemetry\Transport\VoidTransport`                  | removed                                                   |
| `Transport::sendSpans()` / `sendMetrics()` / `sendLogs()` | `Transport::send(Flow\Telemetry\Signal\Signals $signal)`  |
| `Flow\Bridge\Telemetry\OTLP\Exception\Exception`          | removed                                                   |
| `Flow\Bridge\Telemetry\OTLP\Exception\TransportException` | `Flow\Bridge\Telemetry\OTLP\Transport\TransportException` |

### 3) `flow-php/telemetry` - Processor interfaces

Applies to `SpanProcessor`, `MetricProcessor`, `LogProcessor`.

| Before                             | After                       |
|------------------------------------|-----------------------------|
| `exporter() : SpanExporter` (etc.) | removed                     |
| -                                  | `shutdown() : void` (added) |

### 4) `flow-php/telemetry` - `Serializer` contract removed

| Before                                                                 | After                                                                                                         |
|------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------|
| `Flow\Telemetry\Serializer\Serializer`                                 | removed                                                                                                       |
| `Flow\Bridge\Telemetry\OTLP\Serializer\GrpcSerializer` (interface)     | renamed to `Flow\Bridge\Telemetry\OTLP\Serializer\GrpcRequestFactory` (class)                                 |
| `CurlTransport(string $endpoint, Serializer $serializer, ...)`         | `CurlTransport(string $endpoint, JsonSerializer\|ProtobufSerializer $serializer = new JsonSerializer(), ...)` |
| `GrpcTransport(string $endpoint, ProtobufSerializer $serializer, ...)` | `GrpcTransport(string $endpoint, ...)` - serializer parameter removed                                         |

### 5) `flow-php/telemetry` - `ErrorHandler` contract added

New namespace `Flow\Telemetry\ErrorHandler` with: `ErrorHandler` (interface), `ErrorLogHandler`, `NullErrorHandler`,
`StreamHandler`, `SyslogHandler`, `UdpSyslogHandler`, `CompositeErrorHandler`.

| Before                                                              | After                                                                                                         |
|---------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------|
| `otlp_exporter($transport)`                                         | `otlp_exporter($transport, ErrorHandler $errorHandler = new ErrorLogHandler())`                               |
| `telemetry_handler($logger, $converter, $level, $bubble)` (Monolog) | `telemetry_handler($logger, $converter, $level, $bubble, ErrorHandler $errorHandler = new ErrorLogHandler())` |

### 6) `flow-php/telemetry-otlp-bridge` - `HttpTransport` removed

| Before                                               | After                                 |
|------------------------------------------------------|---------------------------------------|
| `Flow\Bridge\Telemetry\OTLP\Transport\HttpTransport` | removed                               |
| `otlp_http_transport()`                              | removed (use `otlp_curl_transport()`) |
| `psr/http-client` (runtime require)                  | removed (dev only)                    |
| `psr/http-factory` (runtime require)                 | removed (dev only)                    |

### 7) `flow-php/telemetry-otlp-bridge` - Per-signal OTLP exporters merged

| Before                                                      | After                                              |
|-------------------------------------------------------------|----------------------------------------------------|
| `OTLPSpanExporter`, `OTLPMetricExporter`, `OTLPLogExporter` | `Flow\Bridge\Telemetry\OTLP\Exporter\OTLPExporter` |
| `otlp_span_exporter($transport)`                            | `otlp_exporter($transport)`                        |
| `otlp_metric_exporter($transport)`                          | `otlp_exporter($transport)`                        |
| `otlp_log_exporter($transport)`                             | `otlp_exporter($transport)`                        |

### 8) `flow-php/telemetry-otlp-bridge` - Curl/gRPC timeouts switched to milliseconds

| Before                                                                                                              | After                                                                                                                                                                                                   |
|---------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `CurlTransportOptions::withTimeout(int $seconds)`, default `30`                                                     | `CurlTransportOptions::withTimeout(int $milliseconds)`, default `250`                                                                                                                                   |
| `CurlTransportOptions::withConnectTimeout(int $seconds)`, default `10`                                              | `CurlTransportOptions::withConnectTimeout(int $milliseconds)`, default `250`                                                                                                                            |
| `CurlTransportOptions::timeout()`                                                                                   | `CurlTransportOptions::timeoutMs()`                                                                                                                                                                     |
| `CurlTransportOptions::connectTimeout()`                                                                            | `CurlTransportOptions::connectTimeoutMs()`                                                                                                                                                              |
| -                                                                                                                   | `CurlTransportOptions::withShutdownTimeout(int $milliseconds)` / `shutdownTimeoutMs()`, default `5000` (added)                                                                                          |
| `otlp_curl_transport(string $endpoint, Serializer $serializer, CurlTransportOptions $options)`                      | `otlp_curl_transport(string $endpoint, JsonSerializer\|ProtobufSerializer $serializer = new JsonSerializer(), CurlTransportOptions $options = new CurlTransportOptions(), ?Transport $failover = null)` |
| `otlp_grpc_transport(string $endpoint, ProtobufSerializer $serializer, array $headers = [], bool $insecure = true)` | `otlp_grpc_transport(string $endpoint, array $headers = [], bool $insecure = true, int $timeoutMs = 250, int $shutdownTimeoutMs = 5000, ?Transport $failover = null)`                                   |

### 9) `flow-php/telemetry-otlp-bridge` - `StreamTransport` added

New transport, no removal counterpart.

| Before | After                                                                                                     |
|--------|-----------------------------------------------------------------------------------------------------------|
| -      | `Flow\Bridge\Telemetry\OTLP\Transport\StreamTransport`                                                    |
| -      | `otlp_stream_transport(string $destination, int $filePermissions = 0644, bool $createDirectories = true)` |

### 10) `flow-php/telemetry-otlp-bridge` - `open-telemetry/gen-otlp-protobuf` dependency dropped

| Before                                            | After                                                                                                  |
|---------------------------------------------------|--------------------------------------------------------------------------------------------------------|
| `open-telemetry/gen-otlp-protobuf` (required dep) | removed; protobuf classes shipped inside the bridge under the same `Opentelemetry\Proto\...` namespace |

### 11) `flow-php/symfony-telemetry-bundle` - Configuration schema rewrite

No BC shim. Configurations from 0.36 must be rewritten.

| Before                                                                                          | After                                                                                  |
|-------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------|
| `exporters.<name>.type: otlp\|service\|console\|memory\|void`                                   | sub-block keyed by implementation: `otlp:`, `service:`, `console:`, `memory:`, `void:` |
| `exporters.<name>.service_id: <id>` (under `type: service`)                                     | `exporters.<name>.service: { id: <id> }`                                               |
| Inline `processor.exporter: { type: otlp, transport: {...} }`                                   | `processor.exporter: <name>` referencing top-level `exporters:` map                    |
| `transport.type: http`                                                                          | removed                                                                                |
| `transport.timeout` (seconds, default `30`)                                                     | `transport.timeout_ms` (default `250`)                                                 |
| `transport.connect_timeout` (seconds, default `10`)                                             | `transport.connect_timeout_ms` (default `250`)                                         |
| `transport.http_client_service_id` / `request_factory_service_id` / `stream_factory_service_id` | removed                                                                                |
| -                                                                                               | `transport.type: stream` (added)                                                       |
| -                                                                                               | `transport.shutdown_timeout_ms` default `5000` (added)                                 |
| -                                                                                               | `transport.failover: { type: ..., ... }` on curl/grpc primaries (added)                |
| -                                                                                               | top-level `error_handlers:` map (added)                                                |
| -                                                                                               | `error_handler: <name>` references on providers, processors, otlp exporter (added)     |
| -                                                                                               | top-level `framework_logger: <name>` (added)                                           |

Before:

```yaml
flow_telemetry:
  exporters:
    otlp:
      type: otlp
      transport:
        type: curl
        endpoint: 'http://otel-collector:4318'
        timeout: 30
        connect_timeout: 10
    custom: { type: service, service_id: 'app.x' }
    debug: { type: console }

  tracer_provider:
    processor:
      type: batching
      exporter: { type: otlp, transport: { type: curl, endpoint: 'http://otel-collector:4318' } }
```

After:

```yaml
flow_telemetry:
  error_handlers:
    default: { type: error_log }

  exporters:
    otlp:
      otlp:
        transport:
          type: curl
          endpoint: 'http://otel-collector:4318'
          timeout_ms: 250
          connect_timeout_ms: 250
    custom: { service: { id: 'app.x' } }
    debug: { console: ~ }

  tracer_provider:
    processor:
      type: batching
      exporter: otlp
```

### 12) `flow-php/phpunit-telemetry-bridge` - Configuration parameters

| Before (parameter / default)                     | After (parameter / default)                                                      |
|--------------------------------------------------|----------------------------------------------------------------------------------|
| `curl_timeout` / `30` (seconds)                  | `curl_timeout_ms` / `250` (ms)                                                   |
| `curl_connect_timeout` / `10` (s)                | `curl_connect_timeout_ms` / `250` (ms)                                           |
| `transport: curl\|grpc`                          | `transport: curl\|grpc\|stream`                                                  |
| -                                                | `grpc_timeout_ms` / `250` (added)                                                |
| -                                                | `shutdown_timeout_ms` / `5000` (added)                                           |
| -                                                | `batch_size` / `512` (added)                                                     |
| -                                                | `error_handler` / `error_log` and `error_handler_*` family (added)               |
| -                                                | `stream_file_permissions` / `0644`, `stream_create_directories` / `true` (added) |
| Default span/metric/log processors: pass-through | Default span/metric/log processors: batching (`batch_size: 512`)                 |

`otel_collector_url` / `FLOW_PHPUNIT_OTEL_COLLECTOR_URL` remain deprecated aliases for `endpoint` with
`transport=curl`.

---

## Upgrading from 0.35.x to 0.36.x

### 1) `flow-php/postgresql` - `RawCondition` and `RawExpression` removed

The `raw_cond()` and `raw_expr()` escape hatches have been removed. All query builder operations are now covered by
type-safe DSL functions.

| Removed                               | Replacement                                                               |
|---------------------------------------|---------------------------------------------------------------------------|
| `raw_cond('NOT col')`                 | `not_(is_true(col('col')))`                                               |
| `raw_cond('a = ANY(b)')`              | `any_(col('a'), ComparisonOperator::EQ, col('b'))`                        |
| `raw_cond("x IN ('a', 'b')")`         | `in_(col('x'), [literal('a'), literal('b')])`                             |
| `raw_cond("x NOT LIKE 'pg_%'")`       | `not_like(col('x'), literal('pg_%'))`                                     |
| `raw_expr('NOT col')`                 | `not_(col('col'))`                                                        |
| `raw_expr('a \|\| b')`                | `concat(col('a'), col('b'))` or `binary_expr(col('a'), '\|\|', col('b'))` |
| `raw_expr('CASE x WHEN ...')`         | `case_when([when(...)], operand: col('x'))`                               |
| `raw_expr('array_agg(DISTINCT ...)')` | `agg('array_agg', [...], distinct: true)->withOrderBy(...)`               |
| `RawCondition` class                  | Use specific condition classes                                            |
| `RawExpression` class                 | Use specific expression classes                                           |

### 2) `flow-php/postgresql` - `Condition` now extends `Expression`

Conditions are now expressions - they can be used in SELECT lists, CASE WHEN, ORDER BY, etc.

```php
// Conditions can now be aliased and used as expressions:
select(eq(col('a'), col('b'))->as('is_equal'));

// NOT works in both WHERE and SELECT:
not_(col('is_deleted'))->as('is_active');

// CASE WHEN accepts conditions directly:
case_when([when(eq(col('x'), literal(0)), literal('zero'))]);
```

### 3) `flow-php/postgresql` - DSL condition function renames

Function names have been unified following standard SQL builder conventions (jOOQ, Diesel, SQLAlchemy).

| Removed              | Replacement               | Reason                                                    |
|----------------------|---------------------------|-----------------------------------------------------------|
| `neq()`              | `ne()`                    | Standard short form                                       |
| `lte()`              | `le()`                    | Standard short form                                       |
| `gte()`              | `ge()`                    | Standard short form                                       |
| `is_in()`            | `in_()`                   | Drop `is_` prefix, trailing underscore for PHP keyword    |
| `is_distinct_from()` | `distinct_from()`         | Drop `is_` prefix                                         |
| `cond_and()`         | `and_()`                  | Drop `cond_` prefix, trailing underscore for PHP keyword  |
| `cond_or()`          | `or_()`                   | Drop `cond_` prefix, trailing underscore for PHP keyword  |
| `cond_not()`         | `not_()`                  | Drop `cond_` prefix, trailing underscore for PHP keyword  |
| `any_sub_select()`   | `any_()`                  | Unified - accepts both `Expression` and `SelectFinalStep` |
| `all_sub_select()`   | `all_()`                  | Unified - accepts both `Expression` and `SelectFinalStep` |
| `cond_true()`        | `is_true(literal(true))`  | Use `is_true()` with literal                              |
| `cond_false()`       | `is_true(literal(false))` | Use `is_true()` with literal                              |
| `bool_cond()`        | `is_true()`               | Wraps expression as boolean condition                     |
| `any_array()`        | `any_()`                  | Merged into unified `any_()`                              |
| `all_array()`        | `all_()`                  | Merged into unified `all_()`                              |

New functions added:

| Function                           | Purpose                                        |
|------------------------------------|------------------------------------------------|
| `is_true(Expression)`              | Wrap expression as boolean condition for WHERE |
| `not_like(Expression, Expression)` | NOT LIKE condition                             |
| `concat(Expression, ...)`          | String concatenation with `\|\|` operator      |

Before:

```php
use function Flow\PostgreSql\DSL\{cond_and, cond_not, cond_true, neq, lte, gte, is_in, any_sub_select};

select(col('name'))
    ->where(cond_and(
        neq(col('status'), literal('deleted')),
        lte(col('age'), literal(65)),
        gte(col('age'), literal(18)),
        is_in(col('role'), [literal('admin'), literal('user')]),
    ));
```

After:

```php
use function Flow\PostgreSql\DSL\{and_, ne, le, ge, in_};

select(col('name'))
    ->where(and_(
        ne(col('status'), literal('deleted')),
        le(col('age'), literal(65)),
        ge(col('age'), literal(18)),
        in_(col('role'), [literal('admin'), literal('user')]),
    ));
```

### 4) `flow-php/postgresql` - Schema builder methods accept `Expression`/`Condition` instead of strings

Methods that previously accepted raw SQL strings now require typed `Expression` or `Condition` objects.

| Method                                       | Before (string)                           | After (typed)                                           |
|----------------------------------------------|-------------------------------------------|---------------------------------------------------------|
| `ColumnDefinition::check()`                  | `->check('age > 0')`                      | `->check(gt(col('age'), literal(0)))`                   |
| `ColumnDefinition::defaultRaw()`             | `->defaultRaw('CURRENT_TIMESTAMP')`       | `->defaultRaw(current_timestamp())`                     |
| `ColumnDefinition::generatedAs()`            | `->generatedAs("a \|\| b")`               | `->generatedAs(concat(col('a'), col('b')))`             |
| `CheckConstraint::create()`                  | `::create('age > 0')`                     | `::create(gt(col('age'), literal(0)))`                  |
| `ExcludeConstraint::element()`               | `->element('col', '=')`                   | `->element(col('col'), '=')`                            |
| `ExcludeConstraint::where()`                 | `->where('active = true')`                | `->where(eq(col('active'), literal(true)))`             |
| `CreateDomainBuilder::check()`               | `->check('VALUE > 0')`                    | `->check(gt(col('VALUE'), literal(0)))`                 |
| `CreateDomainBuilder::default()`             | `->default("'text'")`                     | `->default(literal('text'))`                            |
| `AlterDomainBuilder::addConstraint()`        | `->addConstraint('name', 'VALUE > 0')`    | `->addConstraint('name', gt(col('VALUE'), literal(0)))` |
| `AlterDomainBuilder::setDefault()`           | `->setDefault('100')`                     | `->setDefault(literal(100))`                            |
| `AlterTableBuilder::alterColumnSetDefault()` | `->alterColumnSetDefault('col', "'val'")` | `->alterColumnSetDefault('col', literal('val'))`        |
| `CreateRuleBuilder::where()`                 | `->where("OLD.role = 'admin'")`           | `->where(eq(col('role', 'OLD'), literal('admin')))`     |

### 5) `flow-php/postgresql` - DSL functions split into separate files

The monolithic `functions.php` has been split into 5 focused files (same namespace, no import changes needed):

| File            | Purpose                                                                                    |
|-----------------|--------------------------------------------------------------------------------------------|
| `query.php`     | Query building, expressions, tables, ordering, CTE, window, locking, transactions, cursors |
| `condition.php` | Comparisons, predicates, logic, JSON/array/regex operators                                 |
| `schema.php`    | DDL, constraints, indexes, maintenance, privileges, types, schema definitions              |
| `client.php`    | Connections, telemetry, mappers                                                            |
| `parser.php`    | SQL parsing, formatting, analysis                                                          |

### 6) `flow-php/filesystem` - `Protocol` and `Backend` removed, `Mount` rewritten

The filesystem library has been redesigned around a single mount-protocol string. The `Protocol` and
`Backend` value types are gone; `Mount` now wraps just a protocol name.

**`Protocol` class removed.** `Path::protocol()` now returns `string` instead of a `Protocol` object. Callsites that
unpacked `Protocol::$name` / `Protocol::scheme()` / `Protocol::is()` are mechanical updates:

| Before                                   | After                                                                                              |
|------------------------------------------|----------------------------------------------------------------------------------------------------|
| `$path->protocol()->name`                | `$path->protocol()`                                                                                |
| `$path->protocol()->scheme()`            | `$path->protocol() . '://'`                                                                        |
| `$path->protocol()->is('file')`          | `$path->protocol() === 'file'`                                                                     |
| `$fs->protocol()->validateScheme($path)` | `$fs->mount()->supports($path) \|\| throw new InvalidSchemeException(...)`                         |
| `new Protocol('file')`                   | `new Mount('file')` (if you need a Mount) or plain `'file'` (FilesystemTable::for accepts strings) |

**`Backend` enum removed.** There's no closed set of backends anymore - any filesystem can mount under any protocol. The
Symfony bundle schema now uses a plain string `type:` field (see below). If you branched on `Backend` cases in
application code, replace with string comparisons against the factory `type()` or the mount protocol, whichever fits.

**`Mount` rewritten.** The shape is now:

```php
final readonly class Mount
{
    public string $protocol;

    public function __construct(string $protocol); // validates against PROTOCOL_REGEX
    public function supports(Path|string $path) : bool;
}
```

**`Filesystem::protocol()` renamed to `Filesystem::mount()`.** The return type changed from
`Protocol` to `Mount`. Every `Filesystem` implementation must rename the method.

**`Filesystem` ctors take `Mount` directly.** `NativeLocalFilesystem`, `MemoryFilesystem`,
`StdOutFilesystem`, `AsyncAWSS3Filesystem`, `AzureBlobFilesystem` now accept `Mount` as the first constructor argument
(local filesystems have a sensible default). DSL factory functions (`native_local_filesystem`, `memory_filesystem`,
`stdout_filesystem`, `aws_s3_filesystem`,
`azure_filesystem`) accept `string $protocol` as the **last** argument with a sensible default (`'file'`, `'memory'`,
`'stdout'`, `'aws-s3'`, `'azure-blob'`) and build the `Mount` internally - no caller change needed unless you
instantiate the filesystem class directly or mount two filesystems of the same backend under distinct protocols.

**Auto-alias dropped.** Previously, mounting a single filesystem of a given backend would auto-register its canonical
scheme as an additional alias (e.g. mounting S3 as `warehouse` also made `aws-s3`
available). That behavior is gone - every mount is registered under exactly the protocol you pick. If you need two
protocols for the same filesystem, mount it twice explicitly.

**`FilesystemTable::for(Path|Protocol)` → `for(Path|string)`.** Pass a `Path` or a plain protocol string.

### 7) `flow-php/filesystem` - `NativeLocalFilesystem::list()` no longer sorts results

`Glob::glob()` was replaced with lazy `Webmozart\Glob\Iterator\GlobIterator` to avoid materializing the entire matching
set up front (this gives a ~30× speedup on large trees when the caller only needs the first N entries).

**Side effect:** `NativeLocalFilesystem::list()` no longer returns results in alphabetical order. Output now follows
filesystem traversal order. If your code depends on sort order, sort client-side after consuming the generator:

```php
$statuses = iterator_to_array($fs->list(path('/some/dir/**/*.txt')));
usort($statuses, static fn (FileStatus $a, FileStatus $b) => $a->path->uri() <=> $b->path->uri());
```

### 8) `flow-php/symfony-filesystem-bundle` - YAML schema now uses `type:` + protocol-as-key

The configuration schema changed significantly. The YAML key under `filesystems:` is now the **mount protocol** (any
valid URI scheme), and a separate `type:` field picks the factory.

**Before:**

```yaml
flow_filesystem:
  fstabs:
    default:
      filesystems:
        file: ~
        memory: ~
        aws-s3:
          bucket: '%env(S3_BUCKET)%'
```

**After:**

```yaml
flow_filesystem:
  fstabs:
    default:
      filesystems:
        file:
          type: file
        memory:
          type: memory
        aws-s3: # mount protocol - can be any valid URI scheme
          type: aws_s3               # factory lookup key
          bucket: '%env(S3_BUCKET)%'
```

Benefits of the new shape:

- Mount the same backend twice under different protocols (e.g. `warehouse` + `archive` both `type: aws_s3` with
  different buckets).
- Protocol names are no longer tied to factory names - pick whatever reads well in your application.

Built-in `type` values: `file`, `memory`, `stdout`, `aws_s3`, `azure_blob`.

### 9) `flow-php/symfony-filesystem-bundle` - `FilesystemFactory` interface and attribute changed

```php
// Before
interface FilesystemFactory
{
    public function protocol() : Protocol;
    public function create(string $mountName, array $config) : Filesystem;
}

#[AsFilesystemFactory(protocol: 'my-fs')]

// After
interface FilesystemFactory
{
    public function type() : string;
    public function create(string $protocol, array $config) : Filesystem;
}

#[AsFilesystemFactory(type: 'my_backend')]
```

The DI tag attribute is renamed from `protocol` to `type`. `FilesystemFactoryRegistry::get()` takes a
`string $type` instead of a `Backend`.

### 10) `flow-php/symfony-filesystem-bundle` - `flow:filesystem:ls` CLI flags reshuffled

| Before                       | After                                                        |
|------------------------------|--------------------------------------------------------------|
| `--long` (default: off)      | 4-column output is now the default; use `--short` to drop it |
| `--no-limit`                 | Removed - default is unlimited now. Use `--limit=N` to cap.  |
| (no `--page-size`)           | New `--page-size=N` (default `10`) controls table page size  |
| (no `--offset`)              | New `--offset=N` skips the first N entries                   |
| `--format=json` → JSON array | `--format=json` now emits NDJSON (one JSON object per line)  |

Default behavior: list all entries, paginated in tables of 10 rows; interactive terminals prompt between pages (Enter
continues, "no" stops), piped output flows continuously. Size is formatted with binary units, Modified as ISO-8601 -
both read from the backend listing response, no per-file HEAD.

### 11) `flow-php/symfony-filesystem-bundle` - `flow:filesystem:stat` rejects pattern paths

`stat` now returns `Command::FAILURE` with a clear error when given a pattern path (`memory://*.txt`,
`**/*.parquet`, ...). Previously it returned metadata for the first match - confusing semantics. Use
`flow:filesystem:ls` for pattern inspection.

### 12) `flow-php/filesystem-async-aws-bridge`,

`flow-php/filesystem-azure-bridge` - DSL protocol is the last argument with a default

The DSL factories expose the mount protocol as an optional last argument, defaulted to the conventional scheme. Common
cases work without passing it:

```php
aws_s3_filesystem($bucket, $client);                              // mounts under 'aws-s3'
azure_filesystem($blobService);                                   // mounts under 'azure-blob'

// Pick a different protocol - e.g. mount the same bucket twice
aws_s3_filesystem($bucket, $client, protocol: 'warehouse');
azure_filesystem($blobService, protocol: 'archive');
```

### 13) `flow-php/filesystem` - `path_memory()` and `path_stdout()` DSL helpers removed

Build `Path` directly instead:

```php
// Before
$mem = path_memory();
$out = path_stdout(['stream' => 'output']);

// After
$mem = path('memory://' . bin2hex(random_bytes(16)) . '.memory');
$out = path('stdout://' . bin2hex(random_bytes(16)) . '.stdout', ['stream' => 'output']);
```

### 14) `flow-php/etl` - `ConfigBuilder::cacheFilesystem()` and `externalSortFilesystem()` added

Point cache and external-sort mechanisms at any mounted protocol; defaults remain `'file'`. The
`CacheConfig` and `SortConfig` value objects expose the chosen protocol as `->filesystemProtocol`.

```php
$config = config_builder()
    ->mount(aws_s3_filesystem($bucket, $client, protocol: 'sort-scratch'))
    ->externalSortFilesystem('sort-scratch')
    ->build();
```

### 15) `flow-php/symfony-http-foundation-bridge` - `Output` interface collapsed to a single `loader(Path)`

`Output::memoryLoader(string $id)` and `Output::stdoutLoader()` were replaced by
`Output::loader(Path $path)`. `FlowBufferedResponse` gained a `string $filesystem = 'memory'`
constructor argument (buffer protocol); `FlowStreamedResponse` gained
`string $stdoutFilesystemProtocol = 'stdout'`. Each response builds the path with its configured protocol and passes it
to the Output.

```php
// Before
new FlowBufferedResponse($extractor, new CsvOutput(), $transformations);

// After - same defaults, new constructor param available
new FlowBufferedResponse($extractor, new CsvOutput(), $transformations, filesystem: 'memory');
```

### 16) `flow-php/filesystem` - `StdOutFilesystem` tracks open streams per php:// target

Previously the "only one stdout stream" guard lived in `FilesystemStreams` (ETL core) and fired when two writing streams
used the `stdout://` protocol. The check now lives in `StdOutFilesystem` itself and is precise per underlying php://
target (`stdout` / `stderr` / `output`): two streams with
`['stream' => 'stdout']` conflict; one stdout stream + one stderr stream do not. Error message changed from *"Only one
stdout filesystem stream can be open at the same time"* to *"Only one stream can be open at the same time for php:
//{target}"*.
