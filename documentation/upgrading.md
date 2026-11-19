# Upgrade Guide

[TOC]

This document provides guidelines for upgrading between versions of Flow PHP. Please follow the instructions for your
specific version to ensure a smooth upgrade process.

Upgrading from older versions: [0.41.x to 0.44.x](/documentation/upgrading-0.41-0.44.md)

---

## Upgrading from 0.48.x to 0.49.x

### 1) `flow-php/postgresql` - `cte()` and `CTE` lose the `recursive` argument

| Before                                                    | After                                                                                           |
|-----------------------------------------------------------|-------------------------------------------------------------------------------------------------|
| `cte($name, $query, $columns, $materialization, true)`    | `with(cte($name, $query, $columns, $materialization))->recursive()`                             |
| `new CTE($name, $node, $columns, $materialization, true)` | `new CTE($name, $node, $columns, $materialization)` in `new WithClause([...], recursive: true)` |
| `CTE::recursive()`                                        | removed. Use `WithClause::recursive()`                                                          |

Before:

```php
with(cte('tree', $query, recursive: true))->select(star())->from(table('tree'));
```

After:

```php
with(cte('tree', $query))->recursive()->select(star())->from(table('tree'));
```

### 2) `flow-php/postgresql` - the DSL `returning()` / `returning_all()` and the static `ReturningClause` factories are removed

| Before                                                 | After                                                                     |
|--------------------------------------------------------|---------------------------------------------------------------------------|
| `returning('id', col('name'))`                         | removed. Use `->returning(col('id'), col('name'))` on the builder         |
| `returning_all()`                                      | removed. Use `->returningAll()` on the builder                            |
| `ReturningClause::all()`, `ReturningClause::columns()` | removed. Use `(new ReturningClause())->returning(...)`                    |
| `ReturningClause::toAstNodes($clause)`                 | removed. Use `$clause->toAst()->getReturningClause()?->getExprs()`        |
| `ReturningClause::toAst()` gives the first `ResTarget` | gives a `Node` with the whole `ReturningClause`, aliases included         |

### 3) `flow-php/postgresql` - `->returning()` with no expressions throws

| Before                                                                    | After                                                                                                 |
|---------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------|
| `delete()->from('t')->returning()` wrote no RETURNING clause              | throws `InvalidArgumentException`. Call `returningAll()` for `RETURNING *`, or remove the call        |

Applies to the delete, update, insert and merge builders.

---

## Upgrading from 0.47.x to 0.48.x

### 1) `flow-php/etl` - `Transformation` is invokable and every DataFrame method has a pipe step; `print_rows()` and `WithColumn` changed

| Before                                                                                      | After                                                                                                                                                                  |
|---------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `interface Transformation { public function transform(DataFrame $dataFrame): DataFrame; }`  | `public function __invoke(DataFrame $dataFrame): DataFrame;` - rename `transform()` to `__invoke()` in your Transformations; call one as `$transformation($dataFrame)` |
| `print_rows(Rows $rows, int\|bool $truncate = false, ?Formatter $formatter = null): string` | `format_rows(...)`, same arguments; `print_rows()` is now the `printRows()` pipe step                                                                                  |
| `Flow\ETL\Functions\WithColumn`                                                             | `Flow\ETL\API\Transformation\WithColumn`, a `Transformation` that also works as `with_column(...)($df)`                                                                |
| `WithColumn::$name` is `string`, `::$function` is `ScalarFunction`                          | `string\|Definition`, `ScalarFunction\|WindowFunction`, as `withColumn()` takes                                                                                        |
| `DataFrame::with(Transformer\|Transformation\|WithColumn $transformer)`                     | `with(Transformer\|Transformation $transformer)`; a `WithColumn` passes as a `Transformation`                                                                          |
| `duplicateRow()`, `duplicate_row()` accept every `WithColumn`                               | a Definition name or a window function throws `InvalidArgumentException`: add that column with `withColumn()` first                                                    |

### 2) `flow-php/symfony-filesystem-bundle` - each mount is a service; `FstabBuilder` is removed

| Before                                                                                   | After                                                                                                                  |
|------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------|
| `Flow\Bridge\Symfony\FilesystemBundle\Filesystem\FstabBuilder::build()` built each fstab | removed; each mount is the service `flow.filesystem.mount.<fstab>.<mount>`, and the fstab is built from those services |

### 3) `flow-php/postgresql` - schema objects store and inherit their schema, extensions belong to the catalog

| Before                                                                                                                           | After                                                                                                            |
|----------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------|
| `schema_table(..., string $schema = 'public')`                                                                                   | `?string $schema = null`. In `schema('x', ...)` the table takes `x`. A different schema throws `SchemaException` |
| `schema_view()`, `schema_materialized_view()`, `schema_sequence()`, `schema_function()`, `schema_procedure()`, `schema_domain()` | trailing `?string $schema = null`, bound the same way as tables                                                  |
| `Table::$schema` is `string`                                                                                                     | `?string`. `schemaName()` returns the rendered schema                                                            |
| `schema_sequence(..., $startValue = 1, $minValue = 1)` renders `START 1 ... MINVALUE 1`                                          | defaults are `null`. `toSql()` omits `START` and renders `NO MINVALUE`                                           |
| `toSql()` renders `CREATE VIEW v`, `DROP INDEX idx`, `CREATE EXTENSION x`                                                        | `CREATE VIEW public.v`, `DROP INDEX public.idx`, `CREATE EXTENSION x SCHEMA public`                              |
| `schema('x', extensions: [schema_extension('e')])`                                                                               | `catalog([schema('x')], extensions: [schema_extension('e', schema: 'x')])`. The default schema is `public`       |
| `Schema::$extensions`, `SchemaDiff::$addedExtensions`, `$removedExtensions`, `$modifiedExtensions`                               | `Catalog::extensions()`, `CatalogDiff::$addedExtensions`, `$removedExtensions`, `$modifiedExtensions`            |
| `new Catalog($schemas)` rebuilt from another catalog                                                                             | pass `$catalog->extensions()` as the second argument, or the extensions leave the comparison                     |
| `schema_extension('e')` diffs to `ALTER EXTENSION e UPDATE`                                                                      | a `null` version is not pinned. No version diff                                                                  |
| extension member functions, views and domains are read as schema objects                                                         | not read                                                                                                         |
| `Flow\PostgreSql\Schema\Diff\DependentView`                                                                                      | removed. `DependentViews::$toDrop` and `$toCreate` hold `View\|MaterializedView`                                 |
| `TableDiff::__construct(...)`, `TableComparator::compare($source, $target)`                                                      | trailing `bool $dropIfExists = false`                                                                            |

### 4) `flow-php/etl` - memory units are binary (1024)

| Before                                                                       | After                                                                          |
|------------------------------------------------------------------------------|--------------------------------------------------------------------------------|
| `Unit::fromKb()`, `fromMb()`, `fromGb()` multiply by 1000                    | multiply by 1024. `Unit::fromMb(64)` is 67 108 864 bytes                       |
| `Unit::inKb()`, `inMb()`, `inGb()` divide by 1000                            | divide by 1024                                                                 |
| `Unit::fromString('128M')` is 128 000 000 bytes                              | 134 217 728 bytes                                                              |
| `Unit::fromString('1GB')` is 1 000 bytes, `'500B'` is 500 000 bytes          | 1 073 741 824 bytes and 500 bytes. `K/KB`, `M/MB`, `G/GB` are read in any case |
| `Unit::fromString('10TB')`, `'1.5G'` or `'M'` return a value                 | throw `InvalidArgumentException`                                               |
| `hash_join()` broadcast limit defaults to 10 000 000 bytes                   | 10 485 760 bytes                                                               |
| `MemoryLimit::default()` is 1 000 000 000 bytes for a `memory_limit` of `-1` | 1 073 741 824 bytes                                                            |
| `explain()` prints `Memory: 64 MB`                                           | `Memory: 64 MiB`, the same number                                              |

### 5) `flow-php/etl` - `FrameAccumulating` computes every frame of a partition in one call

| Before                                                                 | After                                                                                                         |
|------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------|
| `FrameAccumulating::accumulator(): AggregatingFunction`                | `FrameAccumulating::frameValues(Rows $partition, array $bounds, Backend $backend, Progress $progress): array` |
| `Sum::accumulator()`, `Average::accumulator()`, `Count::accumulator()` | removed                                                                                                       |

A custom `FrameAccumulating` gets one `[start, end]` pair per row of the sorted partition, `[1, 0]` for an empty frame,
and returns one value per pair.

### 6) `flow-php/etl` - window functions take a `Progress`

| Before                                                                                             | After                                                                                                                  |
|----------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------|
| `WindowFunction::values(Rows $partition, Backend $backend): array`                                 | `WindowFunction::values(Rows $partition, Backend $backend, Progress $progress): array`                                 |
| `FrameAccumulation::values(FrameAccumulating $function, Rows $partition, Backend $backend): array` | `FrameAccumulation::values(FrameAccumulating $function, Rows $partition, Backend $backend, Progress $progress): array` |
| -                                                                                                  | `NumericTotal::frames(array $values, array $bounds, bool $exact, Progress $progress): array`                           |

A custom window function can call `$progress->advanced($rows)` once per partition. The call is optional.

### 7) `flow-php/cli` - `--config` returns a `ConfigBuilder`

| Before                                                              | After                                                                                  |
|---------------------------------------------------------------------|----------------------------------------------------------------------------------------|
| `return config_builder()->id('x')->build();`                        | `return config_builder()->id('x');`                                                    |
| `ConfigOption::get(): Config`                                       | `ConfigOption::get(): ConfigBuilder`                                                   |
| `Flow\CLI\Loader\ProgressBarLoader`                                 | removed, the commands use `console_heartbeat()` from `flow-php/symfony-console-bridge` |
| `Flow\CLI\Style\FlowStyle::clear()`                                 | removed                                                                                |
| `file:analyze` progress bar on stdout                               | `tasks` on stderr, `log` lines off a terminal, also `file:convert`, `file:rows:count`  |
| `Heap Peak 40.9 MB` in the `file:analyze` and `pipeline:run` report | `Heap Peak 40.9 MiB`, the same for every memory row                                    |
| `flow pipeline:run --config .flow.php pipeline.php`                 | `flow pipeline:run pipeline.php`, the pipeline file configures its own frame           |
| `--config` on `db:table:list`, `db:table:schema`, `schema:format`   | removed, these commands never used it                                                  |

### 8) `flow-php/cli`, `flow-php/symfony-telemetry-bundle`, `flow-php/symfony-postgresql-bundle`, `flow-php/parquet-viewer` - `symfony/console` lower bound raised

| Before                                          | After                                                  |
|-------------------------------------------------|--------------------------------------------------------|
| `"symfony/console": "^6.4 \|\| ^7.4 \|\| ^8.0"` | `"symfony/console": "^6.4.34 \|\| ^7.4.6 \|\| ^8.0.6"` |

### 9) `flow-php/etl` - inference takes a `Progress`

| Before                                                                                         | After                                                                                                              |
|------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------|
| `interface InfersSchema`                                                                       | `interface InfersSchema` with `reportInferenceTo(Progress $progress): void`                                        |
| `SchemaInferrer::infer(Sniffer $sniffer): Inferred`                                            | `SchemaInferrer::infer(Sniffer $sniffer, Progress $progress): Inferred`                                            |
| `Sniffer::sniff(int $unit, ?int $rowBudget): Sample`                                           | `Sniffer::sniff(int $unit, ?int $rowBudget, Progress $progress): Sample`                                           |
| `Sniffs::sniff(Sniffer $sniffer, int $units): ?array`                                          | `Sniffs::sniff(Sniffer $sniffer, int $units, Progress $progress): ?array`                                          |
| `ColumnTypes::sniff(array $names, iterable $rows, ?int $rowBudget, TypeNarrower $typer): self` | `ColumnTypes::sniff(array $names, iterable $rows, ?int $rowBudget, TypeNarrower $typer, Progress $progress): self` |
| `CSVOpenSource::sniff(array $names, ?int $rowBudget, TypeNarrower $typer): ColumnTypes`        | `CSVOpenSource::sniff(array $names, ?int $rowBudget, TypeNarrower $typer, Progress $progress): ColumnTypes`        |
| `flow_php` built for `FLOW_PHP_ABI` 2                                                          | rebuild `flow_php`: `FLOW_PHP_ABI` / `FlowPhpExtension::ABI` 3, an older build is refused                          |

A custom `InfersSchema` extractor adds `reportInferenceTo()`, keeps the `Progress` it gets (a `VoidProgress` until then)
and passes it to `SchemaInferrer::infer()`. A custom `Sniffer` or `CSVOpenSource` passes `$progress` to
`ColumnTypes::sniff()`, or calls `advanced()` every `Progress::ROWS_PER_CALL` observed rows. A custom `Sniffs` hands
`$progress` to the sniffs it runs.

### 10) `flow-php/etl` - memory limits count the memory an operation adds

| Before                                                                                                      | After                                                                                                                                           |
|-------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------|
| `memoryLimit()` compared with the process memory                                                            | compared with the memory the operation itself added; each live operation gets its own limit                                                     |
| `FLOW_MAX_MEMORY`: the default limit, compared with the process memory                                      | the default per-operation limit and, with a finite `memory_limit`, the process cap                                                              |
| -                                                                                                           | with a finite `memory_limit`, the process over 70% of it (or `FLOW_MAX_MEMORY`) also spills, after the operation added `min(128 MiB, cap / 16)` |
| `MemoryBudget::exceeded()` / `exceededWith()` compare the process memory with the limit                     | compare the memory added since the budget started or since `released()` with the limit, and the process with the cap                            |
| `MemoryBudget::released()` calls `gc_mem_caches()`                                                          | only moves the baseline                                                                                                                         |
| `Flow\ETL\Runtime\MemoryLimit`                                                                              | `Flow\ETL\Values\Memory\MemoryLimit`, plus `processCap(): ?Unit`                                                                                |
| `MemoryLimit::default()` takes any `FLOW_MAX_MEMORY`                                                        | throws `InvalidArgumentException` for a value above a finite `memory_limit`, so the config build fails                                          |
| `new MemoryBudget(Backend, Unit)`                                                                           | `new MemoryBudget(Backend, Unit $limit, ?Unit $processCap)` or `MemoryBudget::of(Backend, Unit)`                                                |
| `BucketingConfig::spilling(Backend)`                                                                        | `spilling(Backend, MemoryBudget)`                                                                                                               |
| `new SpillingRowsStorage(RowsStorage, Unit, Backend, BatchSize)`, `::$memoryLimit`                          | `(RowsStorage, MemoryBudget, Backend, BatchSize)`, no `$memoryLimit`                                                                            |
| `new BoundedRead(Unit, Backend)`                                                                            | `new BoundedRead(MemoryBudget)`                                                                                                                 |
| `new ExternalSort(array, Buckets, Buckets, RandomValueGenerator, Unit $memoryLimit, ...)`, `::$memoryLimit` | no `$memoryLimit`: the limit comes from the `MemoryBudget` passed to `sort()`                                                                   |
| `ExternalSort::sort(Generator, Backend, ?int)`                                                              | `sort(Generator, Backend, MemoryBudget, ?int)`                                                                                                  |
| -                                                                                                           | with a finite `memory_limit`, a sort whose working memory would pass the cap sorts and spills in runs that fit                                  |
| `Backend` implementations                                                                                   | implement `sortWorkingBytes(Rows $rows, array $columns): int`                                                                                   |

### 11) `flow-php/etl` - a failed run ends its sources first

| Before                                                                 | After                                              |
|------------------------------------------------------------------------|----------------------------------------------------|
| a failed run destroyed the generator of a source                       | the failure is thrown at the `yield` of the source |
| a `finally` around the `yield` ran late, after the caller's `catch`    | it runs before the failure gets to the caller      |
| a `catch (Throwable)` around the `yield` saw only the source's failure | it also gets the failures of later steps           |

### 12) `flow-php/etl` and the file adapters - a write that ends reports the files it writes

| Before                                   | After                                            |
|------------------------------------------|--------------------------------------------------|
| `Closure::closure(): void`               | `closure(Progress $progress): void`              |
| `FileWrite::closure(): void`             | `closure(Progress $progress): void`              |
| `FileCommit::commit(array $tasks): void` | `commit(array $tasks, Progress $progress): void` |
| `WorkbookManager::close(): void`         | `close(Progress $progress): void`                |

A custom `Closure` loader:

Before:

```php
public function closure(): void
{
    $this->client->flush();
}
```

After:

```php
use Flow\ETL\Foundation\Progress;

public function closure(Progress $progress): void
{
    $this->client->flush();
    $progress->busy();
}
```

`advanced(1)` counts a file that became visible at its target, `busy()` reports any other blocking call.

Code that calls `FileCommit::commit()` outside a run passes `new VoidProgress()`.

### 13) `flow-php/etl` - `Distribution::ordered()` takes the stage of the step it splits

| Before                                | After                                                     |
|---------------------------------------|-----------------------------------------------------------|
| `Distribution::ordered(array $order)` | `Distribution::ordered(array $order, Progress $progress)` |

### 14) `flow-php/etl` - `DataFrame::schema()` runs the heartbeat

| Before                                      | After                                                                                      |
|---------------------------------------------|--------------------------------------------------------------------------------------------|
| `DataFrame::schema()` never beats           | it beats during schema inference and once at its end                                       |
| a heartbeat failure never leaves `schema()` | a heartbeat that fails with `HeartbeatFailure::stop` throws its exception out of `schema()` |

### 15) `flow-php/etl` - an extractor is ended on a stop and no longer receives `Signal::STOP`

When a limit or the consumer stops a read, Flow ends the extractor's generator instead of sending it `Signal::STOP`.
The generator runs its `finally`, and the code in its STOP branch does not run.

| Before                                                                  | After                                       |
|-------------------------------------------------------------------------|---------------------------------------------|
| `if ($signal === Signal::STOP) { $reader->close(); return; }`           | `try { ... } finally { $reader->close(); }` |
| `if ($signal === Signal::STOP) { $inner->send(Signal::STOP); return; }` | `if ($signal === Signal::STOP) { return; }` |

A wrapping extractor delegates with `yield from` as its whole body, or returns on `Signal::STOP` without sending it on.

### 16) `flow-php/etl` - `Processor::process()` returns a generator that has not run past its first yield

Flow iterates a processor's generator with `foreach`. A `process()` that advances its generator past the first yield
before it returns it fails with PHP's `Cannot rewind a generator that was already run`. Return the generator unstarted.

### 17) `flow-php/filesystem-async-aws-bridge` - the client has HTTP limits, `mv()` and appends copy in ranged parts

| Before                                                                                          | After                                                                                                      |
|-------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------|
| idle timeout `default_socket_timeout` (60 s by default)                                         | 30 s, set `http.timeout` in the client configuration                                                       |
| no limit for one call                                                                           | 300 s for one call with its retries; a `content()` of a large object can hit it, raise `http.max_duration` |
| an object moved by `mv()` gets a single-part ETag                                               | an object above `copy_part_size` gets a multipart ETag `<md5>-<parts>`                                     |
| an append writes a `<path>/_flow_php_append_` object                                            | no `_flow_php_append_` object                                                                              |
| `AsyncAWSS3DestinationStream::openAppend($s3Client, $bucket, $path, $blockFactory, $blockSize)` | `openAppend(..., $blockSize, int $copyPartSize = 256 * 1024 * 1024)`                                       |
| `Options::fromArray()` takes a description without `copy_part_size`                             | the description must hold `copy_part_size` (int)                                                           |
| `openBlank()`, `openExclusive()`, `openAppend()` default `$blockSize` 4 MiB                     | 5 MiB, the S3 minimum part size                                                                            |

### 18) `flow-php/filesystem-azure-bridge` - the blob service has HTTP limits and retries

| Before                                                | After                                                                                |
|-------------------------------------------------------|--------------------------------------------------------------------------------------|
| no retries                                            | 3 retries, set `http.max_retries: 0` for the old behaviour                           |
| idle timeout `default_socket_timeout`, no total limit | 30 s idle, 300 s for one call with its retries (`http.timeout`, `http.max_duration`) |
| `ConfiguredBlobService::normalize()` without `http`   | it always holds `http` with the filled limits                                        |

### 19) `flow-php/azure-sdk` and `flow-php/filesystem-azure-bridge` - `mv()` copies synchronously from a signed source

| Before                                                             | After                                                                                                         |
|--------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------|
| `AuthorizationFactory` has `for()`                                 | also `readOnlySourceUrl(string $url, string $container, string $blob, DateTimeImmutable $expiresAt)`          |
| `BlobServiceInterface`                                             | also `putBlockBlobBlockFromBlob()`                                                                            |
| `copyBlob()` starts an asynchronous copy and accepts a pending one | synchronous, throws on a pending copy, the asynchronous Copy Blob is removed                                  |
| `getBlobProperties()` sends GET and downloads the blob             | sends HEAD, `BlobProperties::content()` is removed                                                            |
| `mv()`: Copy Blob, then Delete Blob                                | Get Blob Properties, synchronous Copy Blob or ranged Put Block From URL with Put Block List, then Delete Blob |
| `mv()` of a missing source throws `AzureException` (404)           | throws `Flow\Filesystem\Exception\RuntimeException`                                                           |
| `Options::fromArray()` takes options without `copy_part_size`      | the options must hold `copy_part_size` (int)                                                                  |

### 20) `flow-php/postgresql` - connection string values are quoted

`ConnectionParameters::toString()` quotes a value that is empty or holds whitespace, `'` or `\`. A libpq
`options` value that escapes spaces with a backslash no longer works. `ConnectionParameters::fromString()` and
`pgsql_connection()` now read the string by the libpq rules: a backslash escapes the next character, and a keyword
without `=` or an unterminated quote throws `InvalidArgumentException`.

Before:

```
postgresql://user:pass@localhost/app?options=-cstatement_timeout%3D30s%5C%20-clock_timeout%3D10s
```

After:

```
postgresql://user:pass@localhost/app?options=-c%20statement_timeout%3D30s%20-c%20lock_timeout%3D10s
```

Or `withSessionSettings(['statement_timeout' => '30s', 'lock_timeout' => '10s'])`, or `session_settings` in the
Symfony bundle.

### 21) `flow-php/etl` - the default memory limit is 4 GiB, `ExternalSort::reduce()` takes streams

| Before                                                                | After                                                                          |
|-----------------------------------------------------------------------|--------------------------------------------------------------------------------|
| `MemoryLimit::default()` with `memory_limit = -1`: 1 GiB              | 4 GiB (`MemoryLimit::WITHOUT_MEMORY_LIMIT_GB = 4`)                             |
| `ExternalSort::reduce(list<BucketRun>, KWayMerge, int, ?int)`         | `reduce(list<Generator<Rows>>, KWayMerge, int, ?int)`, removes no run          |

### 22) `flow-php/etl-adapter-parquet` and `flow-php/flow-php-ext` - with `flow_php`, Parquet has its own engine

With `flow_php`, the memory limit of a run counts the Parquet bytes. With `arrow` alone, ETL reads and writes on the
PHP engine.

| Before                                                                                                         | After                                                                                                               |
|----------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------|
| no `engine:` is `AdaptiveParquetEngine`: `RustParquetEngine` when `arrow` is loaded                            | `FlowPhpParquetEngine` when `flow_php` is loaded, else `PhpParquetEngine`                                           |
| `arrow` alone: ETL reads and writes on `RustParquetEngine`                                                     | `PhpParquetEngine`. Pass `engine: new RustParquetEngine()` to keep `arrow`. A memory limit does not count its bytes |
| both loaded, `engine: new RustParquetEngine()`: batches into native columns through the Arrow C data interface | PHP values from `arrow`, adopted by the backend                                                                     |
| `RustParquetOpenSource::__construct(RustParquetFileReader $file)`                                              | `__construct(FlowPhpParquetFileReader $file)`                                                                       |
| `RustParquetFileWriter::__construct(Flow\Parquet\Engine\RustParquetFileWriter $writer, ...)`                   | `__construct(FlowPhpParquetFileWriter $writer, ...)`                                                                |
| `Flow\ETL\Values\Column\RustColumnsBatch`                                                                      | removed                                                                                                             |
| a worker of an extension engine needs the codec extensions                                                     | it needs that extension (`flow_php` or `arrow`) and no codec extension                                              |

### 23) `flow-php/arrow-ext` - the Arrow C data classes are removed

| Before                                                                                            | After                                                                                                                                                        |
|---------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `Flow\Arrow\Parquet\RustBatchReader`, `Flow\Arrow\RustParquetBatch`, `Flow\Arrow\RustArrowSchema` | removed. ETL reads through the `flow_php` engine. `flow-php/parquet` reads with `readColumns()` / `readStoredColumns()`                                      |
| `RustParquetFileWriter::writeArrowBatch()`                                                        | removed. Use `writeColumns()` / `writeStoredColumns()`                                                                                                       |
| `RustBackend::allocatedBytes()` adds `Flow\Arrow\allocated_bytes()`                               | it counts the bytes of `flow_php` only, which hold the Parquet bytes of the `flow_php` engine. `Flow\Arrow\allocated_bytes()` stays for the bytes of `arrow` |
| `FLOW_ARROW_ABI` 2                                                                                | 3: build `arrow` from the release of `flow-php/parquet`                                                                                                      |

### 24) `flow-php/azure-sdk` - `BlobServiceInterface` deletes blobs in one batch

| Before                 | After                                                                                                  |
|------------------------|--------------------------------------------------------------------------------------------------------|
| `BlobServiceInterface` | also `deleteBlobs(array $blobs, DeleteBlobsOptions $options = new DeleteBlobsOptions()): void`         |

---

## Upgrading from 0.46.x to 0.47.x

### 1) `flow-php/types` - leaf types are no longer generic

| Before                                                                          | After                                                                                                 |
|---------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------|
| `StringType<string>`, `DateTimeType<\DateTimeInterface>`, `UuidType<Uuid>`, ... | `StringType`, `DateTimeType`, `UuidType` - leaf types implement `Type<string>`, ... directly          |
| `ListType<list<T>>`, `MapType<array<TKey, TValue>>`                             | `ListType<T>`, `MapType<TKey, TValue>`                                                                |
| Mago `plugins = ["flow-php"]` infers `type_structure()` shapes                  | Built-in plugin no longer infers them - use `flow-php/mago-types-bridge` (`plugins = ["flow/types"]`) |

### 2)

`flow-php/parquet` - statistics are written and read as arrow-rs does; one physical schema for one logical schema on
both engines

| Before                                                                                                                                                                 | After                                                                                                                                                                                                    |
|------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| PHP writer string bounds by PHP `<` (`'9'` below `'10'`), whole strings, no exactness flags, no `nan_count`, no `column_orders`, deprecated `min`/`max` for every type | unsigned-byte order, truncated to `Option::STATISTICS_TRUNCATE_LENGTH` (64) bytes, exactness flags, `nan_count` for floats, `column_orders` per leaf, deprecated `min`/`max` only for signed sort orders |
| statistics of files written by older flow versions read as written                                                                                                     | `ColumnOrder::Absent`: no bounds are trusted, so such files are never pruned; a statistic of the wrong width (older INT64 bounds are 16 bytes) reads as `null`                                           |
| `StatisticsReader` read BYTE_ARRAY statistics with a length prefix, a FLBA decimal as a float, an INT64 decimal as the unscaled int                                    | raw bytes for BYTE_ARRAY/FLBA (decimals as unscaled two's complement bytes, uuid as 16 bytes), the unscaled int for INT32/INT64 decimals                                                                 |
| `FlatColumn::decimal()` of precision up to 18 written as FIXED_LEN_BYTE_ARRAY                                                                                          | INT32 up to precision 9, INT64 up to 18, FIXED_LEN_BYTE_ARRAY above; older files still read                                                                                                              |
| the packer/unpacker encoded FLBA/BYTE_ARRAY decimals and UUIDs; `DecimalConverter` took INT-backed decimals only                                                       | `new DecimalConverter($physicalType, $precision, $scale, $byteLength)` and `UuidConverter` convert to the physical bytes; the packer/unpacker take them as-is                                            |
| `FlatColumn::int32()` / `int64()` wrote converted `INT_32` / `INT_64`                                                                                                  | plain `INT32` / `INT64`, as arrow-rs                                                                                                                                                                     |
| a column with a logical type and no converted type written without one (other readers read a logical `UINT_64` as signed)                                              | the converted type derived from the logical type, as arrow-rs; a `FlatColumn` whose converted and logical types disagree is refused, on read and write                                                   |
| the arrow writer named map entries `m.entries.key`                                                                                                                     | `m.key_value.key` (LogicalTypes.md): `COLUMNS_*` options keyed by `...key_value.key` address the same leaf on both engines                                                                               |
| the arrow writer wrote `FlatColumn::enum()` as `STRING`                                                                                                                | `ENUM`                                                                                                                                                                                                   |
| `Option::ARROW_BATCH_SIZE`, read by nothing                                                                                                                            | removed                                                                                                                                                                                                  |
| `Flow\Parquet\Dremel\Statistics\Comparator`                                                                                                                            | removed: `Flow\Parquet\Data\PhysicalOrder`                                                                                                                                                               |
| `new StatisticsCounter($column)`                                                                                                                                       | `new StatisticsCounter($column, new StatisticsTruncation($length))`                                                                                                                                      |
| `new UnsignedIntegerConverter($path, $bitWidth)`                                                                                                                       | `new UnsignedInt32Converter()`; UINT_64 is `UnsignedInt64Converter`                                                                                                                                      |

### 3) `flow-php/parquet`, `flow-php/etl-adapter-parquet` -

`UINT_64` reads as a decimal string on both engines (reverses rows 84 and 85 of 0.44→0.45 for `UINT_64`)

| Before                                                                                  | After                                                                                            |
|-----------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------|
| `UINT_64` read as `int`, refused above `PHP_INT_MAX` on both engines                    | read as the decimal string (`'18446744073709551615'`) on both engines; `UINT_8/16/32` stay `int` |
| `from_parquet()` typed a `UINT_64` column `integer`                                     | `numeric_string`                                                                                 |
| writes accepted an int from 0 to `PHP_INT_MAX` into `UINT_64`                           | an int >= 0 or a canonical digit string up to `18446744073709551615`                             |
| `to_parquet()` refused a `numeric_string` column (`NumericStringType is not supported`) | written as a parquet `STRING`                                                                    |

### 4) `flow-php/parquet` - the `arrow` extension must be rebuilt (arrow-rs 60, ABI 2)

| Before                                                  | After                                                                                                                                                      |
|---------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------|
| arrow-ext built against arrow-rs 59, `FLOW_ARROW_ABI` 1 | arrow-rs 60, `FLOW_ARROW_ABI` / `ArrowExtension::ABI` 2 (covering every contract change below): an older build is refused by `ArrowExtension::available()` |

### 5) `flow-php/parquet`, `flow-php/etl-adapter-parquet` - reads take a `Query` (or, below the file, a

`ReadPlan`) instead of limit/offset, the PHP writer writes the page index, the reader exposes its `Footer`; the
`arrow` and `flow_php` extensions must be rebuilt

| Before                                                                                                                                                                  | After                                                                                                                                                                                                                                                                                                                                                                               |
|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `ParquetFile::values(array $columns, ?int $limit, ?int $offset)`                                                                                                        | `values(Query $query)`: `$file->values(parquet_query()->select(...$columns)->offset($offset)->limit($limit))`                                                                                                                                                                                                                                                                       |
| `ParquetFile::columns(int $batchSize, array $columns, ?int $limit, ?int $offset)`                                                                                       | `columns(Query $query, int $batchSize = 1024)`                                                                                                                                                                                                                                                                                                                                      |
| a limit of `0` refused (`Limit must be greater than 0`); a negative offset or limit refused by `columns()`                                                              | `limit(0)` reads no row; a negative offset or limit is refused by `Query`                                                                                                                                                                                                                                                                                                           |
| `ParquetFileReader::metadata()`, `schema()`, `rowsNumber()`, `totalByteSize()`                                                                                          | `ParquetFileReader::footer(): Footer` holds them (and `rowGroupChunks()`); a footer taken before `close()` stays readable                                                                                                                                                                                                                                                           |
| `ParquetFileReader::readColumns(array $columns, int $batchSize, ?int $limit, ?int $offset)`                                                                             | `readColumns(array $columns, int $batchSize, ReadPlan $plan)`, returning the rows `$plan->selection` selects that match `$plan->rowFilter`, at most `$plan->limit` after `$plan->offset` matches are skipped, one `SkippedMatches` first when the offset is above 0; build the plan with `ReadPlan::forFilter(...)` or `ReadPlan::window($rows, $file->indexes(), $offset, $limit)` |
| `RustParquetFileReader::thrift()`                                                                                                                                       | `RustParquetFileReader::footer()->thrift()` (`RustParquetFooter`)                                                                                                                                                                                                                                                                                                                   |
| a `ParquetFileReader` / `ParquetFileWriter` implementation                                                                                                              | also `readStoredColumns(array $columns, int $batchSize, ReadPlan $plan): Iterator` and `writeStoredColumns(array $columns): void`: DATE, TIMESTAMP, TIME and UUID values as stored (days, µs since the epoch, µs since midnight, 16 bytes)                                                                                                                                          |
| the arrow writer refused a value outside the range arrow stores it in with `RuntimeException`                                                                           | `InvalidArgumentException`, as the PHP writer refuses it (e.g. a `DateTime` outside `TIMESTAMP(NANOS)`)                                                                                                                                                                                                                                                                             |
| `ParquetOpenSource::batches(Schema $schema, int $batchSize, ?int $offset, ?int $limit, Backend $backend)`                                                               | `batches(Schema $schema, int $batchSize, ReadPlan $plan, Backend $backend)`                                                                                                                                                                                                                                                                                                         |
| `RustBatchReader` / `RustColumnsReader::__construct(..., int $batchSize, ?int $offset, ?int $limit)`                                                                    | `__construct(..., int $batchSize, ReadPlan $plan)`                                                                                                                                                                                                                                                                                                                                  |
| `ColumnReader::read(Column, Metadata, SourceStream, ?int $limit, ?int $offset)`                                                                                         | `readRowGroup(Column, RowGroup, int $group, SourceStream, RowSelection, Indexes, array $decoded = [])`, one row group at a time; `$decoded` pages, keyed by their first row, are served instead of read                                                                                                                                                                             |
| `new ColumnReader(ColumnChunkReader $chunks, DremelAssembler $assembler)`                                                                                               | `new ColumnReader(new SelectedPages($chunks), $assembler)`                                                                                                                                                                                                                                                                                                                          |
| `ColumnChunkReader::read(ColumnChunk, FlatColumn, SourceStream)` yielded a list of pages                                                                                | pages keyed by their first row in the row group; `?OffsetIndex $offsetIndex` with `list<int> $pages` reads only those pages of it, `?Dictionary $dictionary` replaces the dictionary page, the generator returns the dictionary                                                                                                                                                     |
| `RowGroupContainer(string, RowGroup)`                                                                                                                                   | `RowGroupContainer(string, RowGroup, list<?ColumnIndex>, list<OffsetIndex>)`                                                                                                                                                                                                                                                                                                        |
| `new PlainFlatColumnChunkBuilder($column, $options, $compression)` (and the RLE_DICTIONARY, DELTA_BINARY_PACKED builders, `ColumnChunkBuilderFactory::createBuilder()`) | a 4th argument, `StatisticsOptions::fromOptions($options)`                                                                                                                                                                                                                                                                                                                          |
| `ColumnChunk::dataPageOffset(): ?int`                                                                                                                                   | `int`: a column chunk always records it                                                                                                                                                                                                                                                                                                                                             |
| the PHP writer wrote no column index or offset index                                                                                                                    | both, by default (`Option::STATISTICS_LEVEL`, `StatisticsLevel::PAGE`; `CHUNK` drops the column index, `NONE` all statistics)                                                                                                                                                                                                                                                       |
| the PHP writer wrote page-header statistics on `DATA_PAGE_V2`                                                                                                           | none, as arrow-rs: `Option::PAGE_HEADER_STATISTICS` (`true`) writes them, on v1 and v2 pages                                                                                                                                                                                                                                                                                        |
| `Option::PAGE_MAXIMUM_ROWS_COUNT` (default `1000`) was read by nothing                                                                                                  | the PHP writer closes a page at exactly that many rows; the arrow writer treats it as a target (arrow-rs `data_page_row_count_limit`, checked between write batches), so its pages can hold more; below 1 is refused (`InvalidArgumentException`)                                                                                                                                   |
| the arrow writer skipped a `COLUMNS_COMPRESSIONS` / `COLUMNS_ENCODINGS` entry whose column path is digits only (`'2024'`, an int array key)                             | applies it; a key that is not UTF-8 is refused (`InvalidArgumentException`) instead of crashing the process                                                                                                                                                                                                                                                                         |
| `Option::ROW_GROUP_SIZE_BYTES` below 1 was taken by both writers                                                                                                        | refused (`InvalidArgumentException`) by both writers                                                                                                                                                                                                                                                                                                                                |
| the arrow writer threw `Error` (a Rust panic) on `PLAIN_DICTIONARY` / `RLE_DICTIONARY` in `COLUMNS_ENCODINGS`                                                           | enables the column's dictionary: `RLE_DICTIONARY` data pages and a `PLAIN` dictionary page, as the PHP writer writes                                                                                                                                                                                                                                                                |
| the arrow writer skipped a `COLUMNS_COMPRESSIONS` / `COLUMNS_ENCODINGS` value that is not a string                                                                      | refused (`InvalidArgumentException`)                                                                                                                                                                                                                                                                                                                                                |
| files with an `RLE_DICTIONARY` column spanning more than one page, written by the PHP writer                                                                            | were corrupt (later pages decoded to wrong values): rewrite them; one dictionary per chunk, `PLAIN` past `Option::DICTIONARY_PAGE_SIZE`                                                                                                                                                                                                                                             |
| `Option::DICTIONARY_PAGE_MIN_CARDINALITY_RATION`                                                                                                                        | removed: it was never read; the dictionary falls back to `PLAIN` on `Option::DICTIONARY_PAGE_SIZE` only, as arrow-rs                                                                                                                                                                                                                                                                |
| `Options::set()` / `get()` took scalars and arrays                                                                                                                      | also a `UnitEnum`; `Options::getEnum(Option $option, string $class)`                                                                                                                                                                                                                                                                                                                |
| no column index truncation option                                                                                                                                       | `Option::COLUMN_INDEX_TRUNCATE_LENGTH` (64): BYTE_ARRAY/FIXED_LEN_BYTE_ARRAY page bounds in the column index                                                                                                                                                                                                                                                                        |
| `DataPageHeaderV2::statistics(Options $options)`                                                                                                                        | `statistics()`, as `DataPageHeader::statistics()`                                                                                                                                                                                                                                                                                                                                   |
| `ParquetFile::pageHeaders()` repeated every chunk of a row group under each column                                                                                      | each column's own chunk pages, once                                                                                                                                                                                                                                                                                                                                                 |
| `FLOW_PHP_ABI` / `FlowPhpExtension::ABI` 1                                                                                                                              | 2 (covering every contract change below): rebuild the `flow_php` extension (and `arrow`, item 4); an older build is refused by `available()`                                                                                                                                                                                                                                        |

### 6) `flow-php/etl`, `flow-php/types` - string comparisons in filters use byte order, as sort already does

| Before                                                                                         | After                                                                                                                                                                |
|------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `ref('a')->lessThan(lit('10'))` on `'9'`: `true` (PHP `<` compares numeric strings as numbers) | `false`: `lessThan`, `lessThanEqual`, `greaterThan`, `greaterThanEqual`, `between`, `greatest` and `least` order two strings by bytes (`strcmp`), as `sortBy()` does |
| `greatest(lit('9'), lit('10'))`: `'10'`                                                        | `'9'`                                                                                                                                                                |
| a `numeric_string` column was comparable only with another `numeric_string`                    | comparable with a `string` literal too (`ref('id')->greaterThan(lit('1000'))`), and ordered numerically; still not comparable with an `integer`                      |
| `Between\Boundary::compareValues(mixed $left, mixed $right)`                                   | removed: `(new ValueOrder(...$operandTypes))->compare($left, $right)`; `Boundary::compare($value, $lower, $upper, ValueOrder $order = new ValueOrder())`             |

### 7) `flow-php/etl` - a file extractor reads a `Criteria` and returns only rows matching its conjuncts; `Read` and

`Executor\Pipeline` carry it

| Before                                                                                                       | After                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
|--------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `FileExtractor::extract(FlowContext $context, ?int $limit = null, Filter $pathFilter = new OnlyFiles())`     | `FileExtractor::extract(Backend $backend, Criteria $criteria)`, returning only rows matching every one of `$criteria->conjuncts`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| `$extractor->extract($context, pathFilter: $filter)`                                                         | `$extractor->extract($backend, new Criteria(pathFilter: $filter))`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| `Read::limit()`, `Read::pathFilter()`, `new Read($extractor, $limit, $pathFilter)`                           | `Read::criteria()->limit`, `->pathFilter`, `->conjuncts`; `new Read($extractor, new Criteria($limit, $pathFilter))`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| `Read::withPathFilter()` on any extractor                                                                    | refused (`InvalidArgumentException`) on an extractor that is no `FileExtractor`, as `withFilter()`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| `Executor\Pipeline::limit()`, `pathFilter()`, `new Pipeline(..., $limit, $pathFilter, $sources)`             | `new Pipeline($id, Plan\Node\Read\|Pipeline\|null $input, PipelineSteps $steps)`; the read is the `Read` node, its criteria `$pipeline->input->criteria()`; the counters are an `Executor::execute()` argument (row 95)                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| a `filter()` over a file source stayed a `Filter` step                                                       | each deterministic part the source uses is pushed into it (`Criteria::$conjuncts`) and leaves the step: a part over partition columns only, on every source, and a part `FileExtractor::prunes()` accepts (Parquet: a part it translates into a Parquet filter); an emptied step is removed; a `limit()` after it is pushed beside it, so the read stops after that many matching rows; `explain()` prints each pushed part as `Filter: ...` under `Read`; a part after one kept in the step is pushed too, so the kept parts, in written order, can run on fewer rows; a `>`/`>=` on a float column is pushed only into files whose statistics prove no NaN |
| a custom `FileExtractor`                                                                                     | implements `prunes(ScalarFunction $conjunct): bool`: true only when the source skips data with the conjunct before it decodes rows; `false` keeps the conjunct in its `Filter` step                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| `new FileReadLoop($fileColumns, $schema)`, `read($sources, $batches, $batchSize, Backend $backend, $window)` | `new FileReadLoop($fileColumns, $schema, $criteria->conjuncts)`, `read($sources, $batches, $batchSize, Backend $backend, $window)`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| `Optimizer::default()` ran `PushLimitIntoSource` before `PushFilterIntoSource`                               | after it                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| `Rewrite::of(Node $node)`                                                                                    | `of(Node $original, Node $rebuilt)`: the node as it was and rebuilt over its rewritten children                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| `new Flow\ETL\Plan\ReplaceLeaf($target, $replacement)`                                                       | `ReplaceNodes::one($target, $replacement)`, or `new ReplaceNodes($replacements, $removals)` for several in one rewrite (a removed node gives way to its child)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| `new ScalarFunctionFilterTransformer($function, ?ScalarFunction $resolved)`                                  | the second argument is `?Functions\Evaluation\Filter`; `bind()` sets it, as before                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| `explain()`: `Select` printed `Columns: ...`, other nodes `Defines columns: ...`                             | `Keeps only: ...` and `Changes columns: ...` (a column defined, renamed or dropped)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| `Redefined::$unknown`                                                                                        | `Redefined::isUnknown()`; `Redefined::isAllBut()` tells an `allBut()` apart                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |

A custom file extractor:

```php
// before
public function extract(FlowContext $context, ?int $limit = null, Filter $pathFilter = new OnlyFiles()): Generator
{
    // reads with $limit and $pathFilter
}

// after
public function extract(Backend $backend, Criteria $criteria): Generator
{
    // reads with $criteria->limit and $criteria->pathFilter, returns only rows matching every one of
    // $criteria->conjuncts
}

public function prunes(ScalarFunction $conjunct): bool
{
    return false; // true only when the source skips data with $conjunct
}
```

### 8) `flow-php/telemetry`, `flow-php/psr18-telemetry-bridge`,

`flow-php/symfony-telemetry-bundle` - HTTP URLs are redacted, stack traces carry no argument values

| Before                                                                                                                                            | After                                                                                                                                                                                                                                                                                                                        |
|---------------------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `url.full` (PSR-18 and Symfony HttpClient), `url.query` (HttpKernel), `exception.message` and the client span status description held the raw URL | userinfo and the values of `UrlSanitizer::DEFAULT_SENSITIVE_QUERY_PARAMETERS` (OpenTelemetry, OAuth 2.0 / OpenID Connect, API key and credential keys) are `REDACTED`; configure the list with `url_sanitizer([...])` or `flow_telemetry.instrumentation.sanitization.url.sensitive_query_parameters` (replaces the default) |
| Symfony HttpClient `url.full`, `url.scheme`, `server.address`, `server.port` came from the `$url` argument (`/me`, `unknown` with `base_uri`)     | they come from the effective URL, `base_uri` and the `query` option included                                                                                                                                                                                                                                                 |
| `exception.stacktrace` was `getTraceAsString()`, with argument values when `zend.exception_ignore_args` is off                                    | `StackTrace::format()`: every frame renders `()`, whatever `zend.exception_ignore_args` is                                                                                                                                                                                                                                   |

### 9) `flow-php/etl`,

`flow-php/etl-adapter-*` - an empty source without a declared schema fails at the source; Parquet and Floe accept
`withSchema()`

| Before                                                                                                  | After                                                                                                                   |
|---------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------|
| an empty source (`from_all()`, `from_array([])`, a glob that matches no file) described an empty schema | `schema()` throws `InferredSchemaException`; declare the schema with `->withSchema(...)` to read an empty source        |
| `df()->read(from_array([]))->fetch()` returned 0 rows                                                   | throws `InferredSchemaException`; `from_array([], schema: $schema)` returns 0 rows                                      |
| `from_parquet(...)->withSchema()`, `from_floe(...)->withSchema()` threw `InvalidArgumentException`      | read every file by name against the declared schema: undeclared columns are not read, a missing nullable column is null |
| `from_cache($id)` without a cache instance described an empty schema                                    | `SchemaNotDerivableException`; the plan runs unbound                                                                    |

### 10)

`flow-php/parquet` - the PHP engine reads and writes DELTA_LENGTH_BYTE_ARRAY / DELTA_BYTE_ARRAY; both engines write the
same default encodings (dictionary on, arrow-rs fallbacks); COLUMNS_ENCODINGS is literal on both engines

| Before                                                                                                                                                                                          | After                                                                                                                                                                                      |
|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `WRITER_VERSION 2` fallback encoding of BYTE_ARRAY / FIXED_LEN_BYTE_ARRAY: PLAIN                                                                                                                | `DELTA_BYTE_ARRAY`, as arrow-rs and parquet-java                                                                                                                                           |
| dictionary overflow past `DICTIONARY_PAGE_SIZE`: the rest of the chunk PLAIN                                                                                                                    | the `WRITER_VERSION` fallback: under v2 `DELTA_BINARY_PACKED` (INT32/INT64), `DELTA_BYTE_ARRAY` (BYTE_ARRAY/FLBA); PLAIN under v1                                                          |
| arrow writer: a non-dictionary `COLUMNS_ENCODINGS` entry kept the column's dictionary, used only once it fell back                                                                              | turns the column's dictionary off: the entry is the column's encoding, as the PHP writer writes it                                                                                         |
| `DeltaBinaryPackedDecoder::decode(string $data, int $valueCount)`                                                                                                                               | `decode(BinaryBufferReader $reader, int $valueCount)`, leaving the reader at the end of its stream                                                                                         |
| `new PlainFlatColumnChunkBuilder($column, $options, $compression, $statistics)`; `DeltaBinaryPackedColumnChunkBuilder`                                                                          | `new FlatColumnChunkBuilder($column, $options, $compression, $statistics, ValueStorage $valueStorage)`; `DeltaBinaryPackedColumnChunkBuilder` removed                                      |
| `ValueStorage` without an encoding                                                                                                                                                              | `ValueStorage::encoding(): Encodings`; `ValueStorage::size()` renamed to `plainSize()`, the PLAIN-encoded byte size                                                                        |
| `new RLEDictionaryChunkBuilder($column, $options, $compression, $statistics)`                                                                                                                   | `new RLEDictionaryChunkBuilder($column, $options, $compression, $statistics, ValueStorage $fallback)`                                                                                      |
| a failed `Writer::write()` / `Writer::open()` left a 4-byte `PAR1` file behind                                                                                                                  | the file is removed; the exception is rethrown                                                                                                                                             |
| PHP writer: columns without `COLUMNS_ENCODINGS` written without a dictionary                                                                                                                    | `RLE_DICTIONARY` (except BOOLEAN, and FIXED_LEN_BYTE_ARRAY under v1), as the arrow writer, arrow-rs and parquet-java; `Option::DICTIONARY_ENABLED` `false` restores the fallback encodings |
| -                                                                                                                                                                                               | `Option::DICTIONARY_ENABLED` (default `true`), on both engines                                                                                                                             |
| PHP writer `WRITER_VERSION 2`: BOOLEAN written PLAIN                                                                                                                                            | `RLE`, as the arrow writer                                                                                                                                                                 |
| `RLE_DICTIONARY` on a FIXED_LEN_BYTE_ARRAY column threw                                                                                                                                         | allowed under `WRITER_VERSION 2`; still refused under 1                                                                                                                                    |
| PHP writer fell back once the dictionary exceeded `DICTIONARY_PAGE_SIZE`                                                                                                                        | once it reaches it (`>=`), as arrow-rs                                                                                                                                                     |
| PHP writer v2 chunks listed only the data page encoding                                                                                                                                         | also `RLE` for the levels, as arrow-rs                                                                                                                                                     |
| PHP writer `DELTA_BINARY_PACKED`: a miniblock whose largest delta sits at or just above a power of two from about 2^47, or needs more than 55 bits, was packed wrong and read back wrong values | packed exactly; rewrite `WRITER_VERSION 2` files written by earlier versions whose INT32/INT64 columns hold such deltas                                                                    |
| `COLUMNS_ENCODINGS` `RLE_DICTIONARY` on a BOOLEAN column: the PHP writer wrote a dictionary, the arrow writer ignored it                                                                        | refused by both engines (`InvalidArgumentException`), as arrow-rs and parquet-java never dictionary-encode booleans                                                                        |
| arrow writer: a `COLUMNS_ENCODINGS` entry the column type cannot take panicked (`\Error: Rust panic`) or was ignored (FIXED_LEN_BYTE_ARRAY dictionary under v1)                                 | `InvalidArgumentException` with the PHP writer's message                                                                                                                                   |
| INT32 `DELTA_BINARY_PACKED` with consecutive values more than 2^31 apart: written with 64-bit deltas arrow-rs refuses, read back as wrong values                                                | 32-bit wrapping deltas, as arrow-rs and parquet-java; such INT32 files from earlier versions must be rewritten                                                                             |

The `arrow` extension must be rebuilt.

### 11) `flow-php/etl`, `flow-php/etl-adapter-postgresql`, `flow-php/etl-adapter-excel`, `flow-php/etl-adapter-seal`,

`flow-php/etl-adapter-json`, `flow-php/cli` - "entry" renamed to "column"

| Before                                                                                                                                                                                                                           | After                                                                                                  |
|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------|
| `DataFrame::withEntry()` / `withEntries()`                                                                                                                                                                                       | `withColumn()` / `withColumns()` (old names deprecated)                                                |
| `with_entry()`, `Flow\ETL\WithEntry`                                                                                                                                                                                             | `with_column()`, `Flow\ETL\Functions\WithColumn`                                                       |
| `entry()`                                                                                                                                                                                                                        | removed, use `ref()` or `col()`                                                                        |
| `Definition::entry()`                                                                                                                                                                                                            | `Definition::reference()`                                                                              |
| `SchemaDefinitionNotFoundException::entry()`                                                                                                                                                                                     | `column()`                                                                                             |
| `RenameEntryStrategy`, `Rename{Case,Map,Replace}EntryStrategy`                                                                                                                                                                   | `Flow\ETL\Operators\Transformer\Rename\{RenameColumnStrategy, Rename{Case,Map,Replace}ColumnStrategy}` |
| `RenameEntryTransformer`, `RenameEachEntryTransformer`                                                                                                                                                                           | `Flow\ETL\Operators\Transformer\{RenameColumnTransformer, RenameEachColumnTransformer}`                |
| `{Select,Drop,Prune}EntriesTransformer`                                                                                                                                                                                          | `Flow\ETL\Operators\Transformer\{Select,Drop,Prune}ColumnsTransformer`                                 |
| `DuplicatedEntriesException`                                                                                                                                                                                                     | `DuplicatedColumnsException`                                                                           |
| `Plan\Node\{WithColumn,WindowColumn,ExpandColumn}::$entry`, `Plan\Node\{Select,Drop,Distinct,DuplicateRow}::$entries`                                                                                                            | `$column`, `$columns`                                                                                  |
| `new SequenceExtractor($generator)` wrote a column named `entry`                                                                                                                                                                 | the column name is required: `new SequenceExtractor($generator, 'name')`                               |
| `Flow\ETL\Adapter\PostgreSql\EntryTypesMap`                                                                                                                                                                                      | `Flow\ETL\Adapter\PostgreSql\Values\Schema\TypesMap`                                                   |
| `ExcelLoader::withSheetNameFromEntry()`                                                                                                                                                                                          | `withSheetNameFromColumn()`                                                                            |
| `SealLoader::withIdentifierEntry()`                                                                                                                                                                                              | `withIdentifierColumn()`                                                                               |
| `JsonExtractor::withPointer($pointer, pointerToEntryName: true)`                                                                                                                                                                 | `pointerToColumnName: true`                                                                            |
| CLI option `--input-json-pointer-entry-name`                                                                                                                                                                                     | `--input-json-pointer-column-name`                                                                     |
| named arguments `entry:` / `entries:` (`select`, `drop`, `sortBy`, `groupBy`, `duplicateRow`, `ref`, `partition_by`, ...), `entry_name:` (`from_sequence_*`), `entryLeft:` / `entryRight:` (`Join\Comparison\{Equal,Identical}`) | `column:` / `columns:`, `column_name:`, `leftReference:` / `rightReference:`                           |
| exception messages saying `entry` / `entries` for a column (`Schema definition for entry "x" not found`, ...)                                                                                                                    | say `column` / `columns`                                                                               |

---

### 12) `flow-php/etl`, `flow-php/etl-adapter-csv`, `flow-php/etl-adapter-json`, `flow-php/etl-adapter-excel`,

`flow-php/etl-adapter-google-sheet` - `SchemaSampler` + `SniffsColumnTypes` merged into `Sniffer`

Schema inference samples a source through one contract. Each adapter builds its `Sniffer` inside `schema()`; a unit is
one
listed file (one sheet range, one in-memory dataset). A bounded `sampleSize` still drains one budget across the units in
listing order; `everyRow()` sniffs every unit whole and independently and merges the partials in unit order, so the
result does not depend on the order the units ran in.

| Before                                                                          | After                                                                                                                                                                                                                          |
|---------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `Flow\ETL\Schema\Inference\SchemaSampler`, `SniffsColumnTypes`                  | `Flow\ETL\IO\Inference\Sniffer` - `names(): list<string>`, `units(): int`, `sniff(int $unit, int $rowBudget): Sample`, `bytes(int $unit): int`, `parts(int $unit, int $targetBytes): non-empty-list<Sniffer>`, `close(): void` |
| `new SchemaInferrer($inference, $typer)->infer($names, $samples)`               | `new SchemaInferrer($inference)->infer($sniffer): Inferred` (`->schema`, `->units`) - the narrower is the sniffer's; `everyRow()` sniffs through `infer_schema()->sniffWith($launcher)`, in this process by default            |
| `SchemaInferrer::sniff($names, $rows, $rowBudget)`                              | `ColumnTypes::sniff($names, $rows, $rowBudget, $typer)`                                                                                                                                                                        |
| `CSVOpenSource::sniff($names, $rowBudget, $inference, $typer)`                  | `CSVOpenSource::sniff($names, $rowBudget, $typer)` (PHP, adaptive and `flow_php` native sources)                                                                                                                               |
| `CSVFileReader::samples()`, `CSVFileSample`, `CSVSamples`                       | `CSVSniffer(CSVSourceOpener $opener, $sources, InferredTypes $candidates, ?CSVHeader $header = null)` - `header()`; what a sniff read is on its `Sample`                                                                       |
| `JsonFileReader::samples()`, `JsonSamples`, `new JsonFileReader(..., $sources)` | `JsonSniffer($reader, $sources)`; the reader no longer takes the listing; `JsonFileSample::bytes()` / `complete()`                                                                                                             |
| `Flow\ETL\Adapter\CSV\{CSVSampledFiles, CSVSampledRows}`                        | removed: `Flow\ETL\IO\Inference\SampledUnits` (the row estimate over every sampled file) and `Flow\ETL\IO\Inference\Sample` (one file's sniff: columns, bytes, complete)                                                       |
| `Flow\ETL\Adapter\JSON\JSONMachine\{JsonSampledFiles, JsonSampledRows}`         | removed: `Flow\ETL\IO\Inference\SampledUnits` and `Flow\ETL\IO\Inference\Sample`, as for CSV                                                                                                                                   |
| `WorkbookSampler($workbook, $files)`                                            | `ExcelSniffer($workbook, $files, InferredTypes $candidates, ?ExcelHeader $header = null)`                                                                                                                                      |
| `GoogleSheetSampler($reader, $rowBudget)`                                       | `GoogleSheetSniffer($reader, $rowBudget, $typer)`                                                                                                                                                                              |
| `InMemoryRows::samples()`, `SpilledRows::samples()`                             | `sniff(0, $rowBudget)` - one unit, no header                                                                                                                                                                                   |
| `everyRow()`: `filesToSniff` counted files that yielded a row                   | `everyRow()`: every listed unit up to `filesToSniff` is sniffed whole and independently; empty files count                                                                                                                     |
| `ColumnTypes::merge()`, `ColumnTypes::schema()`                                 | `ColumnTypes::folded(): SniffedColumns`, then `SniffedColumns::merge()` / `schema()`                                                                                                                                           |
| `infer_schema()->sampleSize(-1)`, `->filesToSniff(-1)`                          | `infer_schema()->everyRow()`, `->everySource()`; `sampleSize()` and `filesToSniff()` refuse anything below 1                                                                                                                   |

---

### 13) `flow-php/etl`, `flow-php/filesystem`, `flow-php/parquet`, `flow-php/etl-adapter-csv`,

`flow-php/etl-adapter-parquet` and every file adapter - file sources say where they may be cut

Additive. A `SplittableExtractor` cuts its source into `FileSplit`s at plan time by arithmetic alone - one split per
file for JSON documents, text, XML, Excel and listings; byte windows for CSV, JSON lines, Parquet and Floe - and reads
one through `scan()` with the split on its `Criteria`. Splits cover every row exactly once, in source order. A CSV
window resyncs both ends to a record start, so a quoted newline never splits a record; a JSON lines window resyncs both
ends to a line start; a Parquet or Floe window owns the row groups or sections whose midpoint it holds. Through a
`Launcher` with more than one slot, set with `config_builder()->launcher()`, the splits are read
in worker processes. One launcher serves the whole run, joined frames included.

| Before                                                                                                                                          | After                                                                                                                                                                                                                                                                                                                                                                                  |
|-------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `Criteria(?int $limit, Filter $pathFilter, list<ScalarFunction> $conjuncts)`                                                                    | `Criteria(..., ?FileSplit $split = null)`, `withSplit(FileSplit)`; `narrows()` is true with a split; a read with a split lists nothing but the split's file                                                                                                                                                                                                                            |
| every file extractor read whole files only                                                                                                      | `SplittableExtractor extends Extractor { splits(Criteria, int $targetSplitBytes): non-empty-list<Split>; reader(): Reader }` on `files()`, `from_csv()`, `from_json()`, `from_json_lines()`, `from_text()`, `from_xml()`, `from_excel()`, `from_parquet()`, `from_floe()`; each split is read with `extract($backend, $criteria->withSplit($split))`, or through the config's launcher |
| `SelfDescribingFile { close(), schema(), source(), statistics() }`                                                                              | `+ units(): list<ByteUnit>` - row groups (Parquet) or sections (Floe) as byte ranges; a window owns the units whose midpoint it holds                                                                                                                                                                                                                                                  |
| `ParquetFileBatches(..., array $conjuncts)`, `FloeFileBatches(..., ?DerivedSchema $derived)`, `CSVFileBatches(CSVFileReader, ?InferredColumns)` | each reads `Criteria::$split` in `batches()` (see 121)                                                                                                                                                                                                                                                                                                                                 |
| `AdaptiveCSVOpenSource` opened and detected the dialect itself                                                                                  | `CSVSourceOpener(Filesystem, CSVReadOptions, ?FlowPhpExtension)` - `open(SourceFile)`, `openSplit(SourceFile, FileSplit, list<string> $columns)`, `dialect()`, `over()`; `AdaptiveCSVOpenSource` delegates to it                                                                                                                                                                       |
| `CSVDecoder(..., bool $emptyToNull)`, `RecordDecoder(..., bool $dropExtraColumns)`, `RustCSVOpenSource(..., ?int $charactersReadInLine)`        | each gains `?array $headers = null` - the names of a stream that carries no header record (a window past a file's first record); `withHeader` must be false with them                                                                                                                                                                                                                  |

### 14) `flow-php/filesystem-async-aws-bridge`, `-azure-bridge`, `-sftp-bridge` - filesystems take a client source

| Before                                                                         | After                                                                                                                                                     |
|--------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------|
| `new AsyncAWSS3Filesystem($mount, $bucket, S3Client $s3Client, $options)`      | `new AsyncAWSS3Filesystem($mount, $bucket, S3ClientSource $client, $options)` - `new LiveS3Client($s3Client)` or `new ConfiguredS3Client($configuration)` |
| `new AzureBlobFilesystem($mount, BlobServiceInterface $blobService, $options)` | `new AzureBlobFilesystem($mount, BlobServiceSource $blobService, $options)` - `new LiveBlobService(...)` or `ConfiguredBlobService::fromArray([...])`     |
| `new SFTPFilesystem($mount, SFTP $sftp, $options)`                             | `new SFTPFilesystem($mount, SFTPClientSource $client, $options)` - `new LiveSFTPClient($sftp)` or `ConfiguredSFTPClient::fromArray([...])`                |
| async-aws `Options::withBlockSize()` refused 5 MiB                             | 5 MiB, S3's inclusive minimum part size, is accepted                                                                                                      |

`aws_s3_filesystem()`, `azure_filesystem()` and `sftp_filesystem()` keep their parameter names and also accept a plain
configuration array; only a filesystem built from configuration can be described for a worker process. The Symfony
filesystem bundle builds the configured form unless an HTTP client, a factory or a logger is injected.

---

### 15) `flow-php/filesystem`, `flow-php/parquet` - every filesystem and parquet engine is configurable

| Before                                     | After                                                                                                                                                                                                                                                                                   |
|--------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `interface Filesystem` (10 methods)        | `+ normalize(): ?array`, `+ static fromArray(array $configuration): static` - the plain configuration a worker rebuilds the filesystem from; `null` / throw when the instance holds live state (a client, memory). Core wraps it in `Flow\ETL\Foundation\Describe\ConfiguredFilesystem` |
| `interface ParquetEngine` (3 methods)      | the same two methods (`fromArray(): self`); `PhpParquetEngine` and `AdaptiveParquetEngine` normalize `{byte_order, options}`, `RustParquetEngine` `{options}`; wrapped by `Flow\ETL\Adapter\Parquet\IO\ConfiguredParquetEngine`                                                         |
| `Path`                                     | `+ normalize(): PathData`, `+ static fromArray(array)`; `NativeLocalFileBlocksFactory::blockLocation()`; `OptionalType::assert()` is documented as returning `null                                                                                                           \|T`       |

---

### 16) `flow-php/etl` - `BucketsStorage::close()`

| Before                                                          | After                                                                                                                                                                    |
|-----------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `interface BucketsStorage { append(), get(), remove(), set() }` | `+ close(string $bucketId): void` - finishes the bucket's pending writes so another process can read it; a no-op without any. `FilesystemBuckets implements Describable` |

---

### 17) `flow-php/etl` - the default batch size is 2048 rows

| Before                                                   | After                                                                                                                                                                                                                                                                                                                                                                                              |
|----------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `batchSize()` is 100 for every extractor using `Batches` | 2048 - the per-batch cost dominated the read at 100 rows (a parallel read handed rows back in 100-row Floe frames; 10 GiB CSV: 51 s to 39 s single-process, 28 s to 9 s at 8 workers). `withBatchSize()` is unchanged; `PostgreSql*`/`Dbal*` extractors keep 1000. `FilesystemBuckets` defaults to 2048 as well, so a handed-back batch is never re-sliced                                         |

### 18) `flow-php/etl` and every adapter - functions, transformers, loaders and error handlers describe themselves

| Before                                                              | After                                                                                                                                                                                                                                      |
|---------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `interface ScalarFunction`, `Transformer`, `Loader`, `ErrorHandler` | each `extends Describable`: implement `describe(): ?Description` and `static fromDescription(array $data, Rebuild $rebuild): static`; return `null` / throw when the instance holds live state - it then always runs in the parent process |

### 19) `flow-php/etl`, `flow-php/etl-adapter-*` - `FileLoader` copies itself to another destination;

`{part}` is reserved

| Before                                                      | After                                                                                                                                                                         |
|-------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `interface FileLoader { destination(), saveMode() }`        | `interface FileLoader extends Loader`, `+ inTask(Part $task): static`, `+ commit(string $run, int $tasks): ?FileCommit`                                                       |
| `interface PartitioningLoader { partitionBy() }`            | removed: `FileLoader` takes `partitionBy(Partitioning $partitioning): static` and `partitioning(): Partitioning`                                                              |
| `{part}` in a destination named a partition column `part`   | `{part}` is the part number: `to_parquet('out/orders-{part}.parquet')` writes `out/orders-00000.parquet` in one process and one part per task when a launcher splits the read |
| `partitionBy('part')` with a `'.../{part}/...'` destination | throws `InvalidArgumentException` when the plan is built: rename the column                                                                                                   |
| SaveMode on a `{part}` destination checked per file         | checked over every existing part (`orders-*.parquet`): `ExceptionIfExists`/`Ignore` see any part, `Overwrite` removes every stale part, `Append` names carry a run id         |

### 20) `flow-php/filesystem` - `NativeLocalFilesystem::mv()` creates the target's parent directory

| Before                                                   | After                                                       |
|----------------------------------------------------------|-------------------------------------------------------------|
| `mv()` into a missing directory fails (`rename()` error) | the missing parent directories are created, then `rename()` |

### 21) `flow-php/etl` - aggregating and window functions describe themselves and expose a partial state

| Before                                                                                                                                     | After                                                                                                                                                                                                                                            |
|--------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `interface AggregatingFunction extends FunctionTree`                                                                                       | `extends FunctionTree, Describable`; `+ partialType(): StructureType`, `+ partial(): array`, `+ mergePartial(array $partial, FlowContext $context): void` - every custom aggregator implements them (a group by read by workers merges partials) |
| `interface WindowFunction extends FunctionTree`, `interface WindowFrame`                                                                   | each `extends Describable`; return `null` / throw when the instance holds live state - it then runs in the parent process                                                                                                                        |
| `Aggregators::cloned()`                                                                                                                    | `Aggregators::fresh()` - `withChildren(children())` per aggregator, never `clone`: a custom aggregator's `withChildren()` must return an instance without state                                                                                  |
| `Aggregators::aggregate(Rows $rows, array $indices, FlowContext $context)`, `Aggregators::merge(Aggregators $other, FlowContext $context)` | removed: `AggregatedGroups` accumulates and absorbs the groups; a caller aggregates each `AggregatingFunction` itself                                                                                                                            |

### 22) `flow-php/etl` - join expressions describe themselves; `HashJoinConfig` carries the broadcast limit

| Before                                         | After                                                                                                                       |
|------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------|
| `interface Comparison`                         | `extends Describable`; a custom comparison returning `null` keeps the join in the parent process                            |
| `new HashJoinConfig($bucketing, $memoryLimit)` | `new HashJoinConfig($bucketing, $memoryLimit, $broadcastLimit)`; `hash_join()->broadcastLimit(Unit)` sets it, default 10 MB |

### 23) `flow-php/etl` - `dropDuplicates()` runs as a processor

| Before                                                                                                            | After                                                                                                                                            |
|-------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------|
| `dropDuplicates()` planned as a `DropDuplicatesTransformer` step; a failure in it is offered to the error handler | planned as a `Flow\ETL\Operators\Processor\DistinctProcessor`; a failure in it propagates like any processor's; rows and streaming are unchanged |
| `explain()` prints `DropDuplicatesTransformer` among a segment's steps                                            | `explain()` prints `DistinctProcessor` among the steps of its pipeline                                                                           |

### 24) `flow-php/etl` - the step factories take a built algorithm config

| Before                                                                                      | After                                                                                                                                                                                 |
|---------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `GroupBySteps::of($groupBy, $config, ?GroupByAlgorithmBuilder $algorithm)`                  | `GroupBySteps::of($groupBy, $config, ?HashGroupByConfig $grouping)` - pass `$builder->build($config->cache->localFilesystemCacheDir, $config->backend())`                             |
| `SortSteps::of()` / `SortSteps::external($refs, $config, ?SortAlgorithmBuilder $algorithm)` | `SortSteps::of($refs, $config, null\|MemorySortConfig\|ExternalSortConfig $sort)`; `SortSteps::external($refs, ExternalSortConfig $sort, RandomValueGenerator $random): ExternalSort` |
| `RepartitionSteps::of($by, $config, ?RepartitionAlgorithmBuilder $algorithm)`               | `RepartitionSteps::of($by, $config, ?HashRepartitionConfig $repartition)`                                                                                                             |
| `JoinSteps::of($right, $on, $type, $config, ?JoinAlgorithmBuilder $algorithm)`              | `JoinSteps::of($right, $on, $type, $config, ?HashJoinConfig $join)`                                                                                                                   |

### 25) `flow-php/etl` - `count()` without a reference copies itself fresh

| Before                                                           | After                                                                                            |
|------------------------------------------------------------------|--------------------------------------------------------------------------------------------------|
| `Count::withChildren([])` on `count()` returns the same instance | returns `new Count(null, $window)` without state, like every other aggregator's `withChildren()` |

### 26) `flow-php/etl` - `Flow\Floe\Statistics` renamed to `FooterStatistics`

| Before                 | After                                    |
|------------------------|------------------------------------------|
| `Flow\Floe\Statistics` | `Flow\ETL\Storage\Floe\FooterStatistics` |

The footer struct (`rows`, `byteSize`) is no longer a third `Statistics` beside `Flow\ETL\IO\Extractor\Statistics` and
`Flow\ETL\Runtime\Metrics\Statistics`. The Floe bytes are unchanged.

### 27) `flow-php/filesystem`, `flow-php/etl` - `RangeSourceStream` exposes its window

| Before                                                      | After                                                                                          |
|-------------------------------------------------------------|------------------------------------------------------------------------------------------------|
| `RangeSourceStream` reads only through `iterate()`/`read()` | `stream()`, `start()`, `end()` (exclusive) name the inner stream and the window it is cut from |

The native CSV reader opens a `NativeLocalSourceStream` (or a `RangeSourceStream` over one) by its path and reads the
window itself; every other stream is still read through its PHP `iterate()`. `NativeLocalFilesystem::readFrom()` no
longer creates the file's parent directory, and `NativeLocalSourceStream::size()` is the open handle's size.

### 28) `flow-php/etl` - a Floe BATCH frame is decoded by the `Backend`

| Before                                                                             | After                                                                                                                                                                               |
|------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `(new FrameDecoder($codec))->decode($body, $schema, $backend)`                     | `$backend->decodeFrame($body, $schema, $codec)`; `Flow\Floe\FrameDecoder` is removed                                                                                                |
| `Backend` implementors: `builder`, `constant`, `decode`, `adopt`, `allocatedBytes` | also `decodeFrame(string $body, Schema $schema, Codec $codec): Rows`, `encodeFrame(Rows $rows, Codec $codec): string`                                                               |
| -                                                                                  | also `compare(Column $left, Operator $operator, Column $right, string $leftName, string $rightName): Column`, `isNull(Column $column): Column`, `isNotNull(Column $column): Column` |
| -                                                                                  | also `add`, `subtract`, `multiply`, `modulo`, each `(Column $left, Column $right): Column`                                                                                          |
| -                                                                                  | also `sort(Rows $rows, list<SortColumn> $columns): Rows`, rows equal on every column kept in input order                                                                            |
| -                                                                                  | also `groupedTotals(): GroupedTotals`: running sums and counts per group id, as `NumericTotal` keeps them per group                                                                 |
| -                                                                                  | also `groupedCounts(): GroupedCounts`: non-null value counts per group id, as `count(ref)` keeps them                                                                               |
| -                                                                                  | also `groupTable(list<string> $columns, Schema $declared): GroupTable`: dense group ids by key                                                                                      |

`PhpBackend::decodeFrame()` is the former `FrameDecoder` walk. `RustBackend::decodeFrame()` reads the frame directory
natively and points the Arrow buffers into the body string instead of copying every buffer through `decode()`.

`PhpBackend` answers comparisons, null tests, arithmetic and sorts as `Comparison`, `IsNull`, `IsNotNull`, `Plus`,
`Minus`, `Multiply`, `Mod` and the sort processors did; `RustBackend` answers the ones it can natively and the rest
through `PhpBackend`. `Divide` and the `exact` mode of `Plus`, `Minus` and `Multiply` stay in their functions. A custom
backend can delegate and adopt the result:

```php
public function compare(Column $left, Operator $operator, Column $right, string $leftName, string $rightName): Column
{
    return $this->adopt(
        Definitions::of('value', type_boolean(), true),
        (new PhpBackend())->compare($left, $operator, $right, $leftName, $rightName),
    );
}
```

### 29) `flow-php/etl` - a pipeline ends at a processor that needs every row; `Segment`, `Segments` and
`Materialization` removed

| Before                                                                                                                                               | After                                                                                                                                                               |
|------------------------------------------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `Processor` implementors: `bind`, `process`                                                                                                          | also `breaks(): bool` - true when `process()` needs every input row before it emits one; such a processor ends its pipeline                                         |
| a pipeline ended after a blocking node                                                                                                               | it ends at a processor that `breaks()`: a loader after `collect()` runs in the next pipeline, a window's `WindowProcessor` opens the pipeline after its repartition |
| `Executor\Segment`, `Executor\Segments`, `Pipeline::segments()`                                                                                      | removed; `Pipeline::$steps` is a `PipelineSteps(list $steps, null\|Processor\|JoinStep $breaker)`, run by `StepRunner::steps()`                                     |
| `new Pipeline(..., ?Pipeline $input)`                                                                                                                | `new Pipeline($id, $input, $steps)`: `$input` is the pipeline read; `Pipeline::input()` stays                                                                       |
| `Plan\Materialization`, `Node::materialization()`                                                                                                    | removed                                                                                                                                                             |
| `explain()`: `streaming` / `blocking` on a declarations line, `Buffers all rows before passing them on` under a node                                 | removed; `explain()->toString(Stage::physical)` shows where pipelines end                                                                                           |
| `Planner\PipelineSplit`                                                                                                                              | removed: `Planner::pipelines()` builds the physical plan                                                                                                            |
| `new GroupByAggregationProcessor($groupBy, HashBucketing $bucketing, Buckets $buckets, Unit $memoryLimit, int $batchSize)`                           | `new GroupByAggregationProcessor($groupBy, BucketingConfig $config, RandomValueGenerator $random)`                                                                  |
| `new PivotProcessor($groupBy, int $batchSize)`                                                                                                       | `new PivotProcessor($groupBy, BucketingConfig $config)`                                                                                                             |
| `new HashJoinProcessor($right, $executor, $on, $type, Buckets $left, Buckets $right, $random, Unit $memoryLimit, int $bucketsCount, int $batchSize)` | `new HashJoin($on, $type, HashJoinConfig $config, $random)` (row 94)                                                                                                |
| `new TopNProcessor($refs, $limit, ?ExternalSort $fallback)`                                                                                          | `new TopNProcessor($refs, $limit, MemorySortConfig\|ExternalSortConfig $config, RandomValueGenerator $random)`                                                      |
| `TopNProcessor::top(Rows $rows)`                                                                                                                     | `top(Rows $rows, Backend $backend)`: the backend sorts the rows                                                                                                     |
| `new MergeSortProcessor(ExternalSort $sort)`                                                                                                         | `new MergeSortProcessor(ExternalSortConfig $config, References $refs, RandomValueGenerator $random)`                                                                |
| `InvalidLogicException::pipelineWithoutSource()`                                                                                                     | removed - every pipeline reads an extractor or another pipeline                                                                                                     |

### 30) `flow-php/etl`, `flow-php/postgresql`, `flow-php/azure-sdk`, `flow-php/cli` - no traits

| Before                                                                   | After                                                                                                                                                    |
|--------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------|
| `use Flow\ETL\Extractor\Batches`                                         | a `BatchSize` field set in the constructor, plus your own `withBatchSize(int): static` (`new BatchSize($size)`) and `batchSize(): int` (`->size`)        |
| `use Flow\ETL\Extractor\File\FileReading` / `ListingPartitions`          | a `FileColumnsSource(SourceFiles $files)` field: `columns(): FileColumns`; `new SourceFiles($filesystem, $path)` lists the files (`statuses($criteria)`) |
| `use Flow\ETL\Extractor\File\PathFiltering`                              | a `DerivedFromFiles` field: `fold()`, `footers()`, `from()`, `schema()`, `learn()`; `with*()` replaces it with `new DerivedFromFiles()`                  |
| `use Flow\ETL\Extractor\File\MetadataColumns` / `DeclaresPartitionTypes` | `FileColumnsSource::withMetadataColumns()` / `withPartitionTypes()` / `partitionTypes()`; the host replaces its field                                    |
| `use Flow\ETL\Function\ResolvesFromChildren`                             | your own `resolved()` and `deterministic()`, each `true` when every child is                                                                             |
| `use Flow\PostgreSql\QueryBuilder\AstToSql`                              | `public function toSql(): string { return (new AstDeparser())->deparse($this->toAst()); }`                                                               |
| `Statement::assert(SelectStatement::class)`, `StatementTrait`            | `type_instance_of(SelectStatement::class)->assert($statement)`                                                                                           |
| `use Flow\Azure\SDK\Endpoints\UserAgentHeader`                           | removed; every endpoint options class keeps its `userAgentHeader()` / `withUserAgent()`                                                                  |
| `Flow\CLI\Command\Traits\*Options`, `DBOptions::DB_CONNECTION_HELP`      | `Flow\CLI\Options\*Options::configure(Command)`, `ConfigOption::configure()`, `SchemaInferenceOptions::apply()`, `DbConnectionOptions::HELP`             |

`ScalarFunctionChain` is the one trait left. `mago guard` refuses any new trait outside the test folders.

### 31) `flow-php/parquet` - the options given to `Writer` apply on `open()`

| Before                                                                                                                     | After                                                                                                                        |
|----------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------|
| `Writer::open()` / `openForStream()` took `Option::ROW_GROUP_SIZE_CHECK_INTERVAL` from the engine's options                | from the `Writer`'s options, as `write()` does                                                                               |
| the arrow engine took `Option::ARROW_WRITE_BATCH_SIZE` from the options it was built with, on `open()` and `write()` alike | from the options given to `openForWrite()` (the `Writer`'s), returned by `OptionsConverter::toExtension()`, not `toEngine()` |

### 32) `flow-php/etl` - `groupBy`, `dropDuplicates`, `repartition` and partitioned writes key rows by value identity

| Before                                                                                       | After                                                              |
|----------------------------------------------------------------------------------------------|--------------------------------------------------------------------|
| `groupBy` / `dropDuplicates` kept `0.0` and `-0.0` apart                                     | one key                                                            |
| `repartition()` / `partitionBy()` put the strings `"1"` and `"1.0"` in one partition         | two partitions                                                     |
| a `groupBy` spilling past its memory limit returned a date key twice                         | once                                                               |
| `GroupBy::tag()`                                                                             | removed: `(new KeyEncoder($columns))->of($rows)` keys a batch      |
| `Hasher::hash(list<list<mixed>> $values)`                                                    | `hash(list<string> $keys)`: one key per row, from a `KeyEncoder`   |
| `KeyValues::of()` returned `list<list<mixed>>`                                               | `list<string>`, the join-match key                                 |
| `new HashBucketing(list<Reference> $by, ...)`, `HashBucketing::indexOf(list<mixed> $values)` | `new HashBucketing(KeyEncoder $keys, ...)`; `indexOf(string $key)` |
| `new KeyGrouping(KeyValues $keys, ...)`                                                      | `new KeyGrouping(KeyEncoder $keys, Backend $backend)`              |
| `RowHashes` hashed column names with the values                                              | the values only, `KeyEncoder` over the sorted columns              |

### 33) `flow-php/etl` - `explain()` names classes through `ObjectExtractor::shortName()`

| Before                                                                                        | After                                                            |
|-----------------------------------------------------------------------------------------------|------------------------------------------------------------------|
| an anonymous class showed as `Transformer@anonymous` followed by a NUL byte and its file path | `class@anonymous`                                                |
| `Details::name(object $object)`                                                               | removed: `Flow\Telemetry\ObjectExtractor::shortName($object)`    |
| `new StepDetails(Details $details, Condition $condition, StatisticsLine $statistics)`         | `new StepDetails(StatisticsLine $statistics, NodeDetails $node)` |

### 35) `flow-php/etl` - one physical plan class, planner internals simplified

| Before                                                                                         | After                                                                                                                                                                                                                                     |
|------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `Executor\PhysicalPlan` interface, `Executor\Described`, `Executor\Raw`                        | `final readonly class PhysicalPlan(Pipeline $root, ?Schema $schema, ?SchemaNotDerivableException $refusal = null, bool $countsOnly = false)`; the config's `Launcher::read(PhysicalPlan $plan, ...): ?Launch` cuts the launch at run time |
| `$plan instanceof Described`, `Raw::$why`, `Described::$schema`                                | `$plan->described()`, `$plan->refusal()`, `$plan->schema()`                                                                                                                                                                               |
| `NodeTranslator::algorithm()`, `NodeTranslator::toSteps($node, $context, $frames, $algorithm)` | removed; `steps(Node $node)` on an instance (row 103)                                                                                                                                                                                     |
| `Optimizer\TopNRewrite`                                                                        | removed: `CombineSortAndLimit` is the `Rewrite`                                                                                                                                                                                           |
| `new SinkAttachment($planned, $context)`, `SinkAttachment::next()`                             | `new WritePlanner($planned)`, `WritePlanner::next()`; it returns `BranchPipeline` and `TransactionGroup` plan values (row 95)                                                                                                             |
| `(new CountingProcessor())->rows($count, $backend)` / `->schema()`                             | `CountingProcessor::rows($count, $backend)` / `CountingProcessor::schema()`                                                                                                                                                               |
| logs `Error during ETL segment execution.` (and the `Skipping ...` variants)                   | `... ETL pipeline execution.`                                                                                                                                                                                                             |

### 36) `flow-php/etl` and the file adapters - file-read plumbing: one listing object, one read config, one header type

| Before                                                                                                                                                 | After                                                                                                                                 |
|--------------------------------------------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------|
| `PartitionColumns`                                                                                                                                     | removed: `FileColumns::declare()`; the names are listed by `FileColumnsSource::columns()`                                             |
| `new FileColumns(PartitionColumns $columns, array $names, PartitionTypes $types, bool $metadataColumns)`, `FileColumns::fromArray($data, $filesystem)` | `new FileColumns(array $names, PartitionTypes $types, bool $metadataColumns)`, `FileColumns::fromArray($data)`                        |
| `new FileColumnsSource($filesystem, $path, $types, $metadataColumns)`                                                                                  | a fifth argument, `?array $partitionNames = null` (listed on the first `columns()` call when null)                                    |
| `new CSVSplitReader($filesystem, $source, $fileColumns, $schema, $batchSize, ...)` (and the JSON, Excel, XML, Text, Parquet and Floe split readers)    | `new FileSplitReader(new FileRead($filesystem, $source, $fileColumns, $schema, $batchSize), new CSVFileBatches(...))` (see 121)       |
| `CSVFileReader`                                                                                                                                        | removed: `new CSVFileBatches(CSVSourceOpener $opener, ?InferredColumns $inferred)`; `CSVSniffer::header()`                            |
| `CSVHeader`, `ExcelHeader`                                                                                                                             | `Flow\ETL\IO\Extractor\Grid\GridHeader`                                                                                               |
| `ExcelExtractor::reader(?ExcelSniffer $sampled)`, `ParquetExtractor::reader(?ParquetSourceFile $firstFile)`                                            | `reader()`                                                                                                                            |
| a split reader rebuilt in a worker detected the CSV dialect of a file the parent had detected                                                          | `CSVSourceOpener` describes the dialects it learned                                                                                   |
| `DerivedFromFiles::learn(Schema $schema)`                                                                                                              | `learn(Schema $schema, string $from = '')`; `from()` also names the learned source                                                    |
| `DerivedFromFiles::footers()` after `learn()` returned empty footer statistics                                                                         | throws `InvalidLogicException`                                                                                                        |
| `new InferredRows($source, $sample)`, `InferredRows::of(array $rows, Schema $schema, Backend $backend)`                                                | `new InferredRows($source, $sample, Schema $schema, Backend $backend, int $batchSize)`; `add(array $record): ?Rows`, `flush(): ?Rows` |
| `JsonFileReader::batches(SourceFile $source, int $batchSize, ?FileSplit $window)`                                                                      | removed: `sample($source, window: $window)` yields the records; `Flow\ETL\Values\Rows\RecordBatch` batches them                       |

### 37) `flow-php/etl` - one listing extractor, Floe and row-building internals trimmed

| Before                                                                                                                     | After                                                                                                                               |
|----------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------|
| `files()` returned `FilesExtractor` (a `SplittableExtractor`), `from_path_partitions()` returned `PathPartitionsExtractor` | both return `FileListingExtractor`; it is not a `SplittableExtractor`; `FilesSplitReader` removed                                   |
| `from_path_partitions()` statistics: rows only                                                                             | rows and the listed bytes                                                                                                           |
| `ListedFileRow` implementors: `row()`                                                                                      | also `schema(): Schema`, the default body columns                                                                                   |
| `Flow\Floe\FrameReader`                                                                                                    | removed, no replacement: read Floe files through `from_floe()`                                                                      |
| `new FloeSourceFile(FloeStreamReader $reader, SourceFile $source)`, `FloeSourceFile::$reader`                              | `new FloeSourceFile(Footer $footer, SourceFile $source)`                                                                            |
| `FloeWriter::createForStream($stream, $metadata)`                                                                          | removed: `(new FloeStreamWriter($schema, $backend, $options))->create($stream, $metadata)`                                          |
| `new FloeFileSinks($filesystem, $declared, $metadata, $options)`, `FloeFileSinks::writer(): FloeWriter`                    | `new FloeFileWriterFactory($declared, $metadata, $options)`, `writer(): FileWriter`                                                 |
| `MidpointOwnership::rowsOf($split, $units)`                                                                                | `$split->owns($units)`                                                                                                              |
| `SchemaInference::fromArray()` clamped a size below 1 to 1                                                                 | `new SchemaInference()` and `fromArray()` refuse a size below 1 other than -1 (`InvalidArgumentException`)                          |
| `Rows::withColumns()` refused a wrongly sized column the schema does not name                                              | ignores it, as it ignores the column                                                                                                |
| `Flow\ETL\RowsFactory`                                                                                                     | removed                                                                                                                             |
| -                                                                                                                          | `Rows::castTo(Schema $schema, Backend $backend)`, `ReadWindow::trim()` / `reached()`, `InvalidArgumentException::unsupportedPath()` |

### 38) `flow-php/etl` - bound steps are immutable

| Before                                                                                                                                        | After                                                                                                                                                                                                         |
|-----------------------------------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `HashJoinProcessor`, `GroupByAggregationProcessor`, `PivotProcessor`, `DuplicateRowTransformer`: `final class`, `bind()` set fields on a copy | `final readonly class`; the bound state is a trailing optional constructor argument (`?Schema $left`, `?Schema $declaredRight`; `?GroupByShape $shape`; `?PivotShape $shape`; `$resolved`, `?Schema $output`) |
| `new DuplicateRowTransformer($condition, WithColumn ...$columns)`                                                                             | `new DuplicateRowTransformer($condition, array $columns)`                                                                                                                                                     |

### 39) `flow-php/etl` - join rules live on the join type; one join schema, one probe loop

| Before                                                                                               | After                                                                                                                  |
|------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------|
| `JoinShape::of($expression, $type)->schema()->of($type, $left, $right)`                              | `JoinSchema::of($expression, $type)->output($left, $right)`; `JoinShape` removed                                       |
| `JoinShape::of(...)->merger()`, `new RowMerger($prefix, $dropLeft, $dropRight)`                      | `JoinSchema::of(...)->merger($left, $right)`, `new RowMerger(Schema $output, list<string> $left, list<string> $right)` |
| `new JoinSchema($prefix, $dropLeft, $dropRight)`, `JoinSchema::of(Join $type, $left, $right)`        | a fourth argument, `Join $type`; `output($left, $right)`                                                               |
| `RowMerger::merge()` threw `DuplicatedColumnsException` on a name collision                          | the collision is refused by `JoinSchema::output()` (`SchemaDefinitionNotUniqueException`)                              |
| `NullRowBuilder`                                                                                     | removed: `Rows::nulls(Schema $schema, Backend $backend)`                                                               |
| `JoinSide::of($rows, ?Rows $nullRow, ?Schema $schema)`                                               | `JoinSide::of($rows, ?Schema $schema)`; `nullRow(Schema $fallback, Backend $backend)`                                  |
| -                                                                                                    | `Join::dropsNullKeys(Side $side)`, `Join::padsWithNullRow(Side $side)`, `enum Side`                                    |

### 40) `flow-php/etl` - join equality, grouping, pivot, batch size and telemetry internals

| Before                                                                  | After                                                                                                    |
|-------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------|
| `join(..., join_on(['k' => 'k']))` matched `'1'` with `'1.0'`           | no match, the same rule as `ref('k')->equals(ref('j'))`                                                  |
| `GroupBy::aggregatedValues()`                                           | removed                                                                                                  |
| `PivotAggregation::aggregateBound($rows, $context, $groupBy, $shape)`   | `PivotAggregation::aggregate($rows, $context, $groupBy, $shape)`                                         |
| `PivotedTable::flush(0, $context)` threw `InvalidArgumentException`     | the batch size is checked by `new PivotAggregation($batchSize)`                                          |
| `new DuplicateRowTransformer($condition, $columns, $resolved, $output)` | a fifth argument, `?array $steps`                                                                        |
| -                                                                       | `ReferenceResolver::bind(FunctionTree $function, Schema $schema, string $method, ?Type $returns = null)` |

| `Flow\ETL\Extractor\BatchSize`                                                                                                                                                      | `Flow\ETL\BatchSize`                                                                                  |
| `new Transformation\BatchSize(int $size)`                                                                                                                                           | `new Transformation\BatchSize(BatchSize $size)`; `batch_size(int $size)` unchanged                    |
| `int $batchSize` in `new Joiner()`, `new ExternalSort()`, `new KWayMerge()`, `new PivotAggregation()`, `new ExpandingProcessor()`, `new SpillingBuckets()`, `PivotedTable::flush()` | `BatchSize $batchSize`                                                                                |
| `new BatchingProcessor(int $size)`, `new Plan\Node\Batch($input, int $size)`, `int $size` property                                                                                  | `BatchSize $size`                                                                                     |
| these threw `InvalidArgumentException` `Batch size must be greater than 0, given: 0`                                                                                                | `new BatchSize(0)` throws `Batch size must be greater than 0, got 0`                                  |
| `TopNProcessor::followedBy($kept, $rest)`, `(new BoundedRead($limit, $backend))->followedBy($read, $rest)`                                                                          | `BoundedRead::followedBy($read, $rest)`                                                               |
| `DerivedColumns::declare($schema, $definition)`                                                                                                                                     | `$schema->put($definition)`                                                                           |
| a transformer or loader opened its own telemetry span                                                                                                                               | the pipeline opens one span per step, user transformers and loaders included                          |
| `FileWriteFrame::write($rows, $context, $loader)`                                                                                                                                   | `FileWrite::write(Rows $rows, Backend $backend)`                                                      |
| -                                                                                                                                                                                   | `Flow\ETL\Foundation\Telemetry\SpanAttributes::spanAttributes()`: extra attributes for a step's span  |
| `OnEach::typeOver(Type $arrayType)`                                                                                                                                                 | `OnEach::typeOver(Type $arrayType, ScalarFunction $body)`                                             |
| `Hashes::add()`, `Hashes::exists()`                                                                                                                                                 | removed: `Hashes::keep()`                                                                             |
| `GroupBy\PivotSchema`                                                                                                                                                               | removed: `GroupBy::pivotSchema(Schema $input, array $pivotColumns, AggregatingFunction $aggregation)` |
| `new ScalarFunctionTransformer($column, $function, $resolved, $derived, $output)`                                                                                                   | a sixth argument, `?Schema $declared`                                                                 |
| `GroupBy::references()`                                                                                                                                                             | `GroupBy::refs()->all()`                                                                              |

### 42) `flow-php/etl`, adapters - file extractor and reader trims

| Before                                                                                                                            | After                                                                                                                 |
|-----------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------|
| `declaredPartitionTypes()` on the CSV, JSON, Text, XML, Excel, Parquet and Floe extractors, `FileColumnsSource::partitionTypes()` | removed                                                                                                               |
| `new AdaptiveCSVOpenSource($filesystem, $source, $options, $extension)`                                                           | `(new CSVSourceOpener($filesystem, $options, extension: $extension))->open($source)`                                  |
| `ParquetSchemaPassFile`                                                                                                           | removed: `DerivedFromFiles::fold()` leaves the first file open for its owner                                          |
| -                                                                                                                                 | `ParquetSourceFile::forSource(SourceFile $source)`, `FileSplit::window(SourceStream $stream, ?int $start, ?int $end)` |
| `JsonLineResync::window($stream, $split)`                                                                                         | `$split->window($stream, $resync->after(...), $resync->after(...))`                                                   |
| `new JsonFileBatches($filesystem, $reader, $format, $pointer, $split)`                                                            | `new JsonFileBatches($filesystem, $reader)`; the split arrives through `batches()` (see 121)                          |
| -                                                                                                                                 | `JsonFileReader::pointsInto()`, `JsonFileReader::window(?FileSplit $split)`                                           |
| `ParquetStoredType::of($normalized)`                                                                                              | `ParquetSchemaConformance::storedType($normalized)`                                                                   |
| `WorkbookSheetSample::rows()`                                                                                                     | removed                                                                                                               |

### 43) `flow-php/parquet`, `flow-php/arrow-ext`, `flow-php/flow-php-ext` - reader and encoder trims

| Before                                                                                                                                                                                                                                                                           | After                                                                                                                                                |
|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------|
| `ResolvedLeaf` (`?PhysicalIntervals $intervals`), `LeafResolver::resolve()`                                                                                                                                                                                                      | `ExactLeaf` (`PhysicalIntervals $intervals`), `InexactLeaf`, `BoundFilter::resolve()`                                                                |
| `StatisticsReader`; `statistics(): ?StatisticsReader` on `ColumnChunk`, `DataPageHeader`, `DataPageHeaderV2`                                                                                                                                                                     | `statistics(): ?Statistics`; `Statistics::min()`, `max()`, `minValue()`, `maxValue()`; counts and exactness as properties (`$statistics->nullCount`) |
| engine configuration `byte_order` as a case name (`'LITTLE_ENDIAN'`)                                                                                                                                                                                                             | the value (`'V'`, `'N'`)                                                                                                                             |
| `ReadFlatColumnValues::rawValues(): Generator`                                                                                                                                                                                                                                   | `rawValues(): array`                                                                                                                                 |
| `new RustCSVEncoder($separator, $enclosure, $escape, $newLineSeparator, $dateTimeFormat, $dateFormat, $php)`                                                                                                                                                                     | `new RustCSVEncoder(CSVWriteOptions $options, PhpCSVEncoder $php)`                                                                                   |
| `new RustJsonEncoder($flags, $dateTimeFormat, $dateFormat, $php)`                                                                                                                                                                                                                | `new RustJsonEncoder(PhpJsonEncoder $php)`; `PhpJsonEncoder::$flags`, `$dateTimeFormat`, `$dateFormat` are public                                    |
| `ParquetFile\Page\ColumnData`, `RowGroup\FlowColumnChunk`, `RowGroup::addColumnChunk()`, `RowGroup::setRowsCount()`, `ColumnChunkBuilders::builders()`, `PageContainer::dataSize()`, `WriteFlatColumnValues::definitionLevelsCount()`, `addRaw()`, `skipRows()`, `splitByRows()` | removed                                                                                                                                              |
| `ColumnRef` threw `\InvalidArgumentException`                                                                                                                                                                                                                                    | `Flow\Parquet\Exception\InvalidArgumentException`                                                                                                    |
| -                                                                                                                                                                                                                                                                                | `Reader\ChunkBuffer`, `RustRowGroupChunks::decoded()`                                                                                                |

### 44) `flow-php/etl` - `NativePHPHash::xxh128()` removed

| Before                           | After                                                                |
|----------------------------------|----------------------------------------------------------------------|
| `NativePHPHash::xxh128($string)` | `hash('xxh128', $string)`, or `(new NativePHPHash())->hash($string)` |

### 45) `flow-php/etl` - foundation and memory accounting moved to their layer namespaces

`Flow\Calculator`, `Flow\Clock` and `Flow\ArrayComparison` move under `Flow\ETL\Foundation`, memory accounting
under `Flow\ETL\Values\Memory`. Class names do not change.

| Before                                                 | After                                                                 |
|--------------------------------------------------------|-----------------------------------------------------------------------|
| `Flow\ArrayComparison\ArrayComparison`                 | `Flow\ETL\Foundation\ArrayComparison\ArrayComparison`                 |
| `Flow\ArrayComparison\ArraySortByKey`                  | `Flow\ETL\Foundation\ArrayComparison\ArraySortByKey`                  |
| `Flow\Calculator\Calculator`                           | `Flow\ETL\Foundation\Calculator\Calculator`                           |
| `Flow\Calculator\Exception\Exception`                  | `Flow\ETL\Foundation\Calculator\Exception\Exception`                  |
| `Flow\Calculator\Exception\InvalidExponentException`   | `Flow\ETL\Foundation\Calculator\Exception\InvalidExponentException`   |
| `Flow\Calculator\Exception\InvalidScaleException`      | `Flow\ETL\Foundation\Calculator\Exception\InvalidScaleException`      |
| `Flow\Calculator\Exception\NonNumericValueException`   | `Flow\ETL\Foundation\Calculator\Exception\NonNumericValueException`   |
| `Flow\Calculator\Exception\RoundingNecessaryException` | `Flow\ETL\Foundation\Calculator\Exception\RoundingNecessaryException` |
| `Flow\Calculator\NumberNormalizer`                     | `Flow\ETL\Foundation\Calculator\NumberNormalizer`                     |
| `Flow\Calculator\Rounding`                             | `Flow\ETL\Foundation\Calculator\Rounding`                             |
| `Flow\Calculator\RunningSum`                           | `Flow\ETL\Foundation\Calculator\RunningSum`                           |
| `Flow\Clock\FakeClock`                                 | `Flow\ETL\Foundation\Clock\FakeClock`                                 |
| `Flow\Clock\SystemClock`                               | `Flow\ETL\Foundation\Clock\SystemClock`                               |
| `Flow\ETL\Dataset\Memory\BoundedRead`                  | `Flow\ETL\Values\Memory\BoundedRead`                                  |
| `Flow\ETL\Dataset\Memory\Configuration`                | `Flow\ETL\Values\Memory\Configuration`                                |
| `Flow\ETL\Dataset\Memory\Consumption`                  | `Flow\ETL\Values\Memory\Consumption`                                  |
| `Flow\ETL\Dataset\Memory\MemoryBudget`                 | `Flow\ETL\Values\Memory\MemoryBudget`                                 |
| `Flow\ETL\Dataset\Memory\Unit`                         | `Flow\ETL\Values\Memory\Unit`                                         |
| `Flow\ETL\FlowPhpExtension`                            | `Flow\ETL\Foundation\FlowPhpExtension`                                |
| `Flow\ETL\NativePHPRandomValueGenerator`               | `Flow\ETL\Foundation\NativePHPRandomValueGenerator`                   |
| `Flow\ETL\RandomValueGenerator`                        | `Flow\ETL\Foundation\RandomValueGenerator`                            |

### 46) `flow-php/etl` - value types moved to the `Flow\ETL\Values` namespace

Rows, schemas, definitions, columns and their physical layouts move under `Flow\ETL\Values`; the Floe frame codec
contract moves to `Flow\ETL\Values\Column\Frame`. Class names do not change.

| Before                                                        | After                                                                |
|---------------------------------------------------------------|----------------------------------------------------------------------|
| `Flow\ETL\BatchSize`                                          | `Flow\ETL\Values\BatchSize`                                          |
| `Flow\ETL\Cardinality`                                        | `Flow\ETL\Values\Cardinality`                                        |
| `Flow\ETL\Column\AdaptiveBackend`                             | `Flow\ETL\Values\Column\AdaptiveBackend`                             |
| `Flow\ETL\Column\Backend`                                     | `Flow\ETL\Values\Column\Backend`                                     |
| `Flow\ETL\Column\Column`                                      | `Flow\ETL\Values\Column\Column`                                      |
| `Flow\ETL\Column\ColumnBuilder`                               | `Flow\ETL\Values\Column\ColumnBuilder`                               |
| `Flow\ETL\Column\ComparableValues`                            | `Flow\ETL\Values\Column\ComparableValues`                            |
| `Flow\ETL\Column\DateFormatPattern`                           | `Flow\ETL\Values\Column\DateFormatPattern`                           |
| `Flow\ETL\Column\Elements`                                    | `Flow\ETL\Values\Column\Elements`                                    |
| `Flow\ETL\Column\Layout\BooleanLayout`                        | `Flow\ETL\Values\Column\Layout\BooleanLayout`                        |
| `Flow\ETL\Column\Layout\BufferLayout`                         | `Flow\ETL\Values\Column\Layout\BufferLayout`                         |
| `Flow\ETL\Column\Layout\Buffers`                              | `Flow\ETL\Values\Column\Layout\Buffers`                              |
| `Flow\ETL\Column\Layout\FixedBinary16Layout`                  | `Flow\ETL\Values\Column\Layout\FixedBinary16Layout`                  |
| `Flow\ETL\Column\Layout\Float64Layout`                        | `Flow\ETL\Values\Column\Layout\Float64Layout`                        |
| `Flow\ETL\Column\Layout\Int32Layout`                          | `Flow\ETL\Values\Column\Layout\Int32Layout`                          |
| `Flow\ETL\Column\Layout\Int64Layout`                          | `Flow\ETL\Values\Column\Layout\Int64Layout`                          |
| `Flow\ETL\Column\Layout\Layout`                               | `Flow\ETL\Values\Column\Layout\Layout`                               |
| `Flow\ETL\Column\Layout\NullCount`                            | `Flow\ETL\Values\Column\Layout\NullCount`                            |
| `Flow\ETL\Column\Layout\Offsets`                              | `Flow\ETL\Values\Column\Layout\Offsets`                              |
| `Flow\ETL\Column\Layout\Utf8Layout`                           | `Flow\ETL\Values\Column\Layout\Utf8Layout`                           |
| `Flow\ETL\Column\Layout\Validity`                             | `Flow\ETL\Values\Column\Layout\Validity`                             |
| `Flow\ETL\Column\PhpBackend`                                  | `Flow\ETL\Values\Column\PhpBackend`                                  |
| `Flow\ETL\Column\Php\CastingColumnBuilder`                    | `Flow\ETL\Values\Column\Php\CastingColumnBuilder`                    |
| `Flow\ETL\Column\Php\ColumnDecoder`                           | `Flow\ETL\Values\Column\Php\ColumnDecoder`                           |
| `Flow\ETL\Column\Php\ConstantColumn`                          | `Flow\ETL\Values\Column\Php\ConstantColumn`                          |
| `Flow\ETL\Column\Php\ListColumn`                              | `Flow\ETL\Values\Column\Php\ListColumn`                              |
| `Flow\ETL\Column\Php\ListColumnBuilder`                       | `Flow\ETL\Values\Column\Php\ListColumnBuilder`                       |
| `Flow\ETL\Column\Php\MapColumn`                               | `Flow\ETL\Values\Column\Php\MapColumn`                               |
| `Flow\ETL\Column\Php\MapColumnBuilder`                        | `Flow\ETL\Values\Column\Php\MapColumnBuilder`                        |
| `Flow\ETL\Column\Php\NullColumnBuilder`                       | `Flow\ETL\Values\Column\Php\NullColumnBuilder`                       |
| `Flow\ETL\Column\Php\PhpColumnBuilder`                        | `Flow\ETL\Values\Column\Php\PhpColumnBuilder`                        |
| `Flow\ETL\Column\Php\ScalarColumn`                            | `Flow\ETL\Values\Column\Php\ScalarColumn`                            |
| `Flow\ETL\Column\Php\ScalarColumnBuilder`                     | `Flow\ETL\Values\Column\Php\ScalarColumnBuilder`                     |
| `Flow\ETL\Column\Php\StructColumn`                            | `Flow\ETL\Values\Column\Php\StructColumn`                            |
| `Flow\ETL\Column\Php\StructColumnBuilder`                     | `Flow\ETL\Values\Column\Php\StructColumnBuilder`                     |
| `Flow\ETL\Column\Physical\DatePhysical`                       | `Flow\ETL\Values\Column\Physical\DatePhysical`                       |
| `Flow\ETL\Column\Physical\DateTimePhysical`                   | `Flow\ETL\Values\Column\Physical\DateTimePhysical`                   |
| `Flow\ETL\Column\Physical\DaysFromCivil`                      | `Flow\ETL\Values\Column\Physical\DaysFromCivil`                      |
| `Flow\ETL\Column\Physical\ElementPosition`                    | `Flow\ETL\Values\Column\Physical\ElementPosition`                    |
| `Flow\ETL\Column\Physical\EnumPhysical`                       | `Flow\ETL\Values\Column\Physical\EnumPhysical`                       |
| `Flow\ETL\Column\Physical\HtmlDocumentPhysical`               | `Flow\ETL\Values\Column\Physical\HtmlDocumentPhysical`               |
| `Flow\ETL\Column\Physical\HtmlElementPhysical`                | `Flow\ETL\Values\Column\Physical\HtmlElementPhysical`                |
| `Flow\ETL\Column\Physical\IdentityPhysical`                   | `Flow\ETL\Values\Column\Physical\IdentityPhysical`                   |
| `Flow\ETL\Column\Physical\JsonFactory`                        | `Flow\ETL\Values\Column\Physical\JsonFactory`                        |
| `Flow\ETL\Column\Physical\JsonPhysical`                       | `Flow\ETL\Values\Column\Physical\JsonPhysical`                       |
| `Flow\ETL\Column\Physical\ListPhysical`                       | `Flow\ETL\Values\Column\Physical\ListPhysical`                       |
| `Flow\ETL\Column\Physical\MapPhysical`                        | `Flow\ETL\Values\Column\Physical\MapPhysical`                        |
| `Flow\ETL\Column\Physical\NullPhysical`                       | `Flow\ETL\Values\Column\Physical\NullPhysical`                       |
| `Flow\ETL\Column\Physical\Physical`                           | `Flow\ETL\Values\Column\Physical\Physical`                           |
| `Flow\ETL\Column\Physical\PhysicalFor`                        | `Flow\ETL\Values\Column\Physical\PhysicalFor`                        |
| `Flow\ETL\Column\Physical\StructPhysical`                     | `Flow\ETL\Values\Column\Physical\StructPhysical`                     |
| `Flow\ETL\Column\Physical\TimePhysical`                       | `Flow\ETL\Values\Column\Physical\TimePhysical`                       |
| `Flow\ETL\Column\Physical\TimeZonePhysical`                   | `Flow\ETL\Values\Column\Physical\TimeZonePhysical`                   |
| `Flow\ETL\Column\Physical\TimeZones`                          | `Flow\ETL\Values\Column\Physical\TimeZones`                          |
| `Flow\ETL\Column\Physical\UuidPhysical`                       | `Flow\ETL\Values\Column\Physical\UuidPhysical`                       |
| `Flow\ETL\Column\Physical\XmlCharacterReferences`             | `Flow\ETL\Values\Column\Physical\XmlCharacterReferences`             |
| `Flow\ETL\Column\Physical\XmlDocumentPhysical`                | `Flow\ETL\Values\Column\Physical\XmlDocumentPhysical`                |
| `Flow\ETL\Column\Physical\XmlElementPhysical`                 | `Flow\ETL\Values\Column\Physical\XmlElementPhysical`                 |
| `Flow\ETL\Column\Retype`                                      | `Flow\ETL\Values\Column\Retype`                                      |
| `Flow\ETL\Column\RustBackend`                                 | `Flow\ETL\Values\Column\RustBackend`                                 |
| `Flow\ETL\Column\RustColumn`                                  | `Flow\ETL\Values\Column\RustColumn`                                  |
| `Flow\ETL\Column\RustColumnBuilder`                           | `Flow\ETL\Values\Column\RustColumnBuilder`                           |
| `Flow\ETL\Column\RustColumnsBatch`                            | `Flow\ETL\Values\Column\RustColumnsBatch`                            |
| `Flow\ETL\Column\TextValues`                                  | `Flow\ETL\Values\Column\TextValues`                                  |
| `Flow\ETL\Column\ValueColumn`                                 | `Flow\ETL\Values\Column\ValueColumn`                                 |
| `Flow\ETL\Row\ColumnName`                                     | `Flow\ETL\Values\Column\ColumnName`                                  |
| `Flow\ETL\Row\NullsOrder`                                     | `Flow\ETL\Values\NullsOrder`                                         |
| `Flow\ETL\Row\SortOrder`                                      | `Flow\ETL\Values\SortOrder`                                          |
| `Flow\ETL\Rows`                                               | `Flow\ETL\Values\Rows`                                               |
| `Flow\ETL\Rows\ColumnRetyping`                                | `Flow\ETL\Values\Rows\ColumnRetyping`                                |
| `Flow\ETL\Rows\RecordBatch`                                   | `Flow\ETL\Values\Rows\RecordBatch`                                   |
| `Flow\ETL\Rows\RowsBuilder`                                   | `Flow\ETL\Values\Rows\RowsBuilder`                                   |
| `Flow\ETL\Schema`                                             | `Flow\ETL\Values\Schema`                                             |
| `Flow\ETL\SchemaValidator`                                    | `Flow\ETL\Values\SchemaValidator`                                    |
| `Flow\ETL\Schema\Definition`                                  | `Flow\ETL\Values\Schema\Definition`                                  |
| `Flow\ETL\Schema\Definition\BooleanDefinition`                | `Flow\ETL\Values\Schema\Definition\BooleanDefinition`                |
| `Flow\ETL\Schema\Definition\DateDefinition`                   | `Flow\ETL\Values\Schema\Definition\DateDefinition`                   |
| `Flow\ETL\Schema\Definition\DateTimeDefinition`               | `Flow\ETL\Values\Schema\Definition\DateTimeDefinition`               |
| `Flow\ETL\Schema\Definition\ElementCompatibility`             | `Flow\ETL\Values\Schema\Definition\ElementCompatibility`             |
| `Flow\ETL\Schema\Definition\EnumDefinition`                   | `Flow\ETL\Values\Schema\Definition\EnumDefinition`                   |
| `Flow\ETL\Schema\Definition\FloatDefinition`                  | `Flow\ETL\Values\Schema\Definition\FloatDefinition`                  |
| `Flow\ETL\Schema\Definition\HTMLDefinition`                   | `Flow\ETL\Values\Schema\Definition\HTMLDefinition`                   |
| `Flow\ETL\Schema\Definition\HTMLElementDefinition`            | `Flow\ETL\Values\Schema\Definition\HTMLElementDefinition`            |
| `Flow\ETL\Schema\Definition\IntegerDefinition`                | `Flow\ETL\Values\Schema\Definition\IntegerDefinition`                |
| `Flow\ETL\Schema\Definition\JsonDefinition`                   | `Flow\ETL\Values\Schema\Definition\JsonDefinition`                   |
| `Flow\ETL\Schema\Definition\ListDefinition`                   | `Flow\ETL\Values\Schema\Definition\ListDefinition`                   |
| `Flow\ETL\Schema\Definition\MapDefinition`                    | `Flow\ETL\Values\Schema\Definition\MapDefinition`                    |
| `Flow\ETL\Schema\Definition\NullDefinition`                   | `Flow\ETL\Values\Schema\Definition\NullDefinition`                   |
| `Flow\ETL\Schema\Definition\StringDefinition`                 | `Flow\ETL\Values\Schema\Definition\StringDefinition`                 |
| `Flow\ETL\Schema\Definition\StructureDefinition`              | `Flow\ETL\Values\Schema\Definition\StructureDefinition`              |
| `Flow\ETL\Schema\Definition\TimeDefinition`                   | `Flow\ETL\Values\Schema\Definition\TimeDefinition`                   |
| `Flow\ETL\Schema\Definition\TimeZoneDefinition`               | `Flow\ETL\Values\Schema\Definition\TimeZoneDefinition`               |
| `Flow\ETL\Schema\Definition\TypeProjection`                   | `Flow\ETL\Values\Schema\Definition\TypeProjection`                   |
| `Flow\ETL\Schema\Definition\UuidDefinition`                   | `Flow\ETL\Values\Schema\Definition\UuidDefinition`                   |
| `Flow\ETL\Schema\Definition\XMLDefinition`                    | `Flow\ETL\Values\Schema\Definition\XMLDefinition`                    |
| `Flow\ETL\Schema\Definition\XMLElementDefinition`             | `Flow\ETL\Values\Schema\Definition\XMLElementDefinition`             |
| `Flow\ETL\Schema\Metadata`                                    | `Flow\ETL\Values\Schema\Metadata`                                    |
| `Flow\ETL\Schema\SameSchemas`                                 | `Flow\ETL\Values\Schema\SameSchemas`                                 |
| `Flow\ETL\Schema\SimilarNames`                                | `Flow\ETL\Foundation\SimilarNames`                                   |
| `Flow\ETL\Schema\SortingStrategy`                             | `Flow\ETL\Values\Schema\SortingStrategy`                             |
| `Flow\ETL\Schema\SortingStrategy\AlphabeticalStrategy`        | `Flow\ETL\Values\Schema\SortingStrategy\AlphabeticalStrategy`        |
| `Flow\ETL\Schema\SortingStrategy\CombinedStrategy`            | `Flow\ETL\Values\Schema\SortingStrategy\CombinedStrategy`            |
| `Flow\ETL\Schema\SortingStrategy\MetadataStrategy`            | `Flow\ETL\Values\Schema\SortingStrategy\MetadataStrategy`            |
| `Flow\ETL\Schema\SortingStrategy\TypeStrategy`                | `Flow\ETL\Values\Schema\SortingStrategy\TypeStrategy`                |
| `Flow\ETL\Schema\SortingStrategy\TypeStrategy\TypePriorities` | `Flow\ETL\Values\Schema\SortingStrategy\TypeStrategy\TypePriorities` |
| `Flow\ETL\Schema\Validator\EvolvingValidator`                 | `Flow\ETL\Values\Schema\Validator\EvolvingValidator`                 |
| `Flow\ETL\Schema\Validator\MismatchedDefinition`              | `Flow\ETL\Values\Schema\Validator\MismatchedDefinition`              |
| `Flow\ETL\Schema\Validator\SelectiveValidator`                | `Flow\ETL\Values\Schema\Validator\SelectiveValidator`                |
| `Flow\ETL\Schema\Validator\StrictValidator`                   | `Flow\ETL\Values\Schema\Validator\StrictValidator`                   |
| `Flow\ETL\Schema\Validator\ValidationContext`                 | `Flow\ETL\Values\Schema\Validator\ValidationContext`                 |
| `Flow\Floe\Codec`                                             | `Flow\ETL\Values\Column\Frame\Codec`                                 |
| `Flow\Floe\Codec\NoopCodec`                                   | `Flow\ETL\Values\Column\Frame\NoopCodec`                             |
| `Flow\Floe\Exception\FloeException`                           | `Flow\ETL\Values\Column\Frame\FloeException`                         |
| `Flow\Floe\Exception\FrameTooLarge`                           | `Flow\ETL\Values\Column\Frame\FrameTooLarge`                         |

### 47) `flow-php/etl` - columns are addressed by name in `Schema`, `Definition` and `Rows::reduceToArray()`

`Schema` methods take column names (`string`), no longer `Reference`. `Definition::reference()` becomes
`Definition::name()`, `Schema::references()` becomes `Schema::names()`.

| Before                                                                        | After                               |
|-------------------------------------------------------------------------------|-------------------------------------|
| `$definition->reference()->name()`                                            | `$definition->name()`               |
| `$schema->references()`                                                       | `$schema->names()` - `list<string>` |
| `$schema->get(ref('id'))`, also `findDefinition`, `keep`, `remove`, `reorder` | `$schema->get('id')`                |
| `$schema->moveAfter(ref('a'), ref('b'))`, also `addAfter`, `moveTo`, `rename` | `$schema->moveAfter('a', 'b')`      |
| `$rows->reduceToArray(ref('id'))`                                             | `$rows->reduceToArray('id')`        |

### 48) `flow-php/etl` - `Rows::concat()` is static, `Rows::sortBy()` is removed

| Before                             | After                                                                                                                       |
|------------------------------------|-----------------------------------------------------------------------------------------------------------------------------|
| `$rows->concat($backend, $other)`  | `Rows::concat($schema, $backend, $rows, $other)`                                                                            |
| `$rows->sortBy(ref('id')->desc())` | `$rows->gather($order->permutation($order->keys($rows), $rows->count()))` with `$order = new RowOrder([ref('id')->desc()])` |

`Rows::concat()` takes the schema every part must carry, so an empty list of parts still has one.

### 49) `flow-php/etl` - `RowsBuilder::append()` and `RowsBuilder::appendProjected()` are removed

| Before                             | After                                                                                      |
|------------------------------------|--------------------------------------------------------------------------------------------|
| `$builder->append($row)`           | `$builder->appendRows([$row])`                                                             |
| `$builder->appendProjected($rows)` | `$builder->appendRows($rows)` - records keyed by position fill the column at that position |

### 50) `flow-php/etl` - one type table in `PhysicalFor`

`LayoutFor`, `PhysicalKind` and `PhysicalBuilderFor` merge into `PhysicalFor`.

| Before                                      | After                                             |
|---------------------------------------------|---------------------------------------------------|
| `(new PhysicalFor())->type($type)`          | `(new PhysicalFor())->physical($type)`            |
| `(new LayoutFor())->type($type)`            | `(new PhysicalFor())->layout($type)`              |
| `(new PhysicalKind())->accepts($type, $v)`  | `(new PhysicalFor())->accepts($type, $v)`         |
| `(new PhysicalKind())->all($type, $values)` | `(new PhysicalFor())->acceptsAll($type, $values)` |
| `(new PhysicalKind())->elements($type, $v)` | `(new PhysicalFor())->acceptsElements($type, $v)` |
| `(new PhysicalBuilderFor())->type($type)`   | `(new PhysicalFor())->builder($type)`             |

### 51) `flow-php/etl` - definition merging goes through `Definitions`

`Definitions::of()` maps a type to its definition (what `definition_from_type()` does, which now delegates to it),
`Definitions::merged()` widens two definitions of one column. `CommonType` and `ValueMatch` are removed.

| Before       | After                                          |
|--------------|------------------------------------------------|
| `CommonType` | removed - `Definitions::merged($left, $right)` |
| `ValueMatch` | removed - `$definition->matches($value)`       |

### 52) `flow-php/etl` - schema errors past an inference sample report the row in the source

`InferredRows` is removed: memory extractors and the native CSV / JSON readers report a refused row at its 0-based
position in the whole source, not in the batch.

| Before                                                                         | After                                                                                                                 |
|--------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------|
| `InferredRows`                                                                 | removed - the extractors' `RecordBatch` keeps the source offset                                                       |
| `InferredSchemaException::pastTheSample($source, $row, $inference, $mismatch)` | `InferredSchemaException::whenPastTheSample($source, $sampleSize, $mismatch)` - returns `$mismatch` within the sample |
| `SchemaMismatchException::$rowIndex` - position in the batch                   | position in the whole source                                                                                          |

### 53) `flow-php/etl` - a refused value in a typed column throws `ColumnMismatchException`

| Before                                                          | After                            |
|-----------------------------------------------------------------|----------------------------------|
| `CastingColumnBuilder::append()` threw `Flow\Types\Exception\*` | throws `ColumnMismatchException` |

---

### 54) `flow-php/etl` - storage types moved to the `Flow\ETL\Storage` namespace

Batch storage, the Floe file encoding, the serializers and `Memory` move under `Flow\ETL\Storage`;
`TelemetryAttributes` moves to `Flow\ETL\Foundation\Telemetry`. The bucket storages are renamed after the one
contract they implement (row 55). The Floe source and sink (`FloeExtractor`, `FloeLoader`) move to
`Flow\ETL\IO\Floe` (row 112); the `Flow\Floe\DSL` and `Flow\Serializer\DSL` functions keep their namespace.

| Before                                             | After                                                          |
|----------------------------------------------------|----------------------------------------------------------------|
| `Flow\ETL\Bucketing\BucketsStorage`                | `Flow\ETL\Storage\RowsStorage`                                 |
| `Flow\ETL\Bucketing\ResidentBucketsStorage`        | `Flow\ETL\Storage\ResidentRowsStorage`                         |
| `Flow\ETL\Bucketing\Storage\FilesystemBuckets`     | `Flow\ETL\Storage\FilesystemRowsStorage`                       |
| `Flow\ETL\Bucketing\Storage\MemoryBuckets`         | `Flow\ETL\Storage\MemoryRowsStorage`                           |
| `Flow\ETL\Bucketing\Storage\PSRCacheBuckets`       | `Flow\ETL\Storage\PsrRowsStorage`                              |
| `Flow\ETL\Bucketing\Storage\SpillingBuckets`       | `Flow\ETL\Storage\SpillingRowsStorage`                         |
| `Flow\ETL\Cache\Implementation\ApcuCache`          | `Flow\ETL\Storage\ApcuRowsStorage`                             |
| `Flow\ETL\Cache\Implementation\TraceableCache`     | `Flow\ETL\Runtime\Telemetry\TraceableRowsStorage`              |
| `Flow\ETL\Config\Telemetry\TelemetryAttributes`    | `Flow\ETL\Foundation\Telemetry\TelemetryAttributes`            |
| `Flow\ETL\Memory\ArrayMemory`                      | `Flow\ETL\Storage\Memory\ArrayMemory`                          |
| `Flow\ETL\Memory\Memory`                           | `Flow\ETL\Storage\Memory\Memory`                               |
| `Flow\Floe\Exception\IncompatibleSchemaException`  | `Flow\ETL\Storage\Floe\Exception\IncompatibleSchemaException`  |
| `Flow\Floe\FloeMerger`                             | `Flow\ETL\Storage\Floe\FloeMerger`                             |
| `Flow\Floe\FloeSerializer`                         | `Flow\ETL\Storage\Floe\FloeSerializer`                         |
| `Flow\Floe\FloeStreamReader`                       | `Flow\ETL\Storage\Floe\FloeStreamReader`                       |
| `Flow\Floe\FloeStreamWriter`                       | `Flow\ETL\Storage\Floe\FloeStreamWriter`                       |
| `Flow\Floe\Footer`                                 | `Flow\ETL\Storage\Floe\Footer`                                 |
| `Flow\Floe\FooterLocation`                         | `Flow\ETL\Storage\Floe\FooterLocation`                         |
| `Flow\Floe\FooterReader`                           | `Flow\ETL\Storage\Floe\FooterReader`                           |
| `Flow\Floe\Format`                                 | `Flow\ETL\Storage\Floe\Format`                                 |
| `Flow\Floe\FrameWriter`                            | `Flow\ETL\Storage\Floe\FrameWriter`                            |
| `Flow\Floe\Options`                                | `Flow\ETL\Storage\Floe\Options`                                |
| `Flow\Floe\Section`                                | `Flow\ETL\Storage\Floe\Section`                                |
| `Flow\Floe\SplittingFrameEncoder`                  | `Flow\ETL\Storage\Floe\SplittingFrameEncoder`                  |
| `Flow\Serializer\CompressingSerializer`            | `Flow\ETL\Storage\Serializer\CompressingSerializer`            |
| `Flow\Serializer\Exception\SerializationException` | `Flow\ETL\Storage\Serializer\Exception\SerializationException` |
| `Flow\Serializer\Serializer`                       | `Flow\ETL\Storage\Serializer\Serializer`                       |

### 55) `flow-php/etl` - one `RowsStorage` contract for the cache, spilling and shuffles

`Cache` and `BucketsStorage` merge into `Flow\ETL\Storage\RowsStorage`: a key holds batches appended in order.
`cache()`, `from_cache()`, `config_builder()->cache()` and `filesystem_cache()` keep their names and take or build a
`RowsStorage`. The default cache writes one Floe file per key with the configured backend, so the configured
`Serializer` no longer reaches it. Entries cached by 0.46 are not read: they are written again on the next run.
The filesystem cache moves from `<dir>/<xx>/<xx>/<xx>/<xx>/<key>` to `<dir>/flow-php-rows/<xxh128 of the key>/<key>.floe`.
Remove the old entries under `<dir>` by hand.

| Before                                                                | After                                                                                     |
|-----------------------------------------------------------------------|-------------------------------------------------------------------------------------------|
| `Flow\ETL\Cache`                                                      | `Flow\ETL\Storage\RowsStorage`                                                            |
| `$cache->set($key, $rows)`, `$cache->get($key)`                       | `$storage->append($key, $rows)`, `$storage->close($key)`, `$storage->get($key)` - batches |
| `$cache->schema($key)` - throws for a missing key                     | `$storage->schema($key)` - `null` for a missing key                                       |
| `$cache->delete($key)`, `$cache->clear()`                             | `$storage->remove($key)` - no `clear()`                                                   |
| `$buckets->set($key, $rows)`                                          | `$storage->append($key, $rows)` then `$storage->close($key)` on a fresh key               |
| `KeyNotInCacheException`                                              | removed - `has()` answers, `get()` of a missing key yields nothing                        |
| `InMemoryCache`                                                       | `MemoryRowsStorage`                                                                       |
| `FilesystemCache`, `filesystem_cache($serializer, $dir, $filesystem)` | `FilesystemRowsStorage`, `filesystem_cache($dir, $filesystem, $backend)`                  |
| `PSRSimpleCache($cache, $serializer, $ttl)`                           | `PsrRowsStorage($cache, $serializer, ttl: $ttl)` - keys under the `flow.rows` prefix      |
| `PSRCacheBuckets($cache, $serializer, 'flow.buckets', $ttl)`          | `PsrRowsStorage($cache, $serializer, 'flow.rows', $ttl)`                                  |
| `TraceableCache`                                                      | `TraceableRowsStorage`                                                                    |
| `CacheIndex`                                                          | removed - a key holds its batches                                                         |
| `FilesystemBuckets::rows($key, $offset, $limit)`                      | `FilesystemRowsStorage::slice($key, $offset, $limit)`                                     |
| `SpillingBuckets::around($bucketing, $memoryLimit, $backend)`         | `$bucketing->spilling($memoryLimit, $backend)`                                            |

A filesystem key whose file has no footer, because the process writing it was killed, counts as missing, so the next
`cache()` run writes it again.

### 56) `flow-php/etl` - `NativePHPSerializer`, `Base64Serializer` and PHP serialization of `Rows` are removed

`NativePHPSerializer` decoded every batch into the default backend, whatever the configuration said.

| Before                                      | After                                                                     |
|---------------------------------------------|---------------------------------------------------------------------------|
| `NativePHPSerializer`                       | `FloeSerializer`                                                          |
| `Base64Serializer($serializer)`             | `base64_encode(serialize_to_string($serializer, $rows))`                  |
| `serialize($rows)`, `unserialize($payload)` | `serialize_to_string($serializer, $rows)`, `unserialize_from_string(...)` |

### 57) `flow-php/etl` - `FloeFiles` opens Floe files; `FloeStreamWriter` is created open

| Before                                                                             | After                                                                           |
|------------------------------------------------------------------------------------|---------------------------------------------------------------------------------|
| `new FloeReader($filesystem, $backend, $codec)->read($path)`                       | `new FloeFiles($filesystem, $backend, new Options(codec: $codec))->read($path)` |
| `$writer = new FloeWriter($filesystem, $schema, $backend); $writer->create($path)` | `$writer = new FloeFiles($filesystem, $backend)->create($path, $schema)`        |
| `$writer->append($path, $metadata)` on a `FloeWriter`                              | `new FloeFiles($filesystem, $backend)->append($path, $schema, $metadata)`       |
| `$writer = new FloeStreamWriter($schema, $backend); $writer->create($stream)`      | `$writer = FloeStreamWriter::create($stream, $schema, $backend)`                |
| `$writer->resume($stream, $footer, $length)`                                       | `FloeStreamWriter::resume($stream, $footer, $length, $schema, $backend)`        |
| `FloeStreamReader::head($count)`                                                   | `$reader->rows($batchSize, 0, $count)`                                          |
| `FloeStreamReader::tail($count)`                                                   | `$reader->rows($batchSize, max(0, $reader->totalRows() - $count))`              |
| `FloeStreamReader::byteSize()`                                                     | `$reader->footer()->statistics->byteSize`                                       |

A closed writer refuses `write()` and `close()` with `Floe writer session is closed`.

### 58) `flow-php/etl` - `Flow\ETL\Values\Memory\Configuration` is removed

`MemoryLimit::default()` reads PHP's `memory_limit` itself. `Configuration::isConsumptionBelow()`, `isInfinite()` and
`isLessThan()` had no callers.

### 59) `flow-php/etl` - `ApcuCache` renamed to `ApcuRowsStorage`

`ApcuCache` implements `RowsStorage` (row 55) and is named after it, like the other storages. There is no DSL function
for it.

| Before                                   | After                                          |
|------------------------------------------|------------------------------------------------|
| `new ApcuCache($serializer, $namespace)` | `new ApcuRowsStorage($serializer, $namespace)` |
| `$cache->clear()`                        | removed - `$storage->remove($key)` per key     |

### 60) `flow-php/etl` - kernel types moved to the `Flow\ETL\Kernels` namespace, `Join` renamed to `JoinType`

The join type enum is a value used by the API, the functions and the kernels. It moves to `Flow\ETL\Values\Join` and is
renamed `JoinType`.

| Before                                                | After                                                         |
|-------------------------------------------------------|---------------------------------------------------------------|
| `Flow\ETL\Join\Join`                                  | `Flow\ETL\Values\Join\JoinType`                               |
| `Flow\ETL\Join\JoinSchema`                            | `Flow\ETL\Kernels\Join\JoinSchema`                            |
| `Flow\ETL\Join\HashJoin\*`                            | `Flow\ETL\Kernels\HashJoin\*`                                 |
| `Flow\ETL\Bucketing\{Bucket, BucketRun, Buckets}`     | `Flow\ETL\Kernels\Bucketing\{Bucket, BucketRun, Buckets}`     |
| `Flow\ETL\Bucketing\{HashBucketing, KeyGrouping}`     | `Flow\ETL\Kernels\Bucketing\{HashBucketing, KeyGrouping}`     |
| `Flow\ETL\Hash\{Algorithm, NativePHPHash, PlainText}` | `Flow\ETL\Kernels\Hash\{Algorithm, NativePHPHash, PlainText}` |
| `Flow\ETL\Sort\ExternalSort`                          | `Flow\ETL\Kernels\Sort\ExternalSort`                          |
| `Flow\ETL\Sort\{RowOrder, SortKey}`                   | `Flow\ETL\Values\Sort\{RowOrder, SortKey}`                    |
| `Flow\ETL\Sort\Merge\{BucketCursor, KWayMerge}`       | `Flow\ETL\Kernels\Sort\Merge\{BucketCursor, KWayMerge}`       |
| `$df->join($right, $on, Join::left)`                  | `$df->join($right, $on, JoinType::left)`                      |

### 61) `flow-php/etl` - `=` and join keys of two types compare in one type

When the plan is built, comparisons and join keys of different types are cast to one type. Numbers
widen to float, a date widens to a datetime, and a string casts to the other side's type. An integral string casts
exactly, other numeric strings round half away from zero for an integer side. A value that cannot be cast now throws
instead of not matching. `===` and `!==` still compare without a cast.

| Before                                                | After                                                                           |
|-------------------------------------------------------|---------------------------------------------------------------------------------|
| `ref('id')->equals(lit('x'))` on an int column: false | throws `EvaluationException`: `Column "id" value 'x' cannot be cast to integer` |
| join of an int key and a string key `'x'`: no match   | throws the same exception                                                       |
| join keys keyed loosely (`KeyValues`)                 | join keys cast to one type, then keyed exactly (`KeyEncoder`)                   |

### 62) `flow-php/etl` - NaN is the largest value, uuid, json, xml, enum and html sort by their stored form

NaN equals NaN and orders after every number. This holds in `=`, `>`, `<` and `between`. It also holds
in `sortBy()`, `groupBy()`, `batchBy()` and window peers.

| Before                                                | After                                                                               |
|-------------------------------------------------------|-------------------------------------------------------------------------------------|
| `f > 2.0` drops a NaN row                             | keeps it                                                                            |
| `f = NAN` matches nothing                             | matches NaN                                                                         |
| `batchBy('f')` puts two NaN rows in two batches       | one batch                                                                           |
| `sortBy()` on uuid, json, xml, enum keeps input order | sorts by bytes: uuid by its 16 bytes, json and xml by their text, enum by case name |
| `rank()` over a uuid order: input order               | the uuid's byte order                                                               |

### 63) `flow-php/etl` - one set of seen keys for distinct rows, unique constraints and statistics

`SeenKeys` holds every key a stream brought. It counts the distinct values in statistics and in the CLI
`file:analyze` output ("Distinct Values"). The counts now tell types apart and keep datetime microseconds. They count
-0.0 as 0.0, and they count every column type, json included.

| Before                                                            | After                                  |
|-------------------------------------------------------------------|----------------------------------------|
| `Flow\ETL\Transformer\DropDuplicates\Hashes`                      | `Flow\ETL\Kernels\Bucketing\SeenKeys`  |
| `Flow\ETL\Rows\RowHashes`                                         | removed - `KeyEncoder` keys the rows   |
| `Flow\ETL\Constraint\UniqueConstraint\{Storage, InMemoryStorage}` | `SeenKeys`, inside the constraint      |
| `UniqueConstraint::withStorage(Storage $storage)`                 | removed                                |
| `Flow\ETL\Dataset\Statistics\DistinctCounter`                     | `SeenKeys`, inside `Statistics\Column` |
| two datetimes 1 µs apart: 1 distinct value                        | 2                                      |
| a json column: 0 distinct values                                  | its distinct values                    |

### 64) `flow-php/etl` - one fold for aggregate partials

A custom `AggregatingFunction` drops `merge(AggregatingFunction $other, FlowContext $context)`, and `mergePartial()`
is renamed `merge()`. Group by merges a spilled aggregator through its `partial()`.

```php
// before
public function mergePartial(array $partial, FlowContext $context): void { /* fold $partial */ }

public function merge(AggregatingFunction $other, FlowContext $context): void
{
    $this->mergePartial($other->partial(), $context);
}

// after
public function merge(array $partial, FlowContext $context): void { /* fold $partial */ }
```

### 65) `flow-php/etl` - kernels take column names and values

Kernels take column names, `SortColumn` and `Backend`, never references or the flow context. Hashing runs once per
distinct key.

| Before                                                                         | After                                                                                                              |
|--------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------|
| `new KeyValues(list<Reference>)`                                               | `new KeyEncoder(list<string> $columns, array $types = [], ?Schema $declared = null)`                               |
| `Bucketing\{Hasher, NativeHasher, SingleBucketHasher}` (interface)             | removed                                                                                                            |
| `new HashBucketing($keys, $count, $hasher, $random, ...)`                      | `new HashBucketing(KeyEncoder $keys, $count, $random, ...)`                                                        |
| `HashBucketing::index(string $hash)`                                           | `HashBucketing::indexOf(string $key)`, `HashBucketing::id($namespace, $runId, $index)`                             |
| `new RowOrder(list<Reference>)`                                                | `new RowOrder(list<SortColumn>)`                                                                                   |
| `Window\PeerComparator`, `Row\TypedValueComparator`                            | `RowOrder::ties(Rows $partition)`                                                                                  |
| `ExternalSort::sort($rows, FlowContext $context, $limit)`                      | `ExternalSort::sort($rows, Backend $backend, $limit)`, with `list<SortColumn>`                                     |
| `ExternalSort::sorted(array $buffer, ?int $limit, Backend $backend)`           | `ExternalSort::sorted(Rows $all, ?int $limit, Backend $backend)`: concatenate the buffered batches first           |
| `KWayMerge::merge(list<BucketRun>)`, `KWayMerge::mergeStreams($streams)`       | `KWayMerge::merge(list<Generator<Rows>> $streams)`                                                                 |
| `new Joiner(Expression $expression, Join $type, Backend $backend, ...)`        | `new Joiner(JoinType $type, JoinSchema $schema, ?JoinKeys $keys, PairPredicate $condition, Backend $backend, ...)` |
| `JoinKeys::fromComparison($comparison)`, `JoinSchema::of($expression, $type)`  | `$expression->keys()`, `$expression->schema($type)`                                                                |
| `NullKeyFilter(list<Reference>)::rows($generator)`                             | `NullKeyFilter(list<string>)::of($batch)`                                                                          |
| `RepartitionProcessor::$hasher`, `new PartitionRouter($partitioning, $hasher)` | removed                                                                                                            |
| explain: `Hasher: NativeHasher` under a repartition                            | removed                                                                                                            |
| `ComparableValues::orderedByPhysical($type)`                                   | `ComparableValues::equalByPhysical($type)`                                                                         |

### 66) `flow-php/etl` - `isIn()` casts each candidate as `=` does (row 61)

| Before                                               | After                                                                                 |
|------------------------------------------------------|---------------------------------------------------------------------------------------|
| `ref('n')->isIn(lit(['x']))` on an int column: false | throws `EvaluationException`: `Column "haystack" value 'x' cannot be cast to integer` |
| `ref('n')->isIn(lit(['1', 'x']))` with `n` = 1: true | throws the same exception                                                             |
| `lit('a')->isIn(lit([1, 'a']))`: true                | throws: `Column "needle" value 'a' cannot be cast to integer`                         |

A haystack of mixed types throws when one candidate cannot be cast. Cast the haystack, or make it one type.

### 67) `flow-php/etl` - one `Comparison` function replaces the 8 comparison classes

| Before                                                                     | After                                                                                                  |
|----------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------|
| `new Equals($a, $b)`, `NotEquals`, `Same`, `NotSame`                       | `new Comparison($a, Operator::EQUAL, $b)`, `NOT_EQUAL`, `IDENTICAL`, `NOT_IDENTICAL`                   |
| `new GreaterThan($a, $b)`, `GreaterThanEqual`, `LessThan`, `LessThanEqual` | `new Comparison($a, Operator::GREATER_THAN, $b)`, `GREATER_THAN_EQUAL`, `LESS_THAN`, `LESS_THAN_EQUAL` |
| `$function instanceof Equals`                                              | `$function instanceof Comparison && $function->operator === Operator::EQUAL`                           |

`Operator` is `Flow\Types\Type\Comparison\Operator`. `equals()`, `greaterThan()` and the other DSL methods keep their
names and now return `Comparison`.

### 68) `flow-php/etl` - window functions return one value per partition row

| Before                                                 | After                                                           |
|--------------------------------------------------------|-----------------------------------------------------------------|
| `WindowFunction::apply(WindowContext $window): mixed`  | `WindowFunction::values(Rows $partition): list<mixed>`          |
| `PartitionRanking::rankPartition(Rows $partition)`     | `WindowFunction::values(Rows $partition)`                       |
| `FrameAccumulating::accumulator(FlowContext $context)` | `FrameAccumulating::accumulator()`                              |
| `new SumAccumulator($ref, $exact, $context)`           | `new SumAccumulator($ref, $exact)`, same for `Count`, `Average` |
| `Window\WindowContext`, `Function\PartitionRanking`    | removed                                                         |

A frame-accumulating function returns `(new FrameAccumulation($this->window()->frame()))->values($this, $partition)`.

### 69) `flow-php/etl` - functions take the `Backend`, not the `FlowContext`

| Before                                                                    | After                                                               |
|---------------------------------------------------------------------------|---------------------------------------------------------------------|
| `ScalarFunction::eval(Rows $rows, FlowContext $context)`                  | `ScalarFunction::eval(Rows $rows, Backend $backend)`                |
| `AggregatingFunction::aggregate($rows, $indices, FlowContext $context)`   | `AggregatingFunction::aggregate($rows, $indices, Backend $backend)` |
| `AggregatingFunction::merge(array $partial, FlowContext $context)`        | `AggregatingFunction::merge(array $partial, Backend $backend)`      |
| `Parameter::as*($rows, FlowContext $context, ...)`, `Parameter::values()` | the same with `Backend $backend`                                    |
| `Parameter::column($rows, $context)`                                      | `$function->eval($rows, $backend)`                                  |
| `Selection::evaluate($function, $rows, FlowContext $context)`             | `Selection::evaluate($function, $rows, Backend $backend)`           |
| `Filter::filter(Rows $rows, FlowContext $context)`                        | `Filter::filter(Rows $rows, Backend $backend)`                      |
| `$context->calculator()` inside a function                                | `new Calculator()`                                                  |

In a custom function, use `$backend` where you called `$context->backend()`. Callers pass `$context->backend()`.

### 70) `flow-php/etl` - an alias never picks the column that is read

| Before                                                      | After                                           |
|-------------------------------------------------------------|-------------------------------------------------|
| `select(ref('a')->as('b'))`: column b's values, named b     | column a's values, named b                      |
| `select(ref('a')->as('b'), ref('b'))`: one column b         | throws `select() would output column "b" twice` |
| `dropDuplicates(ref('a')->as('b'))`: dedupes by b           | dedupes by a                                    |
| `drop(ref('a')->as('b'))`: drops b                          | drops a                                         |
| `repartition(ref('a')->as('b'))`, `partitionBy()`: by b     | by a                                            |
| `groupBy(ref('a')->as('k'))`: throws `column "k" not found` | groups by a, the key column is named a          |
| `References::names()`: each alias                           | each column read (`Reference::to()`)            |

Use `ref('b')` to read column b.

### 71) `flow-php/etl` - sort order lives on `SortColumn`, not on `Reference`

| Before                                                                          | After                                                              |
|---------------------------------------------------------------------------------|--------------------------------------------------------------------|
| `ref('a')->desc()`, `->asc()`: a `Reference`                                    | a `SortColumn` (`name`, `order`, `nulls`)                          |
| `Reference::sort()`, `Reference::nulls()`                                       | removed: read `$sortColumn->order`, `$sortColumn->nulls`           |
| `sortBy()`, `Window::orderBy()`, `constraint_sorted_by()`: `Reference`          | `Reference`, `SortColumn` or `string`; a reference sorts ascending |
| `new Sort($input, References $refs)`, `new TopN(...)`, sort processors: `$refs` | `list<SortColumn> $columns`                                        |
| `Window::order()`: `list<Reference>`                                            | `list<SortColumn>`                                                 |

### 72) `flow-php/etl` - function types moved to the `Flow\ETL\Functions` namespace, `Join\Comparison` renamed to `Join\Condition`

| Before                                                                         | After                                                                                          |
|--------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------|
| `Flow\ETL\Function\*`                                                          | `Flow\ETL\Functions\*`                                                                         |
| `Flow\ETL\Window`, `Flow\ETL\Window\*`                                         | `Flow\ETL\Functions\Window`, `Flow\ETL\Functions\Window\*`                                     |
| `Flow\ETL\Row\{Reference, References, ResolvedReference, UnresolvedReference}` | `Flow\ETL\Functions\Reference\{Reference, References, ResolvedReference, UnresolvedReference}` |
| `Flow\ETL\Join\Comparison`                                                     | `Flow\ETL\Functions\Join\Condition`                                                            |
| `Flow\ETL\Join\Comparison\{All, Any, Equal, Identical}`                        | `Flow\ETL\Functions\Join\Condition\{All, Any, Equal, Identical}`                               |
| `Flow\ETL\Join\Expression`                                                     | `Flow\ETL\Functions\Join\Expression`                                                           |
| `Flow\ETL\String\StringStyles`                                                 | `Flow\ETL\Functions\String\StringStyles`                                                       |

DSL function namespaces (`Flow\ETL\DSL`) do not change.

### 73) `flow-php/etl-adapter-json`, `flow-php/etl-adapter-parquet` - a 0-row `load()` writes no file

| Before                                                  | After   |
|---------------------------------------------------------|---------|
| `to_json()`: `load()` of 0 rows, then `closure()`       | no file |
| `to_json_lines()`: `load()` of 0 rows, then `closure()` | no file |
| `to_parquet()`: `load()` of 0 rows, then `closure()`    | no file |

Before: `[]`, an empty `.jsonl` and a 0-row parquet file. CSV, Text, XML and Excel already wrote no file.

### 74) `flow-php/etl` - a file source lists its files once per run

| Before                                                                             | After                                                                                                  |
|------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------|
| a frame run again: the inferred schema, partitions and statistics of the first run | listed and inferred again on every run; an `explain()` or `schema()` before the run shares its listing |
| `new FileColumnsSource($filesystem, $path, $types, $metadata, $names)`             | `new FileColumnsSource(new SourceFiles($filesystem, $path), $types, $metadata)`                        |
| `new FileRead($filesystem, $path, $columns, $schema, $batchSize)`                  | `new FileRead(new SourceFiles($filesystem, $path), $columns, $schema, $batchSize)`                     |
| `FileExtractor`                                                                    | extends `Flow\ETL\IO\Extractor\Refreshable` (`refresh(): void`)                                        |

An extractor that wraps a file source implements `Flow\ETL\IO\Extractor\Refreshable` and calls `refresh()` on the
wrapped source.

### 75) `flow-php/etl` - a partition value that cannot be cast is reported one way

| Before                                                                    | After                                                                   |
|---------------------------------------------------------------------------|-------------------------------------------------------------------------|
| read of a null partition value: `path value "__HIVE_DEFAULT_PARTITION__"` | `path value NULL`                                                       |
| read of a file without the partition: `path value ""`                     | `path value NULL`                                                       |
| `new ScalarFunctionFilter($function, $partitions, $context)`              | `new ScalarFunctionFilter($function, $partitions, $context->backend())` |

### 76) `flow-php/etl` - `batches()`, `batched_by()` and the collecting extractor are removed

| Before                                                                       | After                                                                                                          |
|------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------|
| `read(batches($extractor, $n))`                                              | `read($extractor)->batchSize($n)`                                                                              |
| `read(batched_by($extractor, $column, $min))`                                | `read($extractor)->batchBy($column, $min)`                                                                     |
| `batches($child, $n)` inside `from_all(...)`                                 | `$child->withBatchSize($n)` when the child is a `BatchableExtractor`, otherwise `->batchSize($n)` on the frame |
| `read(new CollectingExtractor($extractor))`                                  | `read($extractor)->collect()`                                                                                  |
| `Flow\ETL\Extractor\{BatchExtractor, BatchByExtractor, CollectingExtractor}` | removed                                                                                                        |

### 77) `flow-php/etl` and every adapter - sources and sinks take the `Backend`, not the `FlowContext`

| Before                                                         | After                                                                            |
|----------------------------------------------------------------|----------------------------------------------------------------------------------|
| `Extractor::extract(FlowContext $context, ?int $limit = null)` | `extract(Backend $backend, Criteria $criteria)`, the limit is `$criteria->limit` |
| `Loader::load(Rows $rows, FlowContext $context)`               | `load(Rows $rows, Backend $backend)`                                             |
| `Loader\Closure::closure(FlowContext $context)`                | `closure()`                                                                      |
| `Loader\Discardable::discard(FlowContext $context)`            | `discard()`                                                                      |
| `$extractor->extract(flow_context())`                          | `$extractor->extract(flow_context()->backend(), new Criteria())`                 |

A custom extractor and loader:

```php
// before
public function extract(FlowContext $context, ?int $limit = null): Generator
{
    yield rows(...); // stops after $limit rows
}

public function load(Rows $rows, FlowContext $context): void
{
    $this->write($rows->toArray());
}

// after
public function extract(Backend $backend, Criteria $criteria): Generator
{
    yield rows(...); // stops after $criteria->limit rows
}

public function load(Rows $rows, Backend $backend): void
{
    $this->write($rows->toArray());
}
```

### 78) `flow-php/etl`, `flow-php/etl-adapter-parquet`, `flow-php/etl-adapter-excel`, `flow-php/etl-adapter-doctrine`,

`flow-php/cli` - `withOffset()` is gone; `offset()` is pushed into file sources

| Before                                                                                                          | After                                                                                                                                                           |
|-----------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `from_parquet($path)->withOffset($n)`, `from_parquet($path, offset: $n)`                                        | `df()->read(from_parquet($path))->offset($n)`                                                                                                                   |
| `from_floe($path)->withOffset($n)`                                                                              | `df()->read(from_floe($path))->offset($n)`                                                                                                                      |
| `from_excel($path)->withOffset($n)` (a start row, the header counted)                                           | `df()->read(from_excel($path))->offset($n - 2)` with a header, `->offset($n - 1)` without one                                                                   |
| `ExcelReadOptions(bool $withHeader, bool $convertEmptyToNull, int $offset, ...)`, `withOffset()`                | `ExcelReadOptions(bool $withHeader, bool $convertEmptyToNull, ?string $sheetName, ?ExcelReader $format)`; `SheetCells(SheetInterface $sheet, bool $withHeader)` |
| `from_dbal_limit_offset_qb($connection, $queryBuilder, offset: $n)`, `DbalLimitOffsetExtractor::withOffset($n)` | `$queryBuilder->setFirstResult($n)`; without a maximum it read no rows, it now reads from the offset                                                            |
| `ExplainedRows::of(..., ?int $maximum, int $offset)`                                                            | `of(..., ?int $maximum)`                                                                                                                                        |
| `FooterStatistics::of(int $listedFiles, int $offset)`                                                           | `of(int $listedFiles)`                                                                                                                                          |
| `flow` CLI `--input-parquet-offset`, `--input-excel-offset`                                                     | `--input-file-offset`                                                                                                                                           |
| a custom `FileExtractor`                                                                                        | skips `$criteria->offset` rows matching every one of `$criteria->conjuncts` before it yields                                                                    |

### 79) `flow-php/etl` and every adapter - `isRepeatable()` is on `Extractor`; `RewindableExtractor`,

`OverridingExtractor` and `Repeatability` are removed

| Before                                                       | After                                                                                     |
|--------------------------------------------------------------|-------------------------------------------------------------------------------------------|
| `implements RewindableExtractor` + `isRepeatable(): bool`    | `Extractor::isRepeatable(): bool` on every extractor; a one-shot stream returns `false`   |
| an extractor that did not implement `RewindableExtractor`    | implements `isRepeatable()` returning `false`                                             |
| `OverridingExtractor::extractors()`                          | removed; a wrapper answers `isRepeatable()` for what it reads (`from_all()`: every child) |
| `(new Repeatability())->of($extractor)`                      | `$extractor->isRepeatable()`                                                              |
| `(new Repeatability())->ofPlan($plan)`                       | `$plan->isRepeatable()`                                                                   |
| `from_cache($id)->withFallbackExtractor($e)->isRepeatable()` | `false` also when `$e` is not repeatable                                                  |

### 80) `flow-php/etl` - `MemoryExtractor` merges into `ArrayExtractor`

| Before                                                                        | After                                                                  |
|-------------------------------------------------------------------------------|------------------------------------------------------------------------|
| `Flow\ETL\Extractor\MemoryExtractor`, `from_memory($memory): MemoryExtractor` | `from_memory($memory): ArrayExtractor` (`new ArrayExtractor($memory)`) |
| `from_memory($memory)->statistics()`: unknown rows                            | the exact `Memory::count()`, read again on every plan                  |

### 81) `flow-php/etl`, `flow-php/etl-adapter-doctrine` - `DataFrameFactory` becomes `ExtractorFactory`

| Before                                                        | After                                                                                                    |
|---------------------------------------------------------------|----------------------------------------------------------------------------------------------------------|
| `Flow\ETL\DataFrameFactory { from(Rows): DataFrame }`         | `Flow\ETL\IO\Extractor\ExtractorFactory { from(Rows): Extractor }`                                       |
| `Flow\ETL\Adapter\Doctrine\DbalDataFrameFactory`              | `Flow\ETL\Adapter\Doctrine\IO\Extractor\DbalExtractorFactory`, `from()` returns the `DbalQueryExtractor` |
| `joinEach(DataFrameFactory $factory, ...)`                    | `joinEach(ExtractorFactory $factory, ...)`: the right rows of a left batch are the extractor's rows      |
| `dbal_dataframe_factory($connection, $query, ...$parameters)` | `dbal_extractor_factory($connection, $query, ...$parameters)`                                            |

### 82) `flow-php/etl-adapter-json`, `flow-php/etl-adapter-parquet`, `flow-php/etl-adapter-xml`, `flow-php/etl-adapter-csv` -

one framing enum, one converter vocabulary, a required XML writer, one copy of the CSV dialect checks

| Before                                                                        | After                                                                                           |
|-------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------|
| `Flow\ETL\Adapter\JSON\JSONMachine\JsonFormat::Document`                      | `Flow\ETL\Adapter\JSON\IO\JsonFraming::Array` (a read of `Array` or `ArrayLines` is a document) |
| `JsonFormat::Lines`                                                           | `JsonFraming::Lines`                                                                            |
| `Flow\ETL\Adapter\Parquet\SchemaConverter::toFlow()`, `toParquet()`           | `toFlowSchema()`, `toParquetSchema()`                                                           |
| `Flow\ETL\Adapter\JSON\SchemaConverter::toFlow()`                             | `toFlowSchema()`                                                                                |
| `new XMLEncoder()` without a writer, then `encode()` threw `RuntimeException` | `XMLEncoder(XMLWriter $xmlWriter, ...)`: the writer is required                                 |
| `new Detector\Option($separator, $enclosure, 'ab')` (any escape)              | refused: an escape is empty or one character, as `CSVDialect::assertParts()` checks everywhere  |

### 83) `flow-php/etl` - the ASCII table is one formatter

| Before                                                                                 | After                                                            |
|----------------------------------------------------------------------------------------|------------------------------------------------------------------|
| `Flow\ETL\Formatter\ASCII\{ASCIITable, Headers, Body, ASCIIHeaders, ASCIIBody}`        | removed; `(new AsciiTableFormatter())->format($rows, $truncate)` |
| `AsciiTableFormatter::format()`, `to_output()` of a batch: time grew with rows squared | one pass over the cells                                          |

### 84) `flow-php/etl-adapter-csv`, `flow-php/etl-adapter-json`, `flow-php/etl-adapter-parquet` - the `Adaptive*` lane

wrappers are gone, the class that builds the sink or source picks the lane

| Before                                                                              | After                                                                                                         |
|-------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------|
| `Flow\ETL\Adapter\CSV\AdaptiveCSVEncoder`                                           | removed; `CSVLoader::open()` picks `RustCSVEncoder` or `PhpCSVEncoder`                                        |
| `Flow\ETL\Adapter\JSON\AdaptiveJsonEncoder`                                         | removed; `JsonLoader::open()` picks `RustJsonEncoder` or `PhpJsonEncoder`                                     |
| `Flow\ETL\Adapter\JSON\AdaptiveJsonOpenSource`                                      | removed; `JsonFileBatches::open(SourceFile $source, ?FileSplit $split = null): JsonOpenSource` picks the lane |
| `Flow\ETL\Adapter\Parquet\AdaptiveParquetOpenSource`                                | removed; `ParquetSourceFile::open()` picks the lane                                                           |
| `Flow\ETL\Adapter\Parquet\AdaptiveParquetOpenSink`                                  | removed; `ParquetFileWriterFactory::writer(): FileWriter` picks the lane                                      |
| `new CSVLoader($path, $filesystem)`, `new JsonLoader($path, $filesystem, $framing)` | a last `?FlowPhpExtension $extension = null`                                                                  |

### 85) `flow-php/etl` and every adapter - IO types moved to `Flow\ETL\IO`, adapter types to their layer namespaces (`IO`, `Values`, `Functions`, `Foundation`)

| Before                                                                                                                                           | After                                                                                                                                                       |
|--------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `Flow\ETL\{Extractor, Formatter, Loader, Transaction}`                                                                                           | `Flow\ETL\IO\{Extractor, Formatter, Loader, Transaction}`                                                                                                   |
| `Flow\ETL\RustIterator`                                                                                                                          | `Flow\ETL\Values\RustIterator`                                                                                                                              |
| `Flow\ETL\Extractor\{ArrayExtractor, BatchableExtractor, CacheExtractor, ChainExtractor}`                                                        | `Flow\ETL\IO\Extractor\{ArrayExtractor, BatchableExtractor, CacheExtractor, ChainExtractor}`                                                                |
| `Flow\ETL\Extractor\{FeedExtractor, InfersSchema, RowsExtractor, SequenceExtractor, Signal}`                                                     | `Flow\ETL\IO\Extractor\{FeedExtractor, InfersSchema, RowsExtractor, SequenceExtractor, Signal}`                                                             |
| `Flow\ETL\Extractor\Statistics`                                                                                                                  | `Flow\ETL\IO\Extractor\Statistics`                                                                                                                          |
| `Flow\ETL\Extractor\File\{DerivedSchema, FileBatches, FileColumns, FileConstants, FileExtractor}`                                                | `Flow\ETL\IO\Extractor\File\{DerivedSchema, FileBatches, FileColumns, FileConstants, FileExtractor}`                                                        |
| `Flow\ETL\Extractor\File\{FileReadLoop, FooterStatistics, InferredColumns, MetadataColumnsExtractor, OffsetSkippingFileBatches}`                 | `Flow\ETL\IO\Extractor\File\{FileReadLoop, FooterStatistics, InferredColumns, MetadataColumnsExtractor, OffsetSkippingFileBatches}`                         |
| `Flow\ETL\Extractor\File\{PartitionTypes, ReadWindow, SelfDescribingFile, SourceFile}`                                                           | `Flow\ETL\IO\Extractor\File\{PartitionTypes, ReadWindow, SelfDescribingFile, SourceFile}`                                                                   |
| `Flow\ETL\Extractor\Grid\{HeaderNames, RecordDecoder}`                                                                                           | `Flow\ETL\IO\Extractor\Grid\{HeaderNames, RecordDecoder}`                                                                                                   |
| `Flow\ETL\Extractor\Memory\{InMemoryRows, SpillState, SpilledRows}`                                                                              | `Flow\ETL\IO\Extractor\Memory\{InMemoryRows, SpillState, SpilledRows}`                                                                                      |
| `Flow\ETL\Extractor\SequenceGenerator\{DatePeriodSequenceGenerator, NumberSequenceGenerator, SequenceGenerator}`                                 | `Flow\ETL\IO\Extractor\SequenceGenerator\{DatePeriodSequenceGenerator, NumberSequenceGenerator, SequenceGenerator}`                                         |
| `Flow\ETL\Filesystem\{SaveMode, ScalarFunctionFilter}`                                                                                           | `Flow\ETL\IO\Filesystem\{SaveMode, ScalarFunctionFilter}`                                                                                                   |
| `Flow\ETL\Formatter\AsciiTableFormatter`                                                                                                         | `Flow\ETL\IO\Formatter\AsciiTableFormatter`                                                                                                                 |
| `Flow\ETL\Formatter\ASCII\ASCIIValue`                                                                                                            | `Flow\ETL\IO\Formatter\ASCII\ASCIIValue`                                                                                                                    |
| `Flow\ETL\Loader\{ArrayLoader, Closure, Discardable, MemoryLoader, Partitioning}`                                                                | `Flow\ETL\IO\Loader\{ArrayLoader, Closure, Discardable, MemoryLoader, Partitioning}`                                                                        |
| `Flow\ETL\Loader\{PartitioningLoader, RowPartitions, SchemaValidationLoader, StreamLoader}`                                                      | `Flow\ETL\IO\Loader\{RowPartitions, SchemaValidationLoader, StreamLoader}` (`PartitioningLoader` removed, see 19)                                           |
| `Flow\ETL\Loader\File\{FileLoader, FileSink, FileSinks, FileWriteFrame, FilesSink}`                                                              | `Flow\ETL\IO\Loader\File\{FileLoader, FileWriter, FileWriterFactory, FileWrite, FileCommit}`                                                                |
| `Flow\ETL\Loader\File\PartitionRouter`                                                                                                           | `Flow\ETL\IO\Loader\File\PartitionRouter`                                                                                                                   |
| `Flow\ETL\Loader\StreamLoader\{Output, Type}`                                                                                                    | `Flow\ETL\IO\Loader\StreamLoader\{Output, Type}`                                                                                                            |
| `Flow\ETL\Row\Formatter\ASCIISchemaFormatter`                                                                                                    | `Flow\ETL\IO\Formatter\Row\ASCIISchemaFormatter`                                                                                                            |
| `Flow\ETL\Schema\SchemaFormatter`                                                                                                                | `Flow\ETL\IO\Formatter\Schema\SchemaFormatter`                                                                                                              |
| `Flow\ETL\Schema\Formatter\{InlineSchemaFormatter, JsonSchemaFormatter, PHPSchemaFormatter}`                                                     | `Flow\ETL\IO\Formatter\Schema\{InlineSchemaFormatter, JsonSchemaFormatter, PHPSchemaFormatter}`                                                             |
| `Flow\ETL\Schema\Formatter\PHPFormatter\{TypeFormatter, ValueFormatter}`                                                                         | `Flow\ETL\IO\Formatter\Schema\PHPFormatter\{TypeFormatter, ValueFormatter}`                                                                                 |
| `Flow\ETL\Schema\Inference\{ColumnTypes, InferredTypes, SchemaInference, SchemaInferenceBuilder, SchemaInferrer}`                                | `Flow\ETL\IO\Inference\{ColumnTypes, InferredTypes, SchemaInference, SchemaInferenceBuilder, SchemaInferrer}`                                               |
| `Flow\ETL\Schema\Inference\TypeFloor`                                                                                                            | `Flow\ETL\IO\Inference\TypeFloor`                                                                                                                           |
| `Flow\ETL\Adapter\Avro\FlixTech\AvroLoader`                                                                                                      | `Flow\ETL\Adapter\Avro\IO\Loader\AvroLoader`                                                                                                                |
| `Flow\ETL\Adapter\Avro\FlixTech\AvroExtractor`                                                                                                   | `Flow\ETL\Adapter\Avro\IO\Extractor\AvroExtractor`                                                                                                          |
| `Flow\ETL\Adapter\CSV\CSVDialect`                                                                                                                | `Flow\ETL\Adapter\CSV\IO\CSVDialect`                                                                                                                        |
| `Flow\ETL\Adapter\CSV\{CSVEncoder, CSVLoader, CSVOpenSink, CSVWriteOptions, PhpCSVEncoder}`                                                      | `Flow\ETL\Adapter\CSV\IO\Loader\{CSVEncoder, CSVLoader, CSVFileWriter, CSVWriteOptions, PhpCSVEncoder}`                                                     |
| `Flow\ETL\Adapter\CSV\RustCSVEncoder`                                                                                                            | `Flow\ETL\Adapter\CSV\IO\Loader\RustCSVEncoder`                                                                                                             |
| `Flow\ETL\Adapter\CSV\{CSVDecoder, CSVDetector, CSVEnclosureScan, CSVExtractor, CSVFileBatches}`                                                 | `Flow\ETL\Adapter\CSV\IO\Extractor\{CSVDecoder, CSVDetector, CSVEnclosureScan, CSVExtractor, CSVFileBatches}`                                               |
| `Flow\ETL\Adapter\CSV\{CSVLineReader, CSVOpenSource, CSVReadOptions, CSVRecordBoundary, PhpCSVOpenSource}`                                       | `Flow\ETL\Adapter\CSV\IO\Extractor\{CSVLineReader, CSVOpenSource, CSVReadOptions, CSVRecordBoundary, PhpCSVOpenSource}`                                     |
| `Flow\ETL\Adapter\CSV\RustCSVOpenSource`                                                                                                         | `Flow\ETL\Adapter\CSV\IO\Extractor\RustCSVOpenSource`                                                                                                       |
| `Flow\ETL\Adapter\CSV\Detector\{Option, Options}`                                                                                                | `Flow\ETL\Adapter\CSV\IO\Extractor\Detector\{Option, Options}`                                                                                              |
| `Flow\ETL\Adapter\CSV\Exception\CantDetectCSVOptions`                                                                                            | `Flow\ETL\Adapter\CSV\Foundation\Exception\CantDetectCSVOptions`                                                                                            |
| `Flow\ETL\Adapter\ChartJS\{Chart, ChartJSLoader}`                                                                                                | `Flow\ETL\Adapter\ChartJS\IO\Loader\{Chart, ChartJSLoader}`                                                                                                 |
| `Flow\ETL\Adapter\ChartJS\Chart\{BarChart, LineChart, PieChart}`                                                                                 | `Flow\ETL\Adapter\ChartJS\IO\Loader\Chart\{BarChart, LineChart, PieChart}`                                                                                  |
| `Flow\ETL\Adapter\Doctrine\{DbalMetadata, SchemaConverter, TypesMap}`                                                                            | `Flow\ETL\Adapter\Doctrine\Values\Schema\{DbalMetadata, SchemaConverter, TypesMap}`                                                                         |
| `Flow\ETL\Adapter\Doctrine\{DbalEncoder, DbalLoader, DbalTransaction, IsolationLevelRestore}`                                                    | `Flow\ETL\Adapter\Doctrine\IO\Loader\{DbalEncoder, DbalLoader, DbalTransaction, IsolationLevelRestore}`                                                     |
| `Flow\ETL\Adapter\Doctrine\{DbalKeySetExtractor, DbalLimitOffsetExtractor, DbalQueryExtractor, DbalResultSchema, DescribeQuery}`                 | `Flow\ETL\Adapter\Doctrine\IO\Extractor\{DbalKeySetExtractor, DbalLimitOffsetExtractor, DbalQueryExtractor, DbalResultSchema, DescribeQuery}`               |
| `Flow\ETL\Adapter\Doctrine\{LiteralParameter, MysqliPlaceholders, MysqliResultColumns, MysqliTypesMap, NativePlaceholders}`                      | `Flow\ETL\Adapter\Doctrine\IO\Extractor\{LiteralParameter, MysqliPlaceholders, MysqliResultColumns, MysqliTypesMap, NativePlaceholders}`                    |
| `Flow\ETL\Adapter\Doctrine\{NativeTypes, Order, OrderBy, Pages, Parameter}`                                                                      | `Flow\ETL\Adapter\Doctrine\IO\Extractor\{NativeTypes, Order, OrderBy, Pages, Parameter}`                                                                    |
| `Flow\ETL\Adapter\Doctrine\{ParametersSet, PdoSqliteColumnNames, PgSqlResultColumns, PgSqlTypesMap, PlaceholderDialect}`                         | `Flow\ETL\Adapter\Doctrine\IO\Extractor\{ParametersSet, PdoSqliteColumnNames, PgSqlResultColumns, PgSqlTypesMap, PlaceholderDialect}`                       |
| `Flow\ETL\Adapter\Doctrine\{PlaceholderRewriter, PostgreSqlPlaceholders, QueryParameter, ResultColumn}`                                          | `Flow\ETL\Adapter\Doctrine\IO\Extractor\{PlaceholderRewriter, PostgreSqlPlaceholders, QueryParameter, ResultColumn}`                                        |
| `Flow\ETL\Adapter\Doctrine\ProbeRefusal`                                                                                                         | `Flow\ETL\Adapter\Doctrine\Foundation\Exception\ProbeRefusal`                                                                                               |
| `Flow\ETL\Adapter\Doctrine\{RewrittenSql, Sqlite3ColumnNames, SqliteResultSchema, SqliteTypes, Table}`                                           | `Flow\ETL\Adapter\Doctrine\IO\Extractor\{RewrittenSql, Sqlite3ColumnNames, SqliteResultSchema, SqliteTypes, Table}`                                         |
| `Flow\ETL\Adapter\Doctrine\TypedColumns`                                                                                                         | `Flow\ETL\Adapter\Doctrine\IO\Extractor\TypedColumns`                                                                                                       |
| `Flow\ETL\Adapter\Doctrine\Explain\{ExplainedRows, MySqlExplainedRows, PostgreSqlExplainedRows}`                                                 | `Flow\ETL\Adapter\Doctrine\IO\Extractor\Explain\{ExplainedRows, MySqlExplainedRows, PostgreSqlExplainedRows}`                                               |
| `Flow\ETL\Adapter\Doctrine\Pagination\{Key, KeySet, KeySetPage, KeyValues, Order}`                                                               | `Flow\ETL\Adapter\Doctrine\IO\Extractor\Pagination\{Key, KeySet, KeySetPage, KeyValues, Order}`                                                             |
| `Flow\ETL\Adapter\Excel\{CellStyler, ExcelEncoder, ExcelLoader, ExcelWriter, WorkbookManager}`                                                   | `Flow\ETL\Adapter\Excel\IO\Loader\{CellStyler, ExcelEncoder, ExcelLoader, ExcelWriter, WorkbookManager}`                                                    |
| `Flow\ETL\Adapter\Excel\CellTypeNarrower`                                                                                                        | `Flow\ETL\Adapter\Excel\IO\Inference\CellTypeNarrower`                                                                                                      |
| `Flow\ETL\Adapter\Excel\{ExcelExtractor, ExcelFileBatches, ExcelFormatDetector, ExcelReadOptions}`                                               | `Flow\ETL\Adapter\Excel\IO\Extractor\{ExcelExtractor, ExcelFileBatches, ExcelFormatDetector, ExcelReadOptions}`                                             |
| `Flow\ETL\Adapter\Excel\{ExcelReader, WorkbookReader, WorkbookSheet}`                                                                            | `Flow\ETL\Adapter\Excel\IO\Extractor\{ExcelReader, WorkbookReader, WorkbookSheet}`                                                                          |
| `Flow\ETL\Adapter\Excel\WorkbookSheetSample`                                                                                                     | `Flow\ETL\Adapter\Excel\IO\Inference\WorkbookSheetSample`                                                                                                   |
| `Flow\ETL\Adapter\Excel\Sheet\SheetNameAssertion`                                                                                                | `Flow\ETL\Adapter\Excel\Functions\SheetNameAssertion`                                                                                                       |
| `Flow\ETL\Adapter\Excel\Sheet\{OpenSheet, SheetCells, SheetsManager, XlsxSheetRows}`                                                             | `Flow\ETL\Adapter\Excel\IO\Extractor\Sheet\{OpenSheet, SheetCells, SheetsManager, XlsxSheetRows}`                                                           |
| `Flow\ETL\Adapter\Excel\Function\IsValidExcelSheetName`                                                                                          | `Flow\ETL\Adapter\Excel\Functions\IsValidExcelSheetName`                                                                                                    |
| `Flow\ETL\Adapter\GoogleSheet\{Columns, GoogleSheetExtractor, GoogleSheetReadOptions, GoogleSheetReader}`                                        | `Flow\ETL\Adapter\GoogleSheet\IO\Extractor\{Columns, GoogleSheetExtractor, GoogleSheetReadOptions, GoogleSheetReader}`                                      |
| `Flow\ETL\Adapter\GoogleSheet\GoogleSheetSample`                                                                                                 | `Flow\ETL\Adapter\GoogleSheet\IO\Inference\GoogleSheetSample`                                                                                               |
| `Flow\ETL\Adapter\GoogleSheet\SheetRange`                                                                                                        | `Flow\ETL\Adapter\GoogleSheet\IO\Extractor\SheetRange`                                                                                                      |
| `Flow\ETL\Adapter\Http\{ContentTypeDetector, HttpDecoder, HttpExchange, PsrHttpClientDynamicExtractor, PsrHttpClientPaginatedExtractor}`         | `Flow\ETL\Adapter\Http\IO\Extractor\{ContentTypeDetector, HttpDecoder, HttpExchange, PsrHttpClientDynamicExtractor, PsrHttpClientPaginatedExtractor}`       |
| `Flow\ETL\Adapter\Http\{PsrHttpClientStaticExtractor, ResponseType}`                                                                             | `Flow\ETL\Adapter\Http\IO\Extractor\{PsrHttpClientStaticExtractor, ResponseType}`                                                                           |
| `Flow\ETL\Adapter\Http\DynamicExtractor\NextRequestFactory`                                                                                      | `Flow\ETL\Adapter\Http\IO\Extractor\NextRequestFactory`                                                                                                     |
| `Flow\ETL\Adapter\Http\Pagination\{DecodedResponse, InjectInto, LinkHeaderParser, PageState, PaginationStrategy}`                                | `Flow\ETL\Adapter\Http\IO\Extractor\Pagination\{DecodedResponse, InjectInto, LinkHeaderParser, PageState, PaginationStrategy}`                              |
| `Flow\ETL\Adapter\Http\Pagination\{Paginator, RelativeUriResolver, RequestComparator, RequestOption, ResultCounter}`                             | `Flow\ETL\Adapter\Http\IO\Extractor\Pagination\{Paginator, RelativeUriResolver, RequestComparator, RequestOption, ResultCounter}`                           |
| `Flow\ETL\Adapter\Http\Pagination\StopWhen`                                                                                                      | `Flow\ETL\Adapter\Http\IO\Extractor\Pagination\StopWhen`                                                                                                    |
| `Flow\ETL\Adapter\Http\Pagination\StopWhen\{AllOf, AnyOf, EmptyPath, EmptyResponseBody, FlagFalse}`                                              | `Flow\ETL\Adapter\Http\IO\Extractor\Pagination\StopWhen\{AllOf, AnyOf, EmptyPath, EmptyResponseBody, FlagFalse}`                                            |
| `Flow\ETL\Adapter\Http\Pagination\StopWhen\{FlagTrue, MaxPages, MaxResults, NeverStop, PathMissing}`                                             | `Flow\ETL\Adapter\Http\IO\Extractor\Pagination\StopWhen\{FlagTrue, MaxPages, MaxResults, NeverStop, PathMissing}`                                           |
| `Flow\ETL\Adapter\Http\Pagination\StopWhen\TotalReached`                                                                                         | `Flow\ETL\Adapter\Http\IO\Extractor\Pagination\StopWhen\TotalReached`                                                                                       |
| `Flow\ETL\Adapter\Http\Pagination\Strategy\{CursorFromBody, LastRecordCursor, LinkHeader, NextUrlFromBody, OffsetLimit}`                         | `Flow\ETL\Adapter\Http\IO\Extractor\Pagination\Strategy\{CursorFromBody, LastRecordCursor, LinkHeader, NextUrlFromBody, OffsetLimit}`                       |
| `Flow\ETL\Adapter\Http\Pagination\Strategy\PageNumber`                                                                                           | `Flow\ETL\Adapter\Http\IO\Extractor\Pagination\Strategy\PageNumber`                                                                                         |
| `Flow\ETL\Adapter\JSON\JsonFraming`                                                                                                              | `Flow\ETL\Adapter\JSON\IO\JsonFraming`                                                                                                                      |
| `Flow\ETL\Adapter\JSON\SchemaConverter`                                                                                                          | `Flow\ETL\Adapter\JSON\IO\Schema\SchemaConverter`                                                                                                           |
| `Flow\ETL\Adapter\JSON\{JsonEncoder, JsonLoader, JsonOpenSink, PhpJsonEncoder, RustJsonEncoder}`                                                 | `Flow\ETL\Adapter\JSON\IO\Loader\{JsonEncoder, JsonLoader, JsonFileWriter, PhpJsonEncoder, RustJsonEncoder}`                                                |
| `Flow\ETL\Adapter\JSON\{JsonOpenSource, PhpJsonOpenSource, RustJsonOpenSource}`                                                                  | `Flow\ETL\Adapter\JSON\IO\Extractor\{JsonOpenSource, PhpJsonOpenSource, RustJsonOpenSource}`                                                                |
| `Flow\ETL\Adapter\JSON\JSONMachine\{JsonArrayElements, JsonExtractor, JsonFileBatches, JsonFileReader}`                                          | `Flow\ETL\Adapter\JSON\IO\Extractor\{JsonArrayElements, JsonExtractor, JsonFileBatches, JsonFileReader}`                                                    |
| `Flow\ETL\Adapter\JSON\JSONMachine\JsonFileSample`                                                                                               | `Flow\ETL\Adapter\JSON\IO\Inference\JsonFileSample`                                                                                                         |
| `Flow\ETL\Adapter\JSON\JSONMachine\JsonReadBytes`                                                                                                | `Flow\ETL\Adapter\JSON\IO\Extractor\JsonReadProgress`                                                                                                       |
| `Flow\ETL\Adapter\JSON\JsonSchema\{JsonSchemaMetadata, ResolvedReference}`                                                                       | `Flow\ETL\Adapter\JSON\Values\Schema\JsonSchema\{JsonSchemaMetadata, ResolvedReference}`                                                                    |
| `Flow\ETL\Adapter\JSON\JsonSchema\ReferenceResolver`                                                                                             | `Flow\ETL\Adapter\JSON\IO\Schema\JsonSchema\ReferenceResolver`                                                                                              |
| `Flow\ETL\Adapter\JSON\JsonSchema\Exception\{CircularReferenceException, UnresolvableReferenceException, UnsupportedKeywordException}`           | `Flow\ETL\Adapter\JSON\Foundation\Exception\{CircularReferenceException, UnresolvableReferenceException, UnsupportedKeywordException}`                      |
| `Flow\ETL\Adapter\Logger\PsrLoggerLoader`                                                                                                        | `Flow\ETL\Adapter\Logger\IO\Loader\PsrLoggerLoader`                                                                                                         |
| `Flow\ETL\Adapter\Logger\Logger\DumpLogger`                                                                                                      | `Flow\ETL\Adapter\Logger\IO\Loader\DumpLogger`                                                                                                              |
| `Flow\ETL\Adapter\Parquet\SchemaConverter`                                                                                                       | `Flow\ETL\Adapter\Parquet\Values\Schema\SchemaConverter`                                                                                                    |
| `Flow\ETL\Adapter\Parquet\{ParquetEncoder, ParquetFileSink, ParquetFileSinks, ParquetLoader, ParquetOpenSink}`                                   | `Flow\ETL\Adapter\Parquet\IO\Loader\{ParquetFileWriter, ParquetFileWriterFactory, ParquetLoader}`; `ParquetEncoder` removed                                 |
| `Flow\ETL\Adapter\Parquet\{ParquetSchemaConformance, PhpParquetOpenSink, RustParquetOpenSink}`                                                   | `Flow\ETL\Adapter\Parquet\IO\Loader\{ParquetSchemaConformance, RustParquetFileWriter}`                                                                      |
| `Flow\ETL\Adapter\Parquet\{ParquetExtractor, ParquetFileBatches, ParquetOpenSource, ParquetSourceFile, ParquetSourceFileOpener}`                 | `Flow\ETL\Adapter\Parquet\IO\Extractor\{ParquetExtractor, ParquetFileBatches, ParquetOpenSource, ParquetSourceFile, ParquetSourceFileOpener}`               |
| `Flow\ETL\Adapter\Parquet\{PhpParquetOpenSource, RustParquetOpenSource}`                                                                         | `Flow\ETL\Adapter\Parquet\IO\Extractor\{PhpParquetOpenSource, RustParquetOpenSource}`                                                                       |
| `Flow\ETL\Adapter\Parquet\ValueConverter\{ElementsValueConverter, JsonValueConverter, StructValueConverter, UuidValueConverter, ValueConverter}` | removed: `ParquetFileWriter` hands each column's physicals to `writeStoredColumns()`                                                                        |
| `Flow\ETL\Adapter\Parquet\ValueConverter\ValueConverters`                                                                                        | removed, as the value converters                                                                                                                            |
| `Flow\ETL\Adapter\PostgreSql\{PostgreSqlMetadata, SchemaConverter}`                                                                              | `Flow\ETL\Adapter\PostgreSql\Values\Schema\{PostgreSqlMetadata, SchemaConverter}`                                                                           |
| `Flow\ETL\Adapter\PostgreSql\{Operation, PostgreSqlEncoder, PostgreSqlLoader, PostgreSqlTransaction}`                                            | `Flow\ETL\Adapter\PostgreSql\IO\Loader\{Operation, PostgreSqlEncoder, PostgreSqlLoader, PostgreSqlTransaction}`                                             |
| `Flow\ETL\Adapter\PostgreSql\{ExplainedRows, PostgreSqlCursorExtractor, PostgreSqlKeySetExtractor, PostgreSqlLimitOffsetExtractor, ReadQuery}`   | `Flow\ETL\Adapter\PostgreSql\IO\Extractor\{ExplainedRows, PostgreSqlCursorExtractor, PostgreSqlKeySetExtractor, PostgreSqlLimitOffsetExtractor, ReadQuery}` |
| `Flow\ETL\Adapter\PostgreSql\ResultSchema`                                                                                                       | `Flow\ETL\Adapter\PostgreSql\IO\Extractor\ResultSchema`                                                                                                     |
| `Flow\ETL\Adapter\PostgreSql\LoaderOptions\{DeleteOptions, InsertOptions, UpdateOptions}`                                                        | `Flow\ETL\Adapter\PostgreSql\IO\Loader\LoaderOptions\{DeleteOptions, InsertOptions, UpdateOptions}`                                                         |
| `Flow\ETL\Adapter\PostgreSql\Pagination\{Key, KeySet, KeySetPage, KeyValues, Order}`                                                             | `Flow\ETL\Adapter\PostgreSql\IO\Extractor\Pagination\{Key, KeySet, KeySetPage, KeyValues, Order}`                                                           |
| `Flow\ETL\Adapter\PostgreSql\QueryBuilder\{DeleteQueryBuilder, InsertQueryBuilder, UpdateQueryBuilder}`                                          | `Flow\ETL\Adapter\PostgreSql\IO\Loader\QueryBuilder\{DeleteQueryBuilder, InsertQueryBuilder, UpdateQueryBuilder}`                                           |
| `Flow\ETL\Adapter\PostgreSql\Schema\SortingStrategy\TypeStrategy`                                                                                | `Flow\ETL\Adapter\PostgreSql\Values\Schema\SortingStrategy\TypeStrategy`                                                                                    |
| `Flow\ETL\Adapter\PostgreSql\ValueConverter\{EnumConverter, HTMLConverter, XMLConverter}`                                                        | `Flow\ETL\Adapter\PostgreSql\IO\Loader\{EnumConverter, HTMLConverter, XMLConverter}`                                                                        |
| `Flow\ETL\Adapter\PostgreSql\Exception\RuntimeException`                                                                                         | removed: the adapter throws `Flow\ETL\Foundation\Exception\RuntimeException`, its parent                                                                    |
| `Flow\ETL\Adapter\PostgreSql\Exception\TypeMappingException`                                                                                     | `Flow\ETL\Adapter\PostgreSql\Foundation\Exception\TypeMappingException`                                                                                     |
| `Flow\ETL\Adapter\Seal\{SchemaConverter, SealMetadata}`                                                                                          | `Flow\ETL\Adapter\Seal\Values\Schema\{SchemaConverter, SealMetadata}`                                                                                       |
| `Flow\ETL\Adapter\Seal\{Operation, SealEncoder, SealLoader}`                                                                                     | `Flow\ETL\Adapter\Seal\IO\Loader\{Operation, SealEncoder, SealLoader}`                                                                                      |
| `Flow\ETL\Adapter\Text\{TextEncoder, TextLoader, TextOpenSink}`                                                                                  | `Flow\ETL\Adapter\Text\IO\Loader\{TextEncoder, TextLoader, TextFileWriter}`                                                                                 |
| `Flow\ETL\Adapter\Text\{TextExtractor, TextFileBatches}`                                                                                         | `Flow\ETL\Adapter\Text\IO\Extractor\{TextExtractor, TextFileBatches}`                                                                                       |
| `Flow\ETL\Adapter\XML\{XMLEncoder, XMLOpenSink, XMLValueNodes, XMLWriter}`                                                                       | `Flow\ETL\Adapter\XML\IO\Loader\{XMLEncoder, XMLFileWriter, XMLValueNodes, XMLWriter}`                                                                      |
| `Flow\ETL\Adapter\XML\{RootlessDocument, XMLFileBatches, XMLNodeBatches, XMLNodeCursor, XMLNodes}`                                               | `Flow\ETL\Adapter\XML\IO\Extractor\{RootlessDocument, XMLFileBatches, XMLNodeBatches, XMLNodeCursor, XMLNodes}`                                             |
| `Flow\ETL\Adapter\XML\XMLParserExtractor`                                                                                                        | `Flow\ETL\Adapter\XML\IO\Extractor\XMLParserExtractor`                                                                                                      |
| `Flow\ETL\Adapter\XML\Abstraction\{XMLAttribute, XMLNode, XMLNodeType}`                                                                          | `Flow\ETL\Adapter\XML\IO\Loader\{XMLAttribute, XMLNode, XMLNodeType}`                                                                                       |
| `Flow\ETL\Adapter\XML\Loader\XMLLoader`                                                                                                          | `Flow\ETL\Adapter\XML\IO\Loader\XMLLoader`                                                                                                                  |
| `Flow\ETL\Adapter\XML\XMLWriter\{DOMDocumentWriter, StringXMLWriter}`                                                                            | `Flow\ETL\Adapter\XML\IO\Loader\XMLWriter\{DOMDocumentWriter, StringXMLWriter}`                                                                             |

DSL function namespaces do not change.

### 86) `flow-php/etl` - constraints start every run fresh, `Stateful` no longer extends `Transformer`

| Before                                                                                  | After                                                        |
|-----------------------------------------------------------------------------------------|--------------------------------------------------------------|
| a frame with `constraint_unique()` or `constraint_sorted_by()` failed on its second run | passes                                                       |
| `interface Stateful extends Transformer { public function fresh(): self; }`             | `interface Stateful { public function fresh(): static; }`    |
| `final class MyTransformer implements Stateful`                                         | `final class MyTransformer implements Stateful, Transformer` |
| a `Constraint` with state between batches kept it across runs                           | `final class MyConstraint implements Constraint, Stateful`   |

### 87) `flow-php/etl` - `DropDuplicatesTransformer` is removed

| Before                                                        | After                               |
|---------------------------------------------------------------|-------------------------------------|
| `$df->transform(new DropDuplicatesTransformer('id', 'name'))` | `$df->dropDuplicates('id', 'name')` |
| `Flow\ETL\Transformer\DropDuplicatesTransformer`              | removed                             |

### 88) `flow-php/etl` - the memory limit is part of `BucketingConfig`, one builder for group by and repartition

| Before                                                                              | After                                                                           |
|-------------------------------------------------------------------------------------|---------------------------------------------------------------------------------|
| `new BucketingConfig($storage, $bucketsCount, $batchSize)`                          | `new BucketingConfig($storage, $bucketsCount, $batchSize, $memoryLimit)`        |
| `BucketingConfig::spilling(Unit $memoryLimit, Backend $backend)`                    | `BucketingConfig::spilling(Backend $backend)`                                   |
| `Flow\ETL\Config\Grouping\HashGroupByConfig`                                        | `Flow\ETL\Storage\BucketingConfig`                                              |
| `Flow\ETL\Config\Repartition\HashRepartitionConfig`                                 | `Flow\ETL\Storage\BucketingConfig`                                              |
| `new HashJoinConfig($bucketing, $memoryLimit, $broadcastLimit)`                     | `new HashJoinConfig($bucketing, $broadcastLimit)`                               |
| `new ExternalSortConfig($bucketing, $memoryLimit, $merge)`                          | `new ExternalSortConfig($bucketing, $merge)`                                    |
| `$config->memoryLimit` of a join, sort, group by or repartition config              | `$config->bucketing->memoryLimit` (join, sort), `$config->memoryLimit` (others) |
| `hash_group_by(): HashGroupByBuilder`, `hash_repartition(): HashRepartitionBuilder` | both return `Flow\ETL\API\Config\Bucketing\BucketingConfigBuilder`              |
| `Flow\ETL\Config\Grouping\HashGroupByBuilder`                                       | removed: `BucketingConfigBuilder::groupBy()`                                    |
| `Flow\ETL\Config\Repartition\HashRepartitionBuilder`                                | removed: `BucketingConfigBuilder::repartition()`                                |
| `Join memory limit must be greater than 0 bytes` (and `Sort`, `Group by`, ...)      | `Bucketing memory limit must be greater than 0 bytes`                           |

### 89) `flow-php/etl` - `GroupBy` is immutable, a group by is placed by `aggregate()`

| Before                                                                                                             | After                                                                              |
|--------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------|
| `$g = new GroupBy('a'); $g->aggregate(sum(ref('v'))); $g->pivot($ref, $values)`                                    | `new GroupBy(refs('a'), new Aggregators(sum(ref('v'))), new Pivot($ref, $values))` |
| `GroupBy::aggregate()`, `GroupBy::pivot()`                                                                         | removed                                                                            |
| `PivotValues::resolve(DataFrame $source, Reference $pivot)`                                                        | `PivotValues::resolve(iterable $batches, Reference $pivot)` (`iterable<Rows>`)     |
| -                                                                                                                  | `DataFrame::aggregateBy(GroupBy $groupBy, ?BucketingAlgorithmBuilder $algorithm)`  |
| `$df->groupBy('a');` without `->aggregate()` grouped `$df` to its distinct keys                                    | `$df` is unchanged: call `->aggregate(...)`                                        |
| `$grouped->aggregate(sum(...), count(...))` then `$grouped->pivot(...)` pivoted the aggregated frame on `sum` only | the aggregated frame is not pivoted: call `->pivot(...)->aggregate(...)`           |

### 90) `flow-php/etl` - transformers and processors take a `Backend`, not a `FlowContext`

| Before                                                                 | After                                                                             |
|------------------------------------------------------------------------|-----------------------------------------------------------------------------------|
| `Transformer::transform(Rows $rows, FlowContext $context): Rows`       | `Transformer::transform(Rows $rows, Backend $backend): Rows`                      |
| `Processor::process(Generator $rows, FlowContext $context): Generator` | `Processor::process(Generator $rows, Backend $backend): Generator`                |
| `new SerializeTransformer($target, $standalone)`                       | `new SerializeTransformer($target, $serializer, $standalone)`                     |
| `new UnserializeTransformer($source, $target, $merge, $mergePrefix)`   | `new UnserializeTransformer($source, $target, $serializer, $merge, $mergePrefix)` |
| `new CachingProcessor(?string $id = null, ?RowsStorage $cache = null)` | `new CachingProcessor(string $id, RowsStorage $cache)`                            |

`explain()` of a cache step always prints the storage in use as `Cache: <storage>`.

A transformer that read the config at run time takes what it needs in its constructor.

Before:

```php
final class MyTransformer implements Transformer
{
    public function transform(Rows $rows, FlowContext $context): Rows
    {
        $serializer = $context->config->serializer();
        // ...
    }
}
```

After:

```php
final class MyTransformer implements Transformer
{
    public function __construct(private Serializer $serializer)
    {
    }

    public function transform(Rows $rows, Backend $backend): Rows
    {
        // ...
    }
}
```

### 91) `flow-php/etl` - operator types moved to `Flow\ETL\Operators`, their configs to `Storage`, `Kernels` and

`Operators\Join`

| Before                                                                                                                                           | After                                                                                                                                                                                         |
|--------------------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `Flow\ETL\{BoundStep, Constraint, GroupBy, Processor, Transformer}`                                                                              | `Flow\ETL\Operators\{BoundStep, Constraint, GroupBy, Processor, Transformer}`                                                                                                                 |
| `Flow\ETL\Config\Bucketing\BucketingConfig`                                                                                                      | `Flow\ETL\Storage\BucketingConfig`                                                                                                                                                            |
| `Flow\ETL\Config\Join\HashJoinConfig`                                                                                                            | `Flow\ETL\Operators\Join\HashJoinConfig`                                                                                                                                                      |
| `Flow\ETL\Config\Sort\{ExternalSortConfig, MemorySortConfig}`                                                                                    | `Flow\ETL\Kernels\Sort\{ExternalSortConfig, MemorySortConfig}`                                                                                                                                |
| `Flow\ETL\Constraint\{SortedByConstraint, UniqueConstraint}`                                                                                     | `Flow\ETL\Operators\Constraint\{SortedByConstraint, UniqueConstraint}`                                                                                                                        |
| `Flow\ETL\GroupBy\{AggregatedGroups, Aggregators, DeclaredPivotValues, DiscoveredPivotValues}`                                                   | `Flow\ETL\Operators\GroupBy\{AggregatedGroups, Aggregators, DeclaredPivotValues, DiscoveredPivotValues}`                                                                                      |
| `Flow\ETL\GroupBy\{GroupByShape, Pivot, PivotShape, PivotValues}`                                                                                | `Flow\ETL\Operators\GroupBy\{GroupByShape, Pivot, PivotShape, PivotValues}`                                                                                                                   |
| `Flow\ETL\GroupBy\PivotedTable`                                                                                                                  | `Flow\ETL\Operators\GroupBy\PivotedTable`                                                                                                                                                     |
| `Flow\ETL\GroupBy\{Group, GroupKey}`                                                                                                             | removed: `Backend::groupTable()` assigns group ids (`Flow\ETL\Values\Grouping\{GroupTable, GroupIds}`)                                                                                        |
| `Flow\ETL\Plan\RequiredColumns`                                                                                                                  | `Flow\ETL\Operators\Processor\RequiredColumns`                                                                                                                                                |
| `Flow\ETL\Processor\{BatchingByProcessor, BatchingProcessor, BoundExpansion, CachingProcessor, CollectingProcessor}`                             | `Flow\ETL\Operators\Processor\{BatchingByProcessor, BatchingProcessor, BoundExpansion, CachingProcessor, CollectingProcessor}`                                                                |
| `Flow\ETL\Processor\{ConstrainedProcessor, CountingProcessor, ExpandingProcessor, GroupByAggregationProcessor, HashJoinProcessor}`               | `Flow\ETL\Operators\Processor\{ConstrainedProcessor, CountingProcessor, ExpandingProcessor, GroupByAggregationProcessor}`, `HashJoinProcessor` -> `Flow\ETL\Operators\Join\HashJoin` (row 94) |
| `Flow\ETL\Processor\{MemorySortProcessor, MergeSortProcessor, OffsetProcessor, PivotProcessor, RepartitionProcessor}`                            | `Flow\ETL\Operators\Processor\{MemorySortProcessor, MergeSortProcessor, OffsetProcessor, PivotProcessor, RepartitionProcessor}`                                                               |
| `Flow\ETL\Processor\{TopNProcessor, VoidProcessor, WindowProcessor}`                                                                             | `Flow\ETL\Operators\Processor\{TopNProcessor, VoidProcessor, WindowProcessor}`                                                                                                                |
| `Flow\ETL\Transformation\AddRowIndex\StartFrom`                                                                                                  | `Flow\ETL\Operators\Transformer\AddRowIndex\StartFrom`                                                                                                                                        |
| `Flow\ETL\Transformer\{AddRowIndexTransformer, CollectReferencesTransformer, CrossJoinRowsTransformer, DerivedColumns, DuplicateRowTransformer}` | `Flow\ETL\Operators\Transformer\{AddRowIndexTransformer, CollectReferencesTransformer, DerivedColumns, DuplicateRowTransformer}`                                                              |
| `Flow\ETL\Transformer\{Expansion, JoinEachRowsTransformer, LimitTransformer, ScalarFunctionFilterTransformer, ScalarFunctionTransformer}`        | `Flow\ETL\Operators\Transformer\{Expansion, JoinEachRowsTransformer, LimitTransformer, ScalarFunctionFilterTransformer, ScalarFunctionTransformer}`                                           |
| `Flow\ETL\Transformer\{SerializeTransformer, SerializedPayloadDecoder, Stateful, UnpackedColumns, UnserializeTransformer}`                       | `Flow\ETL\Operators\Transformer\{SerializeTransformer, SerializedPayloadDecoder, Stateful, UnpackedColumns, UnserializeTransformer}`                                                          |
| `Flow\ETL\Transformer\UntilTransformer`                                                                                                          | `Flow\ETL\Operators\Transformer\UntilTransformer`                                                                                                                                             |

`ConstrainedProcessor` refuses a non-constraint with `Pipeline constraints must be of type Flow\ETL\Operators\Constraint`.

### 92) `flow-php/etl` - the `*Steps` factories are removed

| Before                                                                                                         | After                                                                                                                                                                                             |
|----------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `Flow\ETL\GroupBy\GroupBySteps::of()`                                                                          | removed: the planner builds `[PruneColumnsTransformer, GroupByAggregationProcessor]` (the prune only when `GroupBy::readColumns(): ?list<string>` is not null) or `[PivotProcessor]`              |
| `Flow\ETL\Join\JoinSteps::of()`                                                                                | removed: the planner builds a `JoinStep` over a `HashJoin` (row 94)                                                                                                                               |
| `Flow\ETL\Repartition\RepartitionSteps::of()`                                                                  | removed: `new RepartitionProcessor(References $by, BucketingConfig $config, RandomValueGenerator $random)`                                                                                        |
| `new RepartitionProcessor($by, HashBucketing $bucketing, Buckets $buckets, Unit $memoryLimit, Hasher $hasher)` | `new RepartitionProcessor($by, BucketingConfig $config, RandomValueGenerator $random)`: it builds its bucketing and its spilling buckets from `$config`; `$memoryLimit` is `$config->memoryLimit` |
| `Flow\ETL\Sort\SortSteps::of()`                                                                                | removed: `new MemorySortProcessor($columns)` or `new MergeSortProcessor($config, $columns, $random)`                                                                                              |
| `SortSteps::external()`                                                                                        | `Flow\ETL\Kernels\Sort\ExternalSort::of($columns, ExternalSortConfig $config, RandomValueGenerator $random)`                                                                                      |

### 93) `flow-php/etl` - the optimizer walks are methods of their rule

| Before                                                                           | After                                                                                                    |
|----------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------|
| `Flow\ETL\Optimizer\LimitWalk::of($from, $leaf)`                                 | `Flow\ETL\Planning\Optimizer\Rule\PushLimitIntoSource::limitAt($from, $leaf)`                            |
| `Flow\ETL\Optimizer\FilterWalk::reachedByEveryConsumer()`, `::predicateAtLeaf()` | `Flow\ETL\Planning\Optimizer\Rule\PushFilterIntoSource::reachedByEveryConsumer()`, `::predicateAtLeaf()` |
| `Flow\ETL\Optimizer\ProjectionWalk::below($node, $above)`                        | `Flow\ETL\Planning\Optimizer\Rule\PushProjectionIntoExpand::below($node, $above)`                        |

### 94) `flow-php/etl` - the join operators are fed by the runtime

| Before                                                                                                   | After                                                                                                                                                                            |
|----------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `Flow\ETL\Processor\HashJoinProcessor` (a `Processor`)                                                   | `Flow\ETL\Operators\Join\HashJoin`, not a `Processor`: `join(Generator $left, Generator $right, Backend $backend): Generator`, `bind(Schema $left, Schema $right): BoundStep`    |
| `new HashJoinProcessor(PhysicalPlan $right, Executor $executor, $on, $type, $config, $random, $backend)` | `new HashJoin($on, $type, $config, $random)`; the right plan is not the operator's                                                                                               |
| `Flow\ETL\Transformer\CrossJoinRowsTransformer` (a `Transformer`)                                        | `Flow\ETL\Operators\Join\CrossJoin`, not a `Transformer`: `join(Generator $left, Generator $right, Backend $backend): Generator`, `bind(Schema $left, Schema $right): BoundStep` |
| `new CrossJoinRowsTransformer(PhysicalPlan $right, Executor $executor, string $prefix)`                  | `new CrossJoin(string $prefix)`                                                                                                                                                  |
| a join step holds its right plan                                                                         | the plan holds `Flow\ETL\Planning\Physical\JoinStep(HashJoin\|CrossJoin $join, PhysicalPlan $right)`; the runtime runs the right plan once and hands its rows to `join()`        |
| `crossJoin()`: a failure in it is offered to the error handler (`onTransformation()`)                    | a failure in it propagates like any processor's, a failure of the right side included; the error handler is not asked                                                            |
| `crossJoin()`: one transformation span per batch                                                         | no span, as for every processor and for `join()`                                                                                                                                 |
| `BoundStep::$step` is `Processor\|Transformer`                                                           | `Processor\|Transformer\|HashJoin\|CrossJoin`, typed by `@template`: `Processor::bind()` returns `BoundStep<Processor>`, `Transformer::bind()` `BoundStep<Transformer>`          |
| `explain()` prints `Processor: HashJoinProcessor`, `Transformer: CrossJoinRowsTransformer`               | `Join: HashJoin`, `Join: CrossJoin`                                                                                                                                              |

### 95) `flow-php/etl` - the physical plan holds plan data, the runtime holds the run

| Before                                                                                                              | After                                                                                                                                                                                                                                                                              |
|---------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `Planner::plan(LogicalPlan $logical, FlowContext $context, ?SourceRows $sources)`                                   | `plan(LogicalPlan $logical)` (row 103); the counters go to `Executor::execute()`                                                                                                                                                                                                   |
| `Executor::execute(PhysicalPlan $plan)`, `fetch(PhysicalPlan $plan)`                                                | `execute(LogicalPlan $logical, FlowContext $context, ?RunCounts $sources = null)`, `fetch(LogicalPlan $logical, FlowContext $context)`: the executor plans, and `execute()` returns the `PhysicalPlan` (`getReturn()`)                                                             |
| `Executor::executePlan(PhysicalPlan $plan)`, `executePipeline(Pipeline $pipeline)`                                  | `executePlan(PhysicalPlan $plan, FlowContext $context, ?RunCounts $sources = null)`, `executePipeline(Pipeline $pipeline, FlowContext $context, ?Extractor $source = null)`                                                                                                        |
| `Executor::merge(Generator $batches, PhysicalPlan $plan)`                                                           | `merge(Generator $batches, Backend $backend)`: an empty result takes its schema from the `PhysicalPlan` `$batches` returns                                                                                                                                                         |
| `Flow\ETL\Planner\SinkFeedFactory` (built the sink pipelines at plan time)                                          | `Flow\ETL\Runtime\Executor\WriteLoaderFactory`: `feed(BranchPipeline, ErrorHandler, FlowContext): BranchLoader`, `transactional(TransactionGroup, FlowContext): TransactionLoader`; the plan holds `BranchPipeline`/`TransactionGroup` and the runtime builds the loaders each run |
| `new SinkRun(Pipeline $side, Executor $executor)`                                                                   | `new BranchFiber(Pipeline $side, Executor $executor, FlowContext $context, FeedExtractor $feed)`                                                                                                                                                                                   |
| `explain()` prints `Loader: SinkFeed`, `Loader: TransactionalSinks` for a sink with steps of its own, a transaction | `Branch: BranchPipeline`, `Transaction: TransactionGroup`                                                                                                                                                                                                                          |

### 96) `flow-php/etl` - `explain()` formats a join and a sort once for both plans

| Before                                                                                                | After                                                                                                                 |
|-------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------|
| `Flow\ETL\Plan\Explain\Details`                                                                       | `Flow\ETL\Planning\Explain\NodeDetails`                                                                               |
| `Flow\ETL\Plan\Explain\Outline`                                                                       | `Flow\ETL\Planning\Explain\LogicalOutline`                                                                            |
| `Flow\ETL\Plan\Explain\Branches`, `Branches::of($entry, $indent)`                                     | removed: `TreeLayout::branches($entry, $indent)`                                                                      |
| `Details::join(Node\Join $join)`, `Details::crossJoin(Node\CrossJoin $join)`, `Details::references()` | `NodeDetails::join(JoinType $type, Expression $on)`, `NodeDetails::crossJoin(string $prefix)`; `references()` removed |
| `StepDetails::sort()`, `StepDetails::columns(References $refs)`                                       | `NodeDetails::sort(list<SortColumn> $columns)`, `NodeDetails::columns($refs->all())`                                  |
| `explain()` logical join: `Left on: id, tag` / `Right on: user_id, tag`                               | `On: id = user_id` / `On: tag = tag`, one line per condition, as the physical plan prints it                          |
| `explain()` physical join: `Join: left`, `Join: cross`                                                | `Type: left`, `Type: cross`, as the logical plan prints it                                                            |
| `explain()` logical sort: `Sort by: id, name` (names only)                                            | `Sort by: id desc, name asc nulls last` (direction, and a nulls order that is not the default)                        |
| `explain()` physical sort and top N: `Sort: id asc`                                                   | `Sort by: id asc`                                                                                                     |

### 97) `flow-php/etl` - planning types moved to `Flow\ETL\Planning`

| Before                                                                                                                         | After                                                                                                                                   |
|--------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------|
| `Flow\ETL\Plan`                                                                                                                | `Flow\ETL\Planning\Logical\Plan`                                                                                                        |
| `Flow\ETL\Optimizer`                                                                                                           | `Flow\ETL\Planning\Optimizer\Optimizer`                                                                                                 |
| `Flow\ETL\Planner`                                                                                                             | `Flow\ETL\Planning\Physical\Planner`                                                                                                    |
| `Flow\ETL\Config\Grouping\GroupByAlgorithmBuilder`                                                                             | `Flow\ETL\Planning\Logical\Node\BucketingAlgorithmBuilder` (see 121)                                                                    |
| `Flow\ETL\Config\Join\JoinAlgorithmBuilder`                                                                                    | `Flow\ETL\Planning\Logical\Node\JoinAlgorithmBuilder`                                                                                   |
| `Flow\ETL\Config\Repartition\RepartitionAlgorithmBuilder`                                                                      | `Flow\ETL\Planning\Logical\Node\BucketingAlgorithmBuilder` (see 121)                                                                    |
| `Flow\ETL\Config\Sort\SortAlgorithmBuilder`                                                                                    | `Flow\ETL\Planning\Logical\Node\SortAlgorithmBuilder`                                                                                   |
| `Flow\ETL\Executor\{PhysicalPlan, Pipeline}`                                                                                   | `Flow\ETL\Planning\Physical\{PhysicalPlan, Pipeline}`                                                                                   |
| `Flow\ETL\Optimizer\{JoinSides, Rule}`                                                                                         | `Flow\ETL\Planning\Optimizer\{JoinSides, Rule}`                                                                                         |
| `Flow\ETL\Optimizer\Rule\{CombineLimits, CombineSortAndLimit, CountFromStatistics, PushFilterIntoSource, PushLimitIntoSource}` | `Flow\ETL\Planning\Optimizer\Rule\{CombineLimits, CombineSortAndLimit, CountFromStatistics, PushFilterIntoSource, PushLimitIntoSource}` |
| `Flow\ETL\Optimizer\Rule\PushProjectionIntoExpand`                                                                             | `Flow\ETL\Planning\Optimizer\Rule\PushProjectionIntoExpand`                                                                             |
| `Flow\ETL\Plan\{Explain, Format, LogicalPlan, Node, Redefined}`                                                                | `Flow\ETL\Planning\Logical\{Explain, Format, LogicalPlan, Node, Redefined}`                                                             |
| `Flow\ETL\Plan\{Rewrite, RowCount, Sinks, Stage, TransformUp}`                                                                 | `Flow\ETL\Planning\Logical\{Rewrite, RowCount, Stage, TransformUp, Writes}`                                                             |
| `Flow\ETL\Plan\{Transparency, Trigger}`                                                                                        | `Flow\ETL\Planning\Logical\{Transparency, Trigger}`                                                                                     |
| `Flow\ETL\Plan\Explain\{BoxLayout, Condition, Entry, FlowLayout, Layout}`                                                      | `Flow\ETL\Planning\Explain\{BoxLayout, Condition, Entry, FlowLayout, Layout}`                                                           |
| `Flow\ETL\Plan\Explain\{PhysicalOutline, StatisticsLine, StepDetails, TreeLayout}`                                             | `Flow\ETL\Planning\Explain\{PhysicalOutline, StatisticsLine, StepDetails, TreeLayout}`                                                  |
| `Flow\ETL\Plan\Node\{Aggregate, Batch, BatchBy, Cache, Collect}`                                                               | `Flow\ETL\Planning\Logical\Node\{Aggregate, Batch, BatchBy, Cache, Collect}`                                                            |
| `Flow\ETL\Plan\Node\{CollectRefs, Constrain, Count, CrossJoin, Discard}`                                                       | `Flow\ETL\Planning\Logical\Node\{CollectRefs, Constrain, Count, CrossJoin, Discard}`                                                    |
| `Flow\ETL\Plan\Node\{Distinct, Drop, DuplicateRow, ExpandColumn, Filter}`                                                      | `Flow\ETL\Planning\Logical\Node\{Distinct, Drop, DuplicateRow, ExpandColumn, Filter}`                                                   |
| `Flow\ETL\Plan\Node\{Join, JoinEach, JoinsFrame, Limit, Offset}`                                                               | `Flow\ETL\Planning\Logical\Node\{Join, JoinEach, JoinsFrame, Limit, Offset}`                                                            |
| `Flow\ETL\Plan\Node\{Outputs, Read, Rename, RenameEach, Repartition}`                                                          | `Flow\ETL\Planning\Logical\Node\{Outputs, Read, Rename, RenameEach, Repartition}`                                                       |
| `Flow\ETL\Plan\Node\{Result, Select, Sort, TopN, Transaction}`                                                                 | `Flow\ETL\Planning\Logical\Node\{Result, Select, Sort, TopN, Transaction}`                                                              |
| `Flow\ETL\Plan\Node\{Transform, Until, Validate, WindowColumn, WithColumn}`                                                    | `Flow\ETL\Planning\Logical\Node\{Transform, Until, Validate, WindowColumn, WithColumn}`                                                 |
| `Flow\ETL\Plan\Node\Write`                                                                                                     | `Flow\ETL\Planning\Logical\Node\Write`                                                                                                  |
| `Flow\ETL\Planner\{NodeTranslator, PlannedNode, PlannedNodes, SinkAttachment}`                                                 | `Flow\ETL\Planning\Physical\{NodeTranslator, PlannedNode, PlannedNodes, WritePlanner}`                                                  |

### 98) `flow-php/etl` - `FlowContext` and `Config` hold no calculator

| Before                                                 | After                                     |
|--------------------------------------------------------|-------------------------------------------|
| `$context->calculator()`, `$config->calculator()`      | `new Calculator()`                        |
| `new Config(..., $backend, $calculator, $random, ...)` | `new Config(..., $backend, $random, ...)` |

### 99) `flow-php/etl` - runtime types renamed

| Before                         | After                                      |
|--------------------------------|--------------------------------------------|
| `Flow\ETL\Executor\SinkRun`    | `Flow\ETL\Runtime\Executor\BranchFiber`    |
| `Flow\ETL\Executor\SinkOffers` | `Flow\ETL\Runtime\Executor\OfferedFailure` |

### 100) `flow-php/etl` - `FlowContext::setErrorHandler()` removed, `onError()` changes only its own frame

| Before                                                                                                         | After                                                             |
|----------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------|
| `$context->setErrorHandler($handler)`                                                                          | `$context = $context->withErrorHandler($handler)`                 |
| `onError()` on a frame built with `new DataFrame(..., $context)` changed `$context` and every frame sharing it | `onError()` changes only that frame, `$context` keeps its handler |
| `$df->groupBy(...)`, then `$df->onError($h)`, then `->pivot(...)`: discovery ran under `$h`                    | discovery runs under the handler the frame had at `groupBy()`     |

### 101) `flow-php/etl` - `InferredSchemaException::columnsDiverge()` takes the sample bounds

| Before                                                                    | After                                                                                     |
|---------------------------------------------------------------------------|-------------------------------------------------------------------------------------------|
| `columnsDiverge($source, $inferredFrom, $inferred, $columns, $inference)` | `columnsDiverge($source, $inferredFrom, $inferred, $columns, $sampleSize, $filesToSniff)` |

### 102) `flow-php/etl` - sink failures follow the main pipeline's error-handler rule

The error handler gets a loader or transformer failure once, against that step. Any other failure propagates. This
holds in a `Transactional` child too.

| Before                                                                                              | After                                             |
|-----------------------------------------------------------------------------------------------------|---------------------------------------------------|
| a sink's consumer `closure()` failure was offered to `onLoading()` against an internal `SinkFeed`   | not offered: it propagates, as in a main pipeline |
| a processor failure in a sink was offered the same way                                              | not offered: it propagates                        |
| a failing `Transaction::begin()` or `commit()` was offered against an internal `TransactionalSinks` | not offered: it propagates                        |
| a loader or transformer failure in a `Transactional` child named an internal `SinkFeed`             | offered once, naming that loader or transformer   |

### 103) `flow-php/etl` - planning takes neither `FlowContext` nor `Config`: rules get the `Backend`, the planner holds its settings

| Before                                                                   | After                                                                                                                                     |
|--------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------|
| `Rule::apply(LogicalPlan $plan, FlowContext $context)`                   | `apply(LogicalPlan $plan, Backend $backend)`                                                                                              |
| `Optimizer::optimize(LogicalPlan $plan, FlowContext $context)`           | `optimize(LogicalPlan $plan, Backend $backend)`                                                                                           |
| `new JoinSides(Optimizer $optimizer, FlowContext $context)`              | `new JoinSides(Optimizer $optimizer, Backend $backend)`                                                                                   |
| `new Planner(Optimizer $optimizer = new Optimizer())`                    | `new Planner(Optimizer $optimizer, NodeTranslator $translator, Backend $backend)`; `Config::planner()` returns the one the config builds  |
| `Planner::plan()` (row 95)                                               | `plan(LogicalPlan $logical)`; `Planner::optimize(LogicalPlan $logical)` runs the planner's optimizer alone                                |
| `NodeTranslator::steps()` (row 35), static                               | `new NodeTranslator($backend, $randomValueGenerator, $id, $cache, $cacheDir, $sort, $grouping, $join, $repartition)`, `steps(Node $node)` |
| `Plan::of(LogicalPlan $logical, FlowContext $context)`, `Plan::$context` | `Plan::of(LogicalPlan $logical, Planner $planner)`, `Plan::$planner`                                                                      |

A custom optimizer rule:

```php
// before
public function apply(LogicalPlan $plan, FlowContext $context): LogicalPlan

// after
public function apply(LogicalPlan $plan, Backend $backend): LogicalPlan
```

### 104) `flow-php/etl` - `TelemetryContext` takes the data frame id and name, one step trio for every step kind

| Before                                                                                         | After                                                                                                                    |
|------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------|
| `new TelemetryContext($backend, $logger, $tracer, $meter, $options)`                           | `new TelemetryContext(string $dataFrameId, string $dataFrameName, $backend, $logger, $tracer, $meter, $options)`         |
| `dataFrameStarted(FlowContext $context)`                                                       | `dataFrameStarted(array $configuration)`, the map `Config::telemetryAttributes()` returns                                |
| `dataFrameCompleted(FlowContext $context, array $attributes = [])`                             | `dataFrameCompleted(array $attributes = [])`                                                                             |
| `dataFrameFailed(FlowContext $context, Throwable $exception, array $attributes = [])`          | `dataFrameFailed(Throwable $exception, array $attributes = [])`                                                          |
| `dataFrameBatchProcessed(Rows $rows, FlowContext $context)`                                    | `dataFrameBatchProcessed(Rows $rows)`                                                                                    |
| `loadingStarted(Loader $loader, array $attributes = [])`                                       | with `$options->traceLoading`: `$step = stepStarted(StepKind::loading, $loader::class, $attributes)`                     |
| `transformationStarted(Transformer $transformer, array $attributes = [])`                      | with `$options->traceTransformations`: `$step = stepStarted(StepKind::transformation, $transformer::class, $attributes)` |
| `loadingCompleted($loader, $attributes)`, `transformationCompleted($transformer, $attributes)` | `stepCompleted($step, $attributes)`                                                                                      |
| `loadingFailed($loader, $exception)`, `transformationFailed($transformer, $exception)`         | `stepFailed($step, StepKind::loading, $loader::class, $exception)`, the same with `StepKind::transformation`             |
| `SpanStack::push(Span $span, Scope $scope)`, `SpanStack::pop()`                                | `push(SpanScope $spanScope)`, `remove(SpanScope $spanScope): bool`                                                       |
| `new SpanScope(Span $span, Scope $scope)`                                                      | `new SpanScope(Span $span, ?Scope $scope)`                                                                               |
| `trace_transformations`: one span per transformer                                              | also one span per processor and join (`flow.etl.operator.class`). A failing one logs `Operator failed`                   |

### 105) `flow-php/etl` - runtime types moved to `Flow\ETL\Runtime`, telemetry types to `Flow\ETL\Runtime\Telemetry`

| Before                                                                                                       | After                                                                                                                   |
|--------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------|
| `Flow\ETL\{Analyze, Config, ErrorHandler, FlowContext}`                                                      | `Flow\ETL\Runtime\{Analyze, Config, ErrorHandler, FlowContext}`                                                         |
| `Flow\ETL\Config\Cache\CacheConfig`                                                                          | `Flow\ETL\Runtime\CacheConfig`                                                                                          |
| `Flow\ETL\Config\MemoryLimit`                                                                                | `Flow\ETL\Runtime\MemoryLimit`                                                                                          |
| `Flow\ETL\Config\Telemetry\{SpanScope, SpanStack, TelemetryConfig, TelemetryContext, TelemetryOptions}`      | `Flow\ETL\Runtime\Telemetry\{SpanScope, SpanStack, TelemetryConfig, TelemetryContext, TelemetryOptions}`                |
| `Flow\ETL\Dataset\{Report, SourceStatistics, Statistics}`                                                    | `Flow\ETL\Runtime\Metrics\{Report, SourceStatistics, Statistics}`                                                       |
| `Flow\ETL\Dataset\Statistics\{Column, Columns, ExecutionTime}`                                               | `Flow\ETL\Runtime\Metrics\Statistics\{Column, Columns, ExecutionTime}`                                                  |
| `Flow\ETL\Dataset\Statistics\HighResolutionTime`                                                             | `Flow\ETL\Foundation\Clock\HighResolutionTime`                                                                          |
| `Flow\ETL\ErrorHandler\{ExtractionAction, ExtractionError, IgnoreError, LoadingAction, LoadingError}`        | `Flow\ETL\Runtime\ErrorHandler\{ExtractionAction, ExtractionError, IgnoreError, LoadingAction, LoadingError}`           |
| `Flow\ETL\ErrorHandler\{SkipRows, ThrowError, TransformationAction, TransformationError}`                    | `Flow\ETL\Runtime\ErrorHandler\{SkipRows, ThrowError, TransformationAction, TransformationError}`                       |
| `Flow\ETL\Exception\SinkFailure`                                                                             | removed: a failure out of a branch or a transaction surfaces as the user's own exception                                |
| `Flow\ETL\Executor`                                                                                          | `Flow\ETL\Runtime\Executor\Executor`                                                                                    |
| `Flow\ETL\Executor\{AdoptedBatches, SinkFeed, StatisticsCollector, TransactionalSinks}`                      | `Flow\ETL\Runtime\Executor\{AdoptedBatches, BranchLoader, StatisticsCollector, TransactionLoader}`                      |
| `Flow\ETL\Executor\TransactionRollback`                                                                      | removed: `TransactionLoader` rolls back itself and logs a failing rollback                                              |
| `Flow\ETL\Executor\SourceRows`                                                                               | `Flow\ETL\Runtime\Executor\RunCounts`                                                                                   |
| `new Flow\ETL\Exception\TransactionRolledBack(Loader $loader, Throwable $cause)`                             | removed: the rolled-back error is offered once to the plan's error handler                                              |

### 106) `flow-php/etl` - the serialize transformers lose their mode flags

| Before                                                                     | After                                                                                            |
|----------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------|
| `new SerializeTransformer($target, $serializer, standalone: true)`         | `->with(new SerializeTransformer($target, $serializer))->select($target)`                        |
| `new UnserializeTransformer($source, $target, $serializer, merge: false)`  | `->with(new UnserializeTransformer($source, $target, $serializer))->select(...$target->names())` |
| `new UnserializeTransformer($source, $target, $serializer, true, $prefix)` | `new UnserializeTransformer($source, $target, $serializer, $prefix)`                             |

### 107) `flow-php/etl` - `from_data_frame()` is removed

| Before                                                        | After                                                                                                      |
|---------------------------------------------------------------|------------------------------------------------------------------------------------------------------------|
| `df()->read(from_data_frame($orders))->limit(5)`              | `$orders->limit(5)`                                                                                        |
| `from_data_frame($orders)` to read one frame in two pipelines | build the frame in a function: `$orders = fn (): DataFrame => df()->read(...)`, then `$orders()->limit(5)` |
| `Flow\ETL\Extractor\DataFrameExtractor`                       | removed                                                                                                    |

### 108) `flow-php/symfony-http-foundation-bridge` - `http_stream_open()` takes a DataFrame

| Before                                                                                                                   | After                                                                                                  |
|--------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------|
| `http_stream_open(from_data_frame($df))`                                                                                 | `http_stream_open($df)`: the frame runs under its own config, and the response adds its steps to `$df` |
| `http_stream_open(from_data_frame($df))->config($config)`                                                                | throws `InvalidArgumentException`: set the config on the frame, `df($config)->read(...)`               |
| `new DataStream(extractor: ...)`, `new FlowBufferedResponse(extractor: ...)`, `new FlowStreamedResponse(extractor: ...)` | `source: ...`                                                                                          |

### 109) `flow-php/etl` - one name per `DataFrame` and `Flow` verb

| Before                                                    | After                               |
|-----------------------------------------------------------|-------------------------------------|
| `$df->withEntry($name, $function)`                        | `$df->withColumn($name, $function)` |
| `$df->withEntries([...])`                                 | `$df->withColumns([...])`           |
| `$df->rows($transformer)`, `$df->transform($transformer)` | `$df->with($transformer)`           |
| `$df->load($sink)`                                        | `$df->write($sink)`                 |
| `df()->from($extractor)`, `df()->extract($extractor)`     | `df()->read($extractor)`            |

### 110) `flow-php/etl` - `Config::builder()` and `Config::default()` are removed

| Before              | After                                        |
|---------------------|----------------------------------------------|
| `Config::builder()` | `config_builder()`, or `new ConfigBuilder()` |
| `Config::default()` | `config()`, or `config_builder()->build()`   |

### 111) `flow-php/etl` - `Processor` extends `Describable`

| Before                   | After                                                                                               |
|--------------------------|-----------------------------------------------------------------------------------------------------|
| `Processor` implementors | also `describe(): ?Description` and `static fromDescription(array $data, Rebuild $rebuild): static` |

If the processor holds live state, return `null` from `describe()`.

### 112) `flow-php/etl` - every namespace is a layer root

`Flow\ETL\Flow` and the DSL function namespaces keep their names.

| Before                                                                                               | After                                                                                                                                     |
|------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------|
| `Flow\ETL\{DataFrame, Sink, Transformation, Transformations}`                                        | `Flow\ETL\API\{Branch, DataFrame, Transformation, Transformations}`                                                                       |
| `Flow\ETL\DataFrame\GroupedDataFrame`                                                                | `Flow\ETL\API\GroupedDataFrame`                                                                                                           |
| `Flow\ETL\Config\ConfigBuilder`                                                                      | `Flow\ETL\API\Config\ConfigBuilder`                                                                                                       |
| `Flow\ETL\Config\{Bucketing\BucketingConfigBuilder, Cache\CacheConfigBuilder, Join\HashJoinBuilder}` | `Flow\ETL\API\Config\{Bucketing\BucketingConfigBuilder, Cache\CacheConfigBuilder, Join\HashJoinBuilder}`                                  |
| `Flow\ETL\Config\Sort\{ExternalSortBuilder, MemorySortBuilder}`                                      | `Flow\ETL\API\Config\Sort\{ExternalSortBuilder, MemorySortBuilder}`                                                                       |
| `Flow\ETL\Sink\{Branched, Transactional, Transformed}`                                               | `Flow\ETL\API\{FilteredBranch, Transactional, TransformedBranch}`                                                                         |
| `Flow\ETL\Transformation\{AddRowIndex, BatchBy, BatchSize, Drop, Limit, MaskColumns, Select}`        | `Flow\ETL\API\Transformation\{AddRowIndex, BatchBy, BatchSize, Drop, Limit, MaskColumns, Select}`                                         |
| `Flow\ETL\Exception\*`                                                                               | `Flow\ETL\Foundation\Exception\*`                                                                                                         |
| `Flow\ETL\Exception\LimitReachedException`                                                           | `Flow\ETL\Operators\LimitReachedException`                                                                                                |
| `Flow\Floe\{FloeExtractor, FloeSourceFile, FloeFileBatches}`                                         | `Flow\ETL\IO\Floe\{FloeExtractor, FloeSourceFile, FloeFileBatches}`                                                                       |
| `Flow\Floe\{FloeLoader, FloeFileSink, FloeFileSinks}`                                                | `Flow\ETL\IO\Floe\{FloeLoader, FloeFileWriter, FloeFileWriterFactory}`                                                                    |
| `Flow\Calculator\Exception\Exception extends \Exception`                                             | extends `Flow\ETL\Foundation\Exception\Exception`: a `catch (Flow\ETL\Foundation\Exception\Exception)` now also catches calculator errors |

### 113) `flow-php/etl` - exception factories take names and type strings

| Before                                                                                      | After                                                                                                               |
|---------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------|
| `ColumnMismatchException::missingColumn($definition)`                                       | `ColumnMismatchException::missingColumn($definition->name())`                                                       |
| `ColumnMismatchException::retype($definition, $reason)`                                     | `ColumnMismatchException::retype($definition->name(), $definition->type()->toString(), $reason)`                    |
| `ColumnMismatchException::unsupportedType($definition, $reason)`                            | `ColumnMismatchException::unsupportedType($definition->name(), $definition->type()->toString(), $reason)`           |
| `ColumnMismatchException::untypedColumn($definition)`                                       | `ColumnMismatchException::untypedColumn($definition->name())`                                                       |
| `ColumnMismatchException::valueDoesNotMatch($definition, $value, $reason)`                  | `ColumnMismatchException::valueDoesNotMatch($definition->name(), $definition->type()->toString(), $value, $reason)` |
| `InferredSchemaException::columnsDiverge($source, $inferredFrom, $inferred, $columns, ...)` | `InferredSchemaException::columnsDiverge($source, $inferredFrom, $inferred->names(), $columns, ...)`                |
| `InferredSchemaException::filesDiverge($source, $readFrom, $validation)`                    | `InferredSchemaException::filesDiverge($source, $readFrom, $validation->toString())`                                |
| `new SchemaValidationException($expected, $given, $validation)`                             | `new SchemaValidationException($validation->toString())`                                                            |
| `SchemaValidationException::context()`, `given()`, `schema()`                               | removed: the message carries the mismatches                                                                         |

### 114) `flow-php/etl` - the user's side path is a `Branch`, a transaction group is not a branch

"Sink" is gone: `Sink` is `Branch`, the plan's write roots are `Writes`. `Transactional` no longer implements `Branch`
and has no `write()`: pass it to `DataFrame::write()`, `to_branch()` or `to_transformation()`. A `Transactional`
directly inside a `Transactional` is a `TypeError`. The named argument of `DataFrame::write()`, `to_branch()`,
`to_transformation()` is `$write`, of `to_dbal_transaction()` and `to_pgsql_transaction()` it is `$writes`.

| Before                                                                                       | After                                                                          |
|----------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------|
| `DataFrame::write(Loader\|Sink $sink)`                                                       | `DataFrame::write(Loader\|Branch\|Transactional $write)`                       |
| `Transactional::sinks()`                                                                     | `Transactional::writes()`                                                      |
| `Transactional::write(DataFrame $prefix)`                                                    | removed: `$frame->write($transactional)`                                       |
| `LogicalPlan::sinks()`, `LogicalPlan::sinksOnSpine()`, `Node\Outputs::sinks()`               | `writes()`, `writesOnSpine()`, `writes()`                                      |
| `SinkBranch::consumers()`                                                                    | `BranchPipeline::writes()`                                                     |
| `InvalidLogicException::sinkNotOnSpine()`, `sinkRootRewritten()`, `errorHandlerInsideSink()` | `writeNotOnSpine()`, `writeRootRewritten()`, `branchFrameNotBuiltFromPrefix()` |

### 115) `flow-php/etl` - a file loader writes through a `FileWriter`

One format written to one open stream is a `FileWriter`, made by a `FileWriterFactory`. The factory takes the schema of
the stream's first batch, so a writer opens its file at once. `ParquetFileWriter` conforms and writes on the PHP lane and
`RustParquetFileWriter` does the same on the native lane. Both implement `FileWriter`, so `ParquetOpenSink` and
`PhpParquetOpenSink` are gone. A batch without rows opens nothing, for every file loader (`to_floe()` wrote an empty file
before).

| Before                                                                       | After                                                                                                                    |
|------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------|
| `FileSink`, `FileSinks::open(DestinationStream $stream, Backend $backend)`   | `FileWriter`, `FileWriterFactory::writer(DestinationStream $stream, Schema $schema, Backend $backend)`                   |
| `CSVOpenSink`, `JsonOpenSink`, `TextOpenSink`, `XMLOpenSink`                 | `CSVFileWriter`, `JsonFileWriter`, `TextFileWriter`, `XMLFileWriter`                                                     |
| `FloeFileSink`, `FloeFileSinks`                                              | `FloeFileWriter`, `FloeFileWriterFactory`                                                                                |
| `ParquetFileSink`, `PhpParquetOpenSink`, `ParquetOpenSink`                   | `ParquetFileWriter`; `ParquetOpenSink` removed: both lanes implement `FileWriter`                                        |
| `ParquetFileSinks::conform()`, `ParquetFileSinks::writer(DestinationStream)` | removed: `ParquetFileWriterFactory::writer(DestinationStream, Schema, Backend)` returns the lane                         |
| `new RustParquetOpenSink(RustParquetFileWriter $writer)`                     | `new RustParquetFileWriter(ArrowRustParquetFileWriter $writer, ParquetSchemaConformance $conformance, Backend $backend)` |

### 116) `flow-php/filesystem` - `MemoryFilesystem::mv()` moves files and directories

| Before                                                                  | After                                                                                      |
|-------------------------------------------------------------------------|--------------------------------------------------------------------------------------------|
| `mv()` throws `RuntimeException` `Cannot move files around in memory`   | a file: its content moves to the target, which it replaces; the source is removed          |
| -                                                                       | a directory: every file under it moves to the same relative path under the target          |
| -                                                                       | a missing source returns `false`                                                           |
| `to_csv('memory://...')->saveMode(overwrite())` throws at the end       | the write replaces the file                                                                |

### 117) `flow-php/etl`, `flow-php/etl-adapter-*` - one file commit protocol; readers skip hidden paths

Every file write runs through one `FileCommit`. Files that replace others (Overwrite) and every parallel task's files
are written under a temporary output inside the destination, then moved into place; the other modes write in place.
The save mode decides once for the whole destination. Readers skip the temporary output of a running or crashed write
because it sits under a `.` or `_` path segment.

| Before                                                                                    | After                                                                                                                                                                                   |
|-------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `FilesSink`                                                                               | `FileCommit`: a task stages its files, the job commits them; a serial write is one task                                                                                                 |
| Overwrite wrote `._flow_php_tmp.<file>` beside each file                                  | every Overwrite and every parallel task writes under `<destination dir>/.flow-tmp-<run>-<hash>/` first; an aborted parallel write leaves the destination directory behind, empty        |
| Append beside an existing plain file: `<name>_<random>.<ext>`                             | `<name>-<run>.<ext>`                                                                                                                                                                    |
| Ignore / ExceptionIfExists on a partitioned plain destination: decided per partition file | decided once over `<dir>/**/<name>.<ext>`: an existing file in any partition skips (Ignore) or refuses (ExceptionIfExists) the whole write                                              |
| Overwrite removed `<name>*.<ext>` in each written partition (`data_backup.csv` too)       | removes only `<name>.<ext>` and `<name>-<run>.<ext>`, partitioned or not; files an older Append named `<name>_<random>.<ext>` stay                                                      |
| Overwrite removed the stale files, then moved the new ones                                | moves first, then removes: a failed commit leaves duplicates, never loses the old files                                                                                                 |
| a write without rows touched nothing                                                      | Overwrite empties an unpartitioned destination (a partitioned one keeps every partition); ExceptionIfExists over an existing destination throws; Ignore and Append write nothing        |
| `to_excel()->saveMode(ignore())` over an existing workbook overwrote it                   | keeps it                                                                                                                                                                                |
| file extractors and `files()` list every file the path matches                            | skip a file under a `.` or `_` path segment below the path's static part, except segments with `=`, `_metadata` and `_common_metadata`; a path you name yourself is read                |

### 118) `flow-php/etl` - a limit ends the stream without an exception

| Before                                                                                      | After                                                                                                                                       |
|---------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------|
| `Flow\ETL\Operators\LimitReachedException` thrown by `transform()` to end the stream        | removed: a `Limiting` transformer returns the trimmed batch and `reached()` returns `true`                                                  |
| -                                                                                           | `Flow\ETL\Operators\Transformer\Limiting extends Transformer { reached(): bool }`, implemented by `LimitTransformer` and `UntilTransformer` |
| `Limit reached` log attribute `limit` (`0` for `until()`)                                   | attribute `step`: the class of the transformer that reached it                                                                              |

### 119) `flow-php/etl` - `count(ref)` skips nulls everywhere, window frames reuse the aggregate functions

| Before                                                                         | After                                                                                                  |
|--------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------|
| group-by and pivot `count(ref('x'))` over `1, null, 3`: `3`                    | `2`, the same as the window `count(ref('x'))` and SQL. `count()` without a reference still counts rows |
| `FrameAccumulating::accumulator(): FrameAccumulator`                           | `FrameAccumulating::accumulator(): AggregatingFunction`, a fresh instance with no window               |
| `WindowFunction::values(Rows $partition)`                                      | `WindowFunction::values(Rows $partition, Backend $backend)`                                            |
| `FrameAccumulation::values($function, $partition)`                             | `FrameAccumulation::values($function, $partition, $backend)`                                           |
| `Window\FrameAccumulator`, `Window\Accumulator\{Count,Sum,Average}Accumulator` | removed: a frame feeds the function's own `aggregate()`                                                |

### 121) `flow-php/etl`, `flow-php/etl-adapter-*` - one mechanism per operation

| Before                                                                                                                                           | After                                                                                                                                                                                                                                                                                                  |
|--------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `FileBatches { batches(SourceFile $source, Schema $body, int $batchSize, Backend $backend, ReadWindow $window) }`                                | `FileBatches extends Describable { batches(FileRead $read, Criteria $criteria, SourceFile $source, Schema $body, Backend $backend, ReadWindow $window); close() }` - each format's batches holds its settings and describes itself; the batch size is `$read->batchSize`, the split `$criteria->split` |
| `ConfigBuilder::groupBy(GroupByAlgorithmBuilder $algorithm)`, `ConfigBuilder::repartition(RepartitionAlgorithmBuilder $algorithm)`               | `ConfigBuilder::groupBy(BucketingAlgorithmBuilder $algorithm)`, `ConfigBuilder::repartition(BucketingAlgorithmBuilder $algorithm)`                                                                                                                                                                     |
| `DataFrame::aggregate(array $aggregations, ?GroupByAlgorithmBuilder $algorithm)`, `DataFrame::groupBy(..., ?GroupByAlgorithmBuilder $algorithm)` | `?BucketingAlgorithmBuilder $algorithm`; `BucketingConfigBuilder` implements it                                                                                                                                                                                                                        |
| `to_branch($condition, $write)->withTransformation($transformation)`                                                                             | `to_branch($condition, to_transformation($transformation, $write))`                                                                                                                                                                                                                                    |
| `abstract class StopWhen { shouldStop(); and(); or() }` (`flow-php/etl-adapter-http`)                                                            | `interface StopWhen { shouldStop(DecodedResponse $response, PageState $state): bool }` - a custom condition `implements StopWhen`                                                                                                                                                                      |
| `http_stop_when_flag_false('has_more')->or(http_stop_when_max_pages(1000))`                                                                      | `http_stop_when_any_of(http_stop_when_flag_false('has_more'), http_stop_when_max_pages(1000))`                                                                                                                                                                                                         |
| `$a->and($b)` on a stop condition                                                                                                                | `http_stop_when_all_of($a, $b)`                                                                                                                                                                                                                                                                        |

### 122) `flow-php/etl` - `mod()` refuses a negative modulus on every `brick/math` version

| Before                                                                                              | After                                                                                   |
|-----------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------|
| `Calculator::modulus(7, -3)` on `brick/math` 0.12-0.14: `-2`                                        | `Flow\ETL\Foundation\Exception\InvalidArgumentException` `Modulus must not be negative` |
| `Calculator::modulus(7, -3)` on `brick/math` 0.15+: `Brick\Math\Exception\InvalidArgumentException` | `Flow\ETL\Foundation\Exception\InvalidArgumentException` `Modulus must not be negative` |
| `ref('a')->mod(lit(-3))`: a result or an `EvaluationException`, by `brick/math` version             | always an `EvaluationException` caused by `InvalidArgumentException`                    |

### 123) `flow-php/filesystem` and the filesystem bridges - `Filesystem::create()` and non-recursive `rm()`

| Before                                                   | After                                                                                    |
|----------------------------------------------------------|------------------------------------------------------------------------------------------|
| `interface Filesystem`                                   | `+ create(Path $path): DestinationStream` - custom implementations add it                |
| `Filesystem::rm(Path $path): bool`                       | `rm(Path $path, bool $recursive = true): bool` - custom implementations add the argument |
| `final class Flow\Filesystem\Exception\RuntimeException` | not final; `FileExistsException` extends it                                              |
| `SFTPFilesystem`, `StdOutFilesystem`                     | `create()` throws `RuntimeException`                                                     |

### 124) `flow-php/filesystem` - `NativeLocalFilesystem` takes `Permissions`

| Before                                                                                                                                                 | After                                                                                  |
|--------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------|
| `NativeLocalFilesystem::normalize()`: `{protocol}`                                                                                                     | `{protocol, permissions}`; `fromArray()` rejects a configuration without `permissions` |
| `AsyncAWS\Options::normalize()`, `Azure\Options::normalize()`, `SFTP\Options::normalize()`                                                             | `+ block_permissions`; `fromArray()` rejects a configuration without it                |
| `NativeLocalFilesystem::mv()` from a missing source raised an `E_WARNING`, created the target's parent directory, and removed an existing target first | returns `false` and changes nothing; an existing file target is replaced by `rename()` |

### 125) `flow-php/filesystem-azure-bridge` - a blob written in blocks is invisible until `close()`

| Before                                               | After                                                                 |
|------------------------------------------------------|-----------------------------------------------------------------------|
| the first filled block put an empty blob at the path | nothing is visible at the path until `close()` commits the block list |

### 126) `flow-php/etl` - extractors yield corrupted records, the run routes or refuses them

| Before                                                     | After                                                                                                                                                                                                                           |
|------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `Extractor::extract()`, `Reader::extract()` yielded `Rows` | they may also yield a `CorruptedRecordException`, one per record that cannot become a row; a direct caller routes or refuses it (`instanceof Rows`)                                                                             |
| a reader refused a corrupted record itself                 | the run routes it to `withCorruptedRecords($loader)`, loaded, closed and discarded like the run's other loaders; without a loader it fails the run, whatever the `ErrorHandler` - a corrupted record is not an extraction error |
| `ExtractionError::$extractor` was an `Extractor`           | `Extractor\|Reader`: a task's source is a `Reader`                                                                                                                                                                              |
| `StepRunner::steps()`, `run()`: `?Extractor $source`       | `Extractor\|Reader\|null $source`; a source implementing `CorruptedRecordsRouter` routes its corrupted records                                                                                                                  |

### 127) `flow-php/etl`, `flow-php/etl-adapter-csv`, `-json`, `-excel`, `-google-sheet` - a record that cannot become a row throws

| Before                                                                                                                | After                                                                                                                                                                                    |
|-----------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| a CSV line or a sheet row with fewer cells than the header was padded with nulls                                      | `CorruptedRecordException`; `->withMissingColumnsAsNull(true)` pads it (CSV, Google Sheet), Excel pads by default (`->withMissingColumnsAsNull(false)` refuses)                          |
| a CSV line or an Excel row with more cells than the header lost them                                                  | `CorruptedRecordException` for a non-empty extra cell; an empty one is dropped                                                                                                           |
| Google Sheet `->withDropExtraColumns(bool)`                                                                           | removed: a non-empty extra cell is a corrupted record; `->withMissingColumnsAsNull(bool)` pads a short row                                                                               |
| an empty CSV line was a row of nulls                                                                                  | skipped                                                                                                                                                                                  |
| a value that does not cast to the type `->withSchema()` declares threw `SchemaMismatchException`                      | `CorruptedRecordException` (route it with `->withCorruptedRecords($loader)`)                                                                                                             |
| a value or a JSON key a file shows only after the inference sample: `SchemaMismatchException`, or the key was dropped | `InferredSchemaException::outsideTheSample()`, naming the file, offset, line and sample size; `from_array()` and sequences throw `InferredSchemaException`                               |
| a `->withSchema()` column missing from a CSV, Excel or Google Sheet header was null on every row                      | `SchemaValidationException`                                                                                                                                                              |
| `new RecordDecoder($withHeader, $emptyToNull, $dropExtraColumns, $headers)`, `decode(list $batch): list`              | `new RecordDecoder($withHeader, $emptyToNull, $missingColumnsAsNull, $headers)`, `decode(array $cells): null\|array\|string` (null: header or empty record, string: why it is corrupted) |
| `CSVDecoder::decode(list<string> $lines)`                                                                             | `CSVDecoder::decode(CSVDecoder::cells(string $line))`, one record                                                                                                                        |
| `CSVOpenSource::batches($schema, $batchSize, $backend)`                                                               | `+ ?int $sampleSize = null`; `JsonOpenSource::batches()` also `+ ?Schema $known = null`; both yield `CorruptedRecordException\|Rows`                                                     |
| `to_date_time(null)` threw `InvalidArgumentException`                                                                 | returns null; a null format or time zone still throws                                                                                                                                    |

### 128) `flow-php/etl` - the Report's memory is named for what it measures, and every process has a peak

| Before                          | After                                                                                                            |
|---------------------------------|------------------------------------------------------------------------------------------------------------------|
| `$report->statistics()->memory` | `$report->statistics()->heap`: the parent's PHP heap plus the backend's bytes, sampled once per batch, as before |
| -                               | `$report->statistics()->processes`: the OS peak of the parent and of every worker, `workerPeak()`, `peakSum()`   |

### 129) `flow-php/etl` - `DataFrame` is immutable, every verb returns a new frame

| Before                                                              | After                                                                                                                    |
|---------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------|
| `$df->limit(10);` limits `$df`                                      | does nothing, keep the result: `$df = $df->limit(10);`                                                                   |
| `$df->onError($handler);` sets the handler on `$df`                 | `$df = $df->onError($handler);`                                                                                          |
| `$df->fetch(10)`, `display()`, `printRows()` limit `$df`            | leave `$df` unchanged                                                                                                    |
| `Branch::write(DataFrame $prefix): void`                            | `Branch::write(DataFrame $prefix): DataFrame`                                                                            |
| `new DataFrame(Extractor $extractor, Config\|FlowContext $context)` | `df($config)->read($extractor)`                                                                                          |
| `new DataFrame(Extractor $extractor, FlowContext $context)`         | `new DataFrame(new \Flow\ETL\Planning\Logical\Node\Read($extractor), new \Flow\ETL\Planning\Logical\Writes(), $context)` |

PHP 8.5 warns about a dropped result (`#[\NoDiscard]`); PHP 8.3 and 8.4 do not.

Before:

```php
public function write(DataFrame $prefix): void
{
    $prefix->filter($this->condition)->write($this->loader);
}

public function transform(DataFrame $dataFrame): DataFrame
{
    foreach ($this->columns as $column) {
        $dataFrame->withColumn($column, lit('***'));
    }

    return $dataFrame;
}
```

After:

```php
public function write(DataFrame $prefix): DataFrame
{
    return $prefix->filter($this->condition)->write($this->loader);
}

public function transform(DataFrame $dataFrame): DataFrame
{
    foreach ($this->columns as $column) {
        $dataFrame = $dataFrame->withColumn($column, lit('***'));
    }

    return $dataFrame;
}
```

---

### 130) `flow-php/telemetry`, `flow-php/postgresql`, `flow-php/symfony-http-foundation-bridge`,

`flow-php/monolog-telemetry-bridge`, `flow-php/psr3-telemetry-bridge` - types renamed for what they do

| Before                                                                                   | After                                                           |
|------------------------------------------------------------------------------------------|-----------------------------------------------------------------|
| `Flow\Bridge\Symfony\HttpFoundation\StreamClosure`                                       | `StreamCompletionListener`                                      |
| `Flow\Telemetry\Tracer\Sampler\AttributeMatchingSampler`, `attribute_matching_sampler()` | `AttributeFilteringSampler`, `attribute_filtering_sampler()`    |
| `Flow\Telemetry\Resource\Detector\ManualDetector`, `manual_detector()`                   | `StaticDetector`, `static_detector()`                           |
| `Flow\PostgreSql\AST\Transformers\PaginationModifier`, `PaginationConfig`                | `OffsetPaginationModifier`, `OffsetPaginationConfig`            |
| `Flow\Bridge\Monolog\Telemetry\ValueNormalizer`, `value_normalizer()`                    | `AttributeValueNormalizer`, `attribute_value_normalizer()`      |
| `Flow\Bridge\Psr3\Telemetry\ValueNormalizer`, `psr3_value_normalizer()`                  | `AttributeValueNormalizer`, `psr3_attribute_value_normalizer()` |

### 131) `flow-php/parquet` - both engines cap a row group at `ROW_GROUP_MAXIMUM_ROWS_COUNT` rows and write the same footer

| Before                                                                                                    | After                                                                                                                  |
|-----------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------|
| the PHP engine closed a row group on `ROW_GROUP_SIZE_BYTES` only; the arrow engine also at 1 048 576 rows | `Option::ROW_GROUP_MAXIMUM_ROWS_COUNT` (default `1_048_576`): both engines close a row group at that many rows exactly |
| the arrow engine wrote `created_by` `parquet-rs version 60.0.0` and an `ARROW:schema` key                 | `flow-php parquet version <version>` and no `ARROW:schema` key, as the PHP engine writes                               |

## Upgrading from 0.45.x to 0.46.x

### 1) `flow-php/etl-adapter-postgresql`, `flow-php/etl-adapter-doctrine` - keyset reads reject NULL keys up front

| Before                                                                                 | After                                                                                                         |
|----------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------|
| a NULL key: rows lost or `NULL value found in column ...`, depending on the batch size | one check query before the first page, then `RuntimeException` `Keyset pagination requires non-null keys ...` |
| no query before the first page                                                         | `SELECT 1 FROM (<query>) ... WHERE k IS NULL LIMIT 1` (DBAL: the builder plus `IS NULL` conditions)           |

Filter NULL keys out with `IS NOT NULL`, or page by non-null columns. Index the keys - on an unindexed key the check is
one full scan.

### 2) `flow-php/etl-adapter-postgresql`, `flow-php/etl-adapter-doctrine` - keyset reads reject duplicate keys

| Before                                                                                              | After                                                                                                                  |
|-----------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------|
| two rows with the same key: rows at a page boundary silently skipped                                | `RuntimeException` `Keyset pagination requires unique keys ...`                                                        |
| keys equal in the database but different in PHP (`'a'`/`'A'` under a `_ci` collation, `1.0`/`1.00`) | `RuntimeException` `Keyset pagination expected the next page to start at the key ...` when a page boundary splits them |

Add a unique column (for example the primary key) to the key set as its least significant key.

### 3) `flow-php/etl-adapter-postgresql`, `flow-php/etl-adapter-doctrine` - a short keyset page ends the read

| Before                                                | After                                                                                             |
|-------------------------------------------------------|---------------------------------------------------------------------------------------------------|
| each page `LIMIT n`; reading stopped on an empty page | each page `LIMIT n + 1` (the extra row is not yielded); a page of `n` rows or fewer ends the read |
| one trailing empty `SELECT`                           | none                                                                                              |

Nothing to change unless you count queries: a read sends the NULL check plus the page queries.

### 4) `flow-php/postgresql`, `flow-php/etl-adapter-postgresql` - keyset pagination rejects the query's own LIMIT/OFFSET

| Before                                                                               | After                                                             |
|--------------------------------------------------------------------------------------|-------------------------------------------------------------------|
| `sql_to_keyset_query('SELECT ... LIMIT 5', 10, ...)` - LIMIT silently replaced by 10 | `PaginationException` `Keyset pagination sets its own LIMIT; ...` |
| `from_pgsql_key_set($client, 'SELECT ... LIMIT 5', ...)`                             | same `PaginationException`, before any query                      |

Drop LIMIT/OFFSET from the query; use `withMaximum()` or `DataFrame::limit()`.

### 5) `flow-php/etl-adapter-doctrine` - `from_dbal_key_set_qb()` rejects `setMaxResults()` / `setFirstResult()`

| Before                                                          | After                                                                             |
|-----------------------------------------------------------------|-----------------------------------------------------------------------------------|
| `$qb->setMaxResults(5)` - silently overwritten by the page size | `InvalidArgumentException` `Keyset pagination sets its own LIMIT and OFFSET, ...` |
| `$qb->setFirstResult(5)` - applied to every page                | same `InvalidArgumentException`                                                   |

Remove them from the builder; use `withMaximum()` or `DataFrame::limit()`.

### 6) `flow-php/postgresql`, `flow-php/etl-adapter-postgresql` - a query's own ORDER BY must equal the keys

| Before                                                                                 | After                                                                                          |
|----------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------|
| `'SELECT ... ORDER BY name'` with key `id` - kept, pages wrong                         | `PaginationException` `Keyset pagination requires ORDER BY item #1 to be the key "id ASC" ...` |
| `ORDER BY id NULLS FIRST`, `ORDER BY 1`, `ORDER BY lower(name)`, `ORDER BY id USING <` | same `PaginationException`                                                                     |

Drop the ORDER BY (it is generated from the keys) or make it list exactly the keys, in order and direction.

### 7) `flow-php/postgresql` - `KeysetPaginationConfig` / `sql_to_keyset_query()` reject a NULL cursor value

| Before                                                                                 | After                                                                                |
|----------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------|
| `new KeysetPaginationConfig(10, $columns, [null, 7])` - predicate that matches nothing | `PaginationException` `Keyset cursor value #1 is NULL; key columns must be non-null` |

Page by non-null key columns.

### 8) `flow-php/postgresql` - same-direction composite keys emit a row comparison

| Before                                                       | After                         |
|--------------------------------------------------------------|-------------------------------|
| `created_at > $1 OR (created_at = $1 AND id > $2)`           | `(created_at, id) > ($1, $2)` |
| all DESC: `created_at < $1 OR (created_at = $1 AND id < $2)` | `(created_at, id) < ($1, $2)` |

Nothing to change; the row comparison is an index range bound. Mixed ASC/DESC keys keep the `OR` form. Update assertions
on the generated SQL.

### 9) `flow-php/etl-adapter-doctrine` - a non-scalar keyset value is rejected

| Before                                                                       | After                                                                                        |
|------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------|
| a key value that is not `string`/`int`/`float`/`bool` - bound as a parameter | `RuntimeException` `Unsupported value type "<type>" in column "<key>" for keyset pagination` |

Page by scalar columns.

### 10) `flow-php/etl-adapter-doctrine` - `pagination_key_set()` rejects zero keys

| Before                                                                                                             | After                                                                                       |
|--------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------|
| `pagination_key_set()` built; `from_dbal_key_set_qb()` threw `KeySet must contain at least one key for pagination` | `pagination_key_set()` throws `InvalidArgumentException` `KeySet requires at least one key` |

Pass at least one key.

### 11) `flow-php/filesystem`, `flow-php/etl` - `datetime` and `float` partition values keep their full value

| Before                                                                                                               | After                                    |
|----------------------------------------------------------------------------------------------------------------------|------------------------------------------|
| `partition_by('date')` over `datetime` `2026-10-02 13:00:00 UTC` - `date=2026-10-02`                                 | `date=2026-10-02T13%3A00%3A00%2B00%3A00` |
| reading it back as `datetime` (`writeColumns()` or `partition_types(date: type_datetime())`) - `2026-10-02 00:00:00` | `2026-10-02 13:00:00`                    |
| `partition_by('x')` over `float` `1.0E-9` - `x=0.000000`, read back `0.0`                                            | `x=1.0e-9`, read back `1.0E-9`           |
| `from_path_partitions()` / `files()` / an undeclared partition column - `'2026-10-02'`                               | `'2026-10-02T13:00:00+00:00'`            |

Directories written before keep reading as midnight. Writing into a dataset that has them adds a second directory per
day, and `overwrite()` replaces only the new one - rewrite the dataset, or partition by a `date` column to keep
`date=2026-10-02`:

```php
$df->withEntry('date', ref('date')->cast(type_date()))
```

### 12) `flow-php/etl-adapter-parquet`,

`flow-php/etl` - a partition column the file holds is typed from the file in a pushed filter

| Before                                                                                                                                         | After                                                                |
|------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------|
| `from_parquet()` / `from_floe()` over `writeColumns()` data - `partitionSchema()` types `date` `string`                                        | the file's type (`datetime`, `date`, `integer`, ...)                 |
| `filter(ref('date')->between(lit(DateTime), lit(DateTime)))` - throws `Boundary::compare requires value and bounds to be of a comparable type` | returns the matching rows, directories outside the range are skipped |
| same filter with `lit('2026-10-02')` bounds                                                                                                    | returns the same rows - see `### 14)`                                |

### 13) `flow-php/etl` - `RoundTripPrecision` moved to `flow-php/types`

| Before                                                 | After                                   |
|--------------------------------------------------------|-----------------------------------------|
| `Flow\ETL\Column\RoundTripPrecision`                   | `Flow\Types\Type\RoundTripPrecision`    |
| `force()` throws `Flow\ETL\Exception\RuntimeException` | `Flow\Types\Exception\RuntimeException` |

### 14) `flow-php/etl` - a string compared with a `datetime` / `date` is cast to it

| Before                                                                                      | After                                                            |
|---------------------------------------------------------------------------------------------|------------------------------------------------------------------|
| `ref('ts')->between(lit('2026-10-02'), lit('2026-10-03'))` - throws `Boundary::compare ...` | rows between those dates                                         |
| `ref('ts')->equals(lit('2026-10-02'))` - throws `Can't compare '(datetime == string)'`      | compares as `datetime`                                           |
| `ref('ts')->equals(ref('s'))`, `s` a string column - throws `Can't compare`                 | `s` cast per row; a non-date value throws `Cast function failed` |
| `ref('a')->between(lit(1), lit($dateTime))` - throws at evaluation                          | throws `Can't compare` when the plan binds                       |

`same()` / `notSame()` stay strict.

### 15) `flow-php/etl` - an expanding `withEntry()` emits batches of at most 1,000 rows

| Before                                            | After                                                                            |
|---------------------------------------------------|----------------------------------------------------------------------------------|
| one output batch per input batch, of any size     | batches of at most 1,000 rows; an input that expands to nothing: one empty batch |
| every input column copied into every expanded row | only the columns a later `select()` / `drop()` keeps                             |

### 16) `flow-php/etl` - `ScalarFunctionTransformer` refuses `array_expand()`, `NestedExpandTransformer` removed

```php
// Before - now throws "transform() cannot contain array_expand() ..."
$df->transform(new ScalarFunctionTransformer('item', array_expand(ref('items'))));

// After
$df->withEntry('item', array_expand(ref('items')));
```

### 17) `flow-php/etl` - a serialized `Rows` holds a list of frames

A `Rows` serialized by 0.45 (e.g. a `FilesystemCache` entry) is not read by 0.46: clear such caches after upgrading. A
batch
over 2 GiB of strings in one column is now split across frames instead of throwing `OffsetOverflow`.

---

## Upgrading from 0.44.x to 0.45.x

### 1) `flow-php/etl-adapter-postgresql` - a failed `from_pgsql_cursor()` read throws its own error and rolls back

| Before                                                                                                          | After                                        |
|-----------------------------------------------------------------------------------------------------------------|----------------------------------------------|
| `QueryException` `[25P02] Invalid transaction state. SQL: CLOSE flow_cursor_...`, real error in `getPrevious()` | the failing statement's own `QueryException` |
| the client left inside an aborted transaction - every later query fails with `25P02`                            | the extractor's own transaction rolled back  |
| a failure while reading rows (e.g. a row that does not match the schema) committed the transaction              | rolled back                                  |

### 2) `flow-php/etl-adapter-postgresql` - `from_pgsql_*()` read exactly one read-only `SELECT` or `VALUES` statement

| Query                                                                       | Before                                                                   | After                                    |
|-----------------------------------------------------------------------------|--------------------------------------------------------------------------|------------------------------------------|
| `INSERT ... RETURNING` through `from_pgsql_cursor()`                        | the PHP process crashes (segfault)                                       | `InvalidArgumentException`, nothing runs |
| `INSERT ... RETURNING` through `from_pgsql_key_set()`                       | the `INSERT` runs, then `QueryException` `08P01`                         | `InvalidArgumentException`, nothing runs |
| two statements through `from_pgsql_cursor()`                                | the second statement silently dropped                                    | `InvalidArgumentException`               |
| two statements through `from_pgsql_limit_offset()` / `from_pgsql_key_set()` | `QueryException` `42601`                                                 | `InvalidArgumentException`               |
| a data-modifying `WITH` through `from_pgsql_key_set()`                      | the write runs once per page - an `INSERT` twice, an `UPDATE` never ends | `InvalidArgumentException`, nothing runs |
| `SELECT ... INTO` through `from_pgsql_key_set()`                            | the table is created                                                     | `InvalidArgumentException`, nothing runs |
| either through `from_pgsql_cursor()` / `from_pgsql_limit_offset()`          | `QueryException` after a round trip                                      | `InvalidArgumentException`, nothing runs |

### 3) `flow-php/postgresql` - `declare_cursor()` over SQL takes exactly one `SELECT` or `VALUES`

| Before                                                                              | After                      |
|-------------------------------------------------------------------------------------|----------------------------|
| `declare_cursor('c', 'INSERT INTO t VALUES (1) RETURNING id')->toSql()` - segfault  | `InvalidArgumentException` |
| `declare_cursor('c', 'SELECT 1; SELECT 2')` - the second statement silently dropped | `InvalidArgumentException` |

### 4) `flow-php/postgresql` - `SelectStatement::hasIntoClause()` sees `SELECT ... INTO` in a `UNION` / `INTERSECT` /

`EXCEPT`

| `sql_parse($sql)->statements()->first()->hasIntoClause()`, `$sql` | Before  | After  |
|-------------------------------------------------------------------|---------|--------|
| `SELECT id INTO t FROM x UNION SELECT 1`                          | `false` | `true` |

### 5) `flow-php/postgresql` - `sql_to_*_query()` and the pagination modifiers take exactly one read-only `SELECT` or

`VALUES`

| Before                                                                               | After                       |
|--------------------------------------------------------------------------------------|-----------------------------|
| `sql_to_paginated_query('UPDATE t SET a = 1 RETURNING id', 10)` - returned unchanged | `InvalidStatementException` |
| `sql_to_limited_query('SELECT 1; SELECT 2', 10)` - every statement paginated         | `InvalidStatementException` |
| `sql_to_keyset_query()` over a data-modifying `WITH` - the write paginated           | `InvalidStatementException` |
| `sql_to_count_query('SELECT id INTO t FROM x')` - counted                            | `InvalidStatementException` |

Applies to `PaginationModifier`, `CountModifier` and `KeysetPaginationModifier` passed to `ParsedQuery::traverse()`.

### 6) `flow-php/etl-adapter-postgresql` - `from_pgsql_limit_offset()` requires the query's own `ORDER BY`

| Query                                            | Before                    | After                      |
|--------------------------------------------------|---------------------------|----------------------------|
| `SELECT * FROM (SELECT id FROM t ORDER BY id) s` | pages in no defined order | `InvalidArgumentException` |

### 7) `flow-php/etl` - `Pipeline\Optimizer` replaced by `Optimizer` + `Planner`, the `Pipeline` class removed

| Before                                                                                                                                                                                            | After                                                                                                                                                                                                                         |
|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `Config::optimizer()`, `new Config(..., Optimizer $optimizer, ...)`                                                                                                                               | `Config::optimizer(): Flow\ETL\Optimizer`, `new Config(..., Optimizer $optimizer, Executor $executor, ...)`; added `Config::planner()` / `Config::executor()`                                                                 |
| `config_builder()->optimizer($optimizer)`                                                                                                                                                         | `config_builder()->optimizer(Flow\ETL\Optimizer $optimizer)`, `->executor(Executor $executor)`                                                                                                                                |
| `Flow\ETL\Pipeline` (class, with `Pipeline::has()`), `Pipeline\Optimizer`, `Pipeline\Optimizer\Optimization`, `Pipeline\Optimizer\LimitOptimization`, `Pipeline\BoundPlan`, `Pipeline\PlanBinder` | removed - an `Optimizer\Rule` rewrites the `Plan\LogicalPlan`: `apply(LogicalPlan $plan, FlowContext $context): LogicalPlan`; drop one with `Optimizer::default()->without(Rule::class)`, add one with `->with(new MyRule())` |
| `new Pipeline\Optimizer(Optimization ...)`, `->disabled()`, `->optimizations()`                                                                                                                   | `new Optimizer(Rule ...)`, `new Optimizer()` (no rules - nothing is rewritten), `->rules()`; `Optimizer::default()` holds the built-in rules, `->without(Rule::class)` / `->with(new MyRule())` change them                   |
| -                                                                                                                                                                                                 | `Flow\ETL\Executor\PhysicalPlan` interface (`root(): Executor\Pipeline`, `schema(): Schema`): `Executor\Described` returns its schema, `Executor\Raw` throws its `SchemaNotDerivableException`                                |
| `Segments::replaceExtractor()` / `has()` / `current()` / `segmentFor()`, `Segment::withExtractor()` / `has()` / `contains()`                                                                      | removed - `Segments::extractor()` / `Segment::extractor()`                                                                                                                                                                    |
| `new DataFrame(Pipeline $pipeline, $context)`                                                                                                                                                     | `new DataFrame(Extractor $extractor, Config\|FlowContext $context)`                                                                                                                                                           |
| `new HashJoinProcessor(DataFrame $right, ...)`, `new CrossJoinRowsTransformer(DataFrame $frame, ...)`                                                                                             | take the right side's `Executor\PhysicalPlan` and an `Executor`                                                                                                                                                               |
| `JoinSteps::of(DataFrame $right, ...)`                                                                                                                                                            | `JoinSteps::of(PhysicalPlan $right, Expression $expression, Join $type, Config $config, ?JoinAlgorithmBuilder $algorithm)`                                                                                                    |
| `InvalidLogicException::cyclicPlanOnDescribe()`                                                                                                                                                   | removed                                                                                                                                                                                                                       |
| telemetry debug-log field `optimizers`                                                                                                                                                            | `optimizer_rules`                                                                                                                                                                                                             |

### 8) `flow-php/etl` - `extract()` receives the pushed limit and path filter, `LimitPushDown` and

`withPathFilter()` removed

| Before                                                                                 | After                                                                                                                         |
|----------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------|
| `Extractor::extract(FlowContext $context): Generator`                                  | `extract(FlowContext $context, ?int $limit = null): Generator` - every implementation adds the parameter, and may ignore it   |
| `Extractor\LimitPushDown` + `PushesLimit` trait, `pushLimit()` / `pushedLimit()`       | removed - read `$limit` in `extract()`                                                                                        |
| `FileExtractor::withPathFilter($filter)` / `filter()`, `PathFiltering` held the filter | removed - `FileExtractor::extract(FlowContext $context, ?int $limit = null, Filter $pathFilter = new OnlyFiles()): Generator` |
| `interface FileExtractor`                                                              | gains `partitionSchema(): Schema` - every implementor must add it                                                             |
| `interface Function\FunctionTree`                                                      | gains `deterministic(): bool` - implementors using neither `ScalarFunctionChain` nor `ResolvesFromChildren` must add it       |

### 9) `flow-php/etl` - `filterPartitions()` removed, the optimizer pushes `filter()` into the source

| Before                                                                                    | After                                                                                                                                                                    |
|-------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `$df->filterPartitions(ref('date')->equals(lit('2024-01-01')))`                           | `$df->filter(ref('date')->equals(lit('2024-01-01')))` - pushed into the source as a path filter, only matching partitions are read                                       |
| `$df->filterPartitions(new OnlyFiles())` (`Path\Filter` form)                             | removed, no replacement                                                                                                                                                  |
| `$df->read(files($glob))->filterPartitions(...)`, same over `from_path_partitions($glob)` | `filter()` on the partition column - both sources now emit one string column per `key=value` directory (next to `partitions`), and the filter is pushed into the listing |
| a partition-predicate error was thrown by the verb                                        | thrown at plan / bind time - an optimizer rule can fail a plan                                                                                                           |
| `->write($sink)->filter(...)` narrowed the earlier sink                                   | a filter is pushed only when every root reaches the source through it - the sink gets every row                                                                          |

### 10) `flow-php/etl` - frames are snapshotted when embedded, fewer limits are pushed

| Before                                                                                                                                         | After                                                                                                                                                  |
|------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------|
| `$left->join($right, ...); $right->select('id');` - the `select()` was part of the join's right side                                           | ignored - a frame is snapshotted at `join()` / `from_data_frame()` time                                                                                |
| `$frame->join(df()->read(from_data_frame($frame)), ...)` threw `InvalidLogicException` "Cannot describe this plan:"                            | runs as a self-join against the snapshot; a run-time cycle still throws `cyclicPlanOnRun()`                                                            |
| `->rows($t)->limit(n)`, `->transform($t)->limit(n)`, `->void()->limit(n)` pushed the limit into the source for seven allow-listed transformers | not pushed - `->withEntry(...)->limit(n)` still pushes; with several sinks the widest limit is pushed, a `limit(3)` inside a sink limits the whole run |
| -                                                                                                                                              | `schema()` followed by a terminal verb plans twice, `Extractor::schema()` is called once per planning - memoise a sniffing extractor                   |
| `add_row_index()` on a frame run twice continued counting (`[0,1,2]` then `[3,4,5]`)                                                           | starts again on every run; a `Transformer` keeping state between batches implements `Flow\ETL\Transformer\Stateful` (`fresh()`) to do the same         |
| a joined frame ran with its own config and `onError()`                                                                                         | runs with the outer frame's config, optimizer and error handler; its own `onError()` is ignored - a `from_data_frame()` frame still runs with its own  |
| each joined frame opened its own `DataFrame` span                                                                                              | one `DataFrame` span per run; a `from_data_frame()` frame still opens its own                                                                          |

### 11) `flow-php/etl` - one balanced `DataFrame` telemetry span per run

| Before                                                                                                  | After                                                                |
|---------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------|
| the `DataFrame` span started when the frame was built                                                   | starts when the plan executes - `Executor::execute()` owns both ends |
| a planning failure under `schema()` emitted no span                                                     | `dataFrameStarted` + `dataFrameFailed` for every verb                |
| an abandoned `get*()` generator left its span open                                                      | closes it                                                            |
| a failure inside a verb's own loop body (a `forEach` callback, the formatter) closed the span as failed | closed as completed                                                  |
| a `from_data_frame()` frame emitted no span                                                             | one balanced span per run                                            |
| `to_dbal_transaction()` / `to_pgsql_transaction()` emitted their own span                               | no span                                                              |

### 12) `flow-php/etl` - `to_branch()` / `to_transformation()` return a `Sink`, the wrapper loaders are removed

| Before                                                                                                                                                   | After                                                                                                                                                                                                                                             |
|----------------------------------------------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `to_branch($condition, $loader, $transformation)` returned `BranchingLoader`                                                                             | `to_branch($condition, $sink)->withTransformation($transformation)` returns `Sink\Branched`                                                                                                                                                       |
| `to_transformation($transformation, $loader)` returned `TransformerLoader`                                                                               | returns `Sink\Transformed`; both take a `Loader` or another `Sink`, neither is accepted where a `Loader` is required                                                                                                                              |
| `Loader\OverridingLoader`, `Loader\LoaderTree`, `Loader\TransformerLoader`, `Loader\BranchingLoader`                                                     | removed - `Flow\ETL\Sink` interface (`write(DataFrame $prefix): void`)                                                                                                                                                                            |
| a `Transformation` calling `$df->fetch()` / `count()` / `schema()` / `run()` inside `to_transformation()` or `to_branch()` threw `InvalidLogicException` | executes the prefix plan, as `from_data_frame()` executes a frame                                                                                                                                                                                 |
| `onError()` inside a sink's `Transformation`                                                                                                             | throws `InvalidLogicException` at `write()` - set it on the frame                                                                                                                                                                                 |
| an unresolved column or a non-boolean condition in a sink failed at the first batch                                                                      | fails at plan time; one undescribable sink operation makes the whole plan run raw; `schema()` still describes the frame's own rows                                                                                                                |
| a transformer failing inside a non-transactional sink was a LOADING error                                                                                | a TRANSFORMATION error offered to `onTransformation()` once: `SkipRows` / `IgnoreError` drop the batch (after a blocking operation, the whole buffered batch); a `limit()` completing the sink mid-load is a LOADING error on `Executor\SinkFeed` |
| `write_with_retries($loader)` around `to_transformation(...)` or `to_branch(...)` threw at the first `load()`                                            | removed with the retry surface, see 14)                                                                                                                                                                                                           |
| telemetry `flow.etl.loading.rows` counted the rows offered to the branch                                                                                 | counts the rows the branch's loader writes; an empty batch never reaches the loader                                                                                                                                                               |
| `SkipRows`: a drain failure of `to_transformation()` / `to_branch()` at `closure()` was rethrown                                                         | offered to `onTransformation()` once - `SkipRows` drops the buffered batch                                                                                                                                                                        |
| `$df->load(loader: $l)`, `$df->write(loader: $l)`, `to_transformation($t, loader: $l)`, `to_branch($c, loader: $l)`                                      | the named argument is `sink:`                                                                                                                                                                                                                     |

### 13) `flow-php/etl-adapter-doctrine`, `-postgresql` - `to_dbal_transaction()` /

`to_pgsql_transaction()` are transaction roots

| Before                                                                                                                      | After                                                                                                                                                                                                                                            |
|-----------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `to_dbal_transaction($connection, Loader ...$loaders): TransactionalDbalLoader`                                             | `to_dbal_transaction($connection, Loader\|Sink ...$sinks): Transactional` - `Sink\Transactional` over a `DbalTransaction`; `TransactionalDbalLoader` removed                                                                                     |
| `to_pgsql_transaction($client, Loader ...$loaders): TransactionalPostgreSqlLoader`                                          | `to_pgsql_transaction($client, Loader\|Sink ...$sinks): Transactional` - over a `PostgreSqlTransaction`; `TransactionalPostgreSqlLoader` removed                                                                                                 |
| `to_dbal_transaction(...)->withIsolationLevel($level)`                                                                      | `new Transactional(DbalTransaction::fromConnection($connection)->withIsolationLevel($level), ...$sinks)` - returns a new instance; the same on `PostgreSqlTransaction`, which runs `SET TRANSACTION` after `BEGIN` and rolls back a failed `SET` |
| a failure a non-throwing `onError()` handler suppressed still committed the batch                                           | the batch (or the closure drain) rolls back and is never re-delivered; the handler only decides whether the run continues - the failing sink restarts on the next batch, its buffered rows lost                                                  |
| `LoadingError::$loader` for a `begin()` / `commit()` failure was `TransactionalDbalLoader`                                  | `Executor\TransactionalSinks`; for a sink failure, the sink's own loader when bare, `Executor\SinkFeed` otherwise; the exception is always the cause, never `TransactionRolledBack`                                                              |
| `withIsolationLevel()` on `to_dbal_transaction()` / `to_pgsql_transaction()` applied to every transaction the wrapper opens | set on `DbalTransaction` / `PostgreSqlTransaction`; applies to every transaction opened                                                                                                                                                          |

### 14) `flow-php/etl` - `write_with_retries()` and the retry surface are removed

| Before                                                                         | After                                           |
|--------------------------------------------------------------------------------|-------------------------------------------------|
| `write_with_retries($loader, retry_any_throwable(3))`                          | removed, no replacement                         |
| `Loader\RetryLoader`, `FailedRetryException`, `ReplayAware`, `Flow\ETL\Time\*` | removed                                         |
| `retry_*()`, `delay_*()`, `duration_*()` DSL functions                         | removed                                         |
| `$df->write(write_with_retries(to_csv($p)->saveMode(overwrite())))`            | `$df->write(to_csv($p)->saveMode(overwrite()))` |

### 15) `flow-php/etl-adapter-doctrine` - a DBAL read the driver cannot describe keeps the database error

| Before                                                                                                                               | After                                                                                                                                                                                                                                                                      |
|--------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| a query the driver cannot describe (multi-statement, data-modifying CTE, `INSERT ... RETURNING`) threw `SchemaNotDerivableException` | a missing table or column throws what the read throws (`TableNotFoundException`, `InvalidFieldNameException`, on SQLite `DriverException`); any other query the driver refuses to describe throws `SchemaNotDerivableException` with the DBAL exception as `getPrevious()` |

### 16) `flow-php/etl-adapter-postgresql` - a PostgreSQL read keeps the database error, reads that write are refused

| Before                                                                                                                                  | After                                                                                                                                                                                                                                 |
|-----------------------------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| a query that cannot run as a subquery (multi-statement, data-modifying CTE, `INSERT ... RETURNING`) threw `SchemaNotDerivableException` | a missing table, column, function, type or privilege throws PostgreSQL's `QueryException`, as the read does; a query that writes (a data-modifying `WITH`, `SELECT ... INTO`) throws `InvalidArgumentException` before any query runs |

### 17) `flow-php/flow-php-ext` - the `flow_php` extension is versioned with `flow-php/etl`

| Before                                                      | After                                       |
|-------------------------------------------------------------|---------------------------------------------|
| `flow_php` extension 0.3.0, required by this `flow-php/etl` | same version as this `flow-php/etl` release |

### 18) `flow-php/etl` - `DataFrame` has no `@internal` methods

| Before                                                                                                                          | After                                                                                                                                    |
|---------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------|
| `DataFrame::extractor()` (`@internal`)                                                                                          | removed - `(new Repeatability())->ofPlan($dataFrame->explain()->logical)` answers whether every source the frame reads can be read twice |
| `DataFrame::registerGroupBy($groupBy, $algorithm)` (`@internal`)                                                                | removed - `groupBy($entries, $algorithm)->aggregate(...)`                                                                                |
| `new GroupedDataFrame($df, $groupBy, $algorithm)`                                                                               | `new GroupedDataFrame($df, $input, $groupBy)` - built by `DataFrame::groupBy()`                                                          |
| `SchemaNotDerivableException::nonRewindable($extractorClass)`                                                                   | `nonRewindable()`; the message no longer names the extractor                                                                             |
| `discover_pivot_values()` over a frame joining a source that cannot be read twice - pivot silently null                         | throws `SchemaNotDerivableException`                                                                                                     |
| `discover_pivot_values()` over `from_data_frame()` of a repeatable frame - refused                                              | allowed                                                                                                                                  |
| a `Transformation` writing inside a sink - its write ran after the sink's own write                                             | runs before it; a transaction's writes run in `write()` call order                                                                       |
| a `Transformation` inside a sink returning another frame - failed at `run()` "A sink root shares no node with the plan"         | throws at `write()`                                                                                                                      |
| `$frame->onError()` after `join($frame)` / `from_data_frame($frame)` - reached the embedded frame                               | ignored - a joined frame runs with the outer frame's handler, a `from_data_frame()` frame with the one it had when embedded              |
| an embedded frame sharing the outer `FlowContext` left a `DataFrame` span open                                                  | balanced spans                                                                                                                           |
| `discover_pivot_values()` over a frame with `write()` before `groupBy()` - the sink received every row twice (discovery ran it) | discovery reads only the rows feeding the pivot, sinks run once                                                                          |

### 19) `flow-php/etl` - `Pipeline\` and `Execution\` merged into `Executor\`, `BoundStep` moved to the root

| Before                                   | After                                   |
|------------------------------------------|-----------------------------------------|
| `Flow\ETL\Pipeline\BoundStep`            | `Flow\ETL\BoundStep`                    |
| `Flow\ETL\Pipeline\Segments`             | `Flow\ETL\Executor\Segments`            |
| `Flow\ETL\Pipeline\Segment`              | `Flow\ETL\Executor\Segment`             |
| `Flow\ETL\Execution\StatisticsCollector` | `Flow\ETL\Executor\StatisticsCollector` |

### 20) `flow-php/etl` - the trigger builds the plan's consumer, `run()` takes no callback

| Before                                                                               | After                                                                                        |
|--------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------|
| `$frame->run(fn (Rows $rows) => ...)`                                                | `$frame->forEach(fn (Rows $rows) => ...)`; `run(bool\|Analyze $analyze = false)`             |
| `$frame->explain()` on a frame ending in `write()` - printed a `Result` nobody reads | prints the writes; `explain(Trigger::run)` for the plan `run()` executes                     |
| `Plan\LogicalPlan::of()` / `cursor()` / `withCursor()` / `withSinks()`               | removed - `Trigger::rows->plan($root, $sinks)`, `LogicalPlan::$root`, `LogicalPlan::spine()` |
| `new Plan\Node\Outputs(Result $result, Sinks $sinks)`                                | `new Plan\Node\Outputs(Result\|Write\|Transaction ...$consumers)` - two or more              |
| `InvalidLogicException::resultRewritten()`                                           | `consumerRewritten()`; a `Transaction` as the first consumer is refused                      |

### 21) `flow-php/etl` - explain layouts render a described entry, a joined frame is drawn without its `Result`

`Plan\Explain\Outline` now applies `Details` while it builds, so a layout prints what the entry carries instead of
asking for it. A joined frame's `Result` is no longer drawn: the join reads its right side directly, and the numbers
after it shift by one. `Stage::physical` is new - see the core documentation.

| Before                                                  | After                                                                             |
|---------------------------------------------------------|-----------------------------------------------------------------------------------|
| `new Explain\Entry($node, $number, $shared, $children)` | `new Explain\Entry($source, $name, $lines, $number, $shared, $children, $suffix)` |
| `$entry->node`                                          | `$entry->source`                                                                  |
| `$entry->title($name)`                                  | `$entry->title()` - the name is the entry's own                                   |
| `new Explain\TreeLayout($details, declarations: true)`  | `new Explain\Outline(declarations: true)`, the layout takes no arguments          |
| `new Explain\BoxLayout($details)`                       | `new Explain\BoxLayout()`                                                         |

### 22) `flow-php/etl-adapter-csv` - an escaped or bare enclosure no longer merges a record with the next line

| Input (3 lines)              | Before                                 | After                                             |
|------------------------------|----------------------------------------|---------------------------------------------------|
| `a,b` / `"x\"y",1` / `"p",2` | 1 row: `{"a":"x\\\"y","b":"1\n\"p\""}` | 2 rows: `{"a":"x\\\"y","b":1}`, `{"a":"p","b":2}` |
| `a,b` / `x"y,1` / `"p",2`    | 1 row: `{"a":"x\"y","b":"1\n\"p\""}`   | 2 rows: `{"a":"x\"y","b":1}`, `{"a":"p","b":2}`   |

### 23) `flow-php/etl-adapter-csv` - `CSVOpenSource` is an interface, `CSVSourceOpener` removed,

`CSVLineReader` takes the separator and escape

| Before                                                                                     | After                                                                                                                                                                                             |
|--------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `new CSVLineReader($enclosure, $charactersReadInLine, $removeBOM)`                         | `new CSVLineReader($enclosure, $separator, $escape, $charactersReadInLine, $removeBOM)`                                                                                                           |
| `final class CSVOpenSource`, `new CSVOpenSource($stream, $dialect, $encoder, $lineReader)` | `interface CSVOpenSource`, implemented by `PhpCSVOpenSource` (`new PhpCSVOpenSource($stream, $decoder, $lineReader)`, a `CSVDecoder`), `RustCSVOpenSource` (flow_php) and `AdaptiveCSVOpenSource` |
| `$open->stream`, `$open->dialect`, `$open->encoder`, `$open->lineReader`                   | removed                                                                                                                                                                                           |
| `(new CSVSourceOpener($filesystem, $options))->open($source)`                              | `new AdaptiveCSVOpenSource($filesystem, $source, $options)`                                                                                                                                       |
| `new CSVFileReader($opener, $sources)`                                                     | `new CSVFileReader($filesystem, $options, $sources)`                                                                                                                                              |
| `CSVFileReader::samples()` yields `Generator`s                                             | yields `CSVFileSample` (`IteratorAggregate`); `$unit->getIterator()` for the generator                                                                                                            |
| `CSVFileReader::sample($source)`                                                           | `new CSVFileSample($filesystem, $options, $source)`                                                                                                                                               |

### 24) `flow-php/etl` - Floe footer carries a statistics block, existing `.floe` files must be rewritten

| Before                                          | After                                                           |
|-------------------------------------------------|-----------------------------------------------------------------|
| footer key `totalRows`                          | `statistics.rows`                                               |
| -                                               | `statistics.byteSize` - uncompressed data bytes                 |
| footer/section parsers rejected any unknown key | unknown keys ignored, so later additions are not a break        |
| reading a file written by 0.44.x                | `Floe footer is malformed: ... "statistics"` - rewrite the file |

Header version stays `0x02`: the change is in the footer, not the frame layout. Files written by 0.44.x
cannot be read. Regenerate them from their source, or export them with 0.44.x to another format before
upgrading.

### 25) `flow-php/etl` - every `Extractor` declares `statistics()`

| Before                                                  | After                                                                                           |
|---------------------------------------------------------|-------------------------------------------------------------------------------------------------|
| `Extractor`: `extract()`, `schema()`, `withSchema()`    | `+ statistics(): Statistics` - a custom extractor that knows nothing returns `new Statistics()` |
| `SequenceGenerator`: `generate()`                       | `+ rows(): Cardinality` - how many items `generate()` yields                                    |
| `SelfDescribingFile`: `close()`, `schema()`, `source()` | `+ statistics(): Statistics` - that one file's rows and bytes from its own metadata             |
| `CacheIndex` rows: `key`                                | `key`, `rows` - an index written by 0.44.x reads fine, its row count is unknown                 |
| `JsonFileReader::samples()` yields `Generator`s         | yields `JsonFileSample` (`IteratorAggregate`)                                                   |
| `WorkbookSampler::samples()` yields `Generator`s        | yields `WorkbookSheetSample` (`IteratorAggregate`)                                              |

```php
final class MyExtractor implements Extractor
{
    public function statistics(): Statistics
    {
        return new Statistics();
    }
}
```

### 26) `flow-php/etl` - `Report` takes the source statistics of the run

| Before                                           | After                                                                                                |
|--------------------------------------------------|------------------------------------------------------------------------------------------------------|
| `new Report(?Schema $schema, Statistics $stats)` | `new Report(?Schema $schema, Statistics $stats, ?array $sources)` - `null` unless analyzed with them |

### 27) `flow-php/postgresql` - `Traverser` visits every node

| Before                                                                                            | After                                           |
|---------------------------------------------------------------------------------------------------|-------------------------------------------------|
| `sql_query_tables('CREATE TABLE x AS SELECT * FROM t')` - `[]`                                    | `[t, x]`                                        |
| `sql_query_tables('CREATE VIEW v AS SELECT a FROM src')` - `[]`                                   | `[v, src]`                                      |
| `sql_query_tables('SELECT a INTO new_t FROM src')` - `[src]`                                      | `[new_t, src]`                                  |
| `sql_query_columns('... ON CONFLICT (name) DO UPDATE SET name = excluded.name')` - `[]`           | `[excluded.name]`                               |
| `OrderBy` of `SELECT a, row_number() OVER (ORDER BY b) FROM t ORDER BY a` - 1 clause              | 2 clauses - window `ORDER BY` included          |
| `sql_query_tables('SELECT (SELECT x FROM a) FROM b')` - `[b, a]`                                  | `[a, b]` - descriptor (PostgreSQL walker) order |
| `sql_query_depth()`: `EXPLAIN SELECT 1` 0, `CREATE VIEW v AS SELECT 1` 0, window subquery 1       | 1, 1, 2                                         |
| `sql_to_keyset_query()` cursor on `... WHERE $1 IN (SELECT ...)` - `$1`, clashing with the user's | `$2`                                            |
| `TypeCastStripper` left casts under a `SubLink` test, a window and `COLLATE`                      | stripped                                        |

### 28) `flow-php/postgresql` - traversal contract: messages, depth, replacement, `REMOVE_NODE`

| Before                                                            | After                                                                                    |
|-------------------------------------------------------------------|------------------------------------------------------------------------------------------|
| `ModificationContext::ancestors()` / `parent()` - `Node` wrappers | the real messages (`SelectStmt`, `RangeSubselect`, ...), no `Node` wrappers              |
| depth of a CTE body - 3                                           | 4 - `WithClause`, `WindowDef`, `IntoClause`, `OnConflictClause` are levels too           |
| a replacement returned below the top-level statement - ignored    | written into its slot                                                                    |
| a replacement of the wrong class - ignored                        | `ParserException`                                                                        |
| `NodeVisitor::REMOVE_NODE` - declared, never honoured             | removed; `NodeModifier::REMOVE_NODE` removes a node from a list, throws on a single slot |

### 29) `flow-php/postgresql` - `EXPLAIN` wraps every explainable statement, ANALYZE is rolled back

| Before                                                                                                | After                                                                                                            |
|-------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------|
| `ExplainModifier::nodeClasses()` - `[SelectStmt::class]`                                              | `[ParseResult::class]`                                                                                           |
| INSERT/UPDATE/DELETE/MERGE/CTAS/EXECUTE/DECLARE - returned unwrapped                                  | wrapped in `EXPLAIN`                                                                                             |
| `sql_to_explain('CREATE TABLE x (a int)')`, `sql_to_explain('EXPLAIN SELECT 1')` - returned unwrapped | `InvalidStatementException`                                                                                      |
| `$client->explain('INSERT ...')` - the `INSERT` ran and committed                                     | EXPLAIN; with ANALYZE inside a transaction (savepoint when one is open) that is always rolled back               |
| `traverse(new PaginationModifier(...), new ExplainModifier(...))` - both applied                      | the pagination is dropped (or the traversal throws) - call `traverse()` again with `ExplainModifier` alone, last |

### 30) `flow-php/postgresql` - keyset pagination wraps `UNION` / `INTERSECT` / `EXCEPT`

| Before                                                                                                                        | After                                                                  |
|-------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------|
| `sql_to_keyset_query('SELECT id FROM t UNION SELECT id FROM u ORDER BY id', ...)` - cursor dropped, page 2 fails with `08P01` | `SELECT * FROM (...) _keyset_subq WHERE id > $1 ORDER BY id LIMIT ...` |
| qualified keyset column (`t.id`) on a set operation - `42P01` at run time                                                     | `PaginationException`                                                  |

### 31) `flow-php/postgresql` - schema keeps the declared expression text, compares normalised keys

| Before                                                           | After                                                                              |
|------------------------------------------------------------------|------------------------------------------------------------------------------------|
| `Column::$generationExpression` - normalised (`lower(i)`)        | declared/catalog text (`lower(i::text)`); compare with `generationExpressionKey()` |
| `CheckConstraint::$expression` - normalised                      | declared/catalog text; `expressionKey()`                                           |
| `Index::$predicate` - normalised                                 | declared/catalog text; `predicateKey()`                                            |
| `Trigger::$whenCondition` - compared verbatim                    | declared/catalog text; compared by `whenConditionKey()`                            |
| `ColumnDefault` EXPRESSION `literal` - normalised                | declared/catalog text; `equals()` normalises                                       |
| `CheckDefinitionParser::parse()` - normalised expression         | the catalog text, still validated                                                  |
| `ExpressionParser::normalizeNode(Node)`                          | removed - `deparseNode(Node)` keeps the text, `normalize(string)` strips casts     |
| DDL emitted the normalised text - `lower(i)` failed with `42883` | DDL emits the declared expression                                                  |

### 32) `flow-php/postgresql` - schema DDL emits index `WHERE` and trigger `WHEN`

| Before                                                         | After                                                                           |
|----------------------------------------------------------------|---------------------------------------------------------------------------------|
| `CREATE UNIQUE INDEX t_email_live ON s.t (email)`              | `CREATE UNIQUE INDEX t_email_live ON s.t (email) WHERE deleted_at IS NULL`      |
| `CREATE TRIGGER t_trg ... FOR EACH ROW EXECUTE FUNCTION f()`   | `CREATE TRIGGER t_trg ... FOR EACH ROW WHEN (new.i > 0) EXECUTE FUNCTION s.f()` |
| an unqualified trigger function resolved through `search_path` | resolved to the table's schema                                                  |
| introspected `Trigger::$functionName` - `name`                 | `schema.name`; new `Trigger::withFunctionSchema()`                              |
| a declared `'s.f'` always drifted against the catalog          | no drift                                                                        |

### 33) `flow-php/postgresql` - a failed `SAVEPOINT` leaves the outer transaction open

| Before                                                                                      | After                                                                    |
|---------------------------------------------------------------------------------------------|--------------------------------------------------------------------------|
| nesting level reset to 0, the caller's `rollBack()` throws, the connection stays in `25P02` | nesting level kept; the caller's `rollBack()` ends the outer transaction |

### 34) `flow-php/postgresql` - `sql_query_tables()` skips CTE references, reports `DROP` / `COMMENT ON` targets

| Before                                                                               | After      |
|--------------------------------------------------------------------------------------|------------|
| `sql_query_tables('WITH c AS (SELECT * FROM users) SELECT * FROM c')` - `[c, users]` | `[users]`  |
| `sql_query_tables('DROP TABLE a, s.b')` - `[]`                                       | `[a, s.b]` |
| `sql_query_tables("COMMENT ON COLUMN s.t.c IS 'x'")` - `[]`                          | `[s.t]`    |

### 35) `flow-php/etl` - `AdaptiveFloeEncoder` removed, Floe engines build a `FloeEncoder`

| Before                                   | After                                                                                                     |
|------------------------------------------|-----------------------------------------------------------------------------------------------------------|
| `Flow\Floe\AdaptiveFloeEncoder`          | removed                                                                                                   |
| `FloeEngine::encoder(): Encoder<string>` | `FloeEngine::encoder(): Flow\Floe\FloeEncoder` (`Encoder<string>` plus `decodeRows()` / `encodeFrames()`) |

### 36) `flow-php/etl` - `UnionDefinition` and `union_schema()` removed

| Before                                                                | After                                                     |
|-----------------------------------------------------------------------|-----------------------------------------------------------|
| `union_schema('a', type_union(type_string(), type_null()))`           | `str_schema('a', nullable: true)`                         |
| `new UnionDefinition('a', type_union(type_integer(), type_string()))` | `str_schema('a')`, or `json_schema('a')` for dynamic data |

Removed with them: `UnionMembers`, `UnionTypeNormalizer`, `Row\EntryTypeResolver`, `TypeProjection::union()`.
`TypeProjection` now takes the column's `Reference`; `TypeFloor` no longer takes a `TypeProjection`.

### 37) `flow-php/etl` - a union inside a list, map or structure column type

| Column type                                                              | Before                                                     | After                                                            |
|--------------------------------------------------------------------------|------------------------------------------------------------|------------------------------------------------------------------|
| `list_schema('a', type_list(type_union(type_integer(), type_null())))`   | `list<integer\|null>`, Floe and Parquet refuse to write it | `list<?integer>`                                                 |
| `list_schema('a', type_list(type_union(type_integer(), type_string())))` | accepted, Floe and Parquet refuse to write it              | `UnsupportedUnionTypeException`                                  |
| `ref('a')->cast(type_list(type_union(type_integer(), type_string())))`   | accepted, fails on every row                               | `InvalidArgumentException` `Cast function does not support type` |

The same applies to map values and structure elements.

### 38) `flow-php/etl-adapter-postgresql` - array columns are `list<?T>`

| Before                                                                | After            |
|-----------------------------------------------------------------------|------------------|
| `int4[]` → `list<integer\|null>`, Floe and Parquet refuse to write it | `list<?integer>` |

### 39) `flow-php/etl-adapter-json` - `anyOf` / `oneOf` / multi-type `type` inside `items`, `properties` or

`additionalProperties`

| Before                                                                                   | After                                             |
|------------------------------------------------------------------------------------------|---------------------------------------------------|
| `list<integer\|string>`, `structure{x: integer\|string}`, `map<string, integer\|string>` | `UnsupportedUnionTypeException` naming the column |

### 40) `flow-php/types`, `flow-php/etl` - a `datetime` column has one time zone

| Before                                                                                                      | After                                                                                                                                                                                                                              |
|-------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `type_datetime()->cast('2026-01-02 03:04')` in `date.timezone`                                              | in the column zone: UTC, or `type_datetime('Europe/Warsaw')`                                                                                                                                                                       |
| `cast('2026-01-02T03:04:05+02:00')` keeps `+02:00`, `cast('...Z')` keeps `Z`                                | the same instant in the column zone (`2026-01-02 01:04:05 UTC`)                                                                                                                                                                    |
| a `DateTimeInterface` value in `rows()`, `from_rows()`, a cast or a Floe read keeps its own zone            | converted to the column zone, instant unchanged                                                                                                                                                                                    |
| `to_csv()` / `to_json()` / `to_xml()` write each value's own offset (`+02:00` in, `+02:00` out)             | the column zone's offset (`2026-01-02T01:04:05+00:00`); declare `datetime_schema('at', zone: 'Europe/Warsaw')` on the extractor to write a local one                                                                               |
| `from_dbal_*()` naive `timestamp` / MySQL `datetime` text, and naive CSV/JSON text, read in `date.timezone` | read in the column zone (UTC); `->withSchema(schema(datetime_schema('at', zone: 'Europe/Warsaw')))` keeps a local reading                                                                                                          |
| `type_datetime('CEST')`, `to_timezone($ref, 'PST')`, `'EST'`, `'CET'`                                       | `InvalidArgumentException` - an IANA region name, `UTC` or `+HH:MM`                                                                                                                                                                |
| `to_timezone($v, lit('UTC'))`                                                                               | `TypeError` - pass `'UTC'` or a `DateTimeZone`                                                                                                                                                                                     |
| `to_timezone($ref, ref('tz'))` - a per-row zone                                                             | `TypeError` - the zone of `to_timezone()` and `new ToTimeZone()` is `DateTimeZone\|string`                                                                                                                                         |
| `now($tz)`, `to_date_time(..., $tz)`, `to_timezone(..., $tz)` return a `datetime` (UTC) column              | a `datetime` column in `$tz`; a `ScalarFunction` zone in `now()` / `to_date_time()` yields UTC; `now(new DateTimeZone('EST'))`, `to_date_time(..., new DateTimeZone('PST'))` throw `InvalidArgumentException` (abbreviation zones) |
| `to_date_time($dateTimeObject)` truncates to midnight                                                       | keeps the time, converted to the zone                                                                                                                                                                                              |
| merging `datetime` columns ignores zones                                                                    | equal zones kept, different zones become `UTC`                                                                                                                                                                                     |
| merging `time` with `date` / `datetime` gives `datetime`                                                    | `string`                                                                                                                                                                                                                           |
| `?T` inside a list, map or structure passes a valid value through `cast()` unchanged                        | cast like `T`: a `?datetime` moves to its zone, a `?structure` keeps only its declared keys, in declared order                                                                                                                     |
| `type_date()->cast('2026-01-02T23:00:00-05:00')` midnight `-05:00`                                          | `2026-01-02 00:00:00 UTC`; every non-object date input lands at midnight UTC                                                                                                                                                       |
| `type_date()->cast($dateTime)` sets the caller's `DateTime` to midnight                                     | returns a new `DateTimeImmutable`                                                                                                                                                                                                  |
| `type_time()->cast(new DateTimeImmutable('10:00+05:00'))` = `05:00:00`                                      | `10:00:00` - the value's wall clock                                                                                                                                                                                                |
| Excel date cells in the host zone                                                                           | UTC wall clock                                                                                                                                                                                                                     |
| `new DateTimeDefinition('at', true)`                                                                        | `new DateTimeDefinition('at', type_datetime(), true)` or `datetime_schema('at', nullable: true, zone: 'Europe/Warsaw')`                                                                                                            |
| a `flow_php` extension older than this release                                                              | Composer refuses it (`conflict`) - see 67)                                                                                                                                                                                         |

### 41) `flow-php/parquet` - `Converter::isFor()` replaced by `static Converter::forColumn()`,

`Int32DateTimeConverter` removed

| Before                                                             | After                                                                              |
|--------------------------------------------------------------------|------------------------------------------------------------------------------------|
| `Converter::isFor(FlatColumn, Options): bool` on a shared instance | `static Converter::forColumn(FlatColumn, Options): ?self` - a converter per column |
| `new DataConverter([new TimeConverter(), ...], $options)`          | `new DataConverter([TimeConverter::class, ...], $options)`                         |
| `Int32DateTimeConverter`                                           | removed - INT32 TIMESTAMP is not a legal Parquet carrier                           |

### 42) `flow-php/parquet` - `LogicalType\Timestamp` / `Time` take a `TimeUnit`

| Before                                                      | After                                                                                            |
|-------------------------------------------------------------|--------------------------------------------------------------------------------------------------|
| `new Timestamp($isAdjustedToUTC, $millis, $micros, $nanos)` | `new Timestamp($isAdjustedToUTC, TimeUnit::MICROSECONDS)`, same for `Time`                       |
| `TimeUnit` - pure enum with `MICROSECONDS` only             | string-backed enum `MILLISECONDS = 'MILLIS'`, `MICROSECONDS = 'MICROS'`, `NANOSECONDS = 'NANOS'` |

### 43) `flow-php/parquet` - `Option::ROUND_NANOSECONDS` removed

| Before                      | After                                                                                 |
|-----------------------------|---------------------------------------------------------------------------------------|
| `Option::ROUND_NANOSECONDS` | removed - NANOS timestamps always read as `DateTimeImmutable` floored to microseconds |

### 44) `flow-php/parquet` - `encode_decimal()` / `decode_decimal()` drop `ByteOrder` and the read-side precision check

| Before                                                                          | After                                                                                                        |
|---------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------|
| `encode_decimal(ByteOrder, float, int $byteLength, int $precision, int $scale)` | `encode_decimal(float, int $precision, int $scale, ?int $byteLength)` - `null` = minimal length (BYTE_ARRAY) |
| `decode_decimal(ByteOrder, string, int $precision, int $scale)`                 | `decode_decimal(string, int $scale)` - no precision check                                                    |

### 45) `flow-php/parquet`, `flow-php/arrow-ext` - TIMESTAMP columns are written `isAdjustedToUTC=true`

| Before                                                                                                       | After                                                                                                |
|--------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------|
| `FlatColumn::dateTime()` / arrow `TIMESTAMP` written `isAdjustedToUTC=false` (DuckDB/Spark read `TIMESTAMP`) | written `isAdjustedToUTC=true` (DuckDB/Spark read `TIMESTAMPTZ`); reading `false` files is unchanged |

### 46) `flow-php/parquet`, `flow-php/etl-adapter-parquet` - ConvertedType-only TIMESTAMP/TIME/DECIMAL columns are typed

| Before                                                                        | After      |
|-------------------------------------------------------------------------------|------------|
| `TIMESTAMP_MILLIS` / `TIMESTAMP_MICROS` column without a logical type → `int` | `datetime` |
| `TIME_MILLIS` / `TIME_MICROS` column without a logical type → `int`           | `time`     |
| `DECIMAL` converted type on INT32/INT64 → unscaled `int`                      | `float`    |

### 47) `flow-php/arrow-ext` - `Writer` DATE `int` lane is days since epoch

| Before                                      | After                               |
|---------------------------------------------|-------------------------------------|
| `'d' => [1641600000]` - seconds since epoch | `'d' => [19000]` - days since epoch |

### 48) `flow-php/types` - a structure declares every element it carries

| Before                                                                                                       | After                                                                            |
|--------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------|
| `type_structure($elements, allow_extra: true)`                                                               | `Error: Unknown named parameter $allow_extra`                                    |
| `type_structure($elements, true)`, `StructureType::fromElements($elements, true)`                            | the extra argument is ignored by PHP - the structure is sealed                   |
| `new StructureType($elements, true)`                                                                         | the extra argument is ignored by PHP - the structure is sealed                   |
| `StructureType::allowsExtra()`                                                                               | removed                                                                          |
| `normalize()` / `schema_to_json()` write `"allow_extra": false` on every structure                           | the key is not written                                                           |
| a stored schema with `"allow_extra": false`                                                                  | read, the key is ignored                                                         |
| a stored schema with `"allow_extra": true`                                                                   | `InvalidArgumentException: Structure "allow_extra": true is no longer supported` |
| `Rows` / `Schema` with a structure column natively serialized by 0.44.x (`ApcuCache`, `NativePHPSerializer`) | `Error: Cannot create dynamic property StructureType::$allowExtra`               |

A value with a key its structure does not declare is invalid. Declare every element, or use `type_map()` /
`type_json()` when the keys are dynamic. flow-php 0.44.x cannot read a Floe file, cached schema or `schema_to_json()`
document written by 0.45.x that has a structure column. Clear APCu and natively serialized caches after upgrading.

### 49) `flow-php/etl` - `Rows` is a columnar batch, `Row` is a view

| Before                                                                                                     | After                                                                                                                        |
|------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------|
| `new Rows($schema, ...$rows)`                                                                              | `Rows::of($schema, ...$rows)` / `rows($schema, ...$rows)`                                                                    |
| `Rows::trusted($schema, $rows)` / `Rows::conformed($schema, $rows)`                                        | `Rows::of($schema, ...$rows)` (views) / `Rows::fromColumns($schema, $columns, $count)` (columns)                             |
| `new Row(array $values)`                                                                                   | `array_to_row($values, $schema)`                                                                                             |
| -                                                                                                          | `new Row(Rows $rows, int $index)` - a view over row `$index` of `$rows`                                                      |
| `Row::matchTo()` / `Row::conformTo()` / `Row::project()`                                                   | removed - `Rows::matchTo()` / `Rows::project()`                                                                              |
| `Rows::empty(): bool`                                                                                      | `Rows::isEmpty(): bool`                                                                                                      |
| `Row::get()` of a datetime / date / time / uuid / json / xml / html cell - the instance handed in          | a new object per call - compare with `assertEquals`, not `assertSame`                                                        |
| a `DOMElement` / `HTMLElement` cell keeps its document (parent and siblings reachable)                     | the cell is the element's own markup - navigate from the document column (`xpath()`)                                         |
| an optional and nullable structure element given `null`                                                    | reads back absent (the element's validity bit)                                                                               |
| `$row == $other` - compares the rows' values                                                               | compares both views' whole batches and indices - compare `toArray()`                                                         |
| `serialize(Rows)` payload `{schema, rows}`                                                                 | `{schema, frame}` - the Floe BATCH body; payloads of 0.44.x cannot be unserialized, a corrupt payload throws `FloeException` |
| `Rows::drop()` / `dropRight()` / `take()` / `takeRight()` with a negative size - `array_slice()` semantics | `InvalidArgumentException`                                                                                                   |

### 50) `flow-php/etl` - `row()`, `InferredBatch` and `DataFrame::getEach()` removed

| Before                            | After                                                                          |
|-----------------------------------|--------------------------------------------------------------------------------|
| `row($values)`                    | `array_to_row($values, $schema)`                                               |
| `rows($schema, row($a), row($b))` | `array_to_rows([$a, $b], $schema)`                                             |
| `Flow\ETL\Row\InferredBatch`      | removed - `array_to_rows($data, $schema)`, or `from_array($data)` to infer one |
| `DataFrame::getEach()`            | `getEachAsArray()` (values) / `get()` (batches with their schema)              |

`array_to_row()` / `array_to_rows()` cast values to the declared type and refuse keys the schema does not declare
(`SchemaMismatchException` naming the row and the column; before, those keys were silently dropped).

### 51) `flow-php/etl` - date and time columns store days and microseconds

| Before                                                                                  | After                                                                |
|-----------------------------------------------------------------------------------------|----------------------------------------------------------------------|
| a `date` cell - the value as given                                                      | midnight UTC (zone `UTC`) of its calendar day                        |
| a `time` cell - `d` kept                                                                | `d` folded into hours, `$days` is `false`                            |
| a `time` string with months / years (`'P1M'`) - accepted by the hydrate path            | `SchemaMismatchException` "could not convert 'P1M' (string) to time" |
| a `datetime` / `time` beyond ±9 223 372 036 854.775807 s (about year ±292 000) - stored | refused                                                              |
| a `date` beyond the `i32` day range (about year ±5 880 000) - stored                    | refused                                                              |
| a PHP 8.4 `\Dom\Element` in an `xml_element` column - kept                              | read back as a `DOMElement`                                          |

### 52) `flow-php/etl` - batch columns refuse wildcard enums, `mixed` elements and foreign definitions

| Before                                                             | After                                                |
|--------------------------------------------------------------------|------------------------------------------------------|
| `enum_schema('x', UnitEnum::class)` / `BackedEnum::class` - stored | `ColumnMismatchException` "cannot be a batch column" |
| `list<mixed>` / `map<K, mixed>` / `structure{a: mixed}` - stored   | `ColumnMismatchException` "cannot be a batch column" |
| a `Definition` class outside the 19 of `flow-php/etl` - stored     | `ColumnMismatchException` "cannot be a batch column" |

### 53) `flow-php/etl` - the column backend replaces the adaptive hydrator

| Before                                                                             | After                                         |
|------------------------------------------------------------------------------------|-----------------------------------------------|
| `AdaptiveRowHydrator`, `NativeRowHydrator`                                         | removed                                       |
| `array_to_row(..., Hydrator $hydrator)` / `array_to_rows(..., Hydrator $hydrator)` | `Backend $backend`                            |
| `ConfigBuilder::hydrator(Hydrator)`                                                | removed - `ConfigBuilder::backend(Backend)`   |
| -                                                                                  | `Config::backend()`, `FlowContext::backend()` |

### 54) `flow-php/etl` - `FloeEngine` and `NativeFloeEncoder` removed

| Before                                                                                                                      | After   |
|-----------------------------------------------------------------------------------------------------------------------------|---------|
| `from_floe(..., engine: ...)` / `to_floe(..., engine: ...)`                                                                 | removed |
| the `engine` parameter of `FloeReader`, `FloeWriter`, `FloeStreamReader`, `FloeStreamWriter`, `FloeExtractor`, `FloeLoader` | removed |

### 55) `flow-php/etl` - engine classes follow the batch

| Before                                                                                         | After                                                        |
|------------------------------------------------------------------------------------------------|--------------------------------------------------------------|
| `Schema::zonedDefinitions()`, `ZoneAlignment`, `HydratedBatch`, `RowRenaming`, `DomValueCodec` | removed                                                      |
| `GroupBy::aggregatedRow(): Row`                                                                | `GroupBy::aggregatedValues(): array`                         |
| `RowMerger::merge(): Row`                                                                      | `RowMerger::merge(): array`                                  |
| `DerivedColumns::rows(Schema, Schema, list<Row>)`                                              | `DerivedColumns::rows(Rows, Schema, Schema, string, Column)` |
| `UnpackedColumns::values(array, string, Schema, array)`                                        | `UnpackedColumns::values(string, Schema, array)`             |
| `InferredBatch::of(list<RawRowValues>)`                                                        | `InferredBatch::of(list<array>)`                             |
| `InferredRows::of(..., Hydrator)`                                                              | `InferredRows::of(..., Backend)`                             |
| `FileConstants::fillRows(Rows, Schema)`                                                        | `FileConstants::fillRows(Rows, Schema, Backend $backend)`    |

### 56) `flow-php/etl-adapter-json` - untyped JSON Schema arrays and objects become `json`

| Before                                                                                    | After                           |
|-------------------------------------------------------------------------------------------|---------------------------------|
| `{"type": "array"}` without `items` → `list<mixed>`                                       | `json`                          |
| `{"type": "object"}` without `properties` / `additionalProperties` → `map<string, mixed>` | `json`                          |
| `schema_to_json_schema(schema_from_json_schema(...))` of an untyped `array` / `object`    | `{"type": ["object", "array"]}` |

### 57) `flow-php/etl` - a join name collision suggests a prefix

| Before                                                                                                            | After                                                                                                         |
|-------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------|
| `SchemaDefinitionNotUniqueException`: `Entry definitions must be unique, duplicated entries: [id], all: [id, id]` | the same message with `. Please consider using join prefix option` appended - the text before it is unchanged |

Every join shape (`joinCross()`, `join()`, `joinEach()`) reports the collision through `JoinSchema::of()`; the original
exception is the new one's `previous`. Only an exact match on the whole message is affected.

### 58) `flow-php/types` - a date is midnight at UTC

| Before                                                                                                           | After                                                                                             |
|------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------|
| `type_date()->isValid()` / `DateDefinition::matches()` - midnight in any zone (`2026-01-01 00:00 Europe/Warsaw`) | only midnight at UTC offset `+00:00`                                                              |
| `row(['at' => new DateTimeImmutable('2026-01-01', new DateTimeZone('Europe/Warsaw'))])` - a `date` column        | a `datetime` column - the instant is kept                                                         |
| `rows(schema(date_schema('at')), $rowWithWarsawMidnight)` / `matchTo()` - accepted                               | `SchemaMismatchException` "could not convert 2026-01-01T00:00:00.000000+01:00 (datetime) to date" |
| `type_date()->cast($warsawMidnight)`                                                                             | unchanged - the value's own calendar day                                                          |

A date column stores days and reads back midnight UTC (row 51), so a midnight in another zone was silently moved to a
different instant; it now stays a `datetime` unless it is cast to a date explicitly.

### 59) `flow-php/types` - `Comparator` takes no arguments

| Before                                            | After              |
|---------------------------------------------------|--------------------|
| `new Comparator(StructureComparison $structures)` | `new Comparator()` |

### 60) `flow-php/etl` - the row-shaped reader/writer API is removed

| Before                                                        | After                                                                                                 |
|---------------------------------------------------------------|-------------------------------------------------------------------------------------------------------|
| `Hydrator`, `PhpRowHydrator`, `TypedRowValues`, `Row\Encoder` | removed - readers build with `RowsBuilder`, encoders take `encode(Rows)`                              |
| `Row\RowsBuffer(Schema, int $size, ?Closure $batch)`          | `Rows\RowsBuffer(Schema, Backend, int $size)` - `append()`, `appendFrom()`, `appendTake()`, `flush()` |
| `Config::hydrator()`, `FlowContext::hydrator()`               | removed - `Config::backend()`, `FlowContext::backend()`                                               |

### 61) `flow-php/etl` - Floe takes a `Backend`

No default: the caller passes the backend the rows are built in (`$context->backend()` inside a pipeline).

| Before                                                                                | After                                                                    |
|---------------------------------------------------------------------------------------|--------------------------------------------------------------------------|
| `FloeReader(Filesystem, Codec = Noop, int $chunkSize = 65536, ?Hydrator, FloeEngine)` | `FloeReader(Filesystem, Backend, Codec = Noop, int $chunkSize = 65536)`  |
| `FloeStreamReader(SourceStream, Codec, int $chunkSize, ?Hydrator, FloeEngine)`        | `FloeStreamReader(SourceStream, Codec, int $chunkSize, Backend)`         |
| `FloeWriter(Filesystem, Schema, Options = new Options(), ?Hydrator, FloeEngine)`      | `FloeWriter(Filesystem, Schema, Backend, Options = new Options())`       |
| `FloeStreamWriter(Schema, Options = new Options(), ?Hydrator, FloeEngine)`            | `FloeStreamWriter(Schema, Backend, Options = new Options())`             |
| `FloeMerger(Filesystem, ?Hydrator = null, Codec = Noop)`                              | `FloeMerger(Filesystem, Backend, Codec = Noop)`                          |
| `FloeSerializer(int $batchSize = 1000, ?Hydrator = null)`                             | `FloeSerializer(Backend, int $batchSize = 1000)`                         |
| `merge_floe(array $sources, $dest, bool $compact = false, ?Metadata, Filesystem)`     | `merge_floe(array $sources, $dest, Backend, bool $compact = false, ...)` |

### 62) adapters - encoders split into decoders and encoders

| Before                                                                                                                               | After                                                                                                          |
|--------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------|
| `CSVEncoder::decode()`, `CSVEncoder::headers()`                                                                                      | `CSVDecoder::decode()`, `CSVDecoder::headers()`                                                                |
| `new CSVEncoder(withHeader:, emptyToNull:, ...)`                                                                                     | `new CSVDecoder(withHeader:, emptyToNull:, ...)` - `CSVEncoder` keeps only the write options                   |
| `TextEncoder::decode()`                                                                                                              | removed - `TextExtractor` builds `['text' => $line]` rows itself                                               |
| `XMLEncoder::decode()`                                                                                                               | removed - `XMLReaderExtractor` builds `['node' => $xml]` rows itself                                           |
| `ExcelEncoder(withHeader:, convertEmptyToNull:, timeFormat:)`, `decode()`, `headers()`                                               | `ExcelDecoder(withHeader:, convertEmptyToNull:)` with `decode()`, `headers()`; `ExcelEncoder(timeFormat:)`     |
| `OpenSheet::$encoder` (`ExcelEncoder`)                                                                                               | `OpenSheet::$decoder` (`ExcelDecoder`)                                                                         |
| `GoogleSheetEncoder`                                                                                                                 | `GoogleSheetDecoder` - its throwing `encode()` is removed                                                      |
| `GoogleSheetReadOptions::encoder(): GoogleSheetEncoder`                                                                              | `GoogleSheetReadOptions::decoder(): GoogleSheetDecoder`                                                        |
| `HttpEncoder`                                                                                                                        | `HttpDecoder` - `decode()` returns `list<array>`, its throwing `encode()` is removed, `structuredBody()` stays |
| `JSONEncoder::decode()`, `DbalEncoder::decode()`, `PostgreSqlEncoder::decode()`, `SealEncoder::decode()`, `ParquetEncoder::decode()` | removed                                                                                                        |
| `*Encoder::encode(list<TypedRowValues>)`                                                                                             | `*Encoder::encode(Rows)`; no encoder implements `Row\Encoder`                                                  |
| `CSVLoader::write(Rows, array, FlowContext, array)`                                                                                  | `CSVLoader::write(Rows, array $headers, array $partitions)`                                                    |
| `JsonLoader::write(Rows, array, FlowContext)`, `JsonLinesLoader::write(...)`, `XMLLoader::write(...)`                                | `write(Rows, array $partitions)`                                                                               |
| `XMLEncoder` refusing a structure value with more values than elements                                                               | removed - a structure cast drops undeclared keys, so the case cannot occur                                     |
| `CSVOpenSource` implementations                                                                                                      | must implement `headers(): list<string>` - the header `records()` resolved                                     |

### 63) `flow-php/parquet` - `readColumns()` replaces `readValues()`

| Before                                                                                                          | After                                                                                                                                         |
|-----------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------|
| `ParquetEngine::readValues(SourceStream, Schema, array $columns = [], ?int $limit = null, ?int $offset = null)` | `ParquetEngine::readColumns(SourceStream, Schema, list<string> $columns, int $batchSize, ?int $limit, ?int $offset)` - chunks keyed by column |
| -                                                                                                               | `ParquetFile::columns(int $batchSize, array $columns = [], ?int $limit = null, ?int $offset = null)`                                          |
| `ParquetFile::values(offset: -1)` without a limit - read from row 0                                             | `InvalidArgumentException`                                                                                                                    |

### 64) `flow-php/etl-adapter-parquet`, `flow-php/etl` - self-describing formats take no schema

| Before                                                                  | After                                                      |
|-------------------------------------------------------------------------|------------------------------------------------------------|
| `from_parquet(...)->withSchema(...)`, `from_floe(...)->withSchema(...)` | `InvalidArgumentException` - the file footer is the schema |
| `InferredSchemaException::filesDiverge` suggesting `->withSchema(...)`  | suggests only `->unionByName()`                            |
| `ValueConverter::decode()`                                              | removed - values are cast by the batch builder             |

Project with `from_parquet(..., columns: [...])`. Change a type after reading, e.g. a JSON column another tool wrote as
`string`: `->withEntry('payload', ref('payload')->cast(type_json()))`. Re-zone a datetime after reading:
`->withEntry('at', to_timezone(ref('at'), 'Europe/Warsaw'))`.

### 65) `flow-php/etl` - `RowPartitions` reads a batch

| Before                                        | After                                       |
|-----------------------------------------------|---------------------------------------------|
| `RowPartitions::of(Row $row, Schema $schema)` | `RowPartitions::of(Rows $rows, int $index)` |

### 66) `flow-php/etl` - Floe writes one BATCH frame per batch

| Before                                                                                                                                                                                                     | After                                                                                                                           |
|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------|
| `FloeEncoder`, `PhpFloeEncoder`, `Format::rowFrames()`, `Format::metadataBytes()`, `Format::readMetadata()`, `Format::FRAME_ROW`, `Format::VALUE_*_BYTE`, `Flow\Floe\ValueEncoder`, `Flow\Floe\Encoding\*` | removed - `Rows::encodeFrame()` / `Flow\Floe\FrameEncoder`                                                                      |
| `Flow\Floe\Decoding\*`, `Flow\Floe\ValueDecoder`, `Flow\Floe\SchemaDecoder`, `Flow\Floe\ColumnBlueprint`                                                                                                   | removed - `Flow\Floe\FrameDecoder`                                                                                              |
| `FrameWriter::row(string $body)`                                                                                                                                                                           | `FrameWriter::frame(int $type, string $body)`                                                                                   |
| `ValueEncoder::{xmlDocument,xmlElement,htmlElement}ToString()`, `ValueDecoder::{xmlDocument,xmlElement,htmlDocument,htmlElement}FromString()`                                                              | removed - `(new XmlDocumentPhysical())->toPhysical()` / `fromPhysical()` and the other `Flow\ETL\Column\Php\*Physical`          |
| `Flow\Floe\Decoding\TimeZones`                                                                                                                                                                             | `Flow\ETL\Column\Physical\TimeZones`                                                                                            |
| `Offsets::pack()` throws `InvalidArgumentException` past 2³¹−1                                                                                                                                             | throws `Flow\ETL\Exception\OffsetOverflow` (extends `InvalidArgumentException`)                                                 |
| `from_floe()->withBatchSize($n)`, `FloeReader::rows($n)` - batches of exactly `$n` rows                                                                                                                    | at most `$n` rows, never spanning two BATCH frames - re-batch with `->batchSize($n)`                                            |
| a codec wraps every frame body                                                                                                                                                                             | a codec wraps each buffer of a BATCH frame behind an i64 uncompressed length (`-1` = stored raw); the directory stays plaintext |
| a null in a NOT NULL column refused by the Floe writer (`SchemaMismatchException`)                                                                                                                         | refused by `Rows::fromColumns()` / `withColumns()` with the same exception and message, whatever the sink                       |
| an XML/HTML value that cannot be converted to or from its string while building a column throws `FloeException`                                                                                            | throws `Flow\ETL\Exception\InvalidArgumentException`, same message                                                              |

### 67) `flow-php/flow-php-ext`, `flow-php/etl`, `flow-php/etl-adapter-csv` - the extension is the column backend

| Before                                                                                                                                                                                  | After                                                                                                                          |
|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------|
| `Flow\Floe\RustFloeEncoderNative`, `Flow\ETL\Row\RustRowHydratorNative`                                                                                                                 | removed                                                                                                                        |
| -                                                                                                                                                                                       | `flow_php` registers `Flow\ETL\Column\{Backend, Column, ColumnBuilder, RustBackend, RustColumn, RustColumnBuilder}`            |
| any `flow_php` installs beside `flow-php/etl`                                                                                                                                           | Composer refuses `ext-flow_php` < 0.45 (`conflict`); forced past it: `Class "Flow\ETL\Column\RustBackend" not found`           |
| untagged builds report `0.44.1+65.g...`                                                                                                                                                 | `0.45.0-dev+65.g...` (next minor, pre-release)                                                                                 |
| -                                                                                                                                                                                       | `Backend`: `builder()`, `constant()`, `decode()`, `adopt(Definition, Column): Column`, `allocatedBytes(): int`                 |
| `Consumption` / `analyze()` / telemetry memory = `memory_get_usage()`                                                                                                                   | plus the configured backend's `allocatedBytes()` (native columns are not bound by `memory_limit`)                              |
| `new Consumption(bool $realMemory = true)`                                                                                                                                              | `new Consumption(Backend, bool $realMemory = true)`                                                                            |
| `new TelemetryContext(Logger, Tracer, Meter, TelemetryOptions)`                                                                                                                         | `new TelemetryContext(Backend, Logger, Tracer, Meter, TelemetryOptions)`                                                       |
| `new KWayMerge(References, int $batchSize = 1000)`                                                                                                                                      | `new KWayMerge(References, Backend, int $batchSize = 1000)`                                                                    |
| `new Joiner(Expression, Join, int $batchSize = 1000)`                                                                                                                                   | `new Joiner(Expression, Join, Backend, int $batchSize = 1000)`                                                                 |
| `AggregatedGroups::flush(int $batchSize)`                                                                                                                                               | `flush(int $batchSize, FlowContext)`                                                                                           |
| `BucketingConfigBuilder::build(Path)`, `SortAlgorithmBuilder`, `GroupByAlgorithmBuilder`, `JoinAlgorithmBuilder`, `RepartitionAlgorithmBuilder` and their implementations `build(Path)` | `build(Path, Backend)` - custom algorithm builders implement the new signature                                                 |
| `new FilesystemBuckets(Filesystem, Path, int $batchSize = 1000)`                                                                                                                        | `new FilesystemBuckets(Filesystem, Path, Backend, int $batchSize = 1000)`                                                      |
| sort merges, joins, group-by, `count()`, spilled bucket reads, partition filters, CSV reads                                                                                             | build in the configured backend (`config_builder()->backend()`, `AdaptiveBackend` by default) - `PhpBackend` keeps PHP columns |
| -                                                                                                                                                                                       | a native string/list column past 2 GiB of offsets throws `OffsetOverflow` at `concat()` / `appendMany()` / `encode()`          |

### 68) `flow-php/etl`, adapters - `RawRowValues` removed, CSV sources yield batches

| Before                                                                                                                    | After                                                                         |
|---------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------|
| `RawRowValues`; `SchemaSampler`, `SniffsColumnTypes`, `SchemaInferrer::infer()/sniff()`, `ColumnTypes::observe()` over it | plain `array<array-key, mixed>` rows                                          |
| `CSVOpenSource::records(): Generator<RawRowValues>`                                                                       | `Iterator<array<array-key, ?string>>`                                         |
| -                                                                                                                         | `CSVOpenSource::batches(Schema, int, Backend): Iterator<Rows>`                |
| `CSVFileReader::batches(SourceFile, int): Generator<list<RawRowValues>>` returning the header                             | `batches(SourceFile, Schema, int, Backend): Generator<Rows>`, no return value |

### 69) `flow-php/etl` - `Row` removed, `Rows` is a columnar batch

| Before                                                                        | After                                                                                                                                   |
|-------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------|
| `foreach ($df->fetch() as $row) { $row->get('x'); }`                          | `foreach ($df->fetch()->toArray() as $row) { $row['x']; }`                                                                              |
| `$rows->first()->get('x')`, `$rows[0]->get('x')`                              | `$rows->column('x')->value(0)`, `$rows->values(0)['x']`                                                                                 |
| `$rows->first()->toArray()`                                                   | `$rows->toArray()[0]`                                                                                                                   |
| `new Rows($schema)`, `rows($schema)`                                          | `Rows::empty(Schema, Backend)`, `rows(Schema, Backend = new AdaptiveBackend())`                                                         |
| `new Rows($schema, ...$row)`, `rows($schema, ...$row)`, `$rows->add(...$row)` | `array_to_rows(array $data, Schema, Backend = new AdaptiveBackend())`, `$rows->concat(Backend, ...$others)`, `$rows->gather(list<int>)` |
| `Rows` implements `ArrayAccess`, `IteratorAggregate`                          | `Countable` only                                                                                                                        |
| `Rows::all()`, `first()`, `last()`, `offsetGet()`                             | removed - `toArray()`, `values(int)`, `column(string)`, `slice(int, int)`, `gather(list<int>)`                                          |
| `Rows::unique(Comparator)`, `Row\Comparator`, `NativeComparator`              | removed - `$df->dropDuplicates()`                                                                                                       |
| `array_to_row(array $data, Schema, Hydrator, $partitions)`                    | `array_to_rows([$data], Schema, Backend = new AdaptiveBackend())`                                                                       |
| `SerializedPayloadDecoder::decode(Row): array`                                | `decode(Rows, int $index): Rows` - no payload is a one-row batch without columns                                                        |
| `Formatter\ASCII\Body::rows(): array`                                         | `count(): int`, `value(string $entry, int $index): mixed`                                                                               |

`Rows::values(int)` outside the batch throws `InvalidArgumentException`: `Row N does not exist in a batch of M rows`.

### 70) `flow-php/etl` - `ScalarFunction` evaluates a batch

| Before                                                                                | After                                                                                                             |
|---------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------|
| `ScalarFunction::eval(Row, FlowContext): mixed`                                       | `eval(Rows, FlowContext): Column` - one value per row, built by `ResultColumn`                                    |
| `ExpandResults::eval(Row): array`, `UnpackResults::eval(Row): array`                  | marker interfaces - `eval(Rows): Column` returns a list (expand) or structure (unpack) column                     |
| `Parameter::{as, asArray, asBoolean, asEnum, asFloat, asInstanceOf, asInt}(Row, ...)` | `{asTypes, asArrays, asBooleans, asEnums, asFloats, asInstancesOf, asInts}(Rows, ...)` - one list per batch       |
| `Parameter::{asListOfObjects, asNumber, asObject, asString}(Row, ...)`                | `{asListsOfObjects, asNumbers, asObjects, asStrings}(Rows, ...)`                                                  |
| `Parameter::eval(Row)`, `asValue(Row)`                                                | `values(Rows, FlowContext)`, `column(Rows, FlowContext)`                                                          |
| `$function->eval($rows, $context)` on an unresolved tree                              | `InvalidLogicException` - `(new ReferenceResolver())->resolve($function, $rows->schema())->eval($rows, $context)` |

Before:

```php
public function eval(Row $row, FlowContext $context): ?string
{
    $value = (new Parameter($this->value))->asString($row, $context);

    return $value === null ? null : strtoupper($value);
}
```

After:

```php
public function eval(Rows $rows, FlowContext $context): Column
{
    $results = [];

    foreach ((new Parameter($this->value))->asStrings($rows, $context) as $value) {
        $results[] = $value === null ? null : strtoupper($value);
    }

    return (new ResultColumn())->of($this, $results);
}
```

### 71) `flow-php/etl` - float arithmetic follows IEEE 754

| Before                                                                                                     | After                                                                                                                    |
|------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------|
| `ref('a')->multiply(lit(3))` with `a = 0.1` - `0.3`                                                        | `0.30000000000000004`; `ref('a')->multiply(lit(3), exact: true)` - `0.3`                                                 |
| `plus()`, `minus()`, `power()` - decimal arithmetic                                                        | IEEE 754; `exact: true` keeps decimal arithmetic                                                                         |
| an integer result past `PHP_INT_MAX` - `Brick\Math\Exception\IntegerOverflowException`                     | `EvaluationException`: `Plus function integer overflow (row N)`                                                          |
| `power()` of an integer with a negative exponent                                                           | `EvaluationException`: `Power function of an integer requires a non-negative exponent (row N)`                           |
| `average(ref('amount'))` - decimal sum (`Calculator` per value), then rounded to `scale`                   | IEEE sum with Neumaier compensation, then the same rounding; `average(ref('amount'), exact: true)` keeps the decimal sum |
| `sum(ref('amount'))` (default `exact: false`) - plain IEEE addition, `0.1` ten times: `0.9999999999999999` | compensated IEEE addition: `1.0`                                                                                         |
| `RunningSum::add(float\|int $sum, $value, bool $exact): float\|int`                                        | stateful: `add($value, bool $exact): void`, `value(): float\|int`, `merge(RunningSum, bool $exact)`                      |

### 72) `flow-php/etl` - function errors carry the batch row

| Before                                                               | After                                                                                                                                 |
|----------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------|
| an exception thrown by a function reaches the caller as-is           | `Flow\ETL\Exception\EvaluationException` (extends `InvalidArgumentException`), message `... (row N)`, the original as `getPrevious()` |
| `catch (SomeSpecificException $e)` around `withEntry()` / `filter()` | `catch (EvaluationException $e)` and read `$e->getPrevious()`                                                                         |

### 73) `flow-php/etl` - uuid, json, enum, xml, html and date/time values compare by value

| Before                                                                                                            | After                                                                            |
|-------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------|
| `equals()`, `same()`, `isIn()`, join `Identical`, `batchBy()` over these types - always false / one batch per row | value equality                                                                   |
| json values                                                                                                       | compare by their stored text - `{"a":1,"b":2}` and `{"b":2,"a":1}` are not equal |

### 74) `flow-php/etl` - `array_keys_style_convert()` declares the converted nested shape

| Before                                                                                            | After                                                                                                                    |
|---------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------|
| `returns()` renames the top-level keys only; a nested conversion stored with `withEntry()` failed | `returns()` renames structure fields at every level (structures, lists, maps); key collisions are refused at every level |

### 75) `flow-php/etl` - extension points take a batch

| Before                                                             | After                                                                                          |
|--------------------------------------------------------------------|------------------------------------------------------------------------------------------------|
| `AggregatingFunction::aggregate(Row, FlowContext)`                 | `aggregate(Rows, list<int> $indices, FlowContext)` + `merge(AggregatingFunction, FlowContext)` |
| `Join\Comparison::compare(Row, Row): bool`                         | `compare(Rows $left, Rows $right): list<bool>` - pairwise                                      |
| `Constraint::isSatisfiedBy(Row, Schema)`, `violation(Row, Schema)` | `firstViolation(Rows): ?int`, `violation(Rows, int)`                                           |
| `FrameAccumulator::accumulate(Row)`                                | `accumulate(Rows, int)`                                                                        |
| `WindowContext::row()`                                             | removed - `WindowContext(int, Rows, WindowFrame, FlowContext)`                                 |
| `PeerComparator::arePeers(Row, Row, Schema)`                       | `arePeers(Rows, int, int)`                                                                     |
| `Dataset\Statistics\Columns::add(Definition, mixed)`               | `add(Definition, Column)`                                                                      |

### 76) `flow-php/etl` - one sort order: nulls smallest, strings by bytes

| Before                                                                                                              | After                                                                                               |
|---------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------|
| `sortBy(ref('code')->asc())` over `'9', '10', '100'` - `'9', '10', '100'` (numeric when every value looked numeric) | `'10', '100', '9'` - strings always compare by bytes                                                |
| `sortBy(ref('v')->asc())` over `3, null, 1` - unsorted (a null compared equal to every value)                       | `null, 1, 3` - nulls first ascending, last descending; descending is the exact reverse of ascending |
| NaN - no defined position                                                                                           | after every other value, equal to NaN; `-0.0` equals `0.0`                                          |
| external sort (spilled runs) could order differently from the in-memory sort                                        | both, `constraint_sorted_by()` and window peers use the same order                                  |
| `Flow\ETL\Sort\ValuesSorter`, `Sort\Merge\RowsMinHeap`, `Sort\Merge\ComparableBucketRow`                            | removed - `Flow\ETL\Sort\RowOrder`                                                                  |
| `new BucketCursor(Generator, References)`, `keys(): list<mixed>`, `next()`                                          | `new BucketCursor(Generator, RowOrder)`, `keys(): list<SortKey>`, `advance(int)`                    |
| -                                                                                                                   | `ref('x')->asc(NullsOrder::LAST)`, `ref('x')->desc(NullsOrder::FIRST)` move nulls to the other end  |
| a custom `Reference` implementation                                                                                 | adds `nulls(): NullsOrder`                                                                          |

### 77) `flow-php/etl` - spilling steps stop at a memory limit, not a row count

| Before                                                                                                                                                   | After                                                                                                                                                                                                                         |
|----------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `external_sort()->runSize(int)` - rows per sorted run, default `10000`                                                                                   | `external_sort()->memoryLimit(Unit)`; data under it sorts in memory without spilling                                                                                                                                          |
| -                                                                                                                                                        | `hash_join()->memoryLimit(Unit)`, `hash_group_by()->memoryLimit(Unit)`, `hash_repartition()->memoryLimit(Unit)`                                                                                                               |
| -                                                                                                                                                        | every limit defaults to `Config\MemoryLimit::default()`: `FLOW_MAX_MEMORY`, otherwise 70% of `memory_limit`, 1 GiB when `memory_limit` is `-1`                                                                                |
| -                                                                                                                                                        | a limit is compared with the whole process (`memory_get_usage()` + the backend's native memory), not with the step's own buffer - a sort and a join in one pipeline stay under it together                                    |
| `hash_join()` with a disk storage partitioned both sides into buckets                                                                                    | the right side stays in memory under the limit and the left side streams in its own order; past it both sides are partitioned                                                                                                 |
| `groupBy()` / window partitions always hash-partitioned their input into buckets                                                                         | under the limit a group-by aggregates in one pass and a window groups its partitions in memory - no bucket is written; past it the rest of the stream is partitioned (group-by merges the groups aggregated before the limit) |
| group-by / repartition / join wrote one bucket frame per bucket per input batch                                                                          | buckets stay in memory under the limit; past it every frame holds `batchSize` rows                                                                                                                                            |
| `GroupBySteps::of()` / `RepartitionSteps::of()` - `[BucketingProcessor, GroupByAggregationProcessor]` / `[BucketingProcessor, RepartitionProcessor]`     | `[GroupByAggregationProcessor]` / `[RepartitionProcessor]`; both take `HashBucketing` and `Unit $memoryLimit`; `GroupBy\BucketAggregation` removed                                                                            |
| `new ExternalSortConfig($bucketing, ?BucketsStorage $merge = null, int $runSize)`                                                                        | `new ExternalSortConfig($bucketing, Unit $memoryLimit, ?BucketsStorage $merge = null)`                                                                                                                                        |
| `new HashJoinConfig($bucketing)`, `new HashGroupByConfig($bucketing)`, `new HashRepartitionConfig($bucketing)`                                           | `new ...Config($bucketing, Unit $memoryLimit)`                                                                                                                                                                                |
| `new HashJoinProcessor($right, $executor, $on, $type, $left, $right, $random, $bucketsCount, $batchSize)`                                                | `new HashJoinProcessor($right, $executor, $on, $type, $left, $right, $random, Unit $memoryLimit, $bucketsCount, $batchSize)`                                                                                                  |
| `new MergeSortProcessor(References, Buckets, Buckets, RandomValueGenerator, int, int)`                                                                   | `new MergeSortProcessor(ExternalSort)`                                                                                                                                                                                        |
| `SortSteps::of()` - `[BucketingProcessor, MergeSortProcessor]` for the external sort                                                                     | `[MergeSortProcessor]`; `SortSteps::external(): ?ExternalSort`                                                                                                                                                                |
| `Flow\ETL\Bucketing\SortedRunBucketing`                                                                                                                  | removed - `Flow\ETL\Sort\ExternalSort`                                                                                                                                                                                        |
| `Flow\ETL\Processor\BucketingProcessor`, `Flow\ETL\Bucketing\BucketingStrategy`, `Flow\ETL\Bucketing\BucketShape`, `Bucket::schema()`, `Bucket::toRow()` | removed - sort, join, group-by and repartition partition inside their own step                                                                                                                                                |
| `sortBy()->limit(n)` over the external sort became a top-n only when `n <= runSize`                                                                      | always a top-n; past the memory limit it hands its kept rows and the rest of the stream to the external sort, whose merge stops after `n` rows                                                                                |
| `new TopNRewrite(FlowContext)`                                                                                                                           | `new TopNRewrite()`                                                                                                                                                                                                           |
| `new TopNProcessor(References, int, ?Schema)`                                                                                                            | `new TopNProcessor(References, int, ?ExternalSort $fallback = null, ?Schema)`                                                                                                                                                 |
| `new Plan\Node\TopN(Node, References, int)`                                                                                                              | `new TopN(Node, References, int, ?SortAlgorithmBuilder $algorithm = null)`                                                                                                                                                    |
| explain: `Storage: FilesystemBuckets`                                                                                                                    | adds `Memory: N MB` under the storage of a sort, join, group-by and repartition step                                                                                                                                          |

### 78) `flow-php/etl` - `sum(exact:)` is a bool; `batched_by()`, `string_agg()` and `pivot()` follow the one order

| Before                                                                                                                    | After                                                                                                                                    |
|---------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------|
| `sum(ref('a'), exact: ref('is_exact'))` - exactness chosen per row                                                        | `sum(ref('a'), exact: true)` - one `bool` for the aggregate, as `average()` and `plus/minus/multiply/power`                              |
| `sum(..., exact: ScalarFunction)` disabled spill column pruning (`references()` was `null`)                               | `sum()` always enumerates its reference; a custom `AggregatingFunction` may still return `null`                                          |
| `batched_by(from_..., 'at')` - equal datetimes / uuids split into one batch per row, a `null` value joined the next group | value equality, as `batchBy()`; nulls are a group of their own                                                                           |
| `string_agg(ref('code'), ',', SortOrder::ASC)` over `'9', '10', '100'` - `9,10,100` (numeric-looking strings numerically) | `10,100,9` - strings by bytes, as `sortBy()`                                                                                             |
| `discover_pivot_values()` over `'9', '10', '100'` - columns `9, 10, 100`                                                  | `10, 100, 9`; integer pivot values stay numeric                                                                                          |
| `->stringMatch()` / `->stringMatchAll()` turned a PHP `\Error` into an exception `optional()` swallowed                   | an `\Error` propagates; `->jsonDecode()`, `cast()`, `->arrayPathExists()`, `->stringMatch*()` errors keep their cause as `getPrevious()` |

### 79) `flow-php/etl-adapter-parquet` - with

`flow_php` and arrow-ext loaded and no engine given, Parquet is read and written as native columns

| Before                                                                                                             | After                                                                                                                                                                                                                             |
|--------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `from_parquet($path)` / `to_parquet($path)` - `AdaptiveParquetEngine` (arrow-ext when loaded, else the PHP engine) | with `flow_php` and arrow-ext loaded: arrow-ext's batches straight into native columns and back (Arrow C Data Interface); `flow_php` alone: the PHP engine, its values adopted into native columns; without `flow_php`: unchanged |
| `engine: new PhpParquetEngine()` / `new ArrowParquetEngine()`                                                      | `engine: new PhpParquetEngine()` opts out; `new AdaptiveParquetEngine()` reads and writes native columns when both extensions are loaded, `new RustParquetEngine()` reads them                                                    |
| a column type arrow-ext could not read failed inside arrow-ext                                                     | refused before the first batch: `Flow\Parquet\Exception\RuntimeException` `Parquet column "<name>" (<type>) is not supported by the arrow Parquet reader; read the file with \Flow\Parquet\Reader::php()`                         |
| `flow-php/etl-adapter-parquet` installable with any `flow_php` or arrow-ext version                                | `conflict: ext-flow_php <0.45`, `ext-arrow <0.45` - load extensions of this release or none                                                                                                                                       |

### 80) `flow-php/etl-adapter-parquet` - Flow `float` columns are written as Parquet `DOUBLE`

| Before                                                                                                  | After                                                                                      |
|---------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------|
| `float_schema('amount')` written as Parquet `FLOAT` (32-bit): `0.14` read back as `0.14000000059604645` | written as `DOUBLE` (64-bit), read back exactly; readers of the file see a `DOUBLE` column |

### 81) `flow-php/etl-adapter-parquet` - with

`flow_php` and arrow-ext, a string that is not valid UTF-8 is refused on write

| Before                                                                                                                             | After                                                                                                                                                                                                                           |
|------------------------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| without arrow-ext, `PhpParquetEngine` wrote `"\xff\xfe"` into a `STRING` column other readers reject (DuckDB: `is not valid UTF8`) | `Flow\Parquet\Exception\RuntimeException` `Parquet column "name" row 0 holds a string that is not valid UTF-8; Parquet STRING columns require UTF-8` - the row is 0-based within the batch written                              |
| with arrow-ext: `Column 'name': expected string, got string`                                                                       | the same refusal; invalid UTF-8 into `STRING` / `JSON` / `ENUM` is refused by both engines; `Option::VALIDATE_DATA = false` on `PhpParquetEngine` writes it unvalidated (a file other readers reject); arrow-ext always refuses |

### 82) `flow-php/parquet` - `DELTA_BINARY_PACKED` follows the spec; affected files written by

`PhpParquetEngine` must be rewritten

| Before                                                                                                                                                                               | After                                                                                                                                               |
|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------|
| `PhpParquetEngine` wrote `DELTA_BINARY_PACKED` blocks that other readers misread (a partial last block without all miniblock bit widths, zero-width miniblocks with 4 padding bytes) | spec-conformant blocks; DuckDB, arrow-rs and `flow_php` read the values back                                                                        |
| it read other writers' `DELTA_BINARY_PACKED` `INT64` wrong after an outlier (`9223372036854645807` read as `9223372036788279061`, garbage after)                                     | reads them as DuckDB and arrow-rs do                                                                                                                |
| affected: `INT32`/`INT64` columns `PhpParquetEngine` wrote with `Option::WRITER_VERSION` `2` or `COLUMNS_ENCODINGS` `DELTA_BINARY_PACKED` (the default is version `1`, `PLAIN`)      | no spec-conformant reader returns their values: read them with the Flow version that wrote them and write them again (or re-export from the source) |

### 83) `flow-php/parquet` - `ParquetEngine::openForRead()` replaces `readColumns()`

| Before                                                                          | After                                                                                                                                                                            |
|---------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `$engine->readColumns($stream, $schema, $columns, $batchSize, $limit, $offset)` | `$engine->openForRead($stream)->readColumns($columns, $batchSize, $limit, $offset)`                                                                                              |
| a custom `ParquetEngine` implements `readColumns()`                             | it implements `openForRead(SourceStream): ParquetFileReader` - `metadata()`, `schema()`, `rowsNumber()`, `totalByteSize()`, `readColumns()`, `close()` (which closes the stream) |
| `new ParquetFile($stream, $byteOrder, $options, $engine)`                       | `new ParquetFile($stream, $options, $engine->openForRead($stream))`                                                                                                              |
| a `ParquetFile` closed its stream only when destroyed                           | `ParquetFile::close()` closes its reader (once); `ParquetFile::reader()` returns it                                                                                              |

### 84) `flow-php/parquet` - the arrow engine (`RustParquetEngine`, was

`ArrowParquetEngine`) reads and writes through arrow-ext's new Parquet core

| Before                                                                                                                           | After                                                                                                                                                                                      |
|----------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `DATE` read as `DateTimeImmutable` in zone `+00:00`                                                                              | zone `UTC`, as `PhpParquetEngine`                                                                                                                                                          |
| `TIME` of 25 hours read as `PT25H`, `days` `false`                                                                               | `d` = 1, `h` = 1, `days` an int (the `diff()` interval `PhpParquetEngine` returns)                                                                                                         |
| `INT96` outside 1677-2262 read wrapped (0001-01-01 → 1754-08-30)                                                                 | read exactly, floored to the microsecond                                                                                                                                                   |
| `UINT_64` above `PHP_INT_MAX` read as a numeric string                                                                           | refused, as by `PhpParquetEngine`                                                                                                                                                          |
| writes accepted an int into `FLOAT` / `DOUBLE` / `DATE` / `TIMESTAMP` / `TIME`, a numeric string into `DECIMAL` / `UINT_64`      | refused - writes accept what `PhpParquetEngine` accepts; decimals are written from floats (or ints) as `PhpParquetEngine` writes them; an object with `toString()` into `UUID` is accepted |
| `TIME` writes dropped the interval's `d`                                                                                         | kept                                                                                                                                                                                       |
| an offset read decoded every row group before the offset; page headers were read to the end of the file                          | only the row groups a read needs, each column chunk read once                                                                                                                              |
| an `INTERVAL`, a path into a `LIST` / `MAP`, `LZO` or `INT96` under `Option::INT_96_AS_DATETIME = false` failed inside arrow-ext | refused before the first batch naming the column and `\Flow\Parquet\Reader::php()`                                                                                                         |
| `Reader::read()` returned without touching the file; an invalid file failed at `metadata()`                                      | the footer is read when the file is opened: `Reader::read()` / `readStream()` throw `InvalidArgumentException` `Given file is not valid Parquet file: ...`                                 |
| `Flow\Arrow\Parquet\Reader` / `Writer` / `Exception`                                                                             | removed: `Flow\Parquet\Engine\{RustParquetFileReader, RustParquetFileWriter}`, `Flow\Arrow\Parquet\{RustColumnsReader, RustBatchReader}`; refusals are `Flow\Parquet\Exception\*`          |
| `flow-php/parquet` installable with any arrow-ext version                                                                        | `conflict: ext-arrow <0.45`                                                                                                                                                                |

### 85) `flow-php/parquet` - `PhpParquetEngine` reads unsigned integers and INT96 exactly;

`FlatColumn::enum()` writes the ENUM logical type; values it wrote wrongly are refused

| Before                                                                                                                                              | After                                                                                                                                                                |
|-----------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `UINT_32` 4000000000 read as `-294967296`; `UINT_64` 18446744073709551615 read as `-1`                                                              | `4000000000`; a `UINT_64` above `PHP_INT_MAX` is refused                                                                                                             |
| `INT96` 1960-01-01 23:59:59.999999999 read as 1960-01-02 00:00:00 (rounded)                                                                         | 1960-01-01 23:59:59.999999 (floored, integer arithmetic)                                                                                                             |
| `FlatColumn::enum()` wrote logical `STRING` + converted `ENUM`, a file arrow-rs refuses                                                             | logical `ENUM` + converted `ENUM`                                                                                                                                    |
| `from_parquet()` refused a logical `ENUM` column (`ENUM is not supported`)                                                                          | read as a `string` column, as `STRING`                                                                                                                               |
| an int outside an `INT_8/16/32` or `UINT_8/16/32/64` range written wrapped (4000000000 into `INT32` → `-294967296`), negatives into `UINT` accepted | `ValidationException` `Column "<path>" row <n>: ... is out of the ... range`                                                                                         |
| a `UUID` / `FIXED_LEN_BYTE_ARRAY(n)` value of another byte length written cut or short                                                              | refused naming column and row                                                                                                                                        |
| invalid UTF-8 into `STRING` / `JSON` / `ENUM` written (other readers reject the file)                                                               | refused naming column and row                                                                                                                                        |
| these checks                                                                                                                                        | run under `Option::VALIDATE_DATA` (default `true`), as every other validator check                                                                                   |
| `Validator::validate(Column $column, mixed $data): void`                                                                                            | `Validator::validate(Column $column, mixed $data, int $row): void` - custom validators add the parameter (the 0-based row within the writer)                         |
| logical `INTEGER` read without its width and sign; written back as an `IntType` missing both (a footer arrow-rs refuses)                            | `LogicalType::integerData(): ?LogicalType\Integer` keeps them and writes them back; `LogicalType::integer()` → `LogicalType::integer(int $bitWidth, bool $isSigned)` |

### 86) `flow-php/arrow-ext` - Parquet core replaced

| Before                                                                                          | After                                                                                                                                                                                                                                                                                                                                                                  |
|-------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `Flow\Arrow\Parquet\Reader` / `Writer` - PHP values per call (`readRowGroup()`, `writeBatch()`) | `Flow\Parquet\Engine\{RustParquetEngine, RustParquetFileReader, RustParquetFileWriter}` implement `ParquetEngine`, `ParquetFileReader`, `ParquetFileWriter` - the values `PhpParquetEngine` reads and accepts; `Flow\Arrow\Parquet\RustBatchReader` and `RustParquetFileWriter::writeArrowBatch()` hand batches to other extensions through the Arrow C Data Interface |

### 87) `flow-php/etl-adapter-csv` - `to_csv()` writes a boolean as `true` / `false`

| Before                                                           | After                                                              |
|------------------------------------------------------------------|--------------------------------------------------------------------|
| `true` → `1`, `false` → an empty field, the same bytes as `null` | `true` / `false`; `null` stays an empty field                      |
| a NOT NULL boolean column could not be read back by `from_csv()` | reads back equal; a nullable column keeps `false` and `null` apart |

### 88) `flow-php/etl-adapter-csv`, `-xml`, `-text`,

`-json` - a float is written as the shortest text that reads back as the same float

| Before                                                                                        | After                                                         |
|-----------------------------------------------------------------------------------------------|---------------------------------------------------------------|
| CSV / XML / Text: `(string) $float`, 14 digits under the `precision` ini: `0.1 + 0.2` → `0.3` | `0.30000000000000004`                                         |
| `1 / 3` → `0.33333333333333`, `123456789012345.678` → `1.2345678901235E+14`                   | `0.3333333333333333`, `123456789012345.67`                    |
| `1.0` → `1`, `1e25` → `1.0E+25`, `-0.0` → `-0`                                                | `1.0`, `1.0e+25`, `-0.0` - always a fraction or an exponent   |
| `NAN`, `INF`, `-INF`                                                                          | unchanged; refused inside a nested CSV cell (`JsonException`) |
| JSON: the digits followed the `serialize_precision` ini (`17` → `0.10000000000000001`)        | the shortest digits under any ini value                       |

Writing a float column forces `serialize_precision = -1` for the render and restores the ini afterwards. When the ini
holds another value, `ini_set()` must be allowed; otherwise the writers throw
`RuntimeException('Writing floats requires serialize_precision = -1 and ini_set() cannot change it')`.

### 89) `flow-php/etl-adapter-csv`, `-json`, `-xml`,

`-text` - a nested value is written by its type, as a column of that type is

| Before                                                                                                  | After                                                            |
|---------------------------------------------------------------------------------------------------------|------------------------------------------------------------------|
| CSV `list<datetime>`: `[{"date":"2026-01-02 03:04:05.000000","timezone_type":3,"timezone":"UTC"}]`      | `["2026-01-02T03:04:05+00:00"]` (the loader's datetime format)   |
| CSV `list<uuid>`: `[{}]`; `list<time>`: a `DateInterval` property dump; `list<enum>`: the backing value | `["00000001-..."]`, `[3600000000]`, `["Hearts"]` (the case name) |
| JSON `list<date>` / `structure{d: date}`: `"2026-01-02T00:00:00+00:00"` (the datetime format)           | `"2026-01-02"` (the date format)                                 |
| XML `list<?T>` / `map<K, ?V>`: threw `Given type can't be converted to node`                            | the element of `T`, a null is an empty element                   |
| `html` / `html_element`: CSV and JSON wrote an empty string, XML refused the column                     | the markup                                                       |
| XML `xml_element` and a non-backed `enum`: an empty element                                             | the canonical XML text; the case name                            |
| XML attribute column (`_name`) of a `datetime`: RFC 3339, whatever the loader's format                  | the loader's datetime format                                     |
| Text: a `datetime` / `date` / `time` column threw `Text data loader supports only scalar values`        | `DATE_ATOM`, `Y-m-d`, microseconds                               |

### 90) `flow-php/etl-adapter-csv` - `CSVEncoder` is a contract; `PhpCSVEncoder` takes

`CSVWriteOptions` and returns one string per batch

| Before                                                                                                | After                                                                                                                                                                        |
|-------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `final class CSVEncoder`                                                                              | `interface CSVEncoder { encode(Rows): string; encodeHeader(array $headers): string; }`, implemented by `PhpCSVEncoder`, `RustCSVEncoder` (flow_php) and `AdaptiveCSVEncoder` |
| `new CSVEncoder($separator, $enclosure, $escape, $dateTimeFormat, $dateFormat, $newLineSeparator)`    | `new PhpCSVEncoder(new CSVWriteOptions($separator, $enclosure, $escape, $newLineSeparator, $dateTimeFormat, $dateFormat))`                                                   |
| `CSVEncoder::encode(Rows): list<string>`                                                              | `encode(Rows): string`                                                                                                                                                       |
| `CSVLoader::write(Rows, array $headers, array $partitions)`                                           | removed - `load()` is the only entry                                                                                                                                         |
| a separator or enclosure that is not one byte, an escape longer than one byte - failed in `fputcsv()` | `InvalidArgumentException` from `CSVWriteOptions`                                                                                                                            |

### 91) `flow-php/etl-adapter-json` - `JSONEncoder` is a contract; `PhpJSONEncoder` returns JSON text;

`JsonDocuments` and the loaders' `write()` are removed

| Before                                                             | After                                                                                                                                                         |
|--------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `final class JSONEncoder`                                          | `interface JSONEncoder { encode(Rows, string $separator): string; }`, implemented by `PhpJSONEncoder`, `RustJSONEncoder` (flow_php) and `AdaptiveJSONEncoder` |
| `new JSONEncoder($dateTimeFormat, $dateFormat)`                    | `new PhpJSONEncoder($flags, $dateTimeFormat, $dateFormat)`                                                                                                    |
| `JSONEncoder::encode(Rows): list<array<string, mixed>>`            | `encode(Rows, string $separator): string` - one object per row, joined by `$separator`                                                                        |
| `JsonDocuments`, `JsonLoader::write()`, `JsonLinesLoader::write()` | removed - `load()` is the only entry                                                                                                                          |
| no constraint on the extension                                     | `"conflict": {"ext-flow_php": "<0.45"}` - upgrade the `flow_php` extension with the package                                                                   |

### 92) `flow-php/etl-adapter-xml` - `XMLWriter` renders columns; `XMLEncoder::encode()` returns one string per batch

| Before                                   | After                                                                                              |
|------------------------------------------|----------------------------------------------------------------------------------------------------|
| `XMLWriter::write(XMLNode): string`      | + `elements(string $name, array $values): array`, `attributes(string $name, array $values): array` |
| `XMLEncoder::encode(Rows): list<string>` | `encode(Rows): string` - every row followed by `"\n"`                                              |

A custom `XMLWriter` adds both methods; the smallest implementation delegates to `write()`:

```php
public function elements(string $name, array $values): array
{
    return array_map(fn (?string $value): string => $this->write(XMLNode::flatNode($name, $value ?? '')), $values);
}

public function attributes(string $name, array $values): array
{
    return array_map(
        fn (?string $value): string => substr($this->write(XMLNode::nested('x', new XMLAttribute($name, type_string()->cast($value)))), 2, -2),
        $values,
    );
}
```

### 93) `flow-php/etl-adapter-text` - `TextEncoder::encode()` returns one string per batch

| Before                                    | After                  |
|-------------------------------------------|------------------------|
| `TextEncoder::encode(Rows): list<string>` | `encode(Rows): string` |

### 94) `flow-php/parquet`, `flow-php/etl-adapter-parquet` - `ParquetFileWriter::writeColumns()`

| Before                                             | After                                                                               |
|----------------------------------------------------|-------------------------------------------------------------------------------------|
| `ParquetFileWriter::{writeBatch, writeRow, close}` | + `writeColumns(array $columns): void` - a custom `ParquetFileWriter` implements it |
| `ParquetEncoder::encode(Rows): list<array>`        | `ParquetEncoder::columns(Rows): array<string, list<mixed>>`                         |

`$columns` holds one list per top-level column, every list of one length; a schema column it lacks is written as nulls,
a key the schema lacks is ignored. `Writer::writeColumns()` is the public door.

### 95) `flow-php/etl-adapter-json`, `-csv` - the JSON shape follows the column type, never the data

| Before                                                  | After               |
|---------------------------------------------------------|---------------------|
| `map<int, string>` holding the keys `0, 1`: `["a","b"]` | `{"0":"a","1":"b"}` |
| an empty `map`: `[]`                                    | `{}`                |
| a `json` column holding `{}`: `[]`                      | `{}`                |
| a row whose column names are `0, 1, ...`: a list        | an object           |
| a `list`                                                | `[...]`, unchanged  |

Applies to `to_json()`, `to_json_lines()` and to a `list` / `map` / `structure` cell of `to_csv()`.

### 96) `flow-php/types` - `type_float()->cast()` accepts `'NAN'`, `'INF'`, `'-INF'`

| Before                                               | After                                                  |
|------------------------------------------------------|--------------------------------------------------------|
| `type_float()->cast('NAN')` threw `CastingException` | `NAN`; `'INF'` → `INF`, `'-INF'` → `-INF`              |
| `'nan'`, `'Inf'`, `'+INF'`                           | still refused - exactly these three spellings are read |

`from_csv()` with a `float` column reads what `to_csv()` writes for a float that is not finite.

### 97) `flow-php/etl`, `flow-php/etl-adapter-xml` - XML node documents are UTF-8

|                                                          | Before                                         | After                                                     |
|----------------------------------------------------------|------------------------------------------------|-----------------------------------------------------------|
| a `from_xml()` node                                      | `DOMDocument` without an encoding              | `DOMDocument('1.0', 'UTF-8')`                             |
| `$node->saveXML()` of `<row>ż</row>`                     | `<?xml version="1.0"?>` + `<row>&#x17C;</row>` | `<?xml version="1.0" encoding="UTF-8"?>` + `<row>ż</row>` |
| the bytes an `xml` column stores (Floe, Parquet, spill)  | the `saveXML()` above                          | the `saveXML()` above                                     |
| `cast(type_string())`, `to_json()`, `to_csv()` of a node | `<row>ż</row>`                                 | unchanged                                                 |

Files written before still read, cast and write the same text.

### 98) `flow-php/types` - `type_date()->cast()` of a datetime outside UTC is midnight UTC of its calendar day

|                                                                                                   | Before                           | After                    |
|---------------------------------------------------------------------------------------------------|----------------------------------|--------------------------|
| `type_date()->cast(new DateTimeImmutable('2026-01-02 00:30', new DateTimeZone('Europe/Warsaw')))` | `2026-01-02 00:00 Europe/Warsaw` | `2026-01-02 00:00 UTC`   |
| `type_list(type_date())->cast([...])` of the same value                                           | `CastingException`               | `[2026-01-02 00:00 UTC]` |

A `date` column stored that calendar day before, and still does.

### 99) `flow-php/types` - `type_numeric_string()->cast()` refuses `NAN` and `INF`

|                                    | Before                             | After              |
|------------------------------------|------------------------------------|--------------------|
| `type_numeric_string()->cast(NAN)` | `'NAN'`, which `isValid()` refuses | `CastingException` |
| `INF`, `-INF`                      | `'INF'`, `'-INF'`                  | `CastingException` |

### 100) `flow-php/types` - `type_xml_element()->cast()` of a string that is not XML

|                                       | Before                                                                                                           | After                                                    |
|---------------------------------------|------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------|
| `type_xml_element()->cast('not xml')` | `DOMDocument::loadXML()` warnings, then `InvalidTypeException` `Expected type "object<DOMElement>", got "null".` | `CastingException`, no warning - as `type_xml()->cast()` |

### 101) `flow-php/types` - `NAN` and `INF` are cast without PHP's coercion

|                                                                         | Before                                                  | After                                                     |
|-------------------------------------------------------------------------|---------------------------------------------------------|-----------------------------------------------------------|
| `type_boolean()->cast(NAN)`, `INF`, `-INF`                              | `true`                                                  | `CastingException`                                        |
| `type_date()`, `type_datetime()` `->cast(NAN)`, `INF`, `-INF`           | `CastingException` from `new DateTimeImmutable('@NAN')` | `CastingException`, reason `value is not a point in time` |
| `type_string()`, `type_non_empty_string()` `->cast(NAN)`, `INF`, `-INF` | `'NAN'`, `'INF'`, `'-INF'`, a PHP 8.5 warning for `NAN` | the same text, no warning                                 |

### 102) `flow-php/etl` - `html_element` and `xml_element` bytes carry the owner document

|                                                           | Before                               | After                                                                           |
|-----------------------------------------------------------|--------------------------------------|---------------------------------------------------------------------------------|
| the bytes an element column stores (Floe, Parquet, spill) | the element's markup                 | `"\x01" . <path> . "\0" . <markup length> . "\0" . <markup> . <owner document>` |
| `domElementParent()` of an element read from a column     | `<body>` or nothing, from a re-parse | the element's parent                                                            |
| `cast(type_string())`, `to_json()`, `to_csv()`            | the element's markup                 | unchanged                                                                       |

An element column now stores its whole owner document per value: its size grows with the document.
Files written before still read.

### 103) `flow-php/etl-adapter-json` - a scalar JSON record throws `RuntimeException`

|                                                                     | Before                     | After                                 |
|---------------------------------------------------------------------|----------------------------|---------------------------------------|
| `from_json()` over `[1, {"id":1}]`                                  | `TypeError` from `count()` | `Flow\ETL\Exception\RuntimeException` |
| `from_json()` over a JSON-lines file (`{"id":1}`)                   | `TypeError` from `count()` | `Flow\ETL\Exception\RuntimeException` |
| a pointer at scalars (`withPointer('/items')` over `{"items":[1]}`) | `TypeError` from `count()` | `Flow\ETL\Exception\RuntimeException` |

The message is `A JSON record must be an object or an array, int given in "<uri>".`, the type being the scalar's
`get_debug_type()`.

### 104) `flow-php/etl-adapter-json` - with `flow_php` loaded, `from_json()` / `from_json_lines()` read strict JSON

JSON lines and a document holding a top-level array are read by the extension, which refuses what the PHP reader
tolerated, with `Flow\ETL\Exception\RuntimeException`. A pointer, or a document that is not an array, is read as
before.

|                                                                  | Before                                       | After                                                                |
|------------------------------------------------------------------|----------------------------------------------|----------------------------------------------------------------------|
| `\v`, `\f` or BOM bytes between tokens (`[{"id":1}\v,{"id":2}]`) | read                                         | `Malformed JSON in "<uri>" at element 0: ...`                        |
| a BOM at the start of a later line                               | read                                         | `Malformed JSON in "<uri>" at line N: ...`                           |
| content after a line's record (`{"id":1}{"id":9}`)               | the first record, the rest ignored           | `Malformed JSON in "<uri>" at line N: ...`                           |
| content after the closing `]` (`[{"id":1}] x`)                   | ignored                                      | `Malformed JSON in "<uri>" at element N: ...`                        |
| a scalar JSON line (`5`)                                         | `JsonMachine\Exception\SyntaxErrorException` | `A JSON record must be an object or an array, int given in "<uri>".` |

### 105) `flow-php/parquet` - `ArrowParquetEngine`, `ArrowParquetFileWriter` and the `arrow()` factories removed

| Before                                                                     | After                                                                    |
|----------------------------------------------------------------------------|--------------------------------------------------------------------------|
| `new ArrowParquetEngine($options)`                                         | `new RustParquetEngine($options)` - arrow-ext's class                    |
| `Reader::arrow($options)`                                                  | `new Reader(options: $options, engine: new RustParquetEngine($options))` |
| `Writer::arrow($compression, $options)`                                    | `new Writer($compression, $options, new RustParquetEngine())`            |
| `ArrowParquetEngine::mapCompression($compression)`                         | removed - `RustParquetFileWriter` takes the `Compressions` case          |
| `new ArrowParquetFileWriter(...)`                                          | `RustParquetEngine::openForWrite(...)`, a `RustParquetFileWriter`        |
| `new Reader(ByteOrder::BIG_ENDIAN)` with `arrow` loaded read through arrow | reads through `PhpParquetEngine`                                         |

`AdaptiveParquetEngine` stays the default of `Reader` and `Writer`. `Reader::php()` / `Writer::php()` stay.

### 106) `flow-php/etl` - `Flow\Floe\Exception\ExtensionException` removed

| Before                                              | After                                            |
|-----------------------------------------------------|--------------------------------------------------|
| `catch (Flow\Floe\Exception\ExtensionException $e)` | `catch (Flow\ETL\Exception\RuntimeException $e)` |

flow_php throws `Flow\ETL\Exception\RuntimeException`, plain `\Exception` when `flow-php/etl` is not autoloadable.
`FloeStreamReader` wraps it as `FloeException`, `FloeSerializer` as `SerializationException`, both with the original
as `previous`; `FloeStreamWriter` lets it through. Both catch the base `Flow\ETL\Exception\RuntimeException` now, so
its subclasses thrown while decoding are wrapped as well.

### 107) `flow-php/etl` - `RowsBuffer`, `RowEquality` and eleven `Rows` methods removed, `Rows::join*()` moved to

`RowsJoin`

| Before                                                                          | After                                                                              |
|---------------------------------------------------------------------------------|------------------------------------------------------------------------------------|
| `$rows->head($n)`                                                               | `$rows->take($n)`                                                                  |
| `$rows->tail($n)`                                                               | `$rows->slice(max(0, $rows->count() - $n), min($n, $rows->count()))`               |
| `$rows->takeRight($n)`                                                          | the `tail()` replacement - rows keep their order, not reversed                     |
| `$rows->sortAscending('id')` / `sortDescending('id')`                           | `$rows->sortBy(ref('id')->asc())` / `sortBy(ref('id')->desc())`                    |
| `$rows->unique()`                                                               | `df()->dropDuplicates()`                                                           |
| `$rows->diffLeft($other)`, `diffRight()`, `hash()`, `reverse()`                 | removed, no replacement                                                            |
| `$left->joinCross($right, $prefix)`                                             | `(new RowsJoin($backend))->cross($left, $right, $prefix)`                          |
| `$left->joinInner($right, $on)`, `joinLeft`, `joinLeftAnti`, `joinRight`        | `(new RowsJoin($backend))->inner($left, $right, $on)`, `left`, `leftAnti`, `right` |
| `new RowsBuffer(...)`, `new RowEquality()`                                      | removed, no replacement                                                            |
| `$fileColumns->apply($rows)`, `$partitionColumns->apply($rows, $names, $types)` | `FileConstants::fillRows()`                                                        |
| `$rows->merge($other)`                                                          | `$rows->concat($backend, $other)`                                                  |

`RowsJoin` is `Flow\ETL\Join\RowsJoin`.

`concat()` requires every part to carry the receiver's schema, an empty receiver too - `merge()` adopted the other
batch's schema when the receiver was empty. Start an accumulator from `null` instead of `rows(schema())`:

```php
// before
$all = rows(schema());
foreach ($batches as $batch) {
    $all = $all->merge($batch);
}

// after
$all = null;
foreach ($batches as $batch) {
    $all = $all === null ? $batch : $all->concat($context->backend(), $batch);
}
```

### 108) `flow-php/etl-adapter-xml` - `XMLReaderExtractor` removed

| Before                                        | After                                            |
|-----------------------------------------------|--------------------------------------------------|
| `new XMLReaderExtractor($path, $xmlNodePath)` | `from_xml($path)->withXMLNodePath($xmlNodePath)` |

### 109) `flow-php/arrow-ext` - `Flow\Arrow\RandomAccessFile` and `Flow\Arrow\OutputStream` removed

| Before                                                           | After                                   |
|------------------------------------------------------------------|-----------------------------------------|
| `class S implements Flow\Arrow\RandomAccessFile`, `OutputStream` | removed - no arrow-ext class reads them |

### 110) `flow-php/etl` - `Parameter::asObjects()` and `Parameter::asFloats()` removed

| Before                               | After                                                                                 |
|--------------------------------------|---------------------------------------------------------------------------------------|
| `$parameter->asObjects($rows, $ctx)` | `$parameter->asInstancesOf($rows, $ctx, $class)` or `$parameter->values($rows, $ctx)` |
| `$parameter->asFloats($rows, $ctx)`  | `$parameter->asNumbers($rows, $ctx)`                                                  |

### 111) `flow-php/etl` - `FileConstants::fill()` and `PartitionColumns::fill()` removed

| Before                                                        | After                                                                                                       |
|---------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------|
| `$constants->fill($row)`                                      | `[...$row, ...$constants->values()]` per row, or `$constants->fillRows($rows, $schema, $backend)` per batch |
| `$partitionColumns->fill($row, $names, $values)`              | `FileConstants::values()`                                                                                   |
| `new FileConstants($partitionColumns, $uri, $names, $values)` | `new FileConstants($uri, $names, $values)`                                                                  |

### 112) `flow-php/etl-adapter-json` - `JsonLinesExtractor` and `JsonLinesLoader` removed

| Before                                           | After                                                                                |
|--------------------------------------------------|--------------------------------------------------------------------------------------|
| `new JsonLinesExtractor($path, $filesystem)`     | `new JsonExtractor($path, $filesystem, JsonFormat::Lines)`                           |
| `new JsonLinesLoader($path, $filesystem)`        | `new JsonLoader($path, $filesystem, JsonFraming::Lines)`                             |
| `from_json_lines()` returns `JsonLinesExtractor` | returns `JsonExtractor`                                                              |
| `to_json_lines()` returns `JsonLinesLoader`      | returns `JsonLoader`; `withRowsInNewLines()` on it throws `InvalidArgumentException` |

### 113) `flow-php/etl-adapter-json`, `flow-php/flow-php-ext` - `*JSONEncoder` renamed `*JsonEncoder`

| Before                                  | After                                   |
|-----------------------------------------|-----------------------------------------|
| `Flow\ETL\Adapter\JSON\JSONEncoder`     | `Flow\ETL\Adapter\JSON\JsonEncoder`     |
| `PhpJSONEncoder`, `AdaptiveJSONEncoder` | `PhpJsonEncoder`, `AdaptiveJsonEncoder` |
| `RustJSONEncoder` (flow_php)            | `RustJsonEncoder`                       |

### 114) `flow-php/etl-adapter-csv`, `-excel`,

`-google-sheet` - one record rule: header names, absent cells, blank leading rows

Header names follow the CSV rule in all three: scalar as text, trimmed, a blank cell named `e` + its position.

```
header row [' id ', '', 'name']
Excel        before [' id ', '', 'name']   after ['id', 'e01', 'name']
Google Sheet before [' id ', '', 'name']   after ['id', 'e01', 'name']
```

|                                                                    | Before                                      | After                                                                                          |
|--------------------------------------------------------------------|---------------------------------------------|------------------------------------------------------------------------------------------------|
| CSV `withEmptyToNull(false)`, short line `1,a` under `id,name,age` | `['1', 'a', '']`                            | `['1', 'a', null]` - an absent cell is null, `emptyToNull` only turns a present `''` into null |
| Excel header cell that is not scalar (a date)                      | `''`                                        | `e` + its position                                                                             |
| Google Sheet `withHeader(false)`, a blank first row                | the blank row read as a row with no columns | skipped; the next row sizes the `e00`... names                                                 |

### 115) `flow-php/etl-adapter-xml` - `XMLLoader::write()` removed

| Before                               | After                                                                   |
|--------------------------------------|-------------------------------------------------------------------------|
| `$loader->write($rows, $partitions)` | `$loader->load($rows, $context)` - partitions come from `partitionBy()` |

### 116) `flow-php/etl` - the configured `Backend` reaches every batch: `Rows`, joins, Floe writers

| Before                                                             | After                                                                                               |
|--------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------|
| `$rows->matchTo($schema)`                                          | `$rows->matchTo($schema, $context->backend())`                                                      |
| `$rows->project($schema)`                                          | `$rows->project($schema, $context->backend())`                                                      |
| `rows($schema)` (DSL)                                              | `rows($schema)` - builds in `new AdaptiveBackend()`; `rows($schema, $backend)` builds in `$backend` |
| `$rows->project($rows->schema()->keep(...$names))` to keep columns | `$rows->select(...$names)` - the columns as they are, in schema order, no conversion                |
| `new FloeWriter($filesystem, $schema, $options)`                   | `new FloeWriter($filesystem, $schema, $backend, $options)`                                          |
| `new FloeStreamWriter($schema, $options)`                          | `new FloeStreamWriter($schema, $backend, $options)`                                                 |

Every batch an extractor yields is adopted into the configured backend before the first step sees it - a custom
extractor that builds `PhpBackend` rows reads as `RustColumn`s under `flow_php` with the default backend, and native
rows read as PHP columns under `config_builder()->backend(new PhpBackend())`. `$rows->concat($backend, ...)` returns
columns of `$backend` too.

`Rows::matchTo()` restamps a changed column whose type change the types prove (same kind at every level, no nested
optional → required) and reads values only for any other change; a null under NOT NULL is reported at its row.

### 117) `flow-php/etl` - `ColumnBuilder::appendPhysicals()` added, `PhpColumnBuilder::appendPhysicalMany()` renamed

| Before                                                          | After                                                                                      |
|-----------------------------------------------------------------|--------------------------------------------------------------------------------------------|
| `class MyBuilder implements ColumnBuilder`                      | must add `public function appendPhysicals(array $physicals, ?int $nullCount = null): void` |
| `$phpColumnBuilder->appendPhysicalMany($physicals, $nullCount)` | `$phpColumnBuilder->appendPhysicals($physicals, $nullCount)`                               |

### 118) `flow-php/etl` - `DeclaredColumns` removed

| Before                                                                     | After                                                                    |
|----------------------------------------------------------------------------|--------------------------------------------------------------------------|
| `array_to_rows((new DeclaredColumns())->project($rows, $schema), $schema)` | `(new RowsBuilder($schema, $backend))->appendProjected($rows)->finish()` |

`RowsBuilder::appendRecords($data)` is what `array_to_rows()` does; the DSL function delegates to it.

### 119) `flow-php/etl-adapter-postgresql` - `InsertQueryBuilder::build()` takes columns

| Before                                                                     | After                                                                                       |
|----------------------------------------------------------------------------|---------------------------------------------------------------------------------------------|
| `$builder->build($encoder->encode($rows), $schema, $converters, $options)` | `$builder->build($encoder->columns($rows), $rows->count(), $schema, $converters, $options)` |

### 120) `flow-php/etl-adapter-http` - `from_static_http_requests()` takes `withBatchSize()` and honours the limit

```
3 requests, from_static_http_requests($client, $requests)
before: 3 batches  [1 row] [1 row] [1 row]
after:  3 batches  [1 row] [1 row] [1 row]      (default batch size 1, as before)

3 requests, ->withBatchSize(2)
before: no withBatchSize()
after:  2 batches  [2 rows] [1 row]

extract($context, limit: 2) over 3 requests, ->withBatchSize(5)
before: no withBatchSize(); the pushed limit was ignored
after:  1 batch [2 rows], 2 requests sent
```

### 121) `flow-php/etl-adapter-excel` - markup cells written, nested values rendered as text, styler gets the cell

|                                            | Before                                                                                    | After                                                   |
|--------------------------------------------|-------------------------------------------------------------------------------------------|---------------------------------------------------------|
| `html`, `html_element`, `xml_element` cell | empty                                                                                     | its markup, e.g. `<a b="1"><c></c></a>`                 |
| a datetime inside a list / map / structure | `{"date":"2026-01-02 03:04:05.000000","timezone_type":3,"timezone":"UTC"}`                | `"2026-01-02T03:04:05+00:00"`                           |
| a float inside a list / map / structure    | `{"a":1}`                                                                                 | `{"a":1.0}`                                             |
| a time inside a list / map / structure     | `{"y":0,"m":0,"d":0,"h":1,"i":2,"s":3,"f":0,"invert":0,"days":false,"from_string":false}` | `3723000000` (microseconds, as a time column stores it) |
| `CellStyler::style($value, ...)`           | the logical value (an enum case, a `Uuid`, ...)                                           | the cell as written (`'one'`, `'f47ac10b-...'`, ...)    |

### 122) `flow-php/etl-adapter-seal` - an `xml_element` field keeps an end tag

| Before              | After                  |
|---------------------|------------------------|
| `<a b="1"><c/></a>` | `<a b="1"><c></c></a>` |

### 123) `flow-php/etl-adapter-doctrine`, `flow-php/etl-adapter-postgresql` - markup bound as rendered text

| Before                                                                                | After                                                         |
|---------------------------------------------------------------------------------------|---------------------------------------------------------------|
| `xml_element` `<a b="1"><c/></a>`                                                     | `<a b="1"><c></c></a>`                                        |
| PostgreSQL `xml` `<?xml version="1.0"?>\n<root>...</root>\n`                          | `<root>...</root>`                                            |
| PostgreSQL `xml` with a DOCTYPE, comments or processing instructions outside the root | the root element only - they are dropped, as DBAL already did |

### 124) `flow-php/etl` - `ResultColumn` takes the `Backend`; functions build in the configured backend

| Before                                                                                                  | After                                                              |
|---------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------|
| `(new ResultColumn())->of($this, $values)` in a custom function                                         | `(new ResultColumn($context->backend()))->of($this, $values)`      |
| a function's result under `flow_php` - a PHP column, copied into the extension's buffers after the step | a column of the configured backend (`RustColumn` under `flow_php`) |
| `lit()` / `on_each()` columns                                                                           | built by the configured backend                                    |

### 125) `flow-php/etl` - file reading and writing plumbing moved to `Extractor\File` and `Loader\File`

Class names are unchanged; only the namespace moves. `Statistics`, `FilesExtractor`, `PathPartitionsExtractor`,
`Partitioning`, `PartitioningLoader` and `SaveMode` stay where they are. `FilesSink::touched()` and
`FilesSink::openStreams()` are removed, no replacement.

| Before                                        | After                                              |
|-----------------------------------------------|----------------------------------------------------|
| `Flow\ETL\Extractor\FileExtractor`            | `Flow\ETL\Extractor\File\FileExtractor`            |
| `Flow\ETL\Extractor\FileReading`              | `Flow\ETL\Extractor\File\FileReading`              |
| `Flow\ETL\Extractor\SourceFile`               | `Flow\ETL\Extractor\File\SourceFile`               |
| `Flow\ETL\Extractor\FileColumns`              | `Flow\ETL\Extractor\File\FileColumns`              |
| `Flow\ETL\Extractor\FileConstants`            | `Flow\ETL\Extractor\File\FileConstants`            |
| `Flow\ETL\Extractor\PartitionColumns`         | `Flow\ETL\Extractor\File\PartitionColumns`         |
| `Flow\ETL\Extractor\PartitionTypes`           | `Flow\ETL\Extractor\File\PartitionTypes`           |
| `Flow\ETL\Extractor\DeclaresPartitionTypes`   | `Flow\ETL\Extractor\File\DeclaresPartitionTypes`   |
| `Flow\ETL\Extractor\PathFiltering`            | `Flow\ETL\Extractor\File\PathFiltering`            |
| `Flow\ETL\Extractor\SelfDescribingFile`       | `Flow\ETL\Extractor\File\SelfDescribingFile`       |
| `Flow\ETL\Extractor\MetadataColumns`          | `Flow\ETL\Extractor\File\MetadataColumns`          |
| `Flow\ETL\Extractor\MetadataColumnsExtractor` | `Flow\ETL\Extractor\File\MetadataColumnsExtractor` |
| `Flow\ETL\Loader\FileLoader`                  | `Flow\ETL\Loader\File\FileLoader`                  |
| `Flow\ETL\Loader\PartitionRouter`             | `Flow\ETL\Loader\File\PartitionRouter`             |
| `Flow\ETL\Filesystem\FilesSink`               | `Flow\ETL\Loader\File\FilesSink`                   |
| `Flow\ETL\Extractor\SpillState`               | `Flow\ETL\Extractor\Memory\SpillState`             |
| `Flow\ETL\Extractor\SpilledRows`              | `Flow\ETL\Extractor\Memory\SpilledRows`            |
| `Flow\ETL\Extractor\InMemoryRows`             | `Flow\ETL\Extractor\Memory\InMemoryRows`           |
| `Flow\ETL\Extractor\InferredRows`             | `Flow\ETL\Extractor\Memory\InferredRows`           |

### 126) `flow-php/etl` - `FloeSerializer` and the caches take the backend and the serializer explicitly

| Before                                                     | After                                                                                   |
|------------------------------------------------------------|-----------------------------------------------------------------------------------------|
| `new FloeSerializer()`, `new FloeSerializer($batchSize)`   | `new FloeSerializer($backend)`, `new FloeSerializer($backend, $batchSize)`              |
| `new FilesystemCache($filesystem, $cacheDir, $serializer)` | `new FilesystemCache($filesystem, $serializer, $cacheDir)`                              |
| `new PSRSimpleCache($cache, $ttl, $serializer)`            | `new PSRSimpleCache($cache, $serializer, $ttl)`                                         |
| `new PSRCacheBuckets($cache, $prefix, $ttl, $serializer)`  | `new PSRCacheBuckets($cache, $serializer, $prefix, $ttl)`                               |
| `filesystem_cache($cacheDir, $filesystem, $serializer)`    | `filesystem_cache($serializer, $cacheDir, $filesystem)`                                 |
| `new ApcuCache($namespace)`                                | `new ApcuCache($serializer, $namespace)` - entries stored as serialized strings         |
| `CacheIndex::toRows()`                                     | `toRows(Backend)`                                                                       |
| `new Config(..., HashRepartitionConfig, Calculator, ...)`  | `new Config(..., HashRepartitionConfig, Backend, Calculator, ...)` - `Backend` required |

To only move the cache directory, keep the default cache - it shares the pipeline's serializer and backend:

```php
// before
config_builder()->cache(filesystem_cache($dir));

// after
config_builder()->cacheDir($dir);
```

Build a cache yourself only for a custom serializer or filesystem; it has no default serializer any more and takes it
from its caller, e.g. `filesystem_cache(new FloeSerializer(new AdaptiveBackend()), $dir)`.

### 127) `flow-php/filesystem` - `Path::uri()` of a local file keeps the root slash

|                                                          | Before                   | After                     |
|----------------------------------------------------------|--------------------------|---------------------------|
| `path('/data/orders.csv')->uri()`                        | `file://data/orders.csv` | `file:///data/orders.csv` |
| `path('/')->uri()`                                       | `file://`                | `file:///`                |
| `_input_file_uri`, `explain()` `Source:` of a local file | `file://data/orders.csv` | `file:///data/orders.csv` |

Other protocols are unchanged (`memory://path`, `s3://bucket/key`). Windows drive paths stay `file://C:/...`.

### 128) `flow-php/parquet` - `Option::BYTE_ARRAY_TO_STRING` removed

| Before                                                | After   |
|-------------------------------------------------------|---------|
| `$options->set(Option::BYTE_ARRAY_TO_STRING, $value)` | removed |

### 129) `flow-php/etl`, `flow-php/parquet`, adapters - a mismatched `flow_php` or arrow extension is refused

| Before                                                                              | After                                                                                      |
|-------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------|
| an extension built for another package version: `Class "...\RustBackend" not found` | `RuntimeException` `The loaded flow_php extension (<version>, ABI <n>) does not match ...` |

Rebuild or upgrade the extension with the package, or stop loading it.
