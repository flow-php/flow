# Upgrade Guide

[TOC]

This document provides guidelines for upgrading between versions of Flow PHP. Please follow the instructions for your
specific version to ensure a smooth upgrade process.

---

## Upgrading from 0.43.x to 0.44.x

### 1) `flow-php/etl-adapter-json` - `to_json()`/`to_json_lines()` write list/map/structure/array entries as nested JSON

| Before                                                            | After                                          |
|-------------------------------------------------------------------|------------------------------------------------|
| `{"tags":"[1,2,3]"}` (escaped JSON string)                        | `{"tags":[1,2,3]}`                             |
| nested `DateTimeInterface`/`Uuid`/`UnitEnum` `json_encode`d as-is | datetime format / canonical string / case name |
| non-array value under a container type → `""`                     | `null`                                         |

### 2) `flow-php/etl` - Floe inferred schema keeps the first batch's nullability

| Before                                                            | After                                           |
|-------------------------------------------------------------------|-------------------------------------------------|
| inferred schema - every column nullable                           | nullability taken from the first batch's values |
| `null` in batch ≥ 2 in a column non-nullable in batch 1 - written | throws                                          |

Columns that may only become null in later batches, declare the schema explicitly:
`to_floe($path)->withSchema($schema)`.

### 3) `flow-php/types` - `detectType([])` returns `array{}`

| Before                                                  | After                |
|---------------------------------------------------------|----------------------|
| `(new TypeDetector())->detectType([])` → `array<mixed>` | `array{}`            |
| -                                                       | `type_empty_array()` |

### 4) `flow-php/types` - array type detection never returns a type that rejects its own input

| Before                                                            | After                 |
|-------------------------------------------------------------------|-----------------------|
| `detectType([5 => 'a', 6 => []])` → `map<integer, string>`        | `array<mixed>`        |
| `detectType(['a', []])` → `list<string>`                          | `array<mixed>`        |
| `detectType([['id' => '1'], []])` → `list<structure{id: string}>` | `list<array<mixed>>`  |
| `detectType([[1, 2], [null]])` → `list<list<integer>>`            | `list<array<mixed>>`  |
| `detectType([[1.2], [4.0, 5]])` → `list<list<float>>`             | `list<array<mixed>>`  |
| `detectType([[], [1, 2]])` → `list<array<mixed>>`                 | `list<list<integer>>` |

### 5) `flow-php/types` - structure/list/map casting no longer fabricates missing data

| Before                                                                                                                                 | After                     |
|----------------------------------------------------------------------------------------------------------------------------------------|---------------------------|
| `type_structure(['id' => type_integer(), 'name' => type_string()])->cast(['id' => 1])` → `['id' => 1, 'name' => '']`                   | throws `CastingException` |
| same type, `->cast(['id' => 1, 'name' => null])` → `['id' => 1, 'name' => '']`                                                         | throws `CastingException` |
| same type, `->cast([])`, `->cast(null)` → `['id' => 0, 'name' => '']`                                                                  | throws `CastingException` |
| `type_structure(['id' => type_integer()], ['name' => type_string()])->cast(['id' => 1, 'name' => null])` → `['id' => 1, 'name' => '']` | throws `CastingException` |
| `type_list(type_string())->cast(null)` → `['']`                                                                                        | throws `CastingException` |
| `type_structure(['id' => type_integer()])->cast('{"id":"1"}')` → throws                                                                | `['id' => 1]`             |
| `type_list(type_integer())->cast('["1","2"]')` → throws                                                                                | `[1, 2]`                  |

### 6) `flow-php/etl` - `FilesystemStreams` replaced by `FilesSink`, and a failed run discards its sink

| Before                                                                                  | After                                                                        |
|-----------------------------------------------------------------------------------------|------------------------------------------------------------------------------|
| `Flow\ETL\Filesystem\FilesystemStreams`                                                 | `Flow\ETL\Filesystem\FilesSink`                                              |
| `FilesystemStreams::FLOW_TMP_FILE_PREFIX`                                               | `FilesSink::FLOW_TMP_FILE_PREFIX`                                            |
| `new FilesystemStreams()` + `->setMode($mode)`                                          | `new FilesSink($filesystem, $destination, $mode)`                            |
| `$streams->writeTo($filesystem, $path, $partitions)`                                    | `$files->writeTo($partitions)`                                               |
| `$streams->isOpen($path, $partitions)`                                                  | `$files->touched($partitions)`                                               |
| `$streams->listOpenStreams($path)`                                                      | `$files->openStreams()`                                                      |
| `$streams->closeStreams($filesystem, $path)`                                            | `$files->publish()` / `$files->abandon()`                                    |
| `$streams->read()` / `->rm()` / `->exists()` / `count()` / `getIterator()`              | removed - use the `Filesystem` directly                                      |
| `saveMode()` on one `DataFrame` applied to every later `DataFrame` on the same `Config` | `saveMode()` is set on the sink and belongs to that sink alone               |
| a failed run left its `DestinationStream` registered; the retry appended to it          | the failed run's sink is discarded, the retry starts clean                   |
| an abandoned run left its `._flow_php_tmp.` file behind under `Overwrite`               | the abandoned run removes it and never renames it over the destination       |
| a failed run left its partial file at the destination under the other save modes        | it removes any file it created; a destination it did not create is untouched |
| `Config::filesystemStreams()`                                                           | removed                                                                      |
| `new Config(..., FilesystemStreams $filesystemStreams, ...)` constructor parameter      | removed                                                                      |

Build `Config` through `Config::builder()` / `Config::default()`.

`Closure::closure()` is unchanged and still marks a run that reached its last batch. A run that threw or was abandoned
now ends through the new `Flow\ETL\Loader\Discardable`:

```php
interface Discardable
{
    public function discard(FlowContext $context): void;
}
```

A `Loader` holding anything per run - an open stream, a format writer, a row counter - should implement it and drop that
state there, exactly as `closure()` does on the success path. A `Loader` that wraps another (`OverridingLoader`)
does **not** forward `discard()`: the pipeline walks the whole loader tree through `OverridingLoader::loaders()`, so a
wrapped sink is discarded whether or not its wrapper knows about `Discardable`. A wrapper implements it only to drop
state of its own.

### 7) `flow-php/etl` - `RetryLoader` no longer retries `InvalidLogicException` by default

| Before                                                               | After                                                       |
|----------------------------------------------------------------------|-------------------------------------------------------------|
| `new RetryLoader($loader)` default strategy `new AnyThrowable(3)`    | `new AnyThrowableExcept([InvalidLogicException::class], 3)` |
| `write_with_retries($loader)` default strategy `new AnyThrowable(3)` | `new AnyThrowableExcept([InvalidLogicException::class], 3)` |
| an `InvalidLogicException` was attempted 4 times with delays between | attempted once, no delay                                    |

`AnyThrowable` itself is unchanged. To keep retrying every throwable, pass it explicitly:

```php
write_with_retries($loader, retry_any_throwable(3));
```

New helper for the deny-list strategy:

```php
write_with_retries($loader, retry_any_throwable_except([InvalidLogicException::class], 3));
```

### 8) `flow-php/etl` - operations inside a `Transformation` answer for the whole stream

| Before                                                                                                                                                                   | After                                                                                                                                                                                     |
|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `$df->sortBy(ref('id'))` inside a `Transformation` sorted each batch on its own                                                                                          | sorts the whole stream                                                                                                                                                                    |
| `$df->aggregate(...)` / `groupBy()->aggregate()` / `pivot()` / window functions inside a `Transformation` answered per batch                                             | answer for the whole stream                                                                                                                                                               |
| `$df->offset(2)` inside a `Transformation` lost rows                                                                                                                     | skips exactly the offset across the stream                                                                                                                                                |
| `$df->cache($id)` inside a `Transformation` persisted one batch                                                                                                          | persists the whole stream                                                                                                                                                                 |
| `$df->batchBy(...)` / `$df->partitionBy(...)` / `batch_size(...)` cut chunks at the incoming batches                                                                     | cut chunks over the stream                                                                                                                                                                |
| `$df->join(...)` / `$df->partitionBy(...)` inside a `Transformation` emitted rows in input order                                                                         | emit rows grouped by key                                                                                                                                                                  |
| a `Transformation` calling `$df->fetch()` / `count()` / `schema()` silently answered over an empty stream                                                                | throws `InvalidLogicException`                                                                                                                                                            |
| `write_with_retries($loader)` around `to_transformation(...)` (any wrapped step) or around a `to_branch(...)` armed with `withTransformation(...)`, at any nesting depth | throws `InvalidLogicException` at the first `load()`, use `to_transformation(..., write_with_retries($loader))` or `to_branch(..., write_with_retries($loader))->withTransformation(...)` |
| `Flow\ETL\Extractor\SwappableRowsExtractor`                                                                                                                              | removed (internal `FeedExtractor` replaces it)                                                                                                                                            |

`sortBy()`, `aggregate()`, `groupBy()->aggregate()`, `pivot()`, window functions, `collect()` and `join()` buffer
proportional to the data, as on an outer frame.

### 9) `flow-php/etl` - `to_branch()->withTransformation()` drives its `Transformation` once over the whole stream

| Before                                                                                        | After                                                                                         |
|-----------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------|
| the `Transformation` ran on each filtered batch in its own `DataFrame`                        | one nested pipeline spans the stream                                                          |
| `$df->sortBy(...)` in a branch transformation sorted each batch alone                         | sorts the whole branch stream                                                                 |
| `$df->aggregate(...)` / `limit()` / other `Processor`-backed operations answered per batch    | answer once for the stream                                                                    |
| a `Transformation` calling `$df->fetch()` / `count()` / `schema()` returned per-batch answers | throws `InvalidLogicException`                                                                |
| the wrapped loader received exactly one `load()` per outer batch                              | receives output as the transformation produces it; blocking operations deliver at `closure()` |
| telemetry `flow.etl.loading.rows` counted post-filter, post-transformation rows               | counts the rows offered to the branch                                                         |

### 10) `flow-php/etl-adapter-doctrine` / `flow-php/etl-adapter-postgresql` - transactional loaders run

`closure()` inside a transaction

| Before                                                                                                      | After                                                                               |
|-------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------|
| `to_dbal_transaction()` / `to_pgsql_transaction()` never called `closure()` on wrapped loaders              | forwards `closure()` to every wrapped loader, inside one final transaction          |
| blocking operations inside a wrapped `Transformation` answered per batch, each batch in its own transaction | answer for the whole stream, delivered at `closure()` in a single transaction       |
| -                                                                                                           | a failure during the final transaction rolls back the drained delivery and rethrows |
| `withIsolationLevel()` applied to per-batch transactions                                                    | applies to every transaction the wrapper opens                                      |

### 11) `flow-php/etl` - Floe on-disk format v2, existing `.floe` files must be rewritten

| Before                      | After                                           |
|-----------------------------|-------------------------------------------------|
| header version byte `0x01`  | `0x02`                                          |
| uuid payload - 36 raw bytes | 4-byte little-endian length prefix + bytes      |
| reading a v1 file           | throws `Floe does not support format version 1` |

Rewrite existing files with the new writer: `data_frame()->read(from_floe($old))->write(to_floe($new))->run()`.

### 12) `flow-php/etl` - Floe rejects columns whose type is only known per value

| Before                                     | After                                                      |
|--------------------------------------------|------------------------------------------------------------|
| `list<mixed>` element - written with a tag | `Floe does not support values of type "mixed"`             |
| `union_schema()` column - written          | `Floe does not support columns of type "integer\|string"`  |
| `type_structure(..., allow_extra: true)`   | `Floe does not support structures that allow extra values` |
| map key other than `integer`/`string`      | `Floe does not support map keys of type "..."`             |

Thrown when the write session opens, before any bytes. Declare an element type, or use
`json_entry()` when the shape is genuinely dynamic.

### 13) `flow-php/etl` - Floe validates every value against its column type

| Before                                          | After                                |
|-------------------------------------------------|--------------------------------------|
| `'AB-1'` into an `integer` column - wrote `0`   | throws `IncompatibleSchemaException` |
| `1.5` into an `integer` column - wrote `1`      | throws                               |
| `1000` into a `string` column - raw `TypeError` | throws `IncompatibleSchemaException` |
| `int` into a `float` column - written           | throws                               |
| column absent from a row                        | unchanged, still written             |

`floe_options(validate_data: false)` skips the per-value check only; a row carrying an undeclared column is always
rejected.

### 14) `flow-php/etl` - aggregate result type follows the column, not the value

| Before                                                              | After            |
|---------------------------------------------------------------------|------------------|
| `sum()` over a `float` column, whole total - `int`                  | `float`          |
| `avg()`/`min()`/`max()` over a `float` column, whole result - `int` | `float`          |
| `sum()` over an `integer` column                                    | unchanged, `int` |

### 15) `flow-php/etl` - aggregates ignore a row missing the aggregated column

| Before                                                                                 | After               |
|----------------------------------------------------------------------------------------|---------------------|
| missing column in `ExecutionMode::STRICT` - `Sum error: Entry "amount" does not exist` | contributes nothing |
| missing column in lenient mode - contributed nothing                                   | unchanged           |

### 16) `flow-php/types` - `EnumType::isValid()` requires an object

| Before                                    | After                                               |
|-------------------------------------------|-----------------------------------------------------|
| `type_enum(Suit::class)->isValid('Suit')` | `false` (was `true`)                                |
| `type_enum(Suit::class)->assert('Suit')`  | throws `InvalidTypeException` (was raw `TypeError`) |
| `type_enum(Suit::class)->cast('Suit')`    | throws `CastingException` (was raw `TypeError`)     |

### 17)

`flow-php/etl` - filesystems are passed in, engine algorithms take a storage, and every operation can override its
algorithm

`Config` no longer carries a `FilesystemTable`. Every file source and sink takes a `Filesystem`
argument that defaults to the native local filesystem. Every engine algorithm - cache, sort, group by, join - takes a
**storage object**, never a filesystem and never a path. Reading or writing a non-`file://` path now requires passing
the filesystem.

#### Filesystems

| Before                                                                                 | After                                                                                        |
|----------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------|
| `config_builder()->mount(aws_s3_filesystem(...))` + `from_csv(path('aws-s3://x.csv'))` | `from_csv(path('aws-s3://x.csv'), filesystem: aws_s3_filesystem(...))`                       |
| `config_builder()->unmount($fs)`                                                       | removed - it had no callers                                                                  |
| `$config->fstab()`                                                                     | removed - use `Flow\Filesystem\DSL\fstab()` for `file_copy()` / `file_move()`                |
| `new Config(..., FilesystemTable $filesystemTable, ...)`                               | parameter removed                                                                            |
| `$context->filesystem($path)`                                                          | the source/sink owns its filesystem; pass `filesystem:` to change it                         |
| `$context->streams()->list($path, $filter)` -> `SourceStream`                          | `(new FileListing($filesystem))->list($path, $filter)` -> `FileStatus`                       |
| `$context->streams()->writeTo($path, $partitions)`                                     | `$this->files->writeTo($partitions)` on a sink-held `FilesSink` - see 6)                     |
| `$context->streams()->closeStreams($path)` in an extractor                             | removed - the read path never registered a stream                                            |
| `new FilesystemStreams($filesystemTable)`                                              | `new FilesSink($filesystem, $destination, $saveMode)` - see 6)                               |
| a custom file source reading through `FlowContext`                                     | take `Filesystem $filesystem = new NativeLocalFilesystem()` last                             |
| `schema_from_json_schema($s)` resolved local `$ref`s through `fstab()`                 | takes a trailing `Filesystem $filesystem = new NativeLocalFilesystem()`                      |
| a `$ref` on `memory://` or `stdout://`                                                 | throws - pass the filesystem that serves it                                                  |
| `ChartJSLoader::withOutputPath($path)` / `::withTemplate($path)`                       | both gain a trailing `Filesystem $filesystem = new NativeLocalFilesystem()`; `withOutputPath()` now throws on a path with no extension |
| `FilePathArgument::getExisting($input, $config)` / `::getNotExisting(...)` (CLI)        | the `Config` parameter is gone; the constructor takes `Filesystem` instead |
| a custom `FileLoader`                                                                  | implement `saveMode()` yourself, and implement `Discardable` - see 6)                        |
| a custom `Flow\Filesystem\Filesystem` implementation                                   | add `public function supports(Path $path): bool` - `return $this->mount()->supports($path);`. It is abstract on the interface, so without it the class is a **fatal at load**, not an error on first use |

`to_x(path('memory://...'), filesystem: memory_filesystem())` followed by
`from_x(path('memory://...'), filesystem: memory_filesystem())` now reads **no rows**: two
`memory_filesystem()` calls are two separate stores. Hold one `$fs = memory_filesystem()` and pass the same instance to
both.

#### Filesystem telemetry is switched off

`withTelemetry()` no longer traces filesystems. Reading a file, writing a file and engine spill emit **no**
`filesystem.read` / `filesystem.write` spans and no filesystem metrics. Cache tracing (`trace_cache`), loading tracing
(`trace_loading`), transformation tracing and DataFrame metrics are unaffected.

`TraceableFilesystem` and `traceable_filesystem()` still work - wrap the filesystem you pass in:

```php
$fs = traceable_filesystem(
    aws_s3_filesystem($bucket, $client),
    filesystem_telemetry_config($telemetry, $clock, filesystem_telemetry_options(traceStreams: true)),
);

data_frame()->read(from_csv(path('aws-s3://x.csv'), filesystem: $fs))->…
```

| Before                                                             | After                                           |
|--------------------------------------------------------------------|-------------------------------------------------|
| `telemetry_options(filesystem: filesystem_telemetry_options(...))` | removed - wrap the filesystem by hand           |
| `TelemetryOptions::filesystem()` / `->filesystem`                  | removed                                         |
| the pipeline-start debug log's `fstab` field                       | a `spill` field naming the three spill storages |

#### Engine algorithms

| Before                                                                              | After                                                         |
|-------------------------------------------------------------------------------------|---------------------------------------------------------------|
| `config_builder()->cacheFilesystem('s3')`                                           | `config_builder()->cache($cache)`                             |
| `external_sort()->filesystemProtocol('file')`                                       | `external_sort()->storage(new FilesystemBuckets($fs, $path))` |
| `hash_join()->filesystemProtocol(...)` / `hash_group_by()->filesystemProtocol(...)` | `->storage(BucketsStorage)`                                   |
| `CacheConfig::$filesystemMount`                                                     | removed                                                       |
| `SortAlgorithmBuilder::build(FilesystemTable, Path)`                                | `build(Path $spillRoot)`                                      |
| -                                                                                   | `external_sort()->mergeStorage($s)` - merged runs only        |

`filesystemProtocol()` and `cacheFilesystem()` never worked for any value but `'file'` - the spill root is always a
`file://` path, so anything else threw `InvalidSchemeException` at the first spill.

External sort now keeps **spill runs** and **merged runs** in two `Buckets`, each read back only through the storage
that wrote it. `external_sort()->storage($s)` still covers **both** phases - the merge storage defaults to the spill
storage - so nothing changes unless you call `mergeStorage()`.
`ExternalSortConfig::__construct` gained `?BucketsStorage $merge` as its **second** parameter, so positional
construction shifts; use named arguments.

#### Per-operation overrides, and the broken variadic

`sortBy()`, `groupBy()` and `aggregate()` take an **array** now, so that they can carry a per-operation algorithm.
`join()` and `cache()` gained a trailing optional argument. `Rows::sortBy()` is a different method and is unchanged.

| Before                                        | After                                                                               |
|-----------------------------------------------|-------------------------------------------------------------------------------------|
| `$df->sortBy(ref('a'), ref('b'))`             | `$df->sortBy([ref('a'), ref('b')])`                                                 |
| `$df->groupBy('a', 'b')`                      | `$df->groupBy(['a', 'b'])`                                                          |
| `$df->aggregate(sum(ref('a')))`               | `$df->aggregate([sum(ref('a'))])`                                                   |
| `$df->groupBy('a')->aggregate(sum(ref('b')))` | `$df->groupBy(['a'])->aggregate(sum(ref('b')))` - `aggregate()` here is unchanged   |
| -                                             | `$df->sortBy([ref('a')], external_sort()->storage(new MemoryBuckets()))`            |
| -                                             | `$df->groupBy(['a'], hash_group_by()->storage($s))`                                 |
| -                                             | `$df->join($right, $on, Join::left, hash_join()->storage($s))`                      |
| -                                             | `$df->cache('report', cache: $psrCache)` + `from_cache('report', cache: $psrCache)` |

`joinEach()` is **not** overridable: it joins in memory per batch and never touches a bucket storage.

#### Save mode, the CLI and the HTTP bridge

| Before                                                              | After                                                                       |
|---------------------------------------------------------------------|-----------------------------------------------------------------------------|
| `$df->saveMode(overwrite())` / `$df->mode(overwrite())`             | `to_csv($path)->saveMode(overwrite())` - per sink                           |
| `$df->saveMode(overwrite())->write(write_with_retries(to_csv($p)))` | `$df->write(write_with_retries(to_csv($p)->saveMode(overwrite())))`         |
| `$df->mode(SaveMode::Overwrite)`                                    | `DataFrame::mode()` takes `ExecutionMode` only                              |
| `to_text($path)` returned `Loader`                                  | returns `TextLoader`                                                        |
| `LoaderFactory::get()` returned `Loader`                            | returns `Loader&FileLoader`                                                 |
| `flow read --config .flow.php aws-s3://bucket/x.csv`                | `flow run pipeline.php`, with the filesystem built inside the pipeline file |
| `new FlowBufferedResponse(..., filesystem: 'memory')`               | `new FlowBufferedResponse(..., filesystem: new MemoryFilesystem())`         |
| `new FlowStreamedResponse(..., filesystem: 'stdout')`               | `new FlowStreamedResponse(..., filesystem: new StdOutFilesystem())`         |
| `Output::loader(Path $path)`                                        | `Output::loader(Path $path, Filesystem $filesystem)`                        |

The `flow:filesystem:*` commands, the Symfony filesystem bundle, `file_copy()` / `file_move()` and
`Flow\Filesystem\FilesystemTable` itself are unchanged - and `#[AsFilesystem('warehouse')]` now injects exactly what
`from_csv($path, filesystem: ...)` wants.

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

---

## Upgrading from 0.40.x to 0.41.x

### 1) Removal of Elasticsearch Adapter

The Elasticsearch adapter has been removed from Flow PHP and replaced by the [SEAL](https://php-cmsig.github.io/search/)
adapter (`flow-php/etl-adapter-seal`), a search engine abstraction layer that supports Elasticsearch, OpenSearch,
Meilisearch, Solr, Typesense, Algolia, RediSearch and Loupe.

To migrate, install the SEAL adapter together with the engine adapter for your backend:

```
composer require flow-php/etl-adapter-seal cmsig/seal-elasticsearch-adapter
```

Then build a `CmsIg\Seal\Engine` and pass it to `to_seal_upsert()` instead of the previous `to_es_bulk_index()` (or
Meilisearch) DSL functions:

```php
use CmsIg\Seal\Engine;
use CmsIg\Seal\Adapter\Elasticsearch\ElasticsearchAdapter;

use function Flow\ETL\Adapter\Seal\to_seal_upsert;

$engine = new Engine(
    new ElasticsearchAdapter($client),
    $schema,
);

data_frame()
    ->read(/* ... */)
    ->write(to_seal_upsert($engine, 'index_name'))
    ->run();
```

### 2) `flow-php/symfony-telemetry-bundle` -

`flow-php/symfony-http-foundation-telemetry-bridge` is now an optional dependency

| Before                               | After                                                       |
|--------------------------------------|-------------------------------------------------------------|
| installed transitively by the bundle | install explicitly to enable HTTP trace context propagation |

`instrumentation.http_kernel.context_propagation` is silently disabled when the bridge is absent. To keep extracting
incoming and injecting outgoing W3C trace headers:

```
composer require flow-php/symfony-http-foundation-telemetry-bridge
```

### 3) `flow-php/telemetry` - trace id derived from the active span; root spans start a new trace

| Before                                  | After                                                          |
|-----------------------------------------|----------------------------------------------------------------|
| `Context::create()`                     | `Context::root()`                                              |
| `Context::withTraceId(TraceId)`         | removed                                                        |
| `$context->traceId` (property)          | `$context->traceId(): ?TraceId` (derived from the active span) |
| `Context::withActiveSpan(SpanId)`       | `Context::withActiveSpan(SpanContext)`                         |
| `context(?TraceId, ?Baggage)` (DSL)     | `context(?Baggage)`                                            |
| a root span reused the context trace id | each root span generates a new `TraceId`                       |

`Context` no longer stores a standalone trace id; attach the active span as a `SpanContext` to keep subsequent spans in
the same trace.

### 4) `flow-php/symfony-telemetry-bundle` - each consumed Messenger message is its own trace

| Before                                                     | After                                                            |
|------------------------------------------------------------|------------------------------------------------------------------|
| all messages in a `messenger:consume` run shared one trace | each handled message is a new trace root, linked to the producer |

### 5) `flow-php/telemetry-otlp-bridge`, `flow-php/symfony-telemetry-bundle` - curl is synchronous; async moved to

`async_curl`

| Before                                                         | After                                                                                           |
|----------------------------------------------------------------|-------------------------------------------------------------------------------------------------|
| `CurlTransport` async (`curl_multi`, fire-and-forget `send()`) | `CurlTransport` synchronous; `send()` blocks and throws on failure                              |
| async via `CurlTransport`                                      | async via `AsyncCurlTransport` / `otlp_async_curl_transport()` / `transport.type: 'async_curl'` |
| `CurlTransportOptions::DEFAULT_TIMEOUT_MS` `250`               | `10000`                                                                                         |
| bundle `curl` `timeout_ms` `250`                               | `10000`                                                                                         |
| -                                                              | `async_curl` `connect_timeout_ms` `1500`, `pump_timeout_ms` `100`                               |

### 6) `flow-php/symfony-postgresql-bundle` - migrations run against a single configured connection

| Before                                         | After                                                                              |
|------------------------------------------------|------------------------------------------------------------------------------------|
| `flow:migrations:* --connection=<name>` (`-c`) | removed - every migration command uses the configured migrations connection        |
| migrator stack registered for every connection | registered only for the migrations connection                                      |
| -                                              | `flow_postgresql.migrations.connection: <name>` (defaults to the first connection) |

To run migrations against a non-default connection, set `migrations.connection` instead of passing `-c`:

```yaml
flow_postgresql:
  migrations:
    enabled: true
    connection: analytics
```

### 7) `flow-php/symfony-telemetry-bundle` - static resource cache file is keyed by kernel environment

| Before                                             | After                                                           |
|----------------------------------------------------|-----------------------------------------------------------------|
| `sys_get_temp_dir()/flow_telemetry_resource.cache` | `sys_get_temp_dir()/flow_telemetry_resource_<kernel.env>.cache` |

Delete the orphaned `flow_telemetry_resource.cache` from the temp dir; a per-env file is written on the next run.

### 8) `flow-php/symfony-telemetry-bundle` - messenger worker receive cycle is traced; `link_to_worker` config removed

| Before                                             | After                                                    |
|----------------------------------------------------|----------------------------------------------------------|
| `instrumentation.messenger.link_to_worker: true`   | removed (always on under `messenger:consume`)            |
| `flow.messenger.worker` link → console worker span | → per-pass `messenger.receive` span                      |
| transport poll query = one standalone trace each   | grouped under one `messenger.receive` root span per pass |

Remove the `link_to_worker` key from config. Idle passes (no message received) are marked
`messaging.symfony.worker.idle: true`.

### 9) `flow-php/symfony-telemetry-bundle` - messenger instrumentation also emits messaging metrics

| Before                                       | After                                                                                                           |
|----------------------------------------------|-----------------------------------------------------------------------------------------------------------------|
| messenger instrumentation emitted spans only | also emits `messaging.client.consumed.messages`, `messaging.client.sent.messages`, `messaging.process.duration` |

Disable with `instrumentation.messenger.metrics: false`; set the `messaging.process.duration` unit via
`instrumentation.messenger.metrics_duration_unit` (`s` default, or `ms`).

### 10) `flow-php/etl`, `flow-php/filesystem`, `flow-php/postgresql`, `flow-php/symfony-telemetry-bundle`,

`flow-php/phpunit-telemetry-bridge` - emitted metric names standardized

| Before                      | After                              |
|-----------------------------|------------------------------------|
| `rows_processed`            | `flow.etl.rows.processed`          |
| `rows_throughput`           | `flow.etl.rows.throughput`         |
| `cache_hits`                | `flow.cache.hits`                  |
| `cache_misses`              | `flow.cache.misses`                |
| `cache.hits`                | `flow.cache.hits`                  |
| `cache.misses`              | `flow.cache.misses`                |
| `write_size`                | `flow.filesystem.write.size`       |
| `write_operations`          | `flow.filesystem.write.operations` |
| `read_size`                 | `flow.filesystem.read.size`        |
| `read_operations`           | `flow.filesystem.read.operations`  |
| `operation_duration`        | `db.client.operation.duration`     |
| `response_returned_rows`    | `db.client.response.returned_rows` |
| `phpunit.test.duration`     | `flow.phpunit.test.duration`       |
| `phpunit.test.count`        | `flow.phpunit.test.count`          |
| `phpunit.test.memory.peak`  | `flow.phpunit.test.memory.peak`    |
| `phpunit.test.memory.delta` | `flow.phpunit.test.memory.delta`   |
| `phpunit.suite.duration`    | `flow.phpunit.suite.duration`      |
| `phpunit.suite.test_count`  | `flow.phpunit.suite.test_count`    |

Rename these series in dashboards and alerts.

### 11) `flow-php/symfony-telemetry-bundle` - messenger `trace`/`link` config; `propagation_style` removed

| Before                                        | After                                                                          |
|-----------------------------------------------|--------------------------------------------------------------------------------|
| `instrumentation.messenger.propagation_style` | removed (consumed messages always start their own trace)                       |
| (no span selection)                           | `instrumentation.messenger.trace`: `worker`/`handlers`/`both` (default)/`none` |
| (no link selection)                           | `instrumentation.messenger.link`: `dispatcher`/`worker`/`both` (default)       |

Remove `propagation_style` from config - `continue` mode is gone (it made the consumer span absorb the queue wait time).
`trace` selects which spans are emitted (`handlers` = the `process`/`send` message spans; `none` = metrics only);
`link` selects the consumer span's links. `link: worker`/`both` requires `trace` to include the worker, otherwise the
config is rejected. When `trace` excludes the worker, the transport's poll instrumentation (Doctrine DBAL, HTTP
client, ...) is suppressed during the receive loop so it does not surface as orphan spans - no `messenger.receive` span,
no orphans either way.

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

---

## Upgrading from 0.34.x to 0.35.x

### 1) `flow-php/postgresql` - `DataType` renamed to `ColumnType`

The `DataType` class used for schema/DDL definitions has been renamed to `ColumnType` to better communicate its purpose.
All related DSL functions have been renamed from `data_type_*` to `column_type_*`.

| Removed                                        | Replacement                                      |
|------------------------------------------------|--------------------------------------------------|
| `Flow\PostgreSql\QueryBuilder\Schema\DataType` | `Flow\PostgreSql\QueryBuilder\Schema\ColumnType` |
| `Flow\PostgreSql\Parser\DataTypeParser`        | `Flow\PostgreSql\Parser\ColumnTypeParser`        |
| `data_type_integer()`                          | `column_type_integer()`                          |
| `data_type_smallint()`                         | `column_type_smallint()`                         |
| `data_type_bigint()`                           | `column_type_bigint()`                           |
| `data_type_boolean()`                          | `column_type_boolean()`                          |
| `data_type_text()`                             | `column_type_text()`                             |
| `data_type_varchar()`                          | `column_type_varchar()`                          |
| `data_type_char()`                             | `column_type_char()`                             |
| `data_type_numeric()`                          | `column_type_numeric()`                          |
| `data_type_decimal()`                          | `column_type_decimal()`                          |
| `data_type_real()`                             | `column_type_real()`                             |
| `data_type_double_precision()`                 | `column_type_double_precision()`                 |
| `data_type_date()`                             | `column_type_date()`                             |
| `data_type_time()`                             | `column_type_time()`                             |
| `data_type_timestamp()`                        | `column_type_timestamp()`                        |
| `data_type_timestamptz()`                      | `column_type_timestamptz()`                      |
| `data_type_interval()`                         | `column_type_interval()`                         |
| `data_type_uuid()`                             | `column_type_uuid()`                             |
| `data_type_json()`                             | `column_type_json()`                             |
| `data_type_jsonb()`                            | `column_type_jsonb()`                            |
| `data_type_bytea()`                            | `column_type_bytea()`                            |
| `data_type_inet()`                             | `column_type_inet()`                             |
| `data_type_cidr()`                             | `column_type_cidr()`                             |
| `data_type_macaddr()`                          | `column_type_macaddr()`                          |
| `data_type_serial()`                           | `column_type_serial()`                           |
| `data_type_smallserial()`                      | `column_type_smallserial()`                      |
| `data_type_bigserial()`                        | `column_type_bigserial()`                        |
| `data_type_array()`                            | `column_type_array()`                            |
| `data_type_custom()`                           | `column_type_custom()`                           |
| `data_type_from_string()`                      | `column_type_from_string()`                      |

Before:

```php
use Flow\PostgreSql\QueryBuilder\Schema\DataType;
use function Flow\PostgreSql\DSL\data_type_integer;
use function Flow\PostgreSql\DSL\data_type_varchar;

column('age', data_type_integer());
column('name', data_type_varchar(255));
cast(ref('id'), DataType::bigint());
```

After:

```php
use Flow\PostgreSql\QueryBuilder\Schema\ColumnType;
use function Flow\PostgreSql\DSL\column_type_integer;
use function Flow\PostgreSql\DSL\column_type_varchar;

column('age', column_type_integer());
column('name', column_type_varchar(255));
cast(ref('id'), ColumnType::bigint());
```

### 2) `flow-php/postgresql` - `PostgreSqlType` renamed to `ValueType`

The `PostgreSqlType` enum used for value binding/casting has been renamed to `ValueType` to better communicate its
purpose. All related DSL functions have been renamed from `pgsql_type_*` to `value_type_*`.

| Removed                                       | Replacement                              |
|-----------------------------------------------|------------------------------------------|
| `Flow\PostgreSql\Client\Types\PostgreSqlType` | `Flow\PostgreSql\Client\Types\ValueType` |
| `pgsql_type_text()`                           | `value_type_text()`                      |
| `pgsql_type_varchar()`                        | `value_type_varchar()`                   |
| `pgsql_type_integer()`                        | `value_type_integer()`                   |
| `pgsql_type_bigint()`                         | `value_type_bigint()`                    |
| `pgsql_type_smallint()`                       | `value_type_smallint()`                  |
| `pgsql_type_boolean()`                        | `value_type_boolean()`                   |
| `pgsql_type_float4()`                         | `value_type_float4()`                    |
| `pgsql_type_float8()`                         | `value_type_float8()`                    |
| `pgsql_type_numeric()`                        | `value_type_numeric()`                   |
| `pgsql_type_date()`                           | `value_type_date()`                      |
| `pgsql_type_timestamp()`                      | `value_type_timestamp()`                 |
| `pgsql_type_timestamptz()`                    | `value_type_timestamptz()`               |
| `pgsql_type_json()`                           | `value_type_json()`                      |
| `pgsql_type_jsonb()`                          | `value_type_jsonb()`                     |
| `pgsql_type_uuid()`                           | `value_type_uuid()`                      |
| `pgsql_type_bytea()`                          | `value_type_bytea()`                     |
| `pgsql_type_inet()`                           | `value_type_inet()`                      |
| `pgsql_type_cidr()`                           | `value_type_cidr()`                      |
| All other `pgsql_type_*()` functions          | Corresponding `value_type_*()` functions |

Before:

```php
use Flow\PostgreSql\Client\Types\PostgreSqlType;
use function Flow\PostgreSql\DSL\pgsql_type_uuid;
use function Flow\PostgreSql\DSL\pgsql_type_text_array;

typed('550e8400-e29b-41d4-a716-446655440000', pgsql_type_uuid());
typed(['tag1', 'tag2'], pgsql_type_text_array());
typed(42, PostgreSqlType::INT4);
```

After:

```php
use Flow\PostgreSql\Client\Types\ValueType;
use function Flow\PostgreSql\DSL\value_type_uuid;
use function Flow\PostgreSql\DSL\value_type_text_array;

typed('550e8400-e29b-41d4-a716-446655440000', value_type_uuid());
typed(['tag1', 'tag2'], value_type_text_array());
typed(42, ValueType::INT4);
```

---

## Upgrading from 0.31.x to 0.32.x

### 1) Removal of Meilisearch Adapter

The Meilisearch adapter has been removed from Flow PHP. If you were using it, please migrate to Elasticsearch adapter.

### 2) Removed deprecated DSL functions

All type-related DSL functions have been moved from `Flow\ETL\DSL` to `Flow\Types\DSL`. Update your imports accordingly.

| Removed Function          | Replacement                               |
|---------------------------|-------------------------------------------|
| `chunks_from()`           | `batches()`                               |
| `type_structure()`        | `\Flow\Types\DSL\type_structure()`        |
| `type_union()`            | `\Flow\Types\DSL\type_union()`            |
| `type_optional()`         | `\Flow\Types\DSL\type_optional()`         |
| `type_from_array()`       | `\Flow\Types\DSL\type_from_array()`       |
| `is_nullable()`           | `\Flow\Types\DSL\type_is_nullable()`      |
| `type_equals()`           | `\Flow\Types\DSL\type_equals()`           |
| `types()`                 | `\Flow\Types\DSL\types()`                 |
| `type_list()`             | `\Flow\Types\DSL\type_list()`             |
| `type_map()`              | `\Flow\Types\DSL\type_map()`              |
| `type_json()`             | `\Flow\Types\DSL\type_json()`             |
| `type_datetime()`         | `\Flow\Types\DSL\type_datetime()`         |
| `type_date()`             | `\Flow\Types\DSL\type_date()`             |
| `type_time()`             | `\Flow\Types\DSL\type_time()`             |
| `type_xml()`              | `\Flow\Types\DSL\type_xml()`              |
| `type_xml_element()`      | `\Flow\Types\DSL\type_xml_element()`      |
| `type_uuid()`             | `\Flow\Types\DSL\type_uuid()`             |
| `type_int()`              | `\Flow\Types\DSL\type_integer()`          |
| `type_integer()`          | `\Flow\Types\DSL\type_integer()`          |
| `type_string()`           | `\Flow\Types\DSL\type_string()`           |
| `type_float()`            | `\Flow\Types\DSL\type_float()`            |
| `type_boolean()`          | `\Flow\Types\DSL\type_boolean()`          |
| `type_instance_of()`      | `\Flow\Types\DSL\type_instance_of()`      |
| `type_resource()`         | `\Flow\Types\DSL\type_resource()`         |
| `type_array()`            | `\Flow\Types\DSL\type_array()`            |
| `type_callable()`         | `\Flow\Types\DSL\type_callable()`         |
| `type_null()`             | `\Flow\Types\DSL\type_null()`             |
| `type_enum()`             | `\Flow\Types\DSL\type_enum()`             |
| `struct_schema()`         | `structure_schema()`                      |
| `get_type()`              | `\Flow\Types\DSL\get_type()`              |
| `print_schema()`          | `schema_to_ascii()`                       |
| `type_is()`               | `\Flow\Types\DSL\type_is()`               |
| `type_is_any()`           | `\Flow\Types\DSL\type_is_any()`           |
| `dom_element_to_string()` | `\Flow\Types\DSL\dom_element_to_string()` |

### 3) Removed deprecated DataFrame methods

| Removed Method                         | Replacement                                                  |
|----------------------------------------|--------------------------------------------------------------|
| `DataFrame::validate()`                | `DataFrame::match()`                                         |
| `DataFrame::renameAll()`               | `DataFrame::renameEach(rename_replace(...))`                 |
| `DataFrame::renameAllLowerCase()`      | `DataFrame::renameEach(rename_style(StringStyles::LOWER))`   |
| `DataFrame::renameAllUpperCase()`      | `DataFrame::renameEach(rename_style(StringStyles::UPPER))`   |
| `DataFrame::renameAllUpperCaseFirst()` | `DataFrame::renameEach(rename_style(StringStyles::UCFIRST))` |
| `DataFrame::renameAllUpperCaseWord()`  | `DataFrame::renameEach(rename_style(StringStyles::UCWORDS))` |
| `DataFrame::renameAllStyle()`          | `DataFrame::renameEach(rename_style(...))`                   |

### 4) Removed deprecated Schema methods

| Removed Method            | Replacement                   |
|---------------------------|-------------------------------|
| `Schema::entries()`       | `Schema::references()->all()` |
| `Schema::getDefinition()` | `Schema::get()`               |
| `Schema::nullable()`      | `Schema::makeNullable()`      |

### 5) Removed deprecated Definition methods

| Removed Method           | Replacement                  |
|--------------------------|------------------------------|
| `Definition::nullable()` | `Definition::makeNullable()` |

This applies to all Definition implementations: `BooleanDefinition`, `DateDefinition`, `DateTimeDefinition`,
`EnumDefinition`, `FloatDefinition`, `HTMLDefinition`, `HTMLElementDefinition`, `IntegerDefinition`, `JsonDefinition`,
`ListDefinition`, `MapDefinition`, `StringDefinition`, `StructureDefinition`, `TimeDefinition`, `UuidDefinition`,
`XMLDefinition`, `XMLElementDefinition`.

### 6) Removed deprecated FileExtractor and PathFiltering methods

| Removed Method               | Replacement                       |
|------------------------------|-----------------------------------|
| `FileExtractor::addFilter()` | `FileExtractor::withPathFilter()` |
| `PathFiltering::addFilter()` | `PathFiltering::withPathFilter()` |

### 7) Removed deprecated ScalarFunctionChain methods

| Removed Method                               | Replacement                                       |
|----------------------------------------------|---------------------------------------------------|
| `ScalarFunctionChain::domElementAttribute()` | `ScalarFunctionChain::domElementAttributeValue()` |

### 8) Removed deprecated Config constants

| Removed Constant              | Replacement                       |
|-------------------------------|-----------------------------------|
| `Config::CACHE_DIR_ENV`       | `CacheConfig::CACHE_DIR_ENV`      |
| `Config::SORT_MAX_MEMORY_ENV` | `SortConfig::SORT_MAX_MEMORY_ENV` |

### 9) Removed deprecated Transformers

| Removed Transformer                     | Replacement                                      |
|-----------------------------------------|--------------------------------------------------|
| `EntryNameStyleConverterTransformer`    | Use `DataFrame::renameEach(rename_style(...))`   |
| `RenameAllCaseTransformer`              | Use `DataFrame::renameEach(rename_style(...))`   |
| `RenameStrReplaceAllEntriesTransformer` | Use `DataFrame::renameEach(rename_replace(...))` |

### 10) Removed deprecated classes

| Removed Class                                   | Replacement                    |
|-------------------------------------------------|--------------------------------|
| `Flow\ETL\Function\StyleConverter\StringStyles` | `Flow\ETL\String\StringStyles` |

---

## Upgrading from 0.28.x to 0.29.x

### 1) JsonType now uses Json value object instead of string

The `JsonType` has been refactored to use a dedicated `Json` value object (similar to `Uuid`/`UuidType` pattern). This
allows static analysis tools to distinguish between regular strings and JSON strings.

**Breaking Changes:**

- `JsonType::assert()` now returns `Json` instance instead of `string`
- `JsonType::cast()` now returns `Json` instance instead of `string`
- `JsonType::isValid()` now checks for `Json` instance (plain strings are no longer valid)
- `Cast::cast('json', $value)` function now returns `Json` object instead of string
- `type_json()` return type annotation changed from `Type<string>` to `Type<Json>`
- `JsonEntry::value()` now returns `?Json` instead of `?array` (consistent with `UuidEntry::value()` returning `?Uuid`)
- `JsonEntry::json()` method removed (use `value()` instead)

**Migration:**

If you were using `type_json()->cast($value)` and expected a string, use `->toString()`:

Before:

```php
$jsonString = type_json()->cast($array); // was string
```

After:

```php
$json = type_json()->cast($array); // now Json object
$jsonString = $json->toString(); // get the string
$jsonArray = $json->toArray(); // get as array
```

If you were using `JsonEntry::value()` and expected an array:

Before:

```php
$entry = json_entry('data', ['key' => 'value']);
$array = $entry->value(); // was array
```

After:

```php
$entry = json_entry('data', ['key' => 'value']);
$json = $entry->value(); // now Json object
$array = $json?->toArray(); // get as array
$string = $json?->toString(); // get as string
```

If you were using `JsonEntry::json()`:

Before:

```php
$json = $entry->json();
```

After:

```php
$json = $entry->value(); // json() method removed, use value() instead
```

**New Json value object features:**

```php
use Flow\Types\Value\Json;

// Create from string
$json = new Json('{"key": "value"}');

// Create from array
$json = Json::fromArray(['key' => 'value']);

// Check if valid JSON
Json::isValid('{"key": "value"}'); // true

// Convert to string/array
$json->toString(); // '{"key":"value"}'
$json->toArray(); // ['key' => 'value']

// Json implements Stringable
(string) $json; // '{"key":"value"}'

// Json implements JsonSerializable
json_encode($json); // '{"key":"value"}'
```

**Note:** `JsonEntry::value()` now returns `?Json` for consistency with `UuidEntry::value()` returning `?Uuid`. Use
`->toArray()` or `->toString()` on the Json object to get the underlying data.

**Row methods behavior:**

```php
// Row::toArray() converts Json to array automatically (for convenient serialization)
$row->toArray();           // Returns ['data' => ['key' => 'value']] not ['data' => Json(...)]

// Row::valueOf() returns the raw value (Json object for json entries)
$row->valueOf('data');     // Returns Json object (use ->toArray() if you need array)

// Entry value() returns the typed value
$row->get('data')->value();  // Returns Json object (use ->toArray() if you need array)
```

---

## Upgrading from 0.26.x to 0.27.x

### 1) Force `EntryFactory $entryFactory` to be required on `array_to_row` & `array_to_row(s)`

Before:

```php
to_entry('name', 'data');
array_to_row([]);
array_to_rows([]);
```

After:

```php
to_entry('name', 'data', flow_context(config())->entryFactory());
array_to_row([], flow_context(config())->entryFactory());
array_to_rows([], flow_context(config())->entryFactory());
```

## Upgrading from 0.16.x to 0.17.x

### 1) Removed $nullable property from all types

Before:

```php
type_string(nullable:true)->toString() // ?string
```

After:

```php
type_optional(string())->toString() // ?string
```

### 2) Removed precision from `float_type()`

Before `float_type()` use to have default precision 6. This means that any operations on float had to round values to
given precision. The problem with this approach is that all operations now need to receive a dedicated rounding option.

Instead, end users should handle precision of float columns through `round()` scalar function.

### 3) Moved all Types to `Flow\Types\Type` namespace

Before

```php
\Flow\ETL\DSL\type_string(); // now deprecated, alias for \Flow\Types\DSL\type_string();
```

After

```php
\Flow\Types\DSL\type_string();
```

## Upgrading from 0.15.x to 0.16.x

### 1) Deprecated `Flow\ETL\DataFrame::renameAll*` methods

Methods:

- `Flow\ETL\DataFrame::renameAll()`,
- `Flow\ETL\DataFrame::renameAllLowerCase()`,
- `Flow\ETL\DataFrame::renameAllUpperCase()`,
- `Flow\ETL\DataFrame::renameAllUpperCaseFirst()`,
- `Flow\ETL\DataFrame::renameAllUpperCaseWord()`,

Were deprecated in favor of using new method: `DataFrame::renameEach()` with proper `RenameEntryStrategy` object.

### 2) Deprecated `RenameAllCaseTransformer` & `RenameStrReplaceAllEntriesTransformer`

Selected transformers were deprecated in favor of using `DataFrame::renameEach()` with related `RenameEntryStrategy`:

- `RenameAllCaseTransformer` -> `RenameCaseTransformer`,
- `RenameStrReplaceAllEntriesTransformer` -> `RenameReplaceStrategy`,

---

## Upgrading from 0.14.x to 0.15.x

### 1) Removed `Flow\ETL\Row\Schema\Matcher` and implementations

Schema Matcher was the initial attempt to implement a schema evolution next to schema validation that over time got
replaced with a different implementation of Schema Validator.

### 2) Renamed `Flow\ETL\Row\Schema` namespace into `Flow\ETL\Schema`.

This means all classes related to Schema now live under `Flow\ETL\Schema` namespace.

---

## Upgrading from 0.11.x to 0.14.x

### 1) Replaced `Flow\ETL\DataFrame::validate()` with `Flow\ETL\DataFrame::match()`

The old method is now deprecated and will be removed in the next release.

### 2) Replaced `Flow\ETL\Function\ScalarFunction\TypedScalarFunction` with

`Flow\ETL\Function\ScalarFunction\ScalarResult`.

The old interface was used to allow defining the return type of the ScalarFunctions. It was replaced with a ScalarResult
value object that is much more flexible than the interface, because it's allowing to return any type dynamically without
making the scalar function stateful.

## Upgrading from 0.10.x to 0.11.x

### 1) Removed StructureElement/struct_element/structure_element from StructureType Definition

Before:

```php
type_structure([
    struct_element('name', string()),
    struct_element('age', integer()),
]);
```

After:

```php
type_structure([
    'name' => string(),
    'age' => integer(),
]);
```

### 2) Doctrine DBAL Adapter

From now options for:

- `to_dbal_table_insert()`
- `to_db_table_update()`

are passed as objects (instance of UpdateOptions|InsertOptions interfaces) and they are platform specific, so please use
the proper class for the platform you are using.

- PostgreSQL
    - PostgreSQLInsertOptions
    - PostgreSQLUpdateOptions
- MySQL
    - MySQLInsertOptions
    - MySQLUpdateOptions
- Sqlite
    - SQLiteInsertOptions
    - SQLiteUpdateOptions

## Upgrading from 0.8.x to 0.10.x

### 1) Providing multiple paths to a single extractor

From now to read from multiple locations use `from_all(Extractor ...$extractors) : Exctractor` extractor.

Before:

```php
<?php

from_parquet([
    path(__DIR__ . '/data/1.parquet'),
    path(__DIR__ . '/data/2.parquet'),
]);
```

After:

```php
<?php

from_all(
    from_parquet(path(__DIR__ . '/data/1.parquet')),
    from_parquet(path(__DIR__ . '/data/2.parquet')),
);
```

### 2) Passing optional arguments to extractors/loaders

From now all extractors/loaders are accepting only mandatory arguments, all optional arguments should be passed through
`with*` methods and fluent interface.

Before:

```php
<?php

from_parquet(path(__DIR__ . '/data/1.parquet'), schema: $schema);
```

After:

```php
<?php

from_parquet(path(__DIR__ . '/data/1.parquet'))->withSchema($schema);
```

## Upgrading from 0.7.x to 0.8.x

### 1) Joins

To support joining bigger datasets, we had to move from initial NestedLoop join algorithm into Hash Join algorithm.

- the only supported coin expression is `=` (equals) that can be grouped with `AND` and `OR` operators.
- `joinPrefix` is now always required, and by default is set to 'joined_'
- join will always result all columns from both datasets, columns used in join condition will be prefixed with
  `joinPrefix`.

Other than that, API stays the same.

Above changes were introduced in all 3 types of joins:

- `DataFrame::join()`
- `DataFrame::joinEach()`
- `DataFrame::crossJoin()`

### 2) GroupBy

From now on, `DataFrame::groupBy()` method will return `GroupedDataFrame` object, which is nothing more than a GroupBy
statement Builder. To get the results, you first need to define the aggregation functions or optionally pivot the data.

## Upgrading from 0.6.x to 0.7.x

### 1) DataFrame::appendSafe () method was removed

`DataFrame::appendSafe()` aka `DataFrame::threadSafe()` method was removed as it was introducing additional complexity
and was not used in any of the adapters.

## Upgrading from 0.5.x to 0.6.x

### 1) Rows::merge () accepts single instance of Rows

Before:

```php
Rows::merge(Rows ...$rows) : Rows
```

After:

```php
Rows::merge(Rows $rows) : Rows
```

---

## Upgrading from 0.4.x to 0.5.x

### 1) Entry factory moved from extractors to `FlowContext`

To improve code quality and reduce code coupling `EntryFactory` was removed from all constructors of extractors, in
favor of passing it into `FlowContext` & re-using same entry factory in a whole pipeline.

### 2) Invalid schema has no fallback in `NativeEntryFactory`

Before, passing `Schema` into `NativeEntryFactory::create()` had fallback when the given entry was not found in a passed
schema, now the schema has higher priority & fallback is no longer available, instead when the definition is missing in
a passed schema, `InvalidArgumentException` will be thrown.

### 3) BufferLoader was removed

BufferLoader was removed in favor of `DataFrame::collect(int $batchSize = null)` method which now accepts additional
argument `$batchSize` that will keep collecting Rows from Extractor until the given batch size is reached. Which does
exactly the same thing as BufferLoader did, but in a more generic way.

### 4) Pipeline Closure

Pipeline Closure was reduced to be only Loader Closure and it was moved to \Flow\ETL\Loader namespace. Additionally,
\Closure::close method no longer requires Rows to be passed as an argument.

### 5) Parallelize

DataFrame::parallelize () method is deprecated, and it will be removed, instead use DataFrame::batchSize (int $size)
method.

### 6) Rows in batch - Extractors

From now, file-based Extractors will always throw one Row at time, in order to merge them into bigger groups use
`DataFrame::batchSize(int $size)` just after extractor method.

Before:

```php
<?php

(new Flow())
    ->read(CSV::from(__DIR__ . '/1_mln_rows.csv', rows_in_batch: 100))
    ->write(To::output())
    ->count();
```

After:

```php
(new Flow())
    ->read(CSV::from(__DIR__ . '/1_mln_rows.csv',))
    ->batchSize(100)
    ->write(To::output())
    ->count();
```

Affected extractors:

- CSV
- Parquet
- JSON
- Text
- XML
- Avro
- DoctrineDBAL - `rows_in_batch` wasn't removed, but now results are thrown row by row, instead of whole page.
- GoogleSheet

### 7) `GoogleSheetExtractor`

Argument `$rows_in_batch` was renamed to `$rows_per_page` which no longer determines the size of the batch, but the size
of the page that will be fetched from Google API. Rows are yielded one by one.

### 8) `DataFrame::threadSafe()` method was replaced by `DataFrame::appendSafe()`

`DataFrame::appendSafe()` is doing exactly the same thing as the old method, it's just more descriptive and
self-explanatory. It's no longer mandatory to set this flat to true when using SaveMode::APPEND, it's now set
automatically.

### 9) Loaders - chunk size

Loaders are no longer accepting chunk_size parameter, from now in order to control the number of rows saved at once use
`DataFrame::batchSize(int $size)` method.

### 10) Removed DSL functions: `datetime_string()`, `json_string()`

Those functions were removed in favor of accepting string values in related DSL functions:

- `datetime_string()` => `datetime()`,
- `json_string()` => `json()` & `json_object()`

### 11) Removed Asynchronous Processing

More details can be found in [this issue](https://github.com/flow-php/flow/issues/793).

- Removed etl-adapter-amphp
- Removed etl-adapter-reactphp
- Removed `LocalSocketPipeline`
- Removed `DataFrame::pipeline()`

### 12) `CollectionEntry` removal

After adding native & logical types into the Flow, we remove the `CollectionEntry` as obsolete. New types that cover it
better are: `ListType`, `MapType` & `StructureType` along with related new entry types.

### 13) Removed `from*()` methods from scalar entries

Removed `BooleanEntry::from()`, `FloatEntry::from()`, `IntegerEntry::from()`, `StringEntry::fromDateTime()` methods in
favor of using DSL functions.

### 14) Removed deprecated `Sha1IdFactory`

Class `Sha1IdFactory` was removed, use `HashIdFactory` class:

```php
(new HashIdFactory('entry_name'))->withAlgorithm('sha1');
```

### 15) Deprecate DSL Static classes

DSL static classes were deprecated in favor of using functions defined in `src/core/etl/src/Flow/ETL/DSL/functions.php`
file.

Deprecated classes:

- `src/core/etl/src/Flow/ETL/DSL/From.php`
- `src/core/etl/src/Flow/ETL/DSL/Handler.php`
- `src/core/etl/src/Flow/ETL/DSL/To.php`
- `src/core/etl/src/Flow/ETL/DSL/Transform.php`
- `src/core/etl/src/Flow/ETL/DSL/Partitions.php`
- `src/adapter/etl-adapter-avro/src/Flow/ETL/DSL/Avro.php`
- `src/adapter/etl-adapter-chartjs/src/Flow/ETL/DSL/ChartJS.php`
- `src/adapter/etl-adapter-csv/src/Flow/ETL/DSL/CSV.php`
- `src/adapter/etl-adapter-doctrine/src/Flow/ETL/DSL/Dbal.php`
- `src/adapter/etl-adapter-elasticsearch/src/Flow/ETL/DSL/Elasticsearch.php`
- `src/adapter/etl-adapter-google-sheet/src/Flow/ETL/DSL/GoogleSheet.php`
- `src/adapter/etl-adapter-json/src/Flow/ETL/DSL/Json.php`
- `src/adapter/etl-adapter-meilisearch/src/Flow/ETL/DSL/Meilisearch.php`
- `src/adapter/etl-adapter-parquet/src/Flow/ETL/DSL/Parquet.php`
- `src/adapter/etl-adapter-text/src/Flow/ETL/DSL/Text.php`
- `src/adapter/etl-adapter-xml/src/Flow/ETL/DSL/XML.php`

---

## Upgrading from 0.3.x to 0.4.x

### 1) Transformers replaced with scalar functions

Transformers are a really powerful tool that was used in Flow since the beginning, but that tool was too powerful for
the simple cases that were needed, and introduced additional complexity and maintenance issues when they were
handwritten.

We reworked most of the internal transformers to new scalar functions and entry scalar functions (based on the built-in
functions), and we still internally use that powerful tool, but we don't expose it to end users, instead, we provide
easy-to-use, covering all user needs functions.

All available functions can be found in [`ETL\Row\Function` folder](src/core/etl/src/Flow/ETL/Function) or in [
`ETL\DSL\functions` file](src/core/etl/src/Flow/ETL/DSL/functions.php), and entry scalar functions are defined in
`EntryScalarFunction`.

Before:

```php
<?php

use Flow\ETL\Extractor\MemoryExtractor;
use Flow\ETL\Flow;
use Flow\ETL\DSL\Transform;

(new Flow())
    ->read(new MemoryExtractor())
    ->rows(Transform::string_concat(['name', 'last name'], ' ', 'name'))
```

After:

```php
<?php

use function Flow\ETL\DSL\concat;
use function Flow\ETL\DSL\lit;
use Flow\ETL\Extractor\MemoryExtractor;
use Flow\ETL\Flow;

(new Flow())
    ->read(new MemoryExtractor())
    ->withEntry('name', concat(ref('name'), lit(' '), ref('last name')))
```

### 2) `ref` function nullability

`ref("entry_name")` is no longer returning null when the entry is not found. Instead, it throws an exception. The same
behavior can be achieved through using a newly introduced `optional` function:

Before:

```php
<?php

use function Flow\ETL\DSL\optional;
use function Flow\ETL\DSL\ref;

ref('non_existing_column')->cast('string'); 
```

After:

```php
<?php

use function Flow\ETL\DSL\optional;
use function Flow\ETL\DSL\ref;

optional(ref('non_existing_column'))->cast('string');
// or  
optional(ref('non_existing_column')->cast('string'));
```

### 3) Extractors output

Affected extractors:

* CSV
* JSON
* Avro
* DBAL
* GoogleSheet
* Parquet
* Text
* XML

Extractors are no longer returning data under an array entry called `row`, thanks to this unpacking row become
redundant.

Because of that all DSL functions are no longer expecting `$entry_row_name` parameter, if it was used anywhere, please
remove it.

Before:

```php
<?php 

(new Flow())
    ->read(From::array([['id' => 1, 'array' => ['a' => 1, 'b' => 2, 'c' => 3]]]))
    ->withEntry('row', ref('row')->unpack())
    ->renameAll('row.', '')
    ->drop('row')
    ->withEntry('array', ref('array')->arrayMerge(lit(['d' => 4])))
    ->write(To::memory($memory = new ArrayMemory()))
    ->run();
```

After:

```php
<?php

(new Flow())
    ->read(From::array([['id' => 1, 'array' => ['a' => 1, 'b' => 2, 'c' => 3]]]))
    ->withEntry('array', ref('array')->arrayMerge(lit(['d' => 4])))
    ->write(To::memory($memory = new ArrayMemory()))
    ->run();
```

### 4) ConfigBuilder::putInputIntoRows () output is now prefixed with _           (underscore)

In order to avoid collisions with datasets columns, additional columns created after using putInputIntoRows ()
would now be prefixed with `_` (underscore) symbol.

Before:

```php
<?php

$rows = (new Flow(Config::builder()->putInputIntoRows()))
            ->read(Json::from(__DIR__ . '/../Fixtures/timezones.json', 5))
            ->fetch();

foreach ($rows as $row) {
    $this->assertSame(
        [
            ...
            '_input_file_uri',
        ],
        \array_keys($row->toArray())
    );
}
```

After:

```php
<?php

$rows = (new Flow(Config::builder()->putInputIntoRows()))
            ->read(Json::from(__DIR__ . '/../Fixtures/timezones.json', 5))
            ->fetch();

foreach ($rows as $row) {
    $this->assertSame(
        [
            ...
            '_input_file_uri',
        ],
        \array_keys($row->toArray())
    );
}
```
