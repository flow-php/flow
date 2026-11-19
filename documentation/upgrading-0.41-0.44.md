# Upgrade Guide 0.41.x - 0.44.x

[TOC]

This document provides guidelines for upgrading between versions of Flow PHP. Please follow the instructions for your
specific version to ensure a smooth upgrade process.

Upgrading from older versions: [0.35.x to 0.41.x](/documentation/upgrading-0.35-0.41.md)

---

## Upgrading from 0.43.x to 0.44.x

### 1) `flow-php/etl-adapter-json` - `to_json()`/`to_json_lines()` write list/map/structure/array entries as nested JSON

| Before                                                            | After                                          |
|-------------------------------------------------------------------|------------------------------------------------|
| `{"tags":"[1,2,3]"}` (escaped JSON string)                        | `{"tags":[1,2,3]}`                             |
| nested `DateTimeInterface`/`Uuid`/`UnitEnum` `json_encode`d as-is | datetime format / canonical string / case name |
| non-array value under a container type -> `""`                    | `null`                                         |

### 2) `flow-php/etl` - Floe inferred schema keeps the first batch's nullability

| Before                                                             | After                                           |
|--------------------------------------------------------------------|-------------------------------------------------|
| inferred schema - every column nullable                            | nullability taken from the first batch's values |
| `null` in batch >= 2 in a column non-nullable in batch 1 - written | throws                                          |

Columns that may only become null in later batches, declare the schema explicitly:
`to_floe($path)->withSchema($schema)`.

### 3) `flow-php/types` - `detectType([])` returns `list<null>`

| Before                                                   | After        |
|----------------------------------------------------------|--------------|
| `(new TypeDetector())->detectType([])` -> `array<mixed>` | `list<null>` |

### 4) `flow-php/types` - `detectType()` returns different types for arrays

| Before                                                                                                   | After                                          |
|----------------------------------------------------------------------------------------------------------|------------------------------------------------|
| `detectType([5 => 'a', 6 => []])` -> `map<integer, string>`                                              | `array<mixed>`                                 |
| `detectType(['a', []])` -> `list<string>`                                                                | `array<mixed>`                                 |
| `detectType([['id' => '1'], []])` -> `list<structure{id: string}>`                                       | `array<mixed>`                                 |
| `detectType([[1, 2], [null]])` -> `list<list<integer>>`                                                  | `list<array<mixed>>`                           |
| `detectType([[], [1, 2]])` -> `list<array<mixed>>`                                                       | `list<list<?integer>>`                         |
| `detectType([1 => 1, 5 => 2.5])` -> `array<mixed>`                                                       | `map<integer, float>`                          |
| `detectType(['name' => 'x', 'latlng' => [33, 65.5]])` -> `structure{name: string, latlng: array<mixed>}` | `structure{name: string, latlng: list<float>}` |

### 5) `flow-php/types` - `cast()` refuses values it used to coerce, fabricate or wrap

| Before                                                                                                                                  | After                     |
|-----------------------------------------------------------------------------------------------------------------------------------------|---------------------------|
| `type_structure(['id' => type_integer(), 'name' => type_string()])->cast(['id' => 1])` -> `['id' => 1, 'name' => '']`                   | throws `CastingException` |
| same type, `->cast(['id' => 1, 'name' => null])` -> `['id' => 1, 'name' => '']`                                                         | throws `CastingException` |
| same type, `->cast([])`, `->cast(null)` -> `['id' => 0, 'name' => '']`                                                                  | throws `CastingException` |
| `type_structure(['id' => type_integer()], ['name' => type_string()])->cast(['id' => 1, 'name' => null])` -> `['id' => 1, 'name' => '']` | throws `CastingException` |
| `type_list(type_string())->cast(null)` -> `['']`                                                                                        | throws `CastingException` |
| `type_structure(['id' => type_integer()])->cast('{"id":"1"}')` -> throws                                                                | `['id' => 1]`             |
| `type_list(type_integer())->cast('["1","2"]')` -> throws                                                                                | `[1, 2]`                  |
| `type_integer()->cast('abc')` -> `0`                                                                                                    | throws `CastingException` |
| `type_integer()->cast('2024-01-01')` -> `2024`                                                                                          | throws `CastingException` |
| `type_integer()->cast([1, 2, 3])` -> `1`                                                                                                | throws `CastingException` |
| `type_integer()->cast(null)` -> `0`                                                                                                     | throws `CastingException` |
| `type_integer()->cast('9223372036854775808')` -> `9223372036854775807`; also `'9223372036854775807.0'`, `type_positive_integer()`       | throws `CastingException` |
| `type_integer()->cast(1e300)` -> `0`                                                                                                    | throws `CastingException` |
| `type_integer()->cast(new DOMElement('e', 'abc'))` -> `0`                                                                               | throws `CastingException` |
| `type_float()->cast('abc')` -> `0.0`                                                                                                    | throws `CastingException` |
| `type_float()->cast([1])` -> `1.0`                                                                                                      | throws `CastingException` |
| `type_float()->cast(null)` -> `0.0`                                                                                                     | throws `CastingException` |
| `type_boolean()->cast('weird')` -> `true`                                                                                               | throws `CastingException` |
| `type_boolean()->cast('')` -> `false`                                                                                                   | throws `CastingException` |
| `type_boolean()->cast([1, 2, 3])` / `->cast(new DateTimeImmutable())` -> `true`                                                         | throws `CastingException` |
| `type_boolean()->cast(null)` -> `false`                                                                                                 | throws `CastingException` |
| `type_date()->cast('now')` / `type_datetime()->cast('now')` -> current time; also `''`, `'t'`, `'+12'`, `'yesterday'`, `'10:00:00'`     | throws `CastingException` |
| `type_datetime()->cast('@1609459200')` -> `2021-01-01`; also `'2024-01'`, `'2024-001'`, and under `type_date()`                         | throws `CastingException` |
| `type_string()->cast(null)` / `type_scalar()->cast(null)` -> `''`                                                                       | throws `CastingException` |
| `type_list(type_string())->cast([null])` -> `['']`                                                                                      | throws `CastingException` |
| `type_array()->cast('abc')` -> `['abc']`                                                                                                | throws `CastingException` |
| `type_list(type_integer())->cast(5)` -> `[5]`                                                                                           | throws `CastingException` |
| `type_array()->cast(null)` -> `[]`                                                                                                      | throws `CastingException` |
| `type_integer()->cast(new DateTimeImmutable('@1700000000'))` -> `1700000000000000`                                                      | `1700000000`              |
| `type_float()->cast(new DateTimeImmutable('@1700000000.5'))` -> `1700000000500000.0`                                                    | `1700000000.5`            |
| `type_integer()->cast(new DateInterval('PT90S'))` -> `90000000`                                                                         | `90`                      |
| `type_float()->cast(new DateInterval('PT90S'))` -> `90000000.0`                                                                         | `90.0`                    |

### 6) `flow-php/etl` - `FilesystemStreams` replaced by `FilesSink`, and a failed run discards its sink

| Before                                                                                  | After                                                                                      |
|-----------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------|
| `Flow\ETL\Filesystem\FilesystemStreams`                                                 | `Flow\ETL\Filesystem\FilesSink`                                                            |
| `FilesystemStreams::FLOW_TMP_FILE_PREFIX`                                               | `FilesSink::FLOW_TMP_FILE_PREFIX`                                                          |
| `new FilesystemStreams()` + `->setMode($mode)`                                          | `new FilesSink($filesystem, $destination, $mode)`                                          |
| `$streams->writeTo($filesystem, $path, $partitions)`                                    | `$files->writeTo($partitions)`                                                             |
| `$streams->isOpen($path, $partitions)`                                                  | `$files->touched($partitions)`                                                             |
| `$streams->listOpenStreams($path)`                                                      | `$files->openStreams()`                                                                    |
| `$streams->closeStreams($filesystem, $path)`                                            | `$files->publish()` / `$files->abandon()`                                                  |
| `$streams->read()` / `->rm()` / `->exists()` / `count()` / `getIterator()`              | removed - use the `Filesystem` directly                                                    |
| `saveMode()` on one `DataFrame` applied to every later `DataFrame` on the same `Config` | `saveMode()` is set on the sink and belongs to that sink alone                             |
| a failed run left its `DestinationStream` registered; the retry appended to it          | the failed run's sink is discarded, the retry starts clean                                 |
| an abandoned run left its `._flow_php_tmp.` file behind under `Overwrite`               | the abandoned run removes it and never renames it over the destination                     |
| a failed run left its partial file at the destination under the other save modes        | it removes any file it created; a destination it did not create is untouched               |
| a run that threw or was abandoned never reached its loaders                             | loaders implementing `Flow\ETL\Loader\Discardable` receive `discard(FlowContext $context)` |
| `Config::filesystemStreams()`                                                           | removed                                                                                    |
| `new Config(..., FilesystemStreams $filesystemStreams, ...)` constructor parameter      | removed                                                                                    |

Build `Config` through `Config::builder()` / `Config::default()`. A `Loader` holding per-run state (an open stream, a
writer, a counter) implements `Discardable` and drops that state in `discard()`.

### 7) `flow-php/etl` - `RetryLoader` no longer retries `InvalidLogicException` by default

| Before                                                               | After                                                           |
|----------------------------------------------------------------------|-----------------------------------------------------------------|
| `new RetryLoader($loader)` default strategy `new AnyThrowable(3)`    | `new AnyThrowableExcept([InvalidLogicException::class], 3)`     |
| `write_with_retries($loader)` default strategy `new AnyThrowable(3)` | `new AnyThrowableExcept([InvalidLogicException::class], 3)`     |
| an `InvalidLogicException` was attempted 4 times with delays between | attempted once, no delay                                        |
| -                                                                    | `retry_any_throwable_except([InvalidLogicException::class], 3)` |

To keep retrying every throwable:

```php
write_with_retries($loader, retry_any_throwable(3));
```

### 8) `flow-php/etl` - operations inside a `Transformation` answer for the whole stream

| Before                                                                                                                                                                   | After                                                                                                                                                                                     |
|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `$df->sortBy(ref('id'))` inside a `Transformation` sorted each batch on its own                                                                                          | sorts the whole stream                                                                                                                                                                    |
| `$df->aggregate(...)` / `groupBy()->aggregate()` / `pivot()` / window functions inside a `Transformation` answered per batch                                             | answer for the whole stream                                                                                                                                                               |
| `$df->offset(2)` inside a `Transformation` lost rows                                                                                                                     | skips exactly the offset across the stream                                                                                                                                                |
| `$df->cache($id)` inside a `Transformation` persisted one batch                                                                                                          | persists the whole stream                                                                                                                                                                 |
| `$df->batchBy(...)` / `batch_size(...)` cut chunks at the incoming batches                                                                                               | cut chunks over the stream                                                                                                                                                                |
| `$df->join(...)` inside a `Transformation` emitted rows in input order                                                                                                   | emits rows grouped by key                                                                                                                                                                 |
| `$df->partitionBy(...)` inside a `Transformation`                                                                                                                        | removed - `$df->repartition(...)` groups the whole stream by key, see 60)                                                                                                                 |
| a `Transformation` calling `$df->fetch()` / `count()` / `schema()` silently answered over an empty stream                                                                | throws `InvalidLogicException`                                                                                                                                                            |
| `write_with_retries($loader)` around `to_transformation(...)` (any wrapped step) or around a `to_branch(...)` armed with `withTransformation(...)`, at any nesting depth | throws `InvalidLogicException` at the first `load()`, use `to_transformation(..., write_with_retries($loader))` or `to_branch(..., write_with_retries($loader))->withTransformation(...)` |
| `Flow\ETL\Extractor\SwappableRowsExtractor`                                                                                                                              | removed                                                                                                                                                                                   |

### 9) `flow-php/etl` - `to_branch()->withTransformation()` drives its `Transformation` once over the whole stream

| Before                                                                                        | After                                                                                         |
|-----------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------|
| the `Transformation` ran on each filtered batch in its own `DataFrame`                        | one nested pipeline spans the stream                                                          |
| `$df->sortBy(...)` in a branch transformation sorted each batch alone                         | sorts the whole branch stream                                                                 |
| `$df->aggregate(...)` / `limit()` / other `Processor`-backed operations answered per batch    | answer once for the stream                                                                    |
| a `Transformation` calling `$df->fetch()` / `count()` / `schema()` returned per-batch answers | throws `InvalidLogicException`                                                                |
| the wrapped loader received exactly one `load()` per outer batch                              | receives output as the transformation produces it; blocking operations deliver at `closure()` |
| telemetry `flow.etl.loading.rows` counted post-filter, post-transformation rows               | counts the rows offered to the branch                                                         |

### 10) `flow-php/etl-adapter-doctrine`, `-postgresql` - transactional loaders run `closure()` in a transaction

| Before                                                                                                      | After                                                                               |
|-------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------|
| `to_dbal_transaction()` / `to_pgsql_transaction()` never called `closure()` on wrapped loaders              | forwards `closure()` to every wrapped loader, inside one final transaction          |
| blocking operations inside a wrapped `Transformation` answered per batch, each batch in its own transaction | answer for the whole stream, delivered at `closure()` in a single transaction       |
| -                                                                                                           | a failure during the final transaction rolls back the drained delivery and rethrows |
| `withIsolationLevel()` applied to per-batch transactions                                                    | applies to every transaction the wrapper opens                                      |

### 11) `flow-php/etl` - Floe on-disk format v2, existing `.floe` files must be rewritten

| Before                              | After                                           |
|-------------------------------------|-------------------------------------------------|
| header version byte `0x01`          | `0x02`                                          |
| uuid payload - 36 raw bytes         | 4-byte little-endian length prefix + bytes      |
| partition values stored in the file | not stored - read from the directory path only  |
| reading a v1 file                   | throws `Floe does not support format version 1` |

Files written by 0.43.x cannot be read. Regenerate them from their source, or export them with 0.43.x to another
format before upgrading.

### 12) `flow-php/etl` - Floe rejects columns whose type is only known per value

| Before                                     | After                                                      |
|--------------------------------------------|------------------------------------------------------------|
| `list<mixed>` element - written with a tag | `Floe does not support values of type "mixed"`             |
| `union_schema()` column - written          | `Floe does not support columns of type "integer\|string"`  |
| `type_structure(..., allow_extra: true)`   | `Floe does not support structures that allow extra values` |
| map key other than `integer`/`string`      | `Floe does not support map keys of type "..."`             |

Declare an element type, or declare the column with `json_schema()` when the shape is dynamic.

### 13) `flow-php/etl` - Floe validates every value against its column type

| Before                                                                                           | After                                                      |
|--------------------------------------------------------------------------------------------------|------------------------------------------------------------|
| `'AB-1'` into an `integer` column - wrote `0`                                                    | throws `IncompatibleSchemaException`                       |
| `1.5` into an `integer` column - wrote `1`                                                       | throws                                                     |
| `1000` into a `string` column - raw `TypeError`                                                  | throws `IncompatibleSchemaException`                       |
| `int` into a `float` column - written                                                            | throws                                                     |
| `null` into a non-nullable column - written                                                      | throws                                                     |
| a batch without a NOT NULL session column - written                                              | throws `IncompatibleSchemaException`                       |
| a batch without a nullable session column                                                        | unchanged, reads back as `null`                            |
| `IncompatibleSchemaException` - `column "x" (...) is not compatible with the session type (...)` | a `Missing Definitions:` / `Mismatched Definitions:` block |
| `floe_options(validate_data: false)` / `Options::withValidateData()`                             | removed                                                    |
| `floe_options($validate_data, $buffer_size, $codec)`                                             | `floe_options($buffer_size, $codec)`                       |
| `new Options($validateData, $bufferSize, $codec)`                                                | `new Options($bufferSize, $codec)`                         |

### 14) `flow-php/etl` - aggregate result type no longer depends on the value

| Before                                                              | After   |
|---------------------------------------------------------------------|---------|
| `sum()` over a `float` column, whole total - `int`                  | `float` |
| `avg()`/`min()`/`max()` over a `float` column, whole result - `int` | `float` |
| `sum()` over an `integer` column - `int`                            | `float` |

### 15) `flow-php/etl` - aggregates ignore a row missing the aggregated column

| Before                                                                                            | After                                      |
|---------------------------------------------------------------------------------------------------|--------------------------------------------|
| missing column under `$df->mode(execution_strict())` - `Sum error: Entry "amount" does not exist` | contributes nothing                        |
| missing column in the default mode - contributed nothing                                          | unchanged                                  |
| a column no row declares - contributed nothing                                                    | throws `SchemaDefinitionNotFoundException` |

### 16) `flow-php/types` - `EnumType::isValid()` requires an object

| Before                                    | After                                               |
|-------------------------------------------|-----------------------------------------------------|
| `type_enum(Suit::class)->isValid('Suit')` | `false` (was `true`)                                |
| `type_enum(Suit::class)->assert('Suit')`  | throws `InvalidTypeException` (was raw `TypeError`) |
| `type_enum(Suit::class)->cast('Suit')`    | throws `CastingException` (was raw `TypeError`)     |

### 17) `flow-php/etl` - filesystems are passed to sources and sinks, engine algorithms take a storage

#### Filesystems

| Before                                                                                 | After                                                                                                                              |
|----------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------|
| `config_builder()->mount(aws_s3_filesystem(...))` + `from_csv(path('aws-s3://x.csv'))` | `from_csv(path('aws-s3://x.csv'), filesystem: aws_s3_filesystem(...))`                                                             |
| `config_builder()->unmount($fs)`                                                       | removed                                                                                                                            |
| `$config->fstab()`                                                                     | removed - use `Flow\Filesystem\DSL\fstab()` for `file_copy()` / `file_move()`                                                      |
| `$context->filesystem($path)`                                                          | removed - pass `filesystem:` to the source or sink                                                                                 |
| `$context->streams()->list($path, $filter)` -> `SourceStream`                          | `(new Flow\Filesystem\FileListing($filesystem))->list($path, $filter)` -> `FileStatus`                                             |
| `$context->streams()->writeTo($path, $partitions)`                                     | `$files->writeTo($partitions)` on a sink-held `FilesSink` - see 6)                                                                 |
| `$context->streams()->closeStreams($path)` in an extractor                             | removed                                                                                                                            |
| `new FilesystemStreams($filesystemTable)`                                              | `new FilesSink($filesystem, $destination, $saveMode)` - see 6)                                                                     |
| a custom file source reading through `FlowContext`                                     | takes `Filesystem $filesystem = new NativeLocalFilesystem()` as its last constructor argument                                      |
| `schema_from_json_schema($s)` resolved local `$ref`s through `fstab()`                 | takes a trailing `Filesystem $filesystem = new NativeLocalFilesystem()`                                                            |
| a `$ref` on `memory://` or `stdout://`                                                 | throws - pass the filesystem that serves it                                                                                        |
| `ChartJSLoader::withOutputPath($path)` / `::withTemplate($path)`                       | both take a trailing `Filesystem $filesystem = new NativeLocalFilesystem()`; `withOutputPath()` throws on a path with no extension |
| `FilePathArgument::getExisting($input, $config)` / `::getNotExisting(...)` (CLI)       | `getExisting($input)` / `getNotExisting($input)`; the constructor takes the `Filesystem`                                           |
| a custom `FileLoader`                                                                  | implements `saveMode()` and `Discardable` - see 6)                                                                                 |
| a custom `Flow\Filesystem\Filesystem` implementation                                   | adds `public function supports(Path $path): bool` - `return $this->mount()->supports($path);`                                      |

Two `memory_filesystem()` calls are two separate stores: pass the same instance to the writer and the reader.

#### Filesystem telemetry

| Before                                                                                                         | After                                                                          |
|----------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------|
| `withTelemetry()` traced every filesystem: `filesystem.read` / `filesystem.write` spans and filesystem metrics | filesystems are not traced - wrap the filesystem with `traceable_filesystem()` |
| `telemetry_options(filesystem: filesystem_telemetry_options(...))`                                             | removed                                                                        |
| `TelemetryOptions::filesystem()` / `->filesystem`                                                              | removed                                                                        |
| the pipeline-start debug log's `fstab` field                                                                   | a `spill` field naming the three spill storages                                |

After:

```php
$fs = traceable_filesystem(
    aws_s3_filesystem($bucket, $client),
    filesystem_telemetry_config($telemetry, $clock, filesystem_telemetry_options(trace_streams: true)),
);

data_frame()->read(from_csv(path('aws-s3://x.csv'), filesystem: $fs))->run();
```

#### Engine algorithms

| Before                                                                              | After                                                                                               |
|-------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------|
| `config_builder()->cacheFilesystem('s3')`                                           | `config_builder()->cache($cache)`                                                                   |
| `external_sort()->filesystemProtocol('file')`                                       | `external_sort()->storage(new FilesystemBuckets($fs, $path))`                                       |
| `hash_join()->filesystemProtocol(...)` / `hash_group_by()->filesystemProtocol(...)` | `->storage(BucketsStorage $storage)`                                                                |
| `CacheConfig::$filesystemMount`                                                     | removed                                                                                             |
| `SortAlgorithmBuilder::build(FilesystemTable, Path)`                                | `build(Path $spillRoot)`                                                                            |
| `new ExternalSortConfig($bucketing, $runSize)`                                      | `new ExternalSortConfig($bucketing, ?BucketsStorage $merge = null, $runSize)` - use named arguments |
| -                                                                                   | `external_sort()->mergeStorage($s)` - merged runs only                                              |

#### Per-operation algorithm overrides

| Before                               | After                                                                               |
|--------------------------------------|-------------------------------------------------------------------------------------|
| `$df->sortBy(ref('a'), ref('b'))`    | `$df->sortBy([ref('a'), ref('b')])`                                                 |
| `$df->groupBy('a', 'b')`             | `$df->groupBy(['a', 'b'])`                                                          |
| `$df->aggregate(sum(ref('a')), ...)` | `$df->aggregate([sum(ref('a')), ...])`                                              |
| -                                    | `$df->sortBy([ref('a')], external_sort()->storage(new MemoryBuckets()))`            |
| -                                    | `$df->groupBy(['a'], hash_group_by()->storage($s))`                                 |
| -                                    | `$df->join($right, $on, Join::left, hash_join()->storage($s))`                      |
| -                                    | `$df->cache('report', cache: $psrCache)` + `from_cache('report', cache: $psrCache)` |

`GroupedDataFrame::aggregate()` stays variadic: `$df->groupBy(['a'])->aggregate(sum(ref('b')))`.

#### Save mode, the CLI and the HTTP bridge

| Before                                                              | After                                                                                |
|---------------------------------------------------------------------|--------------------------------------------------------------------------------------|
| `$df->saveMode(overwrite())` / `$df->mode(overwrite())`             | `to_csv($path)->saveMode(overwrite())` - per sink                                    |
| `$df->saveMode(overwrite())->write(write_with_retries(to_csv($p)))` | `$df->write(write_with_retries(to_csv($p)->saveMode(overwrite())))`                  |
| `to_text($path)` returned `Loader`                                  | returns `TextLoader`                                                                 |
| `LoaderFactory::get()` returned `Loader`                            | returns `Loader&FileLoader`                                                          |
| `flow read --config .flow.php aws-s3://bucket/x.csv`                | `flow pipeline:run pipeline.php`, with the filesystem built inside the pipeline file |
| `new FlowBufferedResponse(..., filesystem: 'memory')`               | `new FlowBufferedResponse(..., filesystem: new MemoryFilesystem())`                  |
| `new FlowStreamedResponse(..., filesystem: 'stdout')`               | `new FlowStreamedResponse(..., filesystem: new StdOutFilesystem())`                  |
| `Output::loader(Path $path)`                                        | `Output::loader(Path $path, Filesystem $filesystem)`                                 |

### 18) `flow-php/etl` - `equals()` / `notEquals()` return `null` when either side is `null`

| Before                                                                          | After                             |
|---------------------------------------------------------------------------------|-----------------------------------|
| `ref('a')->equals(ref('b'))`, both `null` - `true`, `filter()` keeps the row    | `null`, `filter()` drops the row  |
| `ref('a')->equals(lit(5))`, `a` is `null` - `false`                             | `null`                            |
| `ref('a')->notEquals(lit(5))`, `a` is `null` - `true`, `filter()` keeps the row | `null`, `filter()` drops the row  |
| `ref('a')->same(ref('b'))` / `ref('a')->notSame(lit(5))`                        | unchanged - `null` same as `null` |

To keep the old result, switch to `same()` / `notSame()`:

Before:

```php
$df->filter(ref('a')->equals(ref('b')));
$df->filter(ref('a')->notEquals(lit(5)));
```

After:

```php
$df->filter(ref('a')->same(ref('b')));
$df->filter(ref('a')->notSame(lit(5)));
```

`same()` compares with `===`: over an integer column `ref('i')->same(lit(1.0))` is `false` where `equals()` is `true`.

### 19) `flow-php/etl` - execution modes are removed, a function given bad data throws

| Before                                                                         | After                                                                   |
|--------------------------------------------------------------------------------|-------------------------------------------------------------------------|
| default mode: a function given bad data returned `null`                        | throws - wrap it: `optional($function)`                                 |
| `$df->mode(execution_lenient())`                                               | removed - `optional($function)` per function                            |
| `$df->mode(execution_strict())`                                                | removed - strict is the only behaviour                                  |
| `coalesce($a, $b)` skipped a branch that threw                                 | rethrows - `coalesce(optional($a), $b)`                                 |
| `cast($v, type_integer())` over `null` returned `null`                         | throws - `cast($v, type_optional(type_integer()))`                      |
| `ref('l')->onEach($fn)` set an element whose `$fn` threw to `null`             | throws - `ref('l')->onEach(optional($fn))`                              |
| `ref('s')->indexOf('a')` / `->indexOfLast('a')` over `null` - `false`          | throws - `optional(ref('s')->indexOf('a'))`                             |
| `DataFrame::mode()`                                                            | removed                                                                 |
| `Flow\ETL\Function\ExecutionMode`, `execution_lenient()`, `execution_strict()` | removed                                                                 |
| `Flow\ETL\Function\Functions`, `FlowContext::functions()`                      | removed - a custom function throws instead of calling `invalidResult()` |

Before:

```php
data_frame()->read($extractor)->withEntry('payload', ref('json')->jsonDecode())->run();
```

After:

```php
data_frame()->read($extractor)->withEntry('payload', optional(ref('json')->jsonDecode()))->run();
```

### 20) `flow-php/etl` - `not()`, `all()`, `any()` and `isType()` return `null` for a `null` operand

| Before                                                                            | After                             |
|-----------------------------------------------------------------------------------|-----------------------------------|
| `not(ref('a')->equals(lit(1)))`, `a` is `null` - `true`, `filter()` keeps the row | `null`, `filter()` drops the row  |
| `all(ref('a')->equals(lit(1)), lit(true))`, `a` is `null` - `false`               | `null`                            |
| `any(ref('a')->equals(lit(1)), lit(false))`, `a` is `null` - `false`              | `null`                            |
| `ref('a')->isType(type_null())`, `a` is `null` - `true`                           | `null` - use `ref('a')->isNull()` |

`isNull()`, `isNotNull()`, `same()`, `notSame()` and `exists()` still return `true` or `false`.

### 21) `flow-php/etl` - expressions and steps are checked before the first row is read

| Before                                                                                                                               | After                                                                             |
|--------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------|
| `$df->filter(ref('id')->greaterThan(ref('created_at')))`, `integer` vs `datetime` - threw at the first row                           | throws at `schema()` / `run()`, before any row                                    |
| the same failure under `onError(skip_rows_handler())` - handled per row by the error handler                                         | throws; the error handler is not consulted                                        |
| `ref('a')->plus(ref('d'))` (`integer` + `date`) over a source with no rows - no rows                                                 | throws `Cannot combine types "integer", "date" - an explicit cast is required.`   |
| `$df->filter(ref('id'))` / `$df->until(ref('id'))` / `to_branch(ref('id'), $loader)` over an `integer` column - accepted             | throws - compare explicitly: `ref('id')->notEquals(lit(0))`                       |
| `greatest(ref('i'), ref('s'))` / `least(...)` over `integer` and `string` - `'apple'` for `5`, `'apple'`                             | throws `InvalidTypeException` - cast one side                                     |
| `coalesce(ref('i'), ref('s'))` / `when($c, ref('i'), ref('s'))` / `match_cases()` with `integer` and `string` arms - typed per value | `string` column                                                                   |
| `coalesce(ref('i'), lit(true))` - typed per value                                                                                    | throws `InvalidTypeException`                                                     |
| `array_expand(ref('j'))` over a `json` column - typed per value                                                                      | throws `SchemaNotDerivableException`                                              |
| `ref('u')` / `select('u')` over a column declared `integer\|string` - one type per value                                             | throws `UnsupportedUnionTypeException` - declare one type, e.g. `str_schema('u')` |

Applies to frames read through `from_data_frame()` and to `to_transformation()` bodies: the inner error fails the
outer `run()` / `schema()`.

### 22) `flow-php/etl` - functions return only values their column type can hold

| Before                                                                               | After                                 |
|--------------------------------------------------------------------------------------|---------------------------------------|
| `ref('d')->toDateTime('Y-m-d')` over an unparseable string - `false`                 | `null`                                |
| `ref('j')->jsonDecode()` over scalar JSON (`'1'`) - `1`                              | throws - `cast()` scalar JSON         |
| `ulid()` - `Symfony\Component\Uid\Ulid`                                              | base32 `string`                       |
| `ref('x')->domElementParent()` of a root element - its `DOMDocument`                 | `null`                                |
| `ref('x')->xpath($path)` - every matched node, text and attribute nodes included     | elements only, `null` when none match |
| `when($condition, $then, $else)` with `$else` of `0`, `false`, `''` or `[]` - `null` | `$else`                               |
| `round($v, 0)` / `ref('v')->round(0)` - `int`                                        | `float`                               |

### 23) `flow-php/etl` - function arguments that decide the output type take plain values

| Before                                                                                              | After                                                                                                                 |
|-----------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------|
| `array_sort($a, $sort_function)` - `ScalarFunction\|Sort\|null`                                     | `?Sort`                                                                                                               |
| `array_reverse($a, $preserveKeys)` / `->arrayReverse($preserveKeys)` - `ScalarFunction\|bool`       | `bool`                                                                                                                |
| `->onEach($fn, $preserveKeys)` - `ScalarFunction\|bool`                                             | `bool`                                                                                                                |
| `regex()` / `regex_all()` / `->regex()` / `->regexAll()` `$flags` - `ScalarFunction\|int`           | `int`; `PREG_OFFSET_CAPTURE`, `PREG_UNMATCHED_AS_NULL` (and `PREG_SET_ORDER` for `regex_all()`) throw at construction |
| `array_get($ref, $path)` / `->arrayGet($path)` - `ScalarFunction\|string`                           | `string`; a wildcard path throws - use `array_get_collection()`                                                       |
| `array_key_rename($ref, $path, $newName)` - `ScalarFunction\|string`                                | `string`                                                                                                              |
| `uuid_v7()` / `uuid_v7(null)`                                                                       | `uuid_v7($value)` - `$value` required                                                                                 |
| `->domElementNextSibling()` / `->domElementPreviousSibling()` - next / previous node, text included | next / previous element                                                                                               |
| `->domElementNextSibling(true)` / `->domElementPreviousSibling(true)`                               | argument removed                                                                                                      |
| `ref('a')->isType('nope')` - threw at the first row                                                 | throws at construction                                                                                                |
| `new Round($value)` - `$precision` defaults to `0`                                                  | defaults to `2`, as `round()`                                                                                         |

### 24) `flow-php/etl` - `cast()` accepts column types only

| Before                                                         | After                                             |
|----------------------------------------------------------------|---------------------------------------------------|
| `cast($v, 'json_pretty')`                                      | removed - throws at construction; use `'json'`    |
| `cast($v, 'object')` / `'mixed'` / `'callable'` / `'resource'` | throws `InvalidArgumentException` at construction |

### 25) `flow-php/etl` - `EntryReference` is renamed to `UnresolvedReference` and is immutable

| Before                                                  | After                                         |
|---------------------------------------------------------|-----------------------------------------------|
| `Flow\ETL\Row\EntryReference`                           | renamed to `Flow\ETL\Row\UnresolvedReference` |
| `ref()` / `col()` / `entry()` return `EntryReference`   | return `UnresolvedReference`                  |
| `$ref->as('b')` / `->asc()` / `->desc()` changed `$ref` | return a copy - use the returned reference    |

### 26) `flow-php/etl` - custom functions declare their output type and their children

| Before                                                                    | After                                                                                                          |
|---------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------|
| `class MyFunction extends ScalarFunctionChain`                            | `final class MyFunction implements ScalarFunction` + `use ScalarFunctionChain;`                                |
| `ScalarFunction` - `eval()`                                               | also `returns(): Type`, `children(): array`, `withChildren(array $children): static`, `resolved(): bool`       |
| a custom `Reference`                                                      | implements the `ScalarFunction` members above - `Reference` extends `ScalarFunction`                           |
| `AggregatingFunction::result(EntryFactory $entryFactory): Entry`          | removed - implement `outputName(): string`, `value(): mixed`, `returns(): Type` and the `FunctionTree` members |
| `WindowFunction`                                                          | adds `returns(): Type` and the `FunctionTree` members; `over()` returns a copy                                 |
| `Flow\ETL\Function\ScalarFunction\ScalarResult`, `ScalarResult::from()`   | removed - return the plain value, declare the type in `returns()`                                              |
| `FrameAccumulator::value(): mixed`                                        | `value(): float\|int\|null`                                                                                    |
| `Parameter::as*()` over a malformed value - `null` / `false` / `$default` | throws `InvalidArgumentException`                                                                              |
| `Parameter::asBoolean(): bool`                                            | `?bool`                                                                                                        |
| `Parameter::asType()`                                                     | removed                                                                                                        |

Before:

```php
final class Shout extends ScalarFunctionChain
{
    public function __construct(private readonly ScalarFunction $value) {}

    public function eval(Row $row, FlowContext $context): ?string
    {
        $value = (new Parameter($this->value))->asString($row, $context);

        return $value === null ? null : strtoupper($value);
    }
}
```

After:

```php
final class Shout implements ScalarFunction
{
    use ScalarFunctionChain;
    use ResolvesFromChildren;

    public function __construct(private readonly ScalarFunction $value) {}

    public function eval(Row $row, FlowContext $context): ?string
    {
        $value = (new Parameter($this->value))->asString($row, $context);

        return $value === null ? null : strtoupper($value);
    }

    public function returns(): Type
    {
        return type_optional(type_string());
    }

    public function children(): array
    {
        return [$this->value];
    }

    public function withChildren(array $children): static
    {
        return new self($children[0]);
    }
}
```

### 27) `flow-php/etl` - user callbacks removed, `call()` takes a `ScalarFunction` and a return type

| Before                                                         | After                                                                    |
|----------------------------------------------------------------|--------------------------------------------------------------------------|
| `$df->map($callback)`                                          | removed                                                                  |
| `Flow\ETL\Transformer\CallbackRowTransformer`                  | removed                                                                  |
| `to_callable($callable)` / `Flow\ETL\Loader\CallbackLoader`    | removed - implement `Flow\ETL\Loader`                                    |
| `Flow\ETL\Transformer\StyleConverter\ArrayKeyConverter`        | removed                                                                  |
| `call('strtoupper', [ref('name')])`                            | `call(lit('strtoupper'), type_string(), [ref('name')])`                  |
| `call($callable, $parameters, $return_type = null)`            | `call(lit($callable), $return_type, $parameters)` - return type required |
| `->call($callable, $arguments, $refAlias, $returnType = null)` | `->call(lit($callable), $returnType, $arguments, $refAlias)`             |
| `new CallUserFunc($callable, $parameters, $returnType)`        | `new CallUserFunc($callable, $returnType, $parameters)`                  |
| `call(..., type_string())` column - `string`                   | `?string`                                                                |
| `lit($closure)`, or a `Closure` inside a `lit()` array         | throws `InvalidArgumentException`                                        |
| `(new TypeDetector())->detectType($closure)`                   | throws `Flow\Types\Exception\InvalidArgumentException`                   |

Before:

```php
$df->map(fn (Row $row): Row => $row->set(str_entry('name', $row->valueOf('name') ?? 'default')));
```

After:

```php
$df->withEntry('name', coalesce(ref('name'), lit('default')));
```

Logic no scalar function expresses: implement `Flow\ETL\Transformer` and pass it to `$df->with($transformer)`.

### 28) `flow-php/etl` - `Transformer` requires `bind()`

| Before                                                           | After                                                   |
|------------------------------------------------------------------|---------------------------------------------------------|
| `Transformer::transform(Rows $rows, FlowContext $context): Rows` | plus `bind(Schema $input): Flow\ETL\Pipeline\BoundStep` |

Return `new BoundStep($this, $output)`, where `$output` is the schema the step emits for `$input`.

### 29) `flow-php/etl` - `Extractor` gains `schema()` and `withSchema()`

| Before                                                               | After                                                            |
|----------------------------------------------------------------------|------------------------------------------------------------------|
| `Extractor` declares only `extract(FlowContext $context): Generator` | also `schema(): Schema` and `withSchema(Schema $schema): static` |
| a custom `Extractor` implementing only `extract()`                   | fatal at load                                                    |

A source that cannot describe its output before reading throws
`Flow\ETL\Exception\SchemaNotDerivableException::extractor(self::class)` from `schema()`.

### 30) `flow-php/etl` - `Entry` and the `*_entry()` functions are removed, a row is a name-keyed array under a `Schema`

| Before                                                                                                                                                                 | After                                                                                                    |
|------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------|
| `Flow\ETL\Row\Entry` and the 18 `Flow\ETL\Row\Entry\*Entry` classes                                                                                                    | removed                                                                                                  |
| `Flow\ETL\Row\Entries`, `entries(...)`                                                                                                                                 | removed                                                                                                  |
| `row(Entry ...$entries)`, `Row::create(...)`, `Row::with(...)`                                                                                                         | `row(array $values)` - `row(['id' => 1])`                                                                |
| `rows(Row ...$rows)`, `new Rows(Row ...$rows)`                                                                                                                         | `rows(Schema $schema, Row ...$rows)`, `new Rows(Schema $schema, Row ...$rows)` - see 31)                 |
| `$row->get('id')` returned an `Entry`                                                                                                                                  | returns the value                                                                                        |
| `$row->valueOf('id')`                                                                                                                                                  | `$row->get('id')`                                                                                        |
| `$row->entries()`                                                                                                                                                      | `$row->values()` / `$row->names()`                                                                       |
| `$row->schema()`                                                                                                                                                       | `$rows->schema()`                                                                                        |
| `$row->hash()`                                                                                                                                                         | `$row->hash($rows->schema())`                                                                            |
| `$row->add()` / `set()` / `remove()` / `keep()` / `rename()` / `renameMany()` / `map()` / `merge()` / `isEqual()` / `sortEntries()`                                    | removed                                                                                                  |
| `$rows->entries()`                                                                                                                                                     | removed                                                                                                  |
| `Metadata` per entry, free to differ row to row: `int_entry('id', 1, $metadata)`                                                                                       | one `Metadata` per column: `int_schema('id', metadata: $metadata)`                                       |
| `compare_entries_by_name()` / `compare_entries_by_name_desc()` / `compare_entries_by_type()` / `compare_entries_by_type_desc()` / `compare_entries_by_type_and_name()` | removed - sort a schema: `$schema->sort(schema_sort_by_name())`                                          |
| `Flow\ETL\Transformer\OrderEntries\{Comparator, CombinedComparator, NameComparator, TypeComparator, TypePriorities, Order}`                                            | removed - implement `Flow\ETL\Schema\SortingStrategy::compare(Definition $left, Definition $right): int` |

Before:

```php
$rows = rows(
    row(int_entry('id', 1), str_entry('name', 'Norbert')),
    row(int_entry('id', 2), str_entry('name', null)),
);
```

After:

```php
$rows = rows(
    schema(int_schema('id'), str_schema('name', nullable: true)),
    row(['id' => 1, 'name' => 'Norbert']),
    row(['id' => 2, 'name' => null]),
);
```

| Before                                                      | Column in `schema(...)`                  | Value in `row([...])`                                           |
|-------------------------------------------------------------|------------------------------------------|-----------------------------------------------------------------|
| `bool_entry('x', true)`, `boolean_entry('x', true)`         | `bool_schema('x')`                       | `true`                                                          |
| `int_entry('x', 1)`, `integer_entry('x', 1)`                | `int_schema('x')`, `integer_schema('x')` | `1`                                                             |
| `float_entry('x', 1.5)`                                     | `float_schema('x')`                      | `1.5`                                                           |
| `float_entry('x', 1)`, `float_entry('x', '1.5')`            | `float_schema('x')`                      | `type_float()->cast(1)`, `type_float()->cast('1.5')`            |
| `str_entry('x', 'a')`, `string_entry('x', 'a')`             | `str_schema('x')`, `string_schema('x')`  | `'a'`                                                           |
| `null_entry('x')`                                           | `null_schema('x')`                       | `null`                                                          |
| `datetime_entry('x', '2024-01-02 10:00:00')`                | `datetime_schema('x')`                   | `type_datetime()->cast('2024-01-02 10:00:00')`                  |
| `date_entry('x', '2024-01-02')`                             | `date_schema('x')`                       | `type_date()->cast('2024-01-02')`                               |
| `time_entry('x', 'PT1H')`                                   | `time_schema('x')`                       | `type_time()->cast('PT1H')`                                     |
| `enum_entry('x', Suit::Hearts)`                             | `enum_schema('x', Suit::class)`          | `Suit::Hearts`                                                  |
| `json_entry('x', ['a' => 1])`, `json_entry('x', '{"a":1}')` | `json_schema('x')`                       | `type_json()->cast(['a' => 1])`, `type_json()->cast('{"a":1}')` |
| `json_object_entry('x', [])`                                | `json_schema('x')`                       | `Flow\Types\Value\Json::fromArray([], asObject: true)`          |
| `uuid_entry('x', $uuid)`                                    | `uuid_schema('x')`                       | `type_uuid()->cast($uuid)`                                      |
| `xml_entry('x', '<a/>')`                                    | `xml_schema('x')`                        | `type_xml()->cast('<a/>')`                                      |
| `xml_element_entry('x', '<a/>')`                            | `xml_element_schema('x')`                | `type_xml_element()->cast('<a/>')`                              |
| `html_entry('x', $html)`                                    | `html_schema('x')`                       | `type_html()->cast($html)`                                      |
| `html_element_entry('x', $html)`                            | `html_element_schema('x')`               | `type_html_element()->cast($html)`                              |
| `structure_entry('x', $value, $type)`, `struct_entry(...)`  | `structure_schema('x', $type)`           | `$value`                                                        |
| `list_entry('x', $value, $type)`                            | `list_schema('x', $type)`                | `$value`                                                        |
| `map_entry('x', $value, $type)`                             | `map_schema('x', $type)`                 | `$value`                                                        |
| `to_entry('x', $value)`                                     | the `*_schema('x')` matching `$value`    | `$value`                                                        |
| any `*_entry('x', null)`                                    | `*_schema('x', nullable: true)`          | `null`                                                          |
| any `*_entry('x', $value, $metadata)`                       | `*_schema('x', metadata: $metadata)`     | `$value`                                                        |

### 31) `flow-php/etl` - `Rows` holds only rows that match its schema

| Before                                                                                                         | After                                                                                                                                                                                      |
|----------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| any row accepted                                                                                               | `new Rows($schema, ...$rows)` throws `Flow\ETL\Exception\SchemaMismatchException` on a wrong-typed value, a `null` in a NOT NULL column, an undeclared column or a missing NOT NULL column |
| -                                                                                                              | `Flow\ETL\Exception\ColumnMismatchException`, the `getPrevious()` of `SchemaMismatchException`                                                                                             |
| a row without a declared nullable column                                                                       | padded with `null`                                                                                                                                                                         |
| `$rows->toArray()` - keys in each row's insertion order                                                        | schema order                                                                                                                                                                               |
| `$rows->map()` / `flatMap()` / `each()` / `filter()` / `find()` / `findOne()` / `reduce()` / `sort($callable)` | removed - see below                                                                                                                                                                        |
| `$rows->sortEntries()` / `$df->reorderEntries()` / `Flow\ETL\Transformer\OrderEntriesTransformer`              | removed - `$df->select('a', 'b')` sets the column order                                                                                                                                    |
| `Schema::isSame()` ignored column order                                                                        | order-sensitive                                                                                                                                                                            |
| `$rows->merge($other)`, `$other` with other columns or another column order - rows concatenated                | throws `InvalidArgumentException`: `Cannot merge Rows with different schemas: [a: integer] and [a: string]`                                                                                |
| declared NOT NULL column, empty cell or short line (`from_csv($p)->withSchema($schema)`) - `null`              | throws `SchemaMismatchException` - declare the column `nullable: true`                                                                                                                     |
| `from_rows($a, $b)` - each batch kept its own columns                                                          | every batch carries every column of the combined schema, missing ones as `null`                                                                                                            |
| `batches($extractor, $n)` / `batched_by(...)` over child batches of different shapes - mixed in one batch      | later child batches matched to the first: missing nullable column padded, extra column throws `SchemaMismatchException`                                                                    |

Before:

```php
$adults = $rows->filter(fn (Row $row): bool => $row->valueOf('age') >= 18);
```

After:

```php
$adults = new Rows($rows->schema(), ...array_filter($rows->all(), fn (Row $row): bool => $row->get('age') >= 18));
```

### 32) `flow-php/etl` / `flow-php/etl-adapter-excel` - `Entry` parameters become the value and its `Definition`

| Before                                                                                       | After                                                                                              |
|----------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------|
| `Definition::matches(Entry $entry)`                                                          | `matches(mixed $value)` - the caller checks the name                                               |
| `Row\Comparator::equals(Row $row, Row $nextRow)`                                             | `equals(Row $row, Row $nextRow, Schema $schema)`                                                   |
| `RenameEntryStrategy::rename(Row $row): Row`                                                 | `renames(Schema $schema): array` - `current_name => new_name`, `[]` when nothing is renamed        |
| `CellStyler::style(Entry $entry, int $rowNumber, int $columnIndex, string $sheetName)`       | `style(mixed $value, Definition $definition, int $rowNumber, int $columnIndex, string $sheetName)` |
| `new ASCIIValue($value)` - an `Entry` or a scalar/array                                      | `new ASCIIValue(Type $type, mixed $value)`                                                         |
| `Dataset\Statistics\Column::__construct(Entry $entry)`                                       | `__construct(Definition $definition, mixed $value)`                                                |
| `Dataset\Statistics\Column::calculate(Entry $entry)`                                         | `add(Definition $definition, mixed $value)`                                                        |
| `Dataset\Statistics\Columns::add(Entry $entry)`                                              | `add(Definition $definition, mixed $value)`                                                        |
| `new NullRowBuilder(EntryFactory $entryFactory)` + `->collect($row)`                         | `new NullRowBuilder(Schema $schema)`, `collect()` removed                                          |
| `new RowsBuffer(int $size)`                                                                  | `new RowsBuffer(Schema $schema, int $size)`                                                        |
| `Rows::joinLeft($right, $on, $entryFactory)` / `Rows::joinRight($right, $on, $entryFactory)` | third argument removed                                                                             |
| `Flow\ETL\Row\EntryFactory`, `Row\Entry\EntryInstantiator`, `Row\Entry\Instantiators`        | removed                                                                                            |
| `EntryTypeResolver::fromDefinition()`                                                        | removed                                                                                            |

### 33) `flow-php/etl` - `Config` options move to the extractors or are removed

| Before                                                                                                 | After                                                                                                    |
|--------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------|
| `data_frame(config_builder()->putInputIntoRows())->read(from_csv($path))`                              | `data_frame()->read(from_csv($path)->withMetadataColumns(true))`                                         |
| `ConfigBuilder::putInputIntoRows()` / `dontPutInputIntoRows()`, `Config::shouldPutInputIntoRows()`     | removed                                                                                                  |
| `config_builder()->extractorBatchSize(500)`                                                            | `from_csv($path)->withBatchSize(500)` - on each extractor                                                |
| `Config::extractorBatchSize()`                                                                         | removed - `$extractor->batchSize()`                                                                      |
| `FlowContext::entryFactory()`                                                                          | removed                                                                                                  |
| `new Config(...)` with `$filesystemTable`, `$putInputIntoRows`, `$extractorBatchSize`, `$entryFactory` | those parameters removed, `HashRepartitionConfig $repartition` added - build through `Config::builder()` |

`withMetadataColumns()` is on `from_csv()`, `from_json()`, `from_json_lines()`, `from_parquet()`, `from_text()`,
`from_xml()`, `from_excel()`, `from_google_sheet()`, `from_google_sheet_columns()` and `from_floe()`.

### 34) `flow-php/etl` - extractors yield batches

| Before                                                                                                                   | After                                                                                                                                                                                                          |
|--------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| every extractor yielded one row per `Rows`                                                                               | up to `batchSize()` rows per `Rows`: 100 by default, 1000 for `DbalLimitOffsetExtractor`, `DbalKeySetExtractor`, `PostgreSqlLimitOffsetExtractor`, `PostgreSqlKeySetExtractor` and `PostgreSqlCursorExtractor` |
| `Flow\ETL\Pipeline\Optimizer\BatchSizeOptimization` re-cut batches to 1000 rows before `DbalLoader` / `PostgreSqlLoader` | removed - `$df->batchSize(1000)` before `write()`                                                                                                                                                              |
| `new BatchExtractor($extractor, chunkSize: 10)`                                                                          | `new BatchExtractor($extractor, batchSize: 10)` - a size below 1 throws                                                                                                                                        |
| `$df->duplicateRow(...)` appended the copies after the batch's last row                                                  | each copy follows the row it duplicates                                                                                                                                                                        |

### 35) `flow-php/etl` - `LimitableExtractor` replaced by `LimitPushDown`

| Before                                                                         | After                                                                            |
|--------------------------------------------------------------------------------|----------------------------------------------------------------------------------|
| `Flow\ETL\Extractor\LimitableExtractor`                                        | `Flow\ETL\Extractor\LimitPushDown`                                               |
| `Flow\ETL\Extractor\Limitable` (trait)                                         | `Flow\ETL\Extractor\PushesLimit`                                                 |
| `changeLimit(int $limit): void`                                                | `pushLimit(int $limit): void` - a second push can only lower the limit           |
| `limit(): ?int`                                                                | `pushedLimit(): ?int`                                                            |
| `isLimited(): bool`                                                            | removed - `pushedLimit() !== null`                                               |
| `incrementReturnedRows()` / `reachedLimit()` / `resetLimit()`                  | removed                                                                          |
| a pushed limit replaced the `limit()` step, so the extractor had to stop at it | the `limit()` step stays; stopping early is optional                             |
| `new LimitReachedException($limit, $previous)`                                 | `new LimitReachedException($limit, $rows, $previous)` - pass `previous:` by name |

### 36) `flow-php/etl` - `ErrorHandler` answers per stage: extraction, transformation, loading

| Before                                                                                            | After                                                                                            |
|---------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------|
| `ErrorHandler::throw(Throwable $error, Rows $rows): bool`                                         | `onExtraction(ExtractionError $error): ExtractionAction` - `propagate` / `endSource`             |
| `ErrorHandler::skipRows(Throwable $error, Rows $rows): bool`                                      | `onTransformation(TransformationError $error): TransformationAction` - `propagate` / `skipBatch` |
| -                                                                                                 | `onLoading(LoadingError $error): LoadingAction` - `propagate` / `skipLoader`                     |
| an extractor failure always propagated                                                            | goes to `onExtraction()`: `IgnoreError` and `SkipRows` end the source, `ThrowError` rethrows     |
| `IgnoreError`: a failed transformation ran the remaining steps on the half-transformed batch      | the batch is dropped                                                                             |
| `SkipRows` / `skip_rows_handler()`: a failed transformation emitted the half-transformed batch    | the batch is dropped                                                                             |
| `SkipRows`: a failed loader was swallowed and the batch's remaining steps skipped                 | rethrown                                                                                         |
| `SkipRows`: a drain failure of `to_transformation()` / `to_branch()` at `closure()` was swallowed | rethrown                                                                                         |

The error objects and the action enums are in `Flow\ETL\ErrorHandler`.

Before:

```php
final class SkipInvalidBatches implements ErrorHandler
{
    public function throw(Throwable $error, Rows $rows): bool
    {
        return !$error instanceof InvalidArgumentException;
    }

    public function skipRows(Throwable $error, Rows $rows): bool
    {
        return true;
    }
}
```

After:

```php
final class SkipInvalidBatches implements ErrorHandler
{
    public function onExtraction(ExtractionError $error): ExtractionAction
    {
        return ExtractionAction::propagate;
    }

    public function onTransformation(TransformationError $error): TransformationAction
    {
        return $error->cause instanceof InvalidArgumentException
            ? TransformationAction::skipBatch
            : TransformationAction::propagate;
    }

    public function onLoading(LoadingError $error): LoadingAction
    {
        return $error->cause instanceof InvalidArgumentException
            ? LoadingAction::skipLoader
            : LoadingAction::propagate;
    }
}
```

### 37) `flow-php/etl` - `Cache` gains `schema()`

| Before                                              | After                                                                                    |
|-----------------------------------------------------|------------------------------------------------------------------------------------------|
| -                                                   | `Cache::schema(string $key): Schema`, throws `KeyNotInCacheException` for an unknown key |
| a custom `Flow\ETL\Cache` implementation without it | fatal at load                                                                            |

### 38) `flow-php/etl` - data cached or serialized by 0.43.x cannot be read back

| Before                                                                                                          | After          |
|-----------------------------------------------------------------------------------------------------------------|----------------|
| `Rows` stored by `FilesystemCache`, `PSRSimpleCache`, `ApcuCache`, `PSRCacheBuckets` or `serialize_to_string()` | cannot be read |

Clear persistent caches and regenerate stored `serialize_to_string()` payloads after upgrading.

### 39) `flow-php/etl` - in-memory sources infer one nullable schema from the first 20,480 rows

| Before                                                                                           | After                                                                                                     |
|--------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------|
| `df()->read(from_array([['id' => 1]]))->schema()` - `id: integer`                                | `id: ?integer`; the same for `from_memory()` and `from_sequence_*()`                                      |
| `from_array()` / `from_memory()` / `from_sequence_*()` typed each row on its own                 | columns inferred from the first 20,480 rows                                                               |
| a row past the sample whose value the inferred type refuses - kept with its own type             | throws `Flow\ETL\Exception\InferredSchemaException`                                                       |
| -                                                                                                | `from_array($rows)->inferSchema(infer_schema()->sampleSize(-1))` - infer from every row                   |
| `from_array([['a' => 1], ['a' => 2, 'b' => 'x']])` - first row has no `b`                        | first row `'b' => null`                                                                                   |
| `from_array([['a' => 'x'], ['a' => 1000]])` - second row `1000`                                  | `'1000'` - values follow the widened column type                                                          |
| `from_array([['tz' => new DateTimeZone('Europe/Warsaw')]])` - `'Europe/Warsaw'`, column `string` | `DateTimeZone` object, column `?timezone`                                                                 |
| `from_array([['s' => ['j' => null]]])` - `s: structure{j: null}`                                 | `s: ?structure{j: ?string}`                                                                               |
| `from_array([['o' => new stdClass()]])` - `InvalidArgumentException` on `schema()` and on read   | `schema()` gives `o: ?string`; the read throws `SchemaMismatchException`                                  |
| `from_array($generator)` - streamed                                                              | read in full into `<spillRoot>/flow-php-source/` before the first row; an endless generator never returns |
| `from_array($generator)` read twice - `Cannot traverse an already closed generator`              | replayed from the spill file                                                                              |
| -                                                                                                | `from_array($generator, $schema)` or `->withSchema($schema)` - streamed, no spill, one read only          |
| -                                                                                                | `from_array($generator, spillRoot: path($dir))` - default is the filesystem's system tmp dir              |
| `from_memory($memory)` read again after `$memory->save()` - saw the new columns                  | keeps the schema of its first non-empty read; create a new `from_memory($memory)`                         |

`InferredSchemaException` is not a `SchemaMismatchException`; the mismatch is its `getPrevious()`.

### 40) `flow-php/etl` - `Hydrator`, `array_to_row()` and `array_to_rows()` require a `Schema`

| Before                                                    | After                                                                           |
|-----------------------------------------------------------|---------------------------------------------------------------------------------|
| `Hydrator::cast(array $batch, ?Schema $schema = null)`    | removed - use `hydrate($batch, $schema)`                                        |
| `Hydrator::hydrate(array $batch, ?Schema $schema = null)` | `hydrate(array $batch, Schema $schema)`                                         |
| `array_to_row($data, $hydrator, $partitions, $schema)`    | `array_to_row($data, $schema, $hydrator, $partitions)`                          |
| `array_to_rows($data, $hydrator, $partitions, $schema)`   | `array_to_rows($data, $schema, $hydrator)` - put partition columns into `$data` |
| `array_to_rows($data)`                                    | `df()->read(from_array($data))->fetch()`                                        |

### 41) `flow-php/etl` / `flow-php/types` - `autoCast()` removed

| Before                                     | After   |
|--------------------------------------------|---------|
| `DataFrame::autoCast()`                    | removed |
| `Flow\ETL\Transformer\AutoCastTransformer` | removed |
| `Flow\Types\Type\AutoCaster`               | removed |

Before:

```php
df()->read(from_csv($path))->autoCast()->fetch();
df()->read(from_array([['id' => '1', 'total' => '10.5']]))->autoCast()->fetch();
```

After:

```php
df()->read(from_csv($path))->fetch();
df()->read(from_array([['id' => '1', 'total' => '10.5']], schema(int_schema('id'), float_schema('total'))))->fetch();
```

`from_array()` types by PHP value: numeric strings stay `?string` unless the schema is declared.

### 42) `flow-php/etl` - a value a declared column refuses throws `SchemaMismatchException`

| Before                                                                                                                  | After                                                                                                                                                   |
|-------------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------|
| `from_csv($path)->withSchema($schema)`, `'n/a'` in a `datetime` column - throws `Flow\Types\Exception\CastingException` | throws `Flow\ETL\Exception\SchemaMismatchException`: `Rows do not match their schema: column "d" (row 1): could not convert 'n/a' (string) to datetime` |
| `getPrevious()` of the thrown exception                                                                                 | `Flow\ETL\Exception\ColumnMismatchException`, never the `CastingException`                                                                              |
| `type_datetime()->cast('n/a')` - throws `CastingException`                                                              | unchanged                                                                                                                                               |

Replace `catch (CastingException $e)` around a read with `catch (SchemaMismatchException $e)`.

### 43) `flow-php/etl` - `DataFrame::schema()` describes the plan without running it

| Before                                                                                                                   | After                                                                                  |
|--------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------|
| `$df->schema()` ran the pipeline, loaders included, and merged every batch's schema                                      | derived from the source's `schema()` through each step; no row is read, no loader runs |
| `$df->schema()` on a plan containing `joinEach()` - ran the pipeline                                                     | throws `DataDependentSchemaException`                                                  |
| `$df->schema()` over a source that cannot describe its schema, also through `from_data_frame($inner)` - ran the pipeline | throws `SchemaNotDerivableException`                                                   |
| `schema()` reported `dataFrameCompleted` / `dataFrameFailed` telemetry                                                   | reports neither                                                                        |
| `$df->fetch()` returning no rows, `$df->void()->fetch()` - `Rows` with an empty schema                                   | `Rows` carrying the plan's columns                                                     |

### 44) `flow-php/etl` - `printSchema()` prints the plan's schema and takes no limit

| Before                                                                                   | After                                                                        |
|------------------------------------------------------------------------------------------|------------------------------------------------------------------------------|
| `printSchema(?int $limit = 20, SchemaFormatter $formatter = new ASCIISchemaFormatter())` | `printSchema(SchemaFormatter $formatter = new ASCIISchemaFormatter())`       |
| `$df->printSchema(20, $formatter)`                                                       | `$df->printSchema($formatter)`                                               |
| ran the pipeline, loaders included, printing one schema per batch                        | prints `$df->schema()` once and runs nothing; throws where `schema()` throws |
| `$df->printSchema(); $df->fetch()` - at most 20 rows                                     | every row                                                                    |

### 45) `flow-php/etl` - `display()` and `printRows()` render one table

| Before                                                      | After                          |
|-------------------------------------------------------------|--------------------------------|
| `$df->display()` / `$df->printRows()` - one table per batch | one table                      |
| `$df->printRows(null)` streamed the frame batch by batch    | collects the whole frame first |

### 46) `flow-php/etl` - `select()` and `groupBy()` refuse an undeclared column

| Before                                                               | After                                                                                          |
|----------------------------------------------------------------------|------------------------------------------------------------------------------------------------|
| `$df->select('missing')` - added a `missing` string column of `null` | throws `SchemaDefinitionNotFoundException`: `Schema definition for entry "missing" not found.` |
| `$df->groupBy(['missing'])` - grouped every row under a `null` key   | throws `SchemaDefinitionNotFoundException`                                                     |

### 47) `flow-php/etl` - renaming onto an existing column throws

| Before                                                                              | After                                                                                                                    |
|-------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------|
| `$df->rename('a_b', 'ab')` while `ab` exists - `a_b` replaced `ab`                  | throws `SchemaDefinitionNotUniqueException`: `Entry definitions must be unique, duplicated entries: [ab], all: [ab, ab]` |
| `$df->renameEach(rename_replace('_', ''))` producing an existing name - replaced it | throws `SchemaDefinitionNotUniqueException`                                                                              |

### 48) `flow-php/etl` - float values compare exactly

| Before                                                                                                      | After                                   |
|-------------------------------------------------------------------------------------------------------------|-----------------------------------------|
| `$rows->unique()` / `diffLeft()` / `diffRight()` - floats sharing an integer part were equal (`1.5 == 1.9`) | exact: `1.5 != 1.9`, `0.1 + 0.2 != 0.3` |
| `rank()` / `dense_rank()` peers over a float column - same rule                                             | exact                                   |

### 49) `flow-php/etl` - `Window` and window functions are immutable

| Before                                                                                  | After                                                           |
|-----------------------------------------------------------------------------------------|-----------------------------------------------------------------|
| `$window->partitionBy(...)` / `->orderBy(...)` / `->rowsBetween(...)` changed `$window` | return a copy - use the returned window                         |
| `$function->over($window)` changed `$function`                                          | returns a copy                                                  |
| `window()->partitionBy(ref('a'))->partitions()` - `array<Reference>`                    | `References`; `->all()` returns the array                       |
| `window()->partitionBy(ref('nope'))` - every row in one partition                       | throws `SchemaDefinitionNotFoundException` before the first row |
| `window()->orderBy(ref('nope'))` - threw per row                                        | throws `SchemaDefinitionNotFoundException` before the first row |

### 50) `flow-php/etl` - group and join key extraction reads the schema

| Before                                                                                        | After                                                                                      |
|-----------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------|
| `GroupBy::keyValues(Row $row)`                                                                | `keyValues(Row $row, Schema $input)`                                                       |
| `GroupBy::aggregatedRow(GroupKey $key, Aggregators $aggregators, EntryFactory $entryFactory)` | `aggregatedRow(GroupKey $key, Aggregators $aggregators, Schema $output)`                   |
| `KeyValues::ofRow(Row $row)`                                                                  | `ofRow(Row $row, Schema $schema)`                                                          |
| `new KeyValues($refs, nullOnMissing: true)`                                                   | parameter removed - a missing key is `null` under a nullable column, throws under NOT NULL |
| `new HashBucketing(..., nullOnMissing: true)`                                                 | parameter removed                                                                          |
| `join()` over a row without the join column - threw                                           | the row is unmatched                                                                       |

### 51) `flow-php/etl` - a join producing a duplicate column throws `SchemaDefinitionNotUniqueException`

| Before                                                                                                                                                                                                                      | After                                                                                                                                                    |
|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------|
| `$df->join($right, join_on(['id' => 'id']))`, both sides carry `name` - `JoinException` (`RuntimeException`): `Merged entries names must be unique, given: [id, name] + [name] try to use a different join prefix than: ""` | `SchemaDefinitionNotUniqueException` (`InvalidArgumentException`): `Entry definitions must be unique, duplicated entries: [name], all: [id, name, name]` |
| thrown when the first pair of rows was merged                                                                                                                                                                               | thrown when the pipeline starts, before any row is read                                                                                                  |
| `$rows->joinCross($right, '')` over shared columns - `InvalidArgumentException`: `... + [id, name]. Please consider using join prefix option`                                                                               | `SchemaDefinitionNotUniqueException`: `Entry definitions must be unique, duplicated entries: [id, name], all: [id, name, id, name]`                      |
| `catch (JoinException $e)` around a join                                                                                                                                                                                    | `catch (SchemaDefinitionNotUniqueException $e)`, or give the right side a prefix: `join_on(['id' => 'id'], 'joined_')`                                   |

### 52) `flow-php/etl` - `Joiner` takes `JoinSide` and no `EntryFactory`, `Expression` drop helpers removed

| Before                                                                                 | After                                                                                                                          |
|----------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------|
| `Flow\ETL\Join\Expression::dropDuplicateLeftEntries()` / `dropDuplicateRightEntries()` | removed                                                                                                                        |
| `$joiner->join($left, $right, $nullLeftRow, $nullRightRow, $buildLeft)`                | `$joiner->join(JoinSide::of($left, $nullLeftRow, $leftSchema), JoinSide::of($right, $nullRightRow, $rightSchema), $buildLeft)` |
| `new Joiner($expression, $type, $entryFactory, $batchSize)`                            | `new Joiner($expression, $type, $batchSize)`                                                                                   |

### 53) `flow-php/etl` - a join against an empty side keeps that side's columns

| Before                                                                                    | After                                  |
|-------------------------------------------------------------------------------------------|----------------------------------------|
| `$df->join($emptyRight, $on, Join::left)` - right-hand columns dropped                    | kept, `null`                           |
| `$df->join($right, $on, Join::right)` with an empty left side - left-hand columns dropped | kept, `null`                           |
| `$rows->joinCross($empty)` / `$empty->joinCross($rows)` - the non-empty side's rows       | zero rows carrying both sides' columns |

### 54) `flow-php/etl` - a global `aggregate()` over zero rows returns one row

| Before                                                                                                             | After                                        |
|--------------------------------------------------------------------------------------------------------------------|----------------------------------------------|
| `$df->aggregate(sum(ref('a')), count(ref('a')))` over zero rows, or after a `filter()` removed every row - no rows | one row, `['a_sum' => null, 'a_count' => 0]` |
| `collect()` / `collect_unique()` / `string_agg()` over zero rows - no rows                                         | `[]` / `[]` / `''`                           |
| `$df->groupBy('k')->aggregate(...)` over zero rows - no rows                                                       | unchanged                                    |

### 55) `flow-php/etl` - `pivot()` takes its values

Before:

```php
$df->groupBy('date')->pivot(ref('product'))->aggregate(sum(ref('amount')));
```

After:

```php
$df->groupBy(['date'])->pivot(ref('product'), pivot_values('A', 'B'))->aggregate(sum(ref('amount')));
// or
$df->groupBy(['date'])->pivot(ref('product'), discover_pivot_values())->aggregate(sum(ref('amount')));
```

| Before                                                           | After                                                                                                          |
|------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------|
| `GroupedDataFrame::pivot(Reference $ref)`                        | `pivot(Reference $ref, PivotValues $values)`                                                                   |
| `GroupBy::pivot(Reference $ref)`                                 | `GroupBy::pivot(Reference $ref, DeclaredPivotValues $values)`                                                  |
| one column per value found in the data                           | `pivot_values(...)`: one per declared value, `null` where no row carries it, undeclared values dropped         |
| -                                                                | `discover_pivot_values()`: one per non-null value in the frame, sorted, at most `maxValues` (default `10_000`) |
| pivot value `0` or `'0'` - column `e00`                          | column `0`                                                                                                     |
| pivot value `7` - column `e07`                                   | column `7`                                                                                                     |
| pivot value equal to a group-by column name - its column dropped | throws `InvalidArgumentException`                                                                              |
| `GroupBy::pivotResult($rows, $context, $batchSize)`              | removed - `(new PivotAggregation($batchSize))->aggregate($rows, $context, $groupBy)`                           |

A custom extractor read by `discover_pivot_values()` must implement `Flow\ETL\Extractor\RewindableExtractor`.

### 56) `flow-php/etl` - `unpack()` / `array_unpack()` take the `Schema` of the columns they produce

Before:

```php
$df->withEntry('row', ref('row')->unpack(['internal_id']));
```

After:

```php
$df->withEntry('row', ref('row')->unpack(schema(int_schema('id'), str_schema('name'))));
```

| Before                                                         | After                                                |
|----------------------------------------------------------------|------------------------------------------------------|
| `ref('x')->unpack($skipKeys, $entryPrefix)`                    | `ref('x')->unpack(Schema $schema)`                   |
| `array_unpack($array, $skip_keys, $entry_prefix)`              | `array_unpack($array, Schema $schema)`               |
| every payload key except `$skipKeys` became a column           | only the declared columns; other keys are dropped    |
| column type inferred from each value                           | the declared type, nullable                          |
| a declared key missing from one payload - no entry in that row | `null`                                               |
| `$entryPrefix` prepended to each key                           | removed - columns are named `<withEntry name>.<key>` |

A custom `Flow\ETL\Function\ScalarFunction\UnpackResults` function must return a `StructureType` of its columns from
`returns()`.

### 57) `flow-php/etl` - `on_each()` requires an operand that declares its element type

| Before                                                                 | After                                                                              |
|------------------------------------------------------------------------|------------------------------------------------------------------------------------|
| `ref('a')->onEach(ref('element'))` with `a` = `[1, 'x']` -> `[1, 'x']` | throws `SchemaNotDerivableException`                                               |
| -                                                                      | `ref('a')->cast(type_list(type_string()))->onEach(ref('element'))` -> `['1', 'x']` |

### 58) `flow-php/etl` - `isIn()` compares numbers loosely and refuses incomparable types

| Before                                                                                     | After                                     |
|--------------------------------------------------------------------------------------------|-------------------------------------------|
| `ref('a')->isIn(lit([1]))` with `a` = `'1'` - `false`                                      | `true`, as `ref('a')->equals(lit(1))`     |
| `ref('a')->isIn(lit([new DateTimeImmutable('2024-01-01')]))` with `a` a `string` - `false` | throws `Can't compare '(string == date)'` |
| `ref('a')->isIn(lit([1, 2]))` with `a` = `null` - `false`                                  | `null`, `filter()` drops the row          |

### 59) `flow-php/etl` - `Calculator::divide()` always returns `float`

| Before                                    | After |
|-------------------------------------------|-------|
| `(new Calculator())->divide(4, 2)` - `2`  | `2.0` |
| `ref('a')->divide(lit(2))` over `4` - `2` | `2.0` |

### 60) `flow-php/etl` - `DataFrame::partitionBy()` removed, loaders partition, `Rows` carry no partitions

| Before                                                                                           | After                                                                     |
|--------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------|
| `$df->partitionBy('year')->write(to_csv($path))`                                                 | `$df->write(to_csv($path)->partitionBy(partition_by('year')))`            |
| partitioned write - the partition column is written into the file body as well                   | path only; `partition_by('year')->writeColumns()` keeps it in the body    |
| `$df->partitionBy('year')` without a partitioned write                                           | `$df->repartition('year')`                                                |
| `$df->dropPartitions()`                                                                          | removed                                                                   |
| `$df->dropPartitions(true)`                                                                      | `$df->drop('year')`                                                       |
| `Rows::partitioned()` / `rows_partitioned()`                                                     | removed                                                                   |
| `Rows::partitionBy()` / `partitions()` / `isPartitioned()` / `dropPartitions()`                  | removed                                                                   |
| `Flow\ETL\Transformer\DropPartitionsTransformer`                                                 | removed                                                                   |
| reading `year=2024/month=01/` - partition columns appended in path order (`year`, `month`)       | appended in name order (`month`, `year`)                                  |
| reading `year=__HIVE_DEFAULT_PARTITION__/` - `year` is the string `'__HIVE_DEFAULT_PARTITION__'` | `null`, and `year` is nullable                                            |
| same read with `withSchema()` declaring `year` NOT NULL - the string                             | throws `InvalidArgumentException`                                         |
| reading `year=abc/` with `withSchema()` declaring `year` as `integer` - `0`                      | throws `InvalidArgumentException` naming the column, file, type and value |
| `display()` / `printRows()` / `to_output()` printed a `Partitions:` footer                       | no footer                                                                 |

### 61) `flow-php/etl` - `filterPartitions()` compares the raw string partition value

| Before                                                                                                        | After                                                                                                                                        |
|---------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------|
| `filterPartitions(ref('date')->equals(lit(new DateTimeImmutable('2024-01-01'))))` - matched `date=2024-01-01` | throws `Can't compare '(string == date)'`; use `lit('2024-01-01')`, or `from_csv($path)->partitionTypes(partition_types(date: type_date()))` |
| `filterPartitions(ref('active')->equals(lit(false)))` - matched `active=false`                                | throws `Can't compare '(string == boolean)'`; use `lit('false')`, or `partition_types(active: type_boolean())`                               |
| `filterPartitions(ref('date')->equals(lit('2024-01-01')))` - throws `Can't compare '(date == string)'`        | matches `date=2024-01-01`                                                                                                                    |
| a `DateTimeImmutable` or `bool` literal over `files()` / `from_path_partitions()` - matched                   | matches nothing; compare a string literal                                                                                                    |
| `new ScalarFunctionFilter($function, $entryFactory, $caster, $context)`                                       | `new ScalarFunctionFilter($function, Schema $partitions, $context)`                                                                          |

### 62) `flow-php/etl` / `flow-php/types` - missing-column and type-mismatch messages changed

| Before                                                                                               | After                                                                                                               |
|------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------|
| `Schema definition for entry "x" not found`                                                          | `Schema definition for entry "x" not found.`, plus ` Did you mean one of: [a]?` when up to 3 column names are close |
| `$row->get('x')`: `Entry "x" does not exist. Did you mean one of the following? ["every", "column"]` | `Column "x" does not exist.`, plus ` Did you mean one of the following? ["a"]` when up to 3 column names are close  |
| `Can't compare '(?integer > ?date)' due to data type mismatch.`                                      | `Can't compare '(?integer > ?date)' due to data type mismatch - an explicit cast is required.`                      |

### 63) `flow-php/etl` - pipeline and generator extractors and `Schema::fromPipeline()` removed

| Before                                                          | After                            |
|-----------------------------------------------------------------|----------------------------------|
| `from_pipeline($pipeline)` / `new PipelineExtractor($pipeline)` | removed - `from_data_frame($df)` |
| `Flow\ETL\Extractor\GeneratorExtractor`                         | removed                          |
| `Schema::fromPipeline($pipeline, $context, $maxRows)`           | removed - `$df->schema()`        |

### 64) `flow-php/etl` - `until()` stops at the first row failing its predicate

| Before                                                                                        | After    |
|-----------------------------------------------------------------------------------------------|----------|
| `$df->until(ref('v')->lessThan(lit(3)))` over `[1, 2, 5, 1, 2]` in one batch - `[1, 2, 1, 2]` | `[1, 2]` |

### 65) `flow-php/etl` - `UnserializeTransformer` requires the payload schema

| Before                                                                                    | After                                                                |
|-------------------------------------------------------------------------------------------|----------------------------------------------------------------------|
| `new UnserializeTransformer($source, $merge, $mergePrefix)`                               | `new UnserializeTransformer($source, $schema, $merge, $mergePrefix)` |
| payload missing, not a string, not unserializable or not one row - row returned unchanged | declared payload columns added as `null`                             |

### 66) `flow-php/etl` - Floe reader drops `RowPadding`, `conform:` and `lenient:`

| Before                                                          | After                                                         |
|-----------------------------------------------------------------|---------------------------------------------------------------|
| `Flow\Floe\RowPadding`                                          | removed                                                       |
| `FloeStreamReader::rows($batchSize, $offset, $limit, $conform)` | `rows($batchSize, $offset, $limit)`                           |
| `FrameReader::frames(lenient: true)`                            | `frames()` - a truncated stream always throws `FloeException` |

### 67) `flow-php/etl` / `flow-php/etl-adapter-parquet` - a Floe or Parquet glob checks every file against the first

| Before                                                                                                                      | After                                                                                                   |
|-----------------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------|
| `from_parquet($glob)` / `from_floe($glob)`, a later file with other columns or column types - rows kept that file's columns | throws `InferredSchemaException` naming both files - read with `->unionByName()` or `->withSchema(...)` |
| same read, a later file with the same columns in another order - rows kept that file's order                                | rows follow the first file's column order                                                               |
| -                                                                                                                           | `->unionByName()` reads every file under the merged schema; a column a file lacks is `null`             |

### 68) `flow-php/etl` and every package with DSL functions - documentation attributes leave the packages

| Before                                    | After                                               |
|-------------------------------------------|-----------------------------------------------------|
| `Flow\ETL\Attribute\DocumentationDSL`     | `Flow\Documentation\Attribute\DocumentationDSL`     |
| `Flow\ETL\Attribute\DocumentationExample` | `Flow\Documentation\Attribute\DocumentationExample` |
| `Flow\ETL\Attribute\Module`               | `Flow\Documentation\Attribute\Module`               |
| `Flow\ETL\Attribute\Type`                 | `Flow\Documentation\Attribute\Type`                 |

No installable package ships `Flow\Documentation\Attribute\*`: `ReflectionAttribute::newInstance()` on them throws
`Attribute class "..." not found`. Read them with `getName()` / `getArguments()`.

### 69) `flow-php/etl` / `flow-php/types` - `TypeMerge` moved to `flow-php/types` as `TypeWidener`

| Before                                               | After                                                |
|------------------------------------------------------|------------------------------------------------------|
| `Flow\ETL\Schema\Definition\TypeMerge`               | `Flow\Types\Type\TypeWidener`                        |
| `merge($left, $right)`                               | `widen($left, $right)`                               |
| `mergeLists()` / `mergeMaps()` / `mergeStructures()` | `widenLists()` / `widenMaps()` / `widenStructures()` |

### 70) `flow-php/types` - structure optional fields are declared inline with `structure_element()`

| Before                                                                                 | After                                                                                                          |
|----------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------|
| `type_structure(['id' => type_integer()], ['nick' => type_string()])`                  | `type_structure(['id' => type_integer(), 'nick' => structure_element('nick', type_string(), optional: true)])` |
| `type_structure($elements, $optionalElements, true)`                                   | `type_structure($elements, true)` - an array second argument throws `TypeError`                                |
| `new StructureType($elements, $optionalElements, $allowExtra)`                         | `StructureType::fromElements($elements, $allowExtra)`                                                          |
| `StructureType::elements()` - `array<name, Type>`, required fields only                | `list<StructureElement>`, optional fields included                                                             |
| `$structure->elements()['id']`                                                         | `$structure->element('id')?->type`                                                                             |
| `StructureType::optionalElements()`                                                    | removed - read `StructureElement::$optional`                                                                   |
| `normalize()` - `{"type": "structure", "elements": {...}, "optional_elements": {...}}` | `{"type": "structure_v2", "fields": [{"name": ..., "type": ..., "optional": ...}]}`                            |
| `type_from_array()` / `schema_from_json()` on a `structure` payload                    | throws `InvalidArgumentException: Unknown type 'structure'`                                                    |

Regenerate any stored `schema_to_json()` output that contains a structure.

### 71) `flow-php/types` - `time`, `date`, `string` and `Json` accept and refuse different values

| Before                                                                          | After                                                                                            |
|---------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------|
| `type_time()->isValid(new DateInterval('P1M'))` - `true`                        | `false`                                                                                          |
| `type_time()->cast(new DateInterval('P1M'))` - returned as-is                   | throws `CastingException`: `... Relative DateInterval (with months/years) can't be cast to time` |
| `type_time()->cast('12:34:56')` - throws `CastingException`                     | `DateInterval` of 12h 34m 56s                                                                    |
| `type_time()->cast('12:34:56.5')` - throws `CastingException`                   | `DateInterval` with `f` = `0.5`                                                                  |
| `type_union(type_time(), type_integer())->cast('12:34:56')` - `12`              | `DateInterval`                                                                                   |
| `type_date()->isValid(new DateTimeImmutable('2024-01-02 00:00:00.5'))` - `true` | `false`                                                                                          |
| `Json::fromArray([1 => 'a'], asObject: true)` - `{"1":"a"}`                     | throws `InvalidArgumentException`: `All keys of a JSON object must be strings`                   |
| `type_string()->cast(new DateInterval('PT1H2M3S'))` - throws `CastingException` | `'01:02:03'`; `P1DT2H` -> `'26:00:00'`; a fraction adds `.uuuuuu`                                |

### 72) `flow-php/types` - `StringTypeNarrower` types compact digit dates as `integer` and month-only dates as `string`

| Before                                                     | After     |
|------------------------------------------------------------|-----------|
| `(new StringTypeNarrower())->narrow('20240101')` -> `date` | `integer` |
| `(new StringTypeNarrower())->narrow('2024-01')` -> `date`  | `string`  |

### 73) `flow-php/filesystem` - `Partition` takes a value and its type, encodes reserved characters and allows `null`

| Before                                                                                         | After                                                                  |
|------------------------------------------------------------------------------------------------|------------------------------------------------------------------------|
| `Partition::valueFromRow(Reference $ref, Row $row)`                                            | `Partition::fromValue(string $name, Type $type, mixed $value): string` |
| `$partition->reference()`                                                                      | removed                                                                |
| `Flow\ETL\Row\Entry\JsonEntry can't be used as a partition`                                    | `Column "d" of type json can't be used as a partition`                 |
| `Partition::$value` - `string`                                                                 | `?string`                                                              |
| `new Partition('path', 'a/b')` - throws `Partition value contains one of forbidden characters` | `->segment()` - `path=a%2Fb`                                           |
| `new Partition('a/b', '1')` - throws `Partition name contains one of forbidden characters`     | `->segment()` - `a%2Fb=1`                                              |
| value `New York` written to `city=New York/`                                                   | `city=New%20York/`                                                     |
| `null` partition value on write - throws `Partition value can't be empty`                      | `year=__HIVE_DEFAULT_PARTITION__/`                                     |
| `new Partitions(new Partition('a', '1'), new Partition('a', '2'))`                             | throws `InvalidArgumentException`                                      |
| `Partition` / `Partitions` throw `Flow\ETL\Exception\InvalidArgumentException`                 | throw `Flow\Filesystem\Exception\InvalidArgumentException`             |

### 74) `flow-php/filesystem` - `SourceStream::readLines()` strips the separator and yields nothing for 0 bytes

| Before                                                                                              | After              |
|-----------------------------------------------------------------------------------------------------|--------------------|
| `memory_filesystem()` stream: each line kept its trailing `"\n"`                                    | separator stripped |
| `memory_filesystem()` stream: `$separator` ignored                                                  | honoured           |
| `memory_filesystem()` stream: 0 bytes yielded one `''`                                              | yields nothing     |
| `StringSourceStream` / `MemorySourceStream` dropped empty lines                                     | yield them as `''` |
| `from_text()` over an empty `memory://` file - one empty row                                        | no rows            |
| `from_json_lines()` over an empty `memory://` file - threw                                          | no rows            |
| `from_csv()->withCharactersReadInLine($n)` on `memory://` - a line longer than `$n` split into rows | one row per line   |

A custom `SourceStream::readLines()` must strip the separator, yield nothing for a 0-byte stream and yield an empty
line as `''`.

### 75) `flow-php/parquet` - FLOAT32 values read without `ext-arrow` are no longer rounded to 7 decimals

| FLOAT32 value written | Before (read) | After (read)          |
|-----------------------|---------------|-----------------------|
| `0.1`                 | `0.1`         | `0.10000000149011612` |
| `18.52`               | `18.5200005`  | `18.520000457763672`  |
| `1.0E-8`              | `0.0`         | `9.99999993922529E-9` |

### 76) `flow-php/parquet` - `FlatColumn::decimal()` sizes `FIXED_LEN_BYTE_ARRAY` by the Parquet spec

| `FlatColumn::decimal($name, $precision)` | Before (bytes) | After (bytes) |
|------------------------------------------|----------------|---------------|
| precision 7                              | 3              | 4             |
| precision 12                             | 5              | 6             |
| precision 19                             | 8              | 9             |
| precision 24                             | 10             | 11            |
| precision 36                             | 15             | 16            |

### 77) `flow-php/postgresql` - `Client` gains `describe()`, and `execute()` accepts `ConvertedParameters`

| Before                                                   | After                                                                                                                    |
|----------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------|
| a custom `Flow\PostgreSql\Client\Client` implementation  | adds `describe(Sql\|string $sql, array $parameters = []): array` returning `list<array{name: string, type: ColumnType}>` |
| `execute(Sql\|string $sql, array $parameters = []): int` | `execute(Sql\|string $sql, array\|ConvertedParameters $parameters = []): int`                                            |

A custom `Client::execute()` binds `ConvertedParameters::$values` as they are, without a converter.

### 78) `flow-php/postgresql` - arrays parsed, `oid` as integer, `timetz` without offset, duplicates cast by name

| Before                                                    | After                                                                         |
|-----------------------------------------------------------|-------------------------------------------------------------------------------|
| `ResultCaster::cast()` returns `bool\|float\|int\|string` | returns `array\|bool\|float\|int\|string`                                     |
| `_int4` value `'{1,NULL,3}'`                              | `[1, null, 3]` - every `_`-prefixed array type, elements cast by element type |
| `oid` value `'42'`                                        | `42`                                                                          |
| `timetz` value `'12:34:56+02'`                            | `'12:34:56'`                                                                  |
| `SELECT 1::int8 AS a, 'x' AS a` - `a` is `0`              | `'x'`                                                                         |
| `SELECT 'x' AS a, 1::int8 AS a` - `a` is `'1'`            | `1`                                                                           |

Applies to rows returned by `Client::fetch*()` and `Client::cursor()`.

### 79) `flow-php/doctrine-dbal-bulk` - `Dialect` requires `maxBindParameters()`

| Before                                                       | After                                                                                               |
|--------------------------------------------------------------|-----------------------------------------------------------------------------------------------------|
| a custom `Flow\Doctrine\Bulk\Dialect\Dialect` implementation | adds `public function maxBindParameters(): int` - the platform's bind-parameter limit, at least `1` |

### 80) `flow-php/etl-adapter-csv`, `-json`, `-excel`, `-google-sheet` - a read without a schema infers one schema

|                                                                                                   | Before                                                                      | After                                                                                   |
|---------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------|-----------------------------------------------------------------------------------------|
| schema of a read without `withSchema()`                                                           | each yielded `Rows` typed from its own values                               | one schema for the whole read, inferred from a sample                                   |
| nullability of an inferred column                                                                 | nullable only where that `Rows` held a `null`                               | always nullable (`?integer`, `?string`, ...)                                            |
| sample                                                                                            | -                                                                           | first 20,480 rows over the first 10 files; Google Sheet: first 100 rows                 |
| value past the sample that the inferred type refuses                                              | read, typed on its own                                                      | throws `Flow\ETL\Exception\SchemaMismatchException`                                     |
| CSV/Excel file or Google Sheet whose header differs from the inferred one (header-only files too) | read with its own columns                                                   | throws `Flow\ETL\Exception\InferredSchemaException`                                     |
| row key order                                                                                     | the row's own; a body column named like a partition is overwritten in place | the schema's: body columns (JSON: first seen across the sample), then partition columns |
| `flow file:schema` over CSV / Excel                                                               | every column `string`, `nullable: false` unless a value was empty           | typed, every column `nullable: true`                                                    |
| infer from every row                                                                              | -                                                                           | `->inferSchema(infer_schema()->sampleSize(-1))`                                         |
| infer from every file                                                                             | -                                                                           | `->inferSchema(infer_schema()->filesToSniff(-1))`                                       |
| every column `?string`                                                                            | -                                                                           | `->inferSchema(infer_schema()->allStrings())`                                           |
| files with different columns                                                                      | -                                                                           | `->inferSchema(infer_schema()->unionByName())`                                          |
| no inference                                                                                      | -                                                                           | `->withSchema($schema)`                                                                 |

On `from_google_sheet()`, `->inferSchema(infer_schema())` without `->sampleSize()` samples 20,480 rows, not 100.

### 81) `flow-php/etl-adapter-csv` - cells are typed, not strings

| Cell                                 | Before   | After                 |
|--------------------------------------|----------|-----------------------|
| `1`                                  | `string` | `?integer`            |
| `1.5`                                | `string` | `?float`              |
| `true` / `false`                     | `string` | `?boolean`            |
| `20240101`                           | `string` | `?integer`            |
| `2024-01-01` / `2024-01-01 10:00:00` | `string` | `?date` / `?datetime` |
| uuid / JSON text                     | `string` | `?uuid` / `?json`     |
| `01234`, `2024-01`, ` 12 ` (padded)  | `string` | `?string`             |

Keep every column a string: `from_csv($path)->inferSchema(infer_schema()->allStrings())`.

### 82) `flow-php/etl-adapter-doctrine` - `from_dbal_*()` type columns from the query

| Driver                              | Column                                                                          | Before        | After                                                                                           |
|-------------------------------------|---------------------------------------------------------------------------------|---------------|-------------------------------------------------------------------------------------------------|
| `pgsql`                             | `NUMERIC`                                                                       | `string`      | `float`                                                                                         |
| `pgsql`                             | `DATE`, `TIMESTAMP`, `TIMESTAMPTZ`                                              | `string`      | `DateTimeImmutable`                                                                             |
| `pgsql`                             | `TIME`                                                                          | `string`      | `DateInterval`                                                                                  |
| `pgsql`                             | `UUID`                                                                          | `string`      | `Flow\Types\Value\Uuid`                                                                         |
| `pgsql`                             | `JSON`, `JSONB`                                                                 | `string`      | `Flow\Types\Value\Json`                                                                         |
| `pgsql`                             | `XML`                                                                           | `string`      | `DOMDocument`                                                                                   |
| `pgsql`                             | any other type (`INTERVAL`, `INET`, `MONEY`, `OID`, arrays, ranges, enums, ...) | `string`      | throws `SchemaNotDerivableException`                                                            |
| `mysqli`                            | `DECIMAL`                                                                       | `string`      | `float`                                                                                         |
| `mysqli`                            | `DATE`, `DATETIME`, `TIMESTAMP`                                                 | `string`      | `DateTimeImmutable`                                                                             |
| `mysqli`                            | `TIME`                                                                          | `string`      | `DateInterval`                                                                                  |
| `mysqli`                            | `JSON`                                                                          | `string`      | `Flow\Types\Value\Json`                                                                         |
| `mysqli`                            | any other type (`BIT`, `GEOMETRY`, ...)                                         | `string`      | throws `SchemaNotDerivableException`                                                            |
| `sqlite3`, `pdo_sqlite`             | `INTEGER`, `REAL`                                                               | `1`, `1.5`    | `'1'`, `'1.5'` - every column is `?string`                                                      |
| `pdo_pgsql`, `pdo_mysql`, any other | every column                                                                    | driver values | throws `SchemaNotDerivableException` - use the `pgsql` / `mysqli` driver or `->withSchema(...)` |

Applies to `from_dbal_query()`, `from_dbal_queries()`, `from_dbal_limit_offset()`, `from_dbal_limit_offset_qb()` and
`from_dbal_key_set_qb()` without `->withSchema()`. A query the driver cannot describe (multi-statement, data-modifying
CTE, `INSERT ... RETURNING`) also throws. Declare `->withSchema(...)` to pick the types.

### 83) `flow-php/etl-adapter-doctrine`, `-postgresql` - `withPageSize()` / `withFetchSize()` become `withBatchSize()`

| Before                                                                                                | After                                        |
|-------------------------------------------------------------------------------------------------------|----------------------------------------------|
| `from_pgsql_cursor(...)->withFetchSize(500)`                                                          | `from_pgsql_cursor(...)->withBatchSize(500)` |
| `from_pgsql_limit_offset(...)->withPageSize(500)`                                                     | `->withBatchSize(500)`                       |
| `from_pgsql_key_set(...)->withPageSize(500)`                                                          | `->withBatchSize(500)`                       |
| `from_dbal_limit_offset(...)->withPageSize(500)`, `from_dbal_limit_offset_qb(...)->withPageSize(500)` | `->withBatchSize(500)`                       |
| `from_dbal_key_set_qb(...)->withPageSize(500)`                                                        | `->withBatchSize(500)`                       |
| `Page size must be greater than 0, got 0` / `Fetch size must be greater than 0, got 0`                | `Batch size must be greater than 0, got 0`   |

### 84) `flow-php/etl-adapter-excel` - text cells narrow to uuid, json and timezone, `to_excel()` writes date cells

|                                                             | Before                                                   | After                                                                       |
|-------------------------------------------------------------|----------------------------------------------------------|-----------------------------------------------------------------------------|
| text cell holding a uuid / JSON / timezone name             | `string`                                                 | `?uuid` / `?json` / `?timezone`                                             |
| any other text cell (`TRUE`, `12.9`, `2023-10-02`)          | `string`                                                 | `?string`                                                                   |
| blank cell under `withConvertEmptyToNull(false)`            | `''` in that row                                         | the whole column is `?string` (`1` reads as `'1'`)                          |
| `to_excel()` date / datetime value                          | text cell                                                | date cell                                                                   |
| `to_excel()->withDateFormat()` / `->withDateTimeFormat()`   | PHP `date()` format, default `'Y-m-d'` / `'Y-m-d H:i:s'` | Excel number format, default `'yyyy-mm-dd'` / `'yyyy-mm-dd hh:mm:ss'`       |
| `to_excel()` then `from_excel()`, date / datetime column    | `string`                                                 | `?date` / `?datetime`                                                       |
| ZIP-signed file without an extension that is not a workbook | `ValueError: Invalid or uninitialized Zip object`        | `Flow\ETL\Exception\InvalidArgumentException: Unsupported file format: n/a` |

Before:

```php
to_excel($path)->withDateFormat('Y-m-d')->withDateTimeFormat('Y-m-d H:i:s');
```

After:

```php
to_excel($path)->withDateFormat('yyyy-mm-dd')->withDateTimeFormat('yyyy-mm-dd hh:mm:ss');
```

### 85) `flow-php/etl-adapter-excel` - requires `openspout/openspout` `~5.3.0`

| Dependency            | Before | After    |
|-----------------------|--------|----------|
| `openspout/openspout` | `^5.2` | `~5.3.0` |

### 86) `flow-php/etl-adapter-google-sheet` - `FORMATTED_VALUE` cells are typed, and `''` reads as `null`

|                                            | Before   | After                                                                                                   |
|--------------------------------------------|----------|---------------------------------------------------------------------------------------------------------|
| cell under `FORMATTED_VALUE` (the default) | `string` | typed as a CSV cell, see 81): `'1234'` -> `?integer`, `'TRUE'` -> `?boolean`, `'2024-01-01'` -> `?date` |
| `''` cell                                  | `''`     | `null`; keep `''` with `->withEmptyToNull(false)`                                                       |

### 87) `flow-php/etl-adapter-http` - `response_body` and `request_body` hold the raw body text

| Before                                                                      | After        |
|-----------------------------------------------------------------------------|--------------|
| `response_body` / `request_body` of a JSON or XML message - decoded `array` | `?string`    |
| a JSON body that fails to decode - threw `RuntimeException`                 | kept as text |

Decode in the pipeline: `->withEntry('response_body', ref('response_body')->jsonDecode())`.

### 88) `flow-php/etl-adapter-json` - `schema_from_json_schema()` rejects a property with more than one non-null type

| Before                                                                 | After                                                     |
|------------------------------------------------------------------------|-----------------------------------------------------------|
| `anyOf` / `oneOf` of `string` and `integer` - `integer\|string` column | throws `Flow\ETL\Exception\UnsupportedUnionTypeException` |
| `enum: ["a", 1]` - `integer\|string` column                            | throws `UnsupportedUnionTypeException`                    |
| `type: ["string", "integer"]` - `integer\|string` column               | throws `UnsupportedUnionTypeException`                    |
| `anyOf` of `string` and `null` - `?string`                             | unchanged                                                 |

Give the property a single type in the JSON Schema, or declare the column by hand: `str_schema('v')`,
`json_schema('v')`.

### 89) `flow-php/etl-adapter-json` - keys outside the sample are dropped, missing keys are `null`

|                                                        | Before                                              | After                                                                 |
|--------------------------------------------------------|-----------------------------------------------------|-----------------------------------------------------------------------|
| key only a later file carries                          | kept in that file's rows                            | dropped; keep it with `->inferSchema(infer_schema()->unionByName())`  |
| key first seen past the sample                         | kept in that row                                    | dropped; keep it with `->inferSchema(infer_schema()->sampleSize(-1))` |
| key missing from a record                              | absent from that row                                | `null`                                                                |
| nested key every sampled record carries, missing later | read                                                | throws `SchemaMismatchException`                                      |
| nested key first seen past the sample                  | kept                                                | dropped                                                               |
| 0-byte file, `from_json()`                             | throws `JsonMachine\Exception\SyntaxErrorException` | skipped, no rows                                                      |

Applies to `from_json()` and `from_json_lines()` unless the row names one.

### 90) `flow-php/etl-adapter-json` - `from_json_lines()` skips blank lines

| Before                                                          | After                                  |
|-----------------------------------------------------------------|----------------------------------------|
| an empty or whitespace-only line - threw `SyntaxErrorException` | skipped, in `schema()` and `extract()` |

### 91) `flow-php/etl-adapter-parquet` - `to_parquet()` does not validate values against the Parquet schema

| Before                                                        | After                                                                |
|---------------------------------------------------------------|----------------------------------------------------------------------|
| `to_parquet($path)` wrote with `Option::VALIDATE_DATA` `true` | `false`                                                              |
| -                                                             | `to_parquet($path)->withOptions(Options::default())` validates again |

### 92) `flow-php/etl-adapter-postgresql` - `from_pgsql_*()` type columns from the query instead of reading strings

| PostgreSQL column                                                     | Before                  | After                                |
|-----------------------------------------------------------------------|-------------------------|--------------------------------------|
| `numeric`                                                             | `string`                | `float`                              |
| `date`, `timestamp`, `timestamptz`                                    | `string`                | `DateTimeImmutable`                  |
| `time`                                                                | `string`                | `DateInterval`                       |
| `timetz`                                                              | `string` with offset    | `DateInterval`, offset dropped       |
| `uuid`                                                                | `string`                | `Flow\Types\Value\Uuid`              |
| `json`, `jsonb`                                                       | `string`                | `Flow\Types\Value\Json`              |
| `xml`                                                                 | `string`                | `DOMDocument`                        |
| `oid`                                                                 | `string`                | `int`                                |
| one-dimensional array (`int4[]`, `text[]`, ...)                       | `string` `'{1,NULL,3}'` | `[1, null, 3]`, `null` elements kept |
| multi-dimensional array                                               | `string`                | throws while hydrating               |
| `record`, `point`, `line`, `lseg`, `box`, `path`, `polygon`, `circle` | `string`                | throws `SchemaNotDerivableException` |

Applies to `from_pgsql_cursor()`, `from_pgsql_limit_offset()` and `from_pgsql_key_set()` without `->withSchema()`. A
query that cannot run as a subquery (multi-statement, data-modifying CTE, `INSERT ... RETURNING`) also throws
`SchemaNotDerivableException`. Declare `->withSchema(...)` to pick the types.

### 93) `flow-php/etl-adapter-postgresql` - `pgsql_table_to_flow_schema()` maps arrays, text-like types, `oid`, `timetz`

| PostgreSQL column                                                                                             | Before                        | After            |
|---------------------------------------------------------------------------------------------------------------|-------------------------------|------------------|
| `inet`, `cidr`, `macaddr`, `money`, `int4range`, `int8range`, `numrange`, `tsrange`, `tstzrange`, `daterange` | throws `TypeMappingException` | `string`         |
| `oid`                                                                                                         | throws `TypeMappingException` | `integer`        |
| `timetz` (`time with time zone`)                                                                              | throws `TypeMappingException` | `time`           |
| `integer[]`, any array column                                                                                 | its element type, `integer`   | `list<?integer>` |

Applies to `pgsql_table_to_flow_schema()` and `EntryTypesMap::toFlowType()`.

| Before                                                                    | After   |
|---------------------------------------------------------------------------|---------|
| `TypeMappingException::ambiguousEntryType()` / `::unsupportedEntryType()` | removed |

### 94) `flow-php/etl-adapter-postgresql` - `InsertQueryBuilder::build()` takes the client's converters

| Before                                        | After                                                                                       |
|-----------------------------------------------|---------------------------------------------------------------------------------------------|
| `$builder->build($values, $schema, $options)` | `$builder->build($values, $schema, $client->converters(), $options)`                        |
| returns `array{Sql, list<?TypedValue>}`       | returns `array{Sql, ConvertedParameters}` - pass the second element to `$client->execute()` |

### 95) `flow-php/etl-adapter-xml` - `from_xml()` keeps each element as the source wrote it

| Before                                                                                         | After                                                                                       |
|------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------|
| `node` re-serialized with added indentation; comments dropped                                  | the element's source text, whitespace and comments included                                 |
| every ancestor namespace declared on `node`                                                    | only the ancestor namespaces the element uses                                               |
| a truncated document returned the elements before the cut                                      | throws `RuntimeException`                                                                   |
| expat error messages                                                                           | libxml error messages, e.g. `XML Error: Premature end of data in tag root line 1 at line 1` |
| `XMLParserExtractor::startElementHandler()` / `endElementHandler()` / `characterDataHandler()` | removed                                                                                     |

### 96) `flow-php/cli` - `--schema-auto-cast` removed, `flow schema` ignores the row window

| Before                                                                                            | After                                          |
|---------------------------------------------------------------------------------------------------|------------------------------------------------|
| `--schema-auto-cast` on `file:analyze`, `file:convert`, `file:read`, `file:schema`                | removed                                        |
| `flow schema <file> --input-file-offset=N` / `--input-file-limit=N` - schema of the selected rows | the file's schema; both options have no effect |

### 97) `flow-php/cli` - command classes name themselves with `#[AsCommand]`

| Before                                             | After                                                                                                 |
|----------------------------------------------------|-------------------------------------------------------------------------------------------------------|
| `new FileConvertCommand()` - named `file:read`     | `file:convert`                                                                                        |
| `new FileRowsCountCommand()` - named `file:schema` | `file:rows:count`                                                                                     |
| `new PipelineRunCommand()` - named `run`           | `pipeline:run`                                                                                        |
| `Flow\CLI\Command\*` classes carried no aliases    | `run`, `read`, `schema`, `count`, `convert`, `analyze`, `format`, as the `flow` binary registers them |

Registering the commands in your own console application: drop the `setName()` / `setAliases()` calls.

### 98) `flow-php/flow-php-ext` - the `flow_php` extension must be reinstalled

| Before                     | After                                  |
|----------------------------|----------------------------------------|
| `flow_php` extension 0.1.0 | 0.3.0, required by this `flow-php/etl` |

Reinstall it with the new release: `pie install flow-php/flow-php-ext`.

### 99) `flow-php/etl-adapter-csv` - `withSeparator()`, `withEnclosure()` and `withEscape()` take a single byte

| Before                                                                                                                            | After                                                |
|-----------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------|
| `withSeparator('\|\|')` / `withEnclosure('\|\|')` / `withEscape('ab')` - PHP 8.3: first byte used; PHP 8.4+: `ValueError` on read | throws `Flow\ETL\Exception\InvalidArgumentException` |
| `withSeparator('')` / `withEnclosure('')` - PHP 8.3: `,` / `"` used; PHP 8.4+: `ValueError` on read                               | throws `Flow\ETL\Exception\InvalidArgumentException` |

### 100) `flow-php/etl` - `array_expand()` nested in an expression gives rows, and is refused outside `withEntry()`

| Before                                                                                                  | After                                                                  |
|---------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------|
| `withEntry('s', concat(ref('id'), ref('tags')->expand()))` - one row, list JSON-encoded: `a["x","y"]`   | one row per element: `ax`, `ay`                                        |
| `withEntry('v', ref('lists')->expand()->expand())` - one row per outer element, inner list JSON-encoded | throws `InvalidArgumentException` - expand one level per `withEntry()` |
| `array_expand()` in `filter()`, `until()`, `duplicateRow()`, `aggregate()`, `over()`                    | throws `InvalidArgumentException` at `schema()` / `run()`              |
| `array_expand()` in an `onEach()` body                                                                  | throws `InvalidArgumentException` when `onEach()` is called            |

### 101) `flow-php/array-dot` - `\` escapes every path grammar character, `array_dot_steps()` deprecated

| Before                                              | After                                         |
|-----------------------------------------------------|-----------------------------------------------|
| `\?x`, `a\*b`, `a\\` read keys `\?x`, `a\*b`, `a\\` | read keys `?x`, `a*b`, `a\`                   |
| `?{a}` reads key `{a}`                              | throws `InvalidPathException` - `?\{a\}`      |
| `x.*.a\.b` reads `a` -> `b` of each element         | reads key `a.b` of each element               |
| `x.{a\.b,c}` result keys `a__ESCAPED_DOT__b`, `c`   | `a.b`, `c`                                    |
| `x.{a?b}` result key `ab`                           | `a?b`                                         |
| `array_dot_steps($path)`                            | deprecated - `Path::fromString($path)->steps` |

### 102) `flow-php/array-dot` - `array_dot_set()` and `array_dot_rename()` change only the addressed key

| Before                                                                                      | After                           |
|---------------------------------------------------------------------------------------------|---------------------------------|
| `array_dot_set(['a' => ['x' => 1]], 'a.y', 2)` -> `['a' => ['y' => 2]]`                     | `['a' => ['x' => 1, 'y' => 2]]` |
| `array_dot_set([5 => 'a', 7 => 'b'], '5', 'c')` -> `['a', 'b', 'c']`                        | `[5 => 'c', 7 => 'b']`          |
| `array_dot_set([], '\{a\}', 1)` / `'?a'` write keys `\{a\}` / `?a`                          | write keys `{a}` / `a`          |
| `array_dot_set($array, 'x.*', $value)` throws                                               | sets every element of `x`       |
| `array_dot_set($array, 'x.{a,b}', $value)` writes key `{a,b}`                               | throws `InvalidPathException`   |
| `array_dot_rename(['{a}' => 1], '\{a\}', 'b')` -> warning, `['{a}' => 1, 'b' => null]`      | `['b' => 1]`                    |
| `array_dot_rename(['a' => 1], '?missing', 'b')` -> warning, `['a' => 1, 'b' => null]`       | `['a' => 1]`                    |
| `array_dot_rename($array, 'x.*', 'c')` / `'x.{a,b}'` -> warning, adds `c => null` under `x` | throws `InvalidPathException`   |
| `array_dot_set(['x' => 5], 'x.*', 1)` throws `InvalidPathException`                         | `['x' => []]`                   |
| `array_dot_set([], 'x.*', 1)` throws `InvalidPathException`                                 | `['x' => []]`                   |
| `array_dot_rename(['a' => 1, 'b' => 2], 'a', 'a')` -> `['b' => 2]`                          | `['a' => 1, 'b' => 2]`          |

### 103) `flow-php/etl` - `array_get_collection()` reads its keys as literal keys

| Before                                       | After                             |
|----------------------------------------------|-----------------------------------|
| key `a.b` reads `a` -> `b`, result key `a_b` | reads key `a.b`, result key `a.b` |
| key `k,l` throws `InvalidArgumentException`  | reads key `k,l`                   |

### 104) `flow-php/etl` - `array_expand()` over a null list gives no rows

| Before                                        | After                |
|-----------------------------------------------|----------------------|
| `null` list throws `InvalidArgumentException` | no rows for that row |

### 105) `flow-php/array-dot`, `flow-php/etl` - a nullsafe multimatch over an empty array reads `null`

| Before                                                                                                  | After                              |
|---------------------------------------------------------------------------------------------------------|------------------------------------|
| `array_dot_get([], '{?a}')` throws `InvalidPathException`                                               | `['a' => null]`                    |
| `array_dot_get([], '{a}')` message `Path "{a}" does not exists ...`                                     | `Path "a" does not exists ...`     |
| `array_get_collection(ref('c'), ['id'])` over `[['name' => 'a'], []]` throws `InvalidArgumentException` | `[['id' => null], ['id' => null]]` |

### 106) `flow-php/filesystem` - every filesystem matches glob patterns the same way

Files: `data/flat.parquet`, `data/.hidden.parquet`, `data/.dir/x.parquet`, `data/date=2026-09-01/one.parquet`,
`data/id=1/date=2026-09-01/two.parquet`

| pattern                 | local, before | memory / S3 / Azure, before | every filesystem, after           |
|-------------------------|---------------|-----------------------------|-----------------------------------|
| `data/**.parquet`       | `flat`        | all five                    | `.hidden`, `flat`                 |
| `data/**/*.parquet`     | all five      | `.dir/x`, `one`, `two`      | all five                          |
| `data/**`               | `flat`        | all five                    | all five                          |
| `data/*.parquet`        | `flat`        | `.hidden`, `flat`           | `.hidden`, `flat`                 |
| `data/*/*.parquet`      | `one`         | `.dir/x`, `one`             | `.dir/x`, `one`                   |
| `data/**/[!f]*.parquet` | `flat`        | none                        | `.dir/x`, `.hidden`, `one`, `two` |
| `data/[a-g]*.parquet`   | `flat`        | none                        | `flat`                            |

Recurse with `data/**/*.parquet`, not `data/**.parquet`. `webmozart/glob` is no longer installed with
`flow-php/filesystem` or `flow-php/etl`.

### 107) `flow-php/parquet` - `ParquetEngine::openForWrite()` returns a `ParquetFileWriter`

| Before                                                                                                | After                                                                                                |
|-------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------|
| `$engine->openForWrite($stream, $schema, $c, $o); $engine->writeBatch($rows); $engine->closeWrite();` | `$file = $engine->openForWrite($stream, $schema, $c, $o); $file->writeBatch($rows); $file->close();` |
| `ParquetEngine::closeWrite()`, `writeBatch()`, `writeRow()`                                           | removed - on `ParquetFileWriter`                                                                     |
| two `Writer`s sharing one engine overwrote each other                                                 | every `openForWrite()` returns an independent writer                                                 |
| `ArrowParquetEngine` left the destination stream open                                                 | `ParquetFileWriter::close()` closes it on every engine                                               |

### 108) `flow-php/etl-adapter-parquet` - path-only partition columns leave the file body

| Before                                                                            | After                                                                                                |
|-----------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------|
| `partitionBy(partition_by('date'))` - file body carries an all-null `date` column | file body without `date`                                                                             |
| `from_parquet()` types `date` from that body column, e.g. `datetime`              | `string` - declare it: `from_parquet($path)->partitionTypes(partition_types(date: type_datetime()))` |

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
