# Upgrade Guide

[TOC]

This document provides guidelines for upgrading between versions of Flow PHP. Please follow the instructions for your
specific version to ensure a smooth upgrade process.

Upgrading from older versions: [0.35.x to 0.40.x](/documentation/upgrading-0.35-0.40.md)

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

### 4) `flow-php/postgresql` - `SelectStatement::hasIntoClause()` sees `SELECT ... INTO` in a `UNION` / `INTERSECT` / `EXCEPT`

| `sql_parse($sql)->statements()->first()->hasIntoClause()`, `$sql` | Before  | After  |
|-------------------------------------------------------------------|---------|--------|
| `SELECT id INTO t FROM x UNION SELECT 1`                          | `false` | `true` |

### 5) `flow-php/postgresql` - `sql_to_*_query()` and the pagination modifiers take exactly one read-only `SELECT` or `VALUES`

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

### 8) `flow-php/etl` - `extract()` receives the pushed limit and path filter, `LimitPushDown` and `withPathFilter()` removed

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

### 13) `flow-php/etl-adapter-doctrine`, `-postgresql` - `to_dbal_transaction()` / `to_pgsql_transaction()` are transaction roots

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

### 23) `flow-php/etl-adapter-csv` - `CSVOpenSource` is an interface, `CSVSourceOpener` removed, `CSVLineReader` takes the separator and escape

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

### 39) `flow-php/etl-adapter-json` - `anyOf` / `oneOf` / multi-type `type` inside `items`, `properties` or `additionalProperties`

| Before                                                                                   | After                                             |
|------------------------------------------------------------------------------------------|---------------------------------------------------|
| `list<integer\|string>`, `structure{x: integer\|string}`, `map<string, integer\|string>` | `UnsupportedUnionTypeException` naming the column |

### 40) `flow-php/types`, `flow-php/etl` - a `datetime` column has one time zone

| Before                                                                                                      | After                                                                                                                                                                                                                            |
|-------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `type_datetime()->cast('2026-01-02 03:04')` in `date.timezone`                                              | in the column zone: UTC, or `type_datetime('Europe/Warsaw')`                                                                                                                                                                     |
| `cast('2026-01-02T03:04:05+02:00')` keeps `+02:00`, `cast('…Z')` keeps `Z`                                  | the same instant in the column zone (`2026-01-02 01:04:05 UTC`)                                                                                                                                                                  |
| a `DateTimeInterface` value in `rows()`, `from_rows()`, a cast or a Floe read keeps its own zone            | converted to the column zone, instant unchanged                                                                                                                                                                                  |
| `to_csv()` / `to_json()` / `to_xml()` write each value's own offset (`+02:00` in, `+02:00` out)             | the column zone's offset (`2026-01-02T01:04:05+00:00`); declare `datetime_schema('at', zone: 'Europe/Warsaw')` on the extractor to write a local one                                                                             |
| `from_dbal_*()` naive `timestamp` / MySQL `datetime` text, and naive CSV/JSON text, read in `date.timezone` | read in the column zone (UTC); `->withSchema(schema(datetime_schema('at', zone: 'Europe/Warsaw')))` keeps a local reading                                                                                                        |
| `type_datetime('CEST')`, `to_timezone($ref, 'PST')`, `'EST'`, `'CET'`                                       | `InvalidArgumentException` - an IANA region name, `UTC` or `+HH:MM`                                                                                                                                                              |
| `to_timezone($v, lit('UTC'))`                                                                               | `TypeError` - pass `'UTC'` or a `DateTimeZone`                                                                                                                                                                                   |
| `to_timezone($ref, ref('tz'))` - a per-row zone                                                             | `TypeError` - the zone of `to_timezone()` and `new ToTimeZone()` is `DateTimeZone\|string`                                                                                                                                       |
| `now($tz)`, `to_date_time(…, $tz)`, `to_timezone(…, $tz)` return a `datetime` (UTC) column                  | a `datetime` column in `$tz`; a `ScalarFunction` zone in `now()` / `to_date_time()` yields UTC; `now(new DateTimeZone('EST'))`, `to_date_time(…, new DateTimeZone('PST'))` throw `InvalidArgumentException` (abbreviation zones) |
| `to_date_time($dateTimeObject)` truncates to midnight                                                       | keeps the time, converted to the zone                                                                                                                                                                                            |
| merging `datetime` columns ignores zones                                                                    | equal zones kept, different zones become `UTC`                                                                                                                                                                                   |
| merging `time` with `date` / `datetime` gives `datetime`                                                    | `string`                                                                                                                                                                                                                         |
| `?T` inside a list, map or structure passes a valid value through `cast()` unchanged                        | cast like `T`: a `?datetime` moves to its zone, a `?structure` keeps only its declared keys, in declared order                                                                                                                   |
| `type_date()->cast('2026-01-02T23:00:00-05:00')` midnight `-05:00`                                          | `2026-01-02 00:00:00 UTC`; every non-object date input lands at midnight UTC                                                                                                                                                     |
| `type_date()->cast($dateTime)` sets the caller's `DateTime` to midnight                                     | returns a new `DateTimeImmutable`                                                                                                                                                                                                |
| `type_time()->cast(new DateTimeImmutable('10:00+05:00'))` = `05:00:00`                                      | `10:00:00` - the value's wall clock                                                                                                                                                                                              |
| Excel date cells in the host zone                                                                           | UTC wall clock                                                                                                                                                                                                                   |
| `new DateTimeDefinition('at', true)`                                                                        | `new DateTimeDefinition('at', type_datetime(), true)` or `datetime_schema('at', nullable: true, zone: 'Europe/Warsaw')`                                                                                                          |
| a `flow_php` extension older than this release                                                              | Composer refuses it (`conflict`) - see 67)                                                                                                                                                                                       |

### 41) `flow-php/parquet` - `Converter::isFor()` replaced by `static Converter::forColumn()`, `Int32DateTimeConverter` removed

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
| untagged builds report `0.44.1+65.g…`                                                                                                                                                   | `0.45.0-dev+65.g…` (next minor, pre-release)                                                                                   |
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

| Before                                                                              | After                                                                                                             |
|-------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------|
| `ScalarFunction::eval(Row, FlowContext): mixed`                                     | `eval(Rows, FlowContext): Column` - one value per row, built by `ResultColumn`                                    |
| `ExpandResults::eval(Row): array`, `UnpackResults::eval(Row): array`                | marker interfaces - `eval(Rows): Column` returns a list (expand) or structure (unpack) column                     |
| `Parameter::{as, asArray, asBoolean, asEnum, asFloat, asInstanceOf, asInt}(Row, …)` | `{asTypes, asArrays, asBooleans, asEnums, asFloats, asInstancesOf, asInts}(Rows, …)` - one list per batch         |
| `Parameter::{asListOfObjects, asNumber, asObject, asString}(Row, …)`                | `{asListsOfObjects, asNumbers, asObjects, asStrings}(Rows, …)`                                                    |
| `Parameter::eval(Row)`, `asValue(Row)`                                              | `values(Rows, FlowContext)`, `column(Rows, FlowContext)`                                                          |
| `$function->eval($rows, $context)` on an unresolved tree                            | `InvalidLogicException` - `(new ReferenceResolver())->resolve($function, $rows->schema())->eval($rows, $context)` |

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

| Before                                                               | After                                                                                                                               |
|----------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------|
| an exception thrown by a function reaches the caller as-is           | `Flow\ETL\Exception\EvaluationException` (extends `InvalidArgumentException`), message `… (row N)`, the original as `getPrevious()` |
| `catch (SomeSpecificException $e)` around `withEntry()` / `filter()` | `catch (EvaluationException $e)` and read `$e->getPrevious()`                                                                       |

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
| `new HashJoinConfig($bucketing)`, `new HashGroupByConfig($bucketing)`, `new HashRepartitionConfig($bucketing)`                                           | `new …Config($bucketing, Unit $memoryLimit)`                                                                                                                                                                                  |
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
| `sum(…, exact: ScalarFunction)` disabled spill column pruning (`references()` was `null`)                                 | `sum()` always enumerates its reference; a custom `AggregatingFunction` may still return `null`                                          |
| `batched_by(from_…, 'at')` - equal datetimes / uuids split into one batch per row, a `null` value joined the next group   | value equality, as `batchBy()`; nulls are a group of their own                                                                           |
| `string_agg(ref('code'), ',', SortOrder::ASC)` over `'9', '10', '100'` - `9,10,100` (numeric-looking strings numerically) | `10,100,9` - strings by bytes, as `sortBy()`                                                                                             |
| `discover_pivot_values()` over `'9', '10', '100'` - columns `9, 10, 100`                                                  | `10, 100, 9`; integer pivot values stay numeric                                                                                          |
| `->stringMatch()` / `->stringMatchAll()` turned a PHP `\Error` into an exception `optional()` swallowed                   | an `\Error` propagates; `->jsonDecode()`, `cast()`, `->arrayPathExists()`, `->stringMatch*()` errors keep their cause as `getPrevious()` |

### 79) `flow-php/etl-adapter-parquet` - with `flow_php` and arrow-ext loaded and no engine given, Parquet is read and written as native columns

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

### 81) `flow-php/etl-adapter-parquet` - with `flow_php` and arrow-ext, a string that is not valid UTF-8 is refused on write

| Before                                                                                                                             | After                                                                                                                                                                                                                           |
|------------------------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| without arrow-ext, `PhpParquetEngine` wrote `"\xff\xfe"` into a `STRING` column other readers reject (DuckDB: `is not valid UTF8`) | `Flow\Parquet\Exception\RuntimeException` `Parquet column "name" row 0 holds a string that is not valid UTF-8; Parquet STRING columns require UTF-8` - the row is 0-based within the batch written                              |
| with arrow-ext: `Column 'name': expected string, got string`                                                                       | the same refusal; invalid UTF-8 into `STRING` / `JSON` / `ENUM` is refused by both engines; `Option::VALIDATE_DATA = false` on `PhpParquetEngine` writes it unvalidated (a file other readers reject); arrow-ext always refuses |

### 82) `flow-php/parquet` - `DELTA_BINARY_PACKED` follows the spec; affected files written by `PhpParquetEngine` must be rewritten

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

### 84) `flow-php/parquet` - the arrow engine (`RustParquetEngine`, was `ArrowParquetEngine`) reads and writes through arrow-ext's new Parquet core

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
| `Reader::read()` returned without touching the file; an invalid file failed at `metadata()`                                      | the footer is read when the file is opened: `Reader::read()` / `readStream()` throw `InvalidArgumentException` `Given file is not valid Parquet file: …`                                   |
| `Flow\Arrow\Parquet\Reader` / `Writer` / `Exception`                                                                             | removed: `Flow\Parquet\Engine\{RustParquetFileReader, RustParquetFileWriter}`, `Flow\Arrow\Parquet\{RustColumnsReader, RustBatchReader}`; refusals are `Flow\Parquet\Exception\*`          |
| `flow-php/parquet` installable with any arrow-ext version                                                                        | `conflict: ext-arrow <0.45`                                                                                                                                                                |

### 85) `flow-php/parquet` - `PhpParquetEngine` reads unsigned integers and INT96 exactly; `FlatColumn::enum()` writes the ENUM logical type; values it wrote wrongly are refused

| Before                                                                                                                                              | After                                                                                                                                                                |
|-----------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `UINT_32` 4000000000 read as `-294967296`; `UINT_64` 18446744073709551615 read as `-1`                                                              | `4000000000`; a `UINT_64` above `PHP_INT_MAX` is refused                                                                                                             |
| `INT96` 1960-01-01 23:59:59.999999999 read as 1960-01-02 00:00:00 (rounded)                                                                         | 1960-01-01 23:59:59.999999 (floored, integer arithmetic)                                                                                                             |
| `FlatColumn::enum()` wrote logical `STRING` + converted `ENUM`, a file arrow-rs refuses                                                             | logical `ENUM` + converted `ENUM`                                                                                                                                    |
| `from_parquet()` refused a logical `ENUM` column (`ENUM is not supported`)                                                                          | read as a `string` column, as `STRING`                                                                                                                               |
| an int outside an `INT_8/16/32` or `UINT_8/16/32/64` range written wrapped (4000000000 into `INT32` → `-294967296`), negatives into `UINT` accepted | `ValidationException` `Column "<path>" row <n>: … is out of the … range`                                                                                             |
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

### 88) `flow-php/etl-adapter-csv`, `-xml`, `-text`, `-json` - a float is written as the shortest text that reads back as the same float

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

### 89) `flow-php/etl-adapter-csv`, `-json`, `-xml`, `-text` - a nested value is written by its type, as a column of that type is

| Before                                                                                                  | After                                                          |
|---------------------------------------------------------------------------------------------------------|----------------------------------------------------------------|
| CSV `list<datetime>`: `[{"date":"2026-01-02 03:04:05.000000","timezone_type":3,"timezone":"UTC"}]`      | `["2026-01-02T03:04:05+00:00"]` (the loader's datetime format) |
| CSV `list<uuid>`: `[{}]`; `list<time>`: a `DateInterval` property dump; `list<enum>`: the backing value | `["00000001-…"]`, `[3600000000]`, `["Hearts"]` (the case name) |
| JSON `list<date>` / `structure{d: date}`: `"2026-01-02T00:00:00+00:00"` (the datetime format)           | `"2026-01-02"` (the date format)                               |
| XML `list<?T>` / `map<K, ?V>`: threw `Given type can't be converted to node`                            | the element of `T`, a null is an empty element                 |
| `html` / `html_element`: CSV and JSON wrote an empty string, XML refused the column                     | the markup                                                     |
| XML `xml_element` and a non-backed `enum`: an empty element                                             | the canonical XML text; the case name                          |
| XML attribute column (`_name`) of a `datetime`: RFC 3339, whatever the loader's format                  | the loader's datetime format                                   |
| Text: a `datetime` / `date` / `time` column threw `Text data loader supports only scalar values`        | `DATE_ATOM`, `Y-m-d`, microseconds                             |

### 90) `flow-php/etl-adapter-csv` - `CSVEncoder` is a contract; `PhpCSVEncoder` takes `CSVWriteOptions` and returns one string per batch

| Before                                                                                                | After                                                                                                                                                                        |
|-------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `final class CSVEncoder`                                                                              | `interface CSVEncoder { encode(Rows): string; encodeHeader(array $headers): string; }`, implemented by `PhpCSVEncoder`, `RustCSVEncoder` (flow_php) and `AdaptiveCSVEncoder` |
| `new CSVEncoder($separator, $enclosure, $escape, $dateTimeFormat, $dateFormat, $newLineSeparator)`    | `new PhpCSVEncoder(new CSVWriteOptions($separator, $enclosure, $escape, $newLineSeparator, $dateTimeFormat, $dateFormat))`                                                   |
| `CSVEncoder::encode(Rows): list<string>`                                                              | `encode(Rows): string`                                                                                                                                                       |
| `CSVLoader::write(Rows, array $headers, array $partitions)`                                           | removed - `load()` is the only entry                                                                                                                                         |
| a separator or enclosure that is not one byte, an escape longer than one byte - failed in `fputcsv()` | `InvalidArgumentException` from `CSVWriteOptions`                                                                                                                            |

### 91) `flow-php/etl-adapter-json` - `JSONEncoder` is a contract; `PhpJSONEncoder` returns JSON text; `JsonDocuments` and the loaders' `write()` are removed

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
| a row whose column names are `0, 1, …`: a list          | an object           |
| a `list`                                                | `[…]`, unchanged    |

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
| `\v`, `\f` or BOM bytes between tokens (`[{"id":1}\v,{"id":2}]`) | read                                         | `Malformed JSON in "<uri>" at element 0: …`                          |
| a BOM at the start of a later line                               | read                                         | `Malformed JSON in "<uri>" at line N: …`                             |
| content after a line's record (`{"id":1}{"id":9}`)               | the first record, the rest ignored           | `Malformed JSON in "<uri>" at line N: …`                             |
| content after the closing `]` (`[{"id":1}] x`)                   | ignored                                      | `Malformed JSON in "<uri>" at element N: …`                          |
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

### 107) `flow-php/etl` - `RowsBuffer`, `RowEquality` and eleven `Rows` methods removed, `Rows::join*()` moved to `RowsJoin`

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

### 114) `flow-php/etl-adapter-csv`, `-excel`, `-google-sheet` - one record rule: header names, absent cells, blank leading rows

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
| Google Sheet `withHeader(false)`, a blank first row                | the blank row read as a row with no columns | skipped; the next row sizes the `e00`… names                                                   |

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
| `CellStyler::style($value, …)`             | the logical value (an enum case, a `Uuid`, …)                                             | the cell as written (`'one'`, `'f47ac10b-…'`, …)        |

### 122) `flow-php/etl-adapter-seal` - an `xml_element` field keeps an end tag

| Before              | After                  |
|---------------------|------------------------|
| `<a b="1"><c/></a>` | `<a b="1"><c></c></a>` |

### 123) `flow-php/etl-adapter-doctrine`, `flow-php/etl-adapter-postgresql` - markup bound as rendered text

| Before                                                                                | After                                                         |
|---------------------------------------------------------------------------------------|---------------------------------------------------------------|
| `xml_element` `<a b="1"><c/></a>`                                                     | `<a b="1"><c></c></a>`                                        |
| PostgreSQL `xml` `<?xml version="1.0"?>\n<root>…</root>\n`                            | `<root>…</root>`                                              |
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

|                                                                | Before                   | After                     |
|----------------------------------------------------------------|--------------------------|---------------------------|
| `path('/data/orders.csv')->uri()`                              | `file://data/orders.csv` | `file:///data/orders.csv` |
| `path('/')->uri()`                                             | `file://`                | `file:///`                |
| `_input_file_uri`, `explain()` `Source:` of a local file       | `file://data/orders.csv` | `file:///data/orders.csv` |

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

### 130) `flow-php/etl-adapter-postgresql`, `flow-php/etl-adapter-doctrine` - keyset reads reject NULL keys up front

| Before                                                                                 | After                                                                                                         |
|----------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------|
| a NULL key: rows lost or `NULL value found in column ...`, depending on the batch size | one check query before the first page, then `RuntimeException` `Keyset pagination requires non-null keys ...` |
| no query before the first page                                                         | `SELECT 1 FROM (<query>) ... WHERE k IS NULL LIMIT 1` (DBAL: the builder plus `IS NULL` conditions)           |

Filter NULL keys out with `IS NOT NULL`, or page by non-null columns. Index the keys - on an unindexed key the check is one full scan.

### 131) `flow-php/etl-adapter-postgresql`, `flow-php/etl-adapter-doctrine` - keyset reads reject duplicate keys

| Before                                                                                              | After                                                                                                                  |
|-----------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------|
| two rows with the same key: rows at a page boundary silently skipped                                | `RuntimeException` `Keyset pagination requires unique keys ...`                                                        |
| keys equal in the database but different in PHP (`'a'`/`'A'` under a `_ci` collation, `1.0`/`1.00`) | `RuntimeException` `Keyset pagination expected the next page to start at the key ...` when a page boundary splits them |

Add a unique column (for example the primary key) to the key set as its least significant key.

### 132) `flow-php/etl-adapter-postgresql`, `flow-php/etl-adapter-doctrine` - a short keyset page ends the read

| Before                                                | After                                                                                             |
|-------------------------------------------------------|---------------------------------------------------------------------------------------------------|
| each page `LIMIT n`; reading stopped on an empty page | each page `LIMIT n + 1` (the extra row is not yielded); a page of `n` rows or fewer ends the read |
| one trailing empty `SELECT`                           | none                                                                                              |

Nothing to change unless you count queries: a read sends the NULL check plus the page queries.

### 133) `flow-php/postgresql`, `flow-php/etl-adapter-postgresql` - keyset pagination rejects the query's own LIMIT/OFFSET

| Before                                                                               | After                                                             |
|--------------------------------------------------------------------------------------|-------------------------------------------------------------------|
| `sql_to_keyset_query('SELECT ... LIMIT 5', 10, ...)` - LIMIT silently replaced by 10 | `PaginationException` `Keyset pagination sets its own LIMIT; ...` |
| `from_pgsql_key_set($client, 'SELECT ... LIMIT 5', ...)`                             | same `PaginationException`, before any query                      |

Drop LIMIT/OFFSET from the query; use `withMaximum()` or `DataFrame::limit()`.

### 134) `flow-php/etl-adapter-doctrine` - `from_dbal_key_set_qb()` rejects `setMaxResults()` / `setFirstResult()`

| Before                                                          | After                                                                             |
|-----------------------------------------------------------------|-----------------------------------------------------------------------------------|
| `$qb->setMaxResults(5)` - silently overwritten by the page size | `InvalidArgumentException` `Keyset pagination sets its own LIMIT and OFFSET, ...` |
| `$qb->setFirstResult(5)` - applied to every page                | same `InvalidArgumentException`                                                   |

Remove them from the builder; use `withMaximum()` or `DataFrame::limit()`.

### 135) `flow-php/postgresql`, `flow-php/etl-adapter-postgresql` - a query's own ORDER BY must equal the keys

| Before                                                                                 | After                                                                                          |
|----------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------|
| `'SELECT ... ORDER BY name'` with key `id` - kept, pages wrong                         | `PaginationException` `Keyset pagination requires ORDER BY item #1 to be the key "id ASC" ...` |
| `ORDER BY id NULLS FIRST`, `ORDER BY 1`, `ORDER BY lower(name)`, `ORDER BY id USING <` | same `PaginationException`                                                                     |

Drop the ORDER BY (it is generated from the keys) or make it list exactly the keys, in order and direction.

### 136) `flow-php/postgresql` - `KeysetPaginationConfig` / `sql_to_keyset_query()` reject a NULL cursor value

| Before                                                                                 | After                                                                                |
|----------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------|
| `new KeysetPaginationConfig(10, $columns, [null, 7])` - predicate that matches nothing | `PaginationException` `Keyset cursor value #1 is NULL; key columns must be non-null` |

Page by non-null key columns.

### 137) `flow-php/postgresql` - same-direction composite keys emit a row comparison

| Before                                                       | After                         |
|--------------------------------------------------------------|-------------------------------|
| `created_at > $1 OR (created_at = $1 AND id > $2)`           | `(created_at, id) > ($1, $2)` |
| all DESC: `created_at < $1 OR (created_at = $1 AND id < $2)` | `(created_at, id) < ($1, $2)` |

Nothing to change; the row comparison is an index range bound. Mixed ASC/DESC keys keep the `OR` form. Update assertions on the generated SQL.

### 138) `flow-php/etl-adapter-doctrine` - a non-scalar keyset value is rejected

| Before                                                                       | After                                                                                        |
|------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------|
| a key value that is not `string`/`int`/`float`/`bool` - bound as a parameter | `RuntimeException` `Unsupported value type "<type>" in column "<key>" for keyset pagination` |

Page by scalar columns.

### 139) `flow-php/etl-adapter-doctrine` - `pagination_key_set()` rejects zero keys

| Before                                                                                                             | After                                                                                       |
|--------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------|
| `pagination_key_set()` built; `from_dbal_key_set_qb()` threw `KeySet must contain at least one key for pagination` | `pagination_key_set()` throws `InvalidArgumentException` `KeySet requires at least one key` |

Pass at least one key.

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
