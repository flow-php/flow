---
package: flow-php/etl
---

# Data Frame

[PACKAGE_NAV]

[TOC]

## Installation

For the installation steps, see the [installation page](/documentation/installation/packages/etl.md).

A Data Frame is the core component of the Flow PHP ETL framework. It is a structured collection of tabular data. You read, transform and load this data with it. A Data Frame works like a spreadsheet that you program. It processes large datasets with little memory.

## Key Features

- Memory Efficient: It processes data in batches with generators. This keeps the memory usage low.
- Columnar: Every batch stores one column per schema definition. A [column backend](/documentation/components/core/column-backend.md) that you can configure builds the columns.
- Lazy Evaluation: A trigger runs the operations. Before the trigger, nothing runs.
- Immutable: Each transformation returns a new DataFrame instance.
- Type Safe: It uses strict types and a schema for all data.
- Chainable API: A fluent interface builds data pipelines.

## Understanding DataFrame Operations

There are two categories of DataFrame methods. Each category runs at a different time:

### Lazy Operations (`@lazy`)

These methods build the processing pipeline. They do not run it:

- Transformations: `filter()`, `withColumn()`, `select()`, `drop()`, `rename()`, `with()`
- Memory-intensive: `collect()`, `sortBy()`, `groupBy()`, `join()`, `cache()`
- Processing control: `batchSize()`, `limit()`, `offset()`, `partitionBy()`

### Trigger Operations (`@trigger`)

These methods execute the entire pipeline and return results:

- Data retrieval: `get()`, `getEachAsArray()`, `fetch()`, `count()`
- Output operations: `run()`, `forEach()`, `printRows()`
- Schema inspection: `display()`

`schema()`, `printSchema()` and `explain()` are not triggers. They get their answer from the plan and do not read a
row. `explain()` returns the `Plan` of the frame. Its `toString()` prints the plan after the configured optimizer ran.
The output is a tree that you read from the bottom up. The children of a node are where its rows come from. The
numbers of the nodes follow the order in which rows reach them, so the source is `#1`. What a node does is listed
under it.

The verbs build a plan with no consumer on top. The trigger adds the consumer that it needs. For this reason,
`explain()` takes the trigger to print. The default is `Trigger::rows`. `Trigger::run` prints the plan that `run()`
executes, and `Trigger::count` prints the plan that `count()` executes.

```php
$dataFrame = data_frame()
    ->read(from_csv('/data/orders.csv'))
    ->filter(ref('email')->isNotNull())
    ->write(to_json('out.json'))
    ->select('id', 'email');

echo $dataFrame->explain()->toString();
```

```text
Outputs
├─ #4 Result
│  │  Rows this plan hands out: to the trigger, or to the node reading it
│  └─ #3 Select
│     │  Keeps only: id, email
│     └─ #2 Filter
│        │  Condition: email IS NOT NULL
│        └─ #1 Read
│              Extractor: CSVExtractor
│              Source: file:///data/orders.csv
└─ #5 Write
   │  Loader: JsonLoader
   └─ #2 Filter (shared)
```

```php
echo data_frame()
    ->read(from_csv('/data/orders.csv'))
    ->filter(ref('email')->isNotNull())
    ->write(to_json('out.json'))
    ->explain(Trigger::run)->toString();
```

```text
#3 Write
│  Loader: JsonLoader
└─ #2 Filter
   │  Condition: email IS NOT NULL
   └─ #1 Read
         Extractor: CSVExtractor
         Source: file:///data/orders.csv
```

`run()` takes no rows, so its plan has only the writes. The `Write` is the root and there is no `Result`. If a verb
follows the last `write()`, the plan gets a `Result` again. Nothing reads the end of that chain, so a `Result` pulls
the rows. `run(analyze: ...)` still counts them.

`Outputs` lists everything that the frame produces. `Result` is what a trigger that returns rows reads. `Write` is a
write. If several consumers read a node, the node is printed once. Every other consumer points back at it by number.
`#2 Filter (shared)` is that same filter, not a second one. A node keeps its number in every format.
A frame that you join with `join()` / `crossJoin()` is part of the same tree. The join reads it like any other
input. Its nodes get numbers with the rest of the plan, and it runs with the configuration of this frame.

`toString()` takes the stage and the format to print:

| Argument                     | Prints                                                                              |
|------------------------------|-------------------------------------------------------------------------------------|
| `Stage::optimized` (default) | the plan the optimizer hands to the planner                                         |
| `Stage::unoptimized`         | the plan as the frame built it                                                      |
| `Stage::physical`            | the plan the executor runs: pipelines, their steps and every setting each was given |
| `Format::tree` (default)     | the tree above: every node above the nodes it reads                                 |
| `Format::flow`               | the same tree turned around: sources first, every node above the nodes that read it |
| `Format::boxes`              | a box per node, children side by side                                               |
| `Format::declarations`       | the tree with the declarations optimizer rules read on every node's line            |

```php
echo $dataFrame->explain()->toString(format: Format::flow);
```

```text
#1 Read
│  Extractor: CSVExtractor
│  Source: file:///data/orders.csv
└─ #2 Filter
   │  Condition: email IS NOT NULL
   ├─ #3 Select
   │  │  Keeps only: id, email
   │  └─ #4 Result
   │        Rows this plan hands out: to the trigger, or to the node reading it
   └─ #5 Write
         Loader: JsonLoader
```

```php
echo $dataFrame->explain()->toString(format: Format::boxes);
```

```text
┌───────────────────────────┐
│          Outputs          ├──────────────┐
└─────────────┬─────────────┘              │
┌─────────────┴─────────────┐┌─────────────┴─────────────┐
│         #4 Result         ││         #5 Write          │
│   ────────────────────    ││   ────────────────────    │
│ Rows this plan hands out: ││    Loader: JsonLoader     │
│ to the trigger, or to the ││                           │
│      node reading it      ││                           │
└─────────────┬─────────────┘└─────────────┬─────────────┘
┌─────────────┴─────────────┐┌─────────────┴─────────────┐
│         #3 Select         ││         #2 Filter         │
│   ────────────────────    ││         (shared)          │
│   Keeps only: id, email   ││                           │
└─────────────┬─────────────┘└───────────────────────────┘
┌─────────────┴─────────────┐
│         #2 Filter         │
│   ────────────────────    │
│  Condition: email IS NOT  │
│           NULL            │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│          #1 Read          │
│   ────────────────────    │
│  Extractor: CSVExtractor  │
│          Source:          │
│  file:///data/orders.csv  │
└───────────────────────────┘
```

`Stage::physical` prints what the executor runs. A pipeline ends at a step that needs every row before it emits one.
A sort, a group by, a join and `collect()` are such steps. Rows stop at the end of the pipeline that holds the step.
Every step lists the configuration that it got. This includes the storage of the algorithm. The logical stages
cannot show the storage, because the planner selects it.

```php
$users = [['id' => 1, 'name' => 'Alice'], ['id' => 2, 'name' => 'Bob']];
$emails = [['id' => 1, 'email' => 'alice@example.com']];

echo data_frame()
    ->read(from_array($users))
    ->join(data_frame()->read(from_array($emails)), join_on(['id' => 'id'], join_prefix: 'joined_'), JoinType::left)
    ->collect()
    ->write(to_output(truncate: false))
    ->explain()->toString(Stage::physical);
```

```text
Physical plan
│  Columns: id, name, joined_id, joined_email
└─ Pipeline #2
   │  Loader: StreamLoader
   └─ Pipeline #1
      │  Processor: CollectingProcessor
      │     Schema: declared
      └─ Pipeline #0
         │  Extractor: ArrayExtractor
         │     Statistics: rows exact 2 · size unknown
         │  Join: HashJoin
         │     Type: left
         │     On: id = id
         │     Prefix: joined_
         │     Storage: FilesystemRowsStorage
         │     Memory: 1000 MB
         │     Buckets: 64
         │     Batch: 1000
         └─ Right side: Pipeline #0
               Extractor: ArrayExtractor
                  Statistics: rows exact 1 · size unknown
```

`Memory: 1000 MB` is the default limit under `memory_limit = -1`. If `memory_limit` has a value, the limit is 70% of
it, or `FLOW_MAX_MEMORY`.

The planner plans a joined frame separately, so its pipelines have their own numbers. `Right side:` tells which
plan they belong to. Before it reads a row, every source lists what it declares about itself. The values are
`exact n`, `≤ n` (a guaranteed bound), `~n ±e%` (an estimate) or `unknown`. A source that declares nothing lists no
`Statistics:` line. This stage plans the frame. A source that infers its schema by reading is therefore read here. The
logical stages never read a row.

> Build your complete pipeline with lazy operations. Then execute it once with a trigger operation. This gives the best performance.

## Creating DataFrames

You create a DataFrame with the `data_frame()` DSL function. Extractors fill it with data. Each adapter has its own extractors for its data source.

```php
<?php

use function Flow\ETL\DSL\{col, data_frame, from_array, lit, to_output};

$dataFrame = data_frame()
    ->read(from_array([
        ['id' => 1, 'name' => 'John', 'age' => 30],
        ['id' => 2, 'name' => 'Jane', 'age' => 25],
        ['id' => 3, 'name' => 'Bob', 'age' => 35],
    ]))
    ->filter(col('age')->greaterThan(lit(25)))
    ->select('id', 'name')
    ->write(to_output())
    ->run();
```

> Flow PHP reads many data sources through adapters. For the extractors of each source, see the documentation of its adapter. Examples are CSV, JSON, Parquet, databases and APIs.

An empty source has no column to infer a schema from, so it throws `InferredSchemaException`. To read it, declare the schema:

```php
<?php

use function Flow\ETL\DSL\{data_frame, from_array, int_schema, schema, str_schema, to_output};

data_frame()
    ->read(from_array([], schema(int_schema('id'), str_schema('name'))))
    ->select('id')
    ->write(to_output())
    ->run();
```

## Memory Management Best Practices

1. Prefer Generator Methods: For large datasets, use `get()` or `getEachAsArray()` instead of `fetch()`.
2. Avoid Memory-Intensive Operations: On large datasets, be careful with `collect()`, `sortBy()`, `groupBy()`, and `join()`.
3. Use Appropriate Batch Sizes: Start with 1000-5000 rows. Then change the size to fit your memory limits.
4. Monitor Memory Usage: During development, use `run(analyze: true)` to see the memory usage.

## Performance Optimization

- Push Operations to Data Source: If possible, filter, sort and join at the database/file level.
- Minimize Data Movement: Apply filters early in the pipeline. This reduces the data volume.
- Cache Strategically: Cache only expensive operations that the pipeline uses multiple times.
- Avoid Large Offsets: To skip many rows, use the pagination of the data source instead of DataFrame `offset()`.

### Optimizer

Before a frame runs, the optimizer rewrites its plan. `Optimizer::default()` runs these rules in this order:

| Rule                   | Rewrite                                                                         |
|------------------------|---------------------------------------------------------------------------------|
| `CombineLimits`        | two stacked `limit()` calls become one, with the smaller limit                  |
| `CombineSortAndLimit`  | `sortBy()` followed by `limit()` keeps only the top rows instead of sorting all |
| `PushLimitIntoSource`  | the extractor stops reading once the limit (plus any `offset()`) is reached     |
| `PushFilterIntoSource` | a `filter()` on partition columns skips whole partition directories             |
| `CountFromStatistics`  | `count()` over a source that knows its rows exactly reads that number, no rows  |

The rules are in `Flow\ETL\Planning\Optimizer\Rule`. You configure them with `config_builder()->optimizer()`:

```php
<?php

use Flow\ETL\Planning\Optimizer\Optimizer;
use Flow\ETL\Planning\Optimizer\Rule\CombineSortAndLimit;

use function Flow\ETL\DSL\{config_builder, data_frame};

// no rewrites at all
data_frame(config_builder()->optimizer(new Optimizer()));

// the defaults without one rule
data_frame(config_builder()->optimizer(Optimizer::default()->without(CombineSortAndLimit::class)));

// the defaults followed by your own Rule implementation
data_frame(config_builder()->optimizer(Optimizer::default()->with(new MyRule())));
```

`without()` throws on a rule that is not registered. `with()` throws on a rule class that is already registered.
`explain()->toString()` prints the plan after the configured optimizer ran.

## Component Documentation

For more information about each DataFrame operation, see this component documentation:

### Core Operations
- [Building Blocks](/documentation/components/core/building-blocks.md): rows, columns and the schema that describes them
- [Transformations](/documentation/components/core/transformations.md): reusable DataFrame transformations and the Transformation interface
- [Select/Drop](/documentation/components/core/select-drop.md): column selection and removal
- [Rename](/documentation/components/core/rename.md): column renaming strategies
- [Filter](/documentation/components/core/filter.md): row filtering and conditions
- [Save Mode](/documentation/components/core/save-mode.md): configure how flow saves files

### Data Processing
- [Join](/documentation/components/core/join.md): DataFrame join operations
- [Group By](/documentation/components/core/group-by.md): grouping and aggregation operations
- [Pivot](/documentation/components/core/pivot.md): transform data from long to wide format
- [Sort](/documentation/components/core/sort.md): data sorting
- [Limit](/documentation/components/core/limit.md): result limiting and pagination
- [Offset](/documentation/components/core/offset.md): skipping rows and pagination
- [Until](/documentation/components/core/until.md): conditional processing termination
- [Window Functions](/documentation/components/core/window-functions.md): advanced analytical functions

### Memory & Performance
- [Column Backend](/documentation/components/core/column-backend.md): where batch columns live: PHP arrays or the flow_php extension
- [Batch Processing](/documentation/components/core/batch-processing.md): controlling batch sizes and memory collection
- [Partitioning](/documentation/components/core/partitioning.md): data partitioning for efficient processing
- [Caching](/documentation/components/core/caching.md): performance optimization through caching
- [Floe File Format](/documentation/components/core/floe.md): flow's native self-describing columnar binary format
- [Source Statistics](/documentation/components/core/statistics.md): what a source declares about its rows and bytes before it is read
- [Data Retrieval](/documentation/components/core/data-retrieval.md): methods for getting processed data

### Data Quality & Validation
- [Schema](/documentation/components/core/schema.md): schema management and validation
- [Constraints](/documentation/components/core/constraints.md): data integrity constraints and business rules
- [Error Handling](/documentation/components/core/error-handling.md): error management strategies
- [Corrupted Records](/documentation/components/core/corrupted-records.md): routing records that cannot become rows

### Observability
- [Telemetry](/documentation/components/core/telemetry.md): integration of distributed tracing, metrics and logging

### Output & Display
- [Display](/documentation/components/core/display.md): data visualization and output
