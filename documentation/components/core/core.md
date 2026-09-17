---
package: flow-php/etl
---

# Data Frame

[PACKAGE_NAV]

[TOC]

## Installation

For detailed installation instructions, see the [installation page](/documentation/installation/packages/etl.md).

A Data Frame is the core component of Flow PHP's ETL framework. It represents a structured collection of tabular data that can be processed, transformed, and loaded efficiently. Think of it as a programmable spreadsheet that can handle large datasets with minimal memory footprint.

## Key Features

- **Memory Efficient**: Processes data in chunks using generators, avoiding memory exhaustion
- **Lazy Evaluation**: Operations are only executed when needed
- **Immutable**: Each transformation returns a new DataFrame instance
- **Type Safe**: Strict typing throughout with comprehensive schema support
- **Chainable API**: Fluent interface for building complex data pipelines

## Understanding DataFrame Operations

DataFrame methods fall into two categories based on when they execute:

### Lazy Operations (`@lazy`)

These methods build the processing pipeline without executing it immediately:

- **Transformations**: `filter()`, `withEntry()`, `select()`, `drop()`, `rename()`, `with()`
- **Memory-intensive**: `collect()`, `sortBy()`, `groupBy()`, `join()`, `cache()`
- **Processing control**: `batchSize()`, `limit()`, `offset()`, `partitionBy()`

### Trigger Operations (`@trigger`)

These methods execute the entire pipeline and return results:

- **Data retrieval**: `get()`, `getEach()`, `fetch()`, `count()`
- **Output operations**: `run()`, `forEach()`, `printRows()`
- **Schema inspection**: `display()`

`schema()`, `printSchema()` and `explain()` are **not** triggers - they answer from the plan without reading a row.
`explain()` returns the frame's `Plan`. Its `toString()` prints the plan after the configured optimizer ran, as a
tree read from the bottom up: a node's children are where its rows come from, and nodes are numbered in the order
rows reach them - the source is `#1`. What a node does is listed under it.

```php
echo data_frame()
    ->read(from_csv('orders.csv'))
    ->filter(ref('email')->isNotNull())
    ->write(to_json('out.json'))
    ->explain()->toString();
```

```text
Outputs
├─ #3 Result
│  │  Rows this plan hands out: to the trigger, or to the node reading it
│  └─ #2 Filter
│     │  Condition: IsNotNull
│     └─ #1 Read
│           Extractor: CSVExtractor
└─ #4 Write
   │  Loader: JsonLoader
   └─ #2 Filter (shared)
```

`Outputs` lists everything the frame produces: `Result` is what the trigger reads, `Write` is the sink, and both
read the same rows. A node several consumers read is printed once, and every other consumer points back at it by
number - `#2 Filter (shared)` is that same filter, not a second one. A node keeps its number in every format.
A frame joined with `join()` / `crossJoin()` is part of the same tree: its own `Result` sits where its rows enter,
numbered with the rest of the plan, and it runs with this frame's configuration. A frame read with `from_data_frame()`
is a `Read` of `DataFrameExtractor` and runs with its own.

`toString()` takes the stage and the format to print:

| Argument                     | Prints                                                                              |
|------------------------------|-------------------------------------------------------------------------------------|
| `Stage::optimized` (default) | the plan the optimizer hands to the planner                                         |
| `Stage::unoptimized`         | the plan as the frame built it                                                      |
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
└─ #2 Filter
   │  Condition: IsNotNull
   ├─ #3 Result
   │     Rows this plan hands out: to the trigger, or to the node reading it
   └─ #4 Write
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
│         #3 Result         ││         #4 Write          │
│   ────────────────────    ││   ────────────────────    │
│ Rows this plan hands out: ││    Loader: JsonLoader     │
│ to the trigger, or to the ││                           │
│      node reading it      ││                           │
└─────────────┬─────────────┘└─────────────┬─────────────┘
┌─────────────┴─────────────┐┌─────────────┴─────────────┐
│         #2 Filter         ││         #2 Filter         │
│   ────────────────────    ││         (shared)          │
│   Condition: IsNotNull    ││                           │
└─────────────┬─────────────┘└───────────────────────────┘
┌─────────────┴─────────────┐
│          #1 Read          │
│   ────────────────────    │
│  Extractor: CSVExtractor  │
└───────────────────────────┘
```

> **Important**: Build your complete pipeline with lazy operations, then execute once with a trigger operation for optimal performance.

## Creating DataFrames

DataFrames are created using the `data_frame()` DSL function and populated with data through extractors. The framework supports various data sources through adapter-specific extractors.

```php
<?php

use function Flow\ETL\DSL\{data_frame, from_array, to_output};

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

> **Note**: Flow PHP supports many data sources through specialized adapters. See individual adapter documentation for specific extractor usage (CSV, JSON, Parquet, databases, APIs, etc.).

## Memory Management Best Practices

1. **Prefer Generator Methods**: Use `get()`, `getEach()`, `getEachAsArray()` over `fetch()` for large datasets
2. **Avoid Memory-Intensive Operations**: Be cautious with `collect()`, `sortBy()`, `groupBy()`, and `join()` on large datasets
3. **Use Appropriate Batch Sizes**: Start with 1000-5000 rows and adjust based on your memory constraints
4. **Monitor Memory Usage**: Use `run(analyze: true)` to track memory consumption during development

## Performance Optimization

- **Push Operations to Data Source**: When possible, perform filtering, sorting, and joins at the database/file level
- **Minimize Data Movement**: Apply filters early in the pipeline to reduce data volume
- **Cache Strategically**: Only cache expensive operations that will be reused multiple times
- **Avoid Large Offsets**: Use data source pagination instead of DataFrame `offset()` for large skips

### Optimizer

Before a frame runs, the optimizer rewrites its plan. `Optimizer::default()` runs these rules, in order:

| Rule                   | Rewrite                                                                          |
|------------------------|----------------------------------------------------------------------------------|
| `CombineLimits`        | two stacked `limit()` calls become one, with the smaller limit                   |
| `CombineSortAndLimit`  | `sortBy()` followed by `limit()` keeps only the top rows instead of sorting all  |
| `PushLimitIntoSource`  | the extractor stops reading once the limit (plus any `offset()`) is reached      |
| `PushFilterIntoSource` | a `filter()` on partition columns skips whole partition directories              |

Rules live in `Flow\ETL\Optimizer\Rule` and are configured through `config_builder()->optimizer()`:

```php
<?php

use Flow\ETL\Optimizer;
use Flow\ETL\Optimizer\Rule\CombineSortAndLimit;

use function Flow\ETL\DSL\{config_builder, data_frame};

// no rewrites at all
data_frame(config_builder()->optimizer(new Optimizer()));

// the defaults without one rule
data_frame(config_builder()->optimizer(Optimizer::default()->without(CombineSortAndLimit::class)));

// the defaults followed by your own Optimizer\Rule implementation
data_frame(config_builder()->optimizer(Optimizer::default()->with(new MyRule())));
```

`without()` throws on a rule that is not registered, `with()` on a rule class that already is.
`explain()->toString()` prints the plan after the configured optimizer ran.

## Component Documentation

For detailed information about specific DataFrame operations, see the following component documentation:

### Core Operations
- **[Building Blocks](/documentation/components/core/building-blocks.md)** - Understanding Rows, Entries, and basic data structures
- **[Transformations](/documentation/components/core/transformations.md)** - Reusable DataFrame transformations and the Transformation interface
- **[Select/Drop](/documentation/components/core/select-drop.md)** - Column selection and removal
- **[Rename](/documentation/components/core/rename.md)** - Column renaming strategies
- **[Filter](/documentation/components/core/filter.md)** - Row filtering and conditions
- **[Save Mode](/documentation/components/core/save-mode.md)** - configure how flow is saving files

### Data Processing
- **[Join](/documentation/components/core/join.md)** - DataFrame joining operations
- **[Group By](/documentation/components/core/group-by.md)** - Grouping and aggregation operations
- **[Pivot](/documentation/components/core/pivot.md)** - Transform data from long to wide format
- **[Sort](/documentation/components/core/sort.md)** - Data sorting
- **[Limit](/documentation/components/core/limit.md)** - Result limiting and pagination
- **[Offset](/documentation/components/core/offset.md)** - Skipping rows and pagination
- **[Until](/documentation/components/core/until.md)** - Conditional processing termination
- **[Window Functions](/documentation/components/core/window-functions.md)** - Advanced analytical functions

### Memory & Performance
- **[Batch Processing](/documentation/components/core/batch-processing.md)** - Controlling batch sizes and memory collection
- **[Partitioning](/documentation/components/core/partitioning.md)** - Data partitioning for efficient processing
- **[Caching](/documentation/components/core/caching.md)** - Performance optimization through caching
- **[Floe File Format](/documentation/components/core/floe.md)** - Flow's native self-describing binary row format
- **[Data Retrieval](/documentation/components/core/data-retrieval.md)** - Methods for getting processed data

### Data Quality & Validation
- **[Schema](/documentation/components/core/schema.md)** - Schema management and validation
- **[Constraints](/documentation/components/core/constraints.md)** - Data integrity constraints and business rules
- **[Error Handling](/documentation/components/core/error-handling.md)** - Error management strategies

### Observability
- **[Telemetry](/documentation/components/core/telemetry.md)** - Distributed tracing, metrics, and logging integration

### Output & Display
- **[Display](/documentation/components/core/display.md)** - Data visualization and output
