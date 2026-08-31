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
- **Output operations**: `run()`, `forEach()`, `printRows()`, `printSchema()`
- **Schema inspection**: `schema()`, `display()`

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

### Reliability & Recovery
- **[Retry Mechanisms](/documentation/components/core/retry.md)** - Automatic retry for transient failures

### Observability
- **[Telemetry](/documentation/components/core/telemetry.md)** - Distributed tracing, metrics, and logging integration

### Output & Display
- **[Display](/documentation/components/core/display.md)** - Data visualization and output
