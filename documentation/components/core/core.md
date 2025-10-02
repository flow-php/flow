# Data Frame

- [⬅️️ Back](/documentation/quick-start.md)
- [📚API Reference](/documentation/api/core)
- [📁Files](/documentation/api/core/indices/files.html)

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

- **Transformations**: `filter()`, `map()`, `withEntry()`, `select()`, `drop()`, `rename()`
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
- **[Building Blocks](building-blocks.md)** - Understanding Rows, Entries, and basic data structures
- **[Transformations](transformations.md)** - Reusable DataFrame transformations and the Transformation interface
- **[Select/Drop](select-drop.md)** - Column selection and removal
- **[Rename](rename.md)** - Column renaming strategies
- **[Map](map.md)** - Row transformations and data mapping
- **[Filter](filter.md)** - Row filtering and conditions

### Data Processing
- **[Join](join.md)** - DataFrame joining operations
- **[Group By](group-by.md)** - Grouping and aggregation operations
- **[Pivot](pivot.md)** - Transform data from long to wide format
- **[Sort](sort.md)** - Data sorting
- **[Limit](limit.md)** - Result limiting and pagination
- **[Offset](offset.md)** - Skipping rows and pagination
- **[Until](until.md)** - Conditional processing termination
- **[Window Functions](window-functions.md)** - Advanced analytical functions

### Memory & Performance
- **[Batch Processing](batch-processing.md)** - Controlling batch sizes and memory collection
- **[Partitioning](partitioning.md)** - Data partitioning for efficient processing
- **[Caching](caching.md)** - Performance optimization through caching
- **[Data Retrieval](data-retrieval.md)** - Methods for getting processed data

### Data Quality & Validation
- **[Schema](schema.md)** - Schema management and validation
- **[Constraints](constraints.md)** - Data integrity constraints and business rules
- **[Error Handling](error-handling.md)** - Error management strategies

### Reliability & Recovery
- **[Retry Mechanisms](retry.md)** - Automatic retry for transient failures

### Output & Display
- **[Display](display.md)** - Data visualization and output
