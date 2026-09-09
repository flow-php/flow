# Query Plan Analysis

[DOC_LINK:/documentation/components/libs/postgresql.md]

[TOC]

The client provides built-in support for analyzing PostgreSQL query execution plans. Use `EXPLAIN ANALYZE` to understand
how PostgreSQL executes your queries and identify performance issues.

## Basic Usage

The `explain()` method executes a query with `EXPLAIN ANALYZE` and returns a structured `Plan` object:

```php
<?php

use function Flow\PostgreSql\DSL\{pgsql_client, pgsql_connection, select, col, table, eq, literal};

$client = pgsql_client(pgsql_connection('host=localhost dbname=mydb'));

$query = select(col('id'), col('name'))
    ->from(table('users'))
    ->where(eq(col('status'), literal('active')));

$plan = $client->explain($query);

echo "Execution time: {$plan->executionTime()}ms\n";
echo "Planning time: {$plan->planningTime()}ms\n";
echo "Total cost: {$plan->rootNode()->cost()->totalCost()}\n";
```

You can also pass raw SQL strings with parameters:

```php
<?php

$plan = $client->explain(
    'SELECT * FROM orders WHERE user_id = $1 AND status = $2',
    [42, 'pending']
);
```

## The Plan Object

The `Plan` object provides access to execution statistics and the full node tree:

```php
<?php

$plan = $client->explain($query);

// Execution statistics
$plan->executionTime();  // Total execution time in milliseconds
$plan->planningTime();   // Query planning time in milliseconds

// Root node of the execution plan
$rootNode = $plan->rootNode();

// All nodes as a flat list
foreach ($plan->allNodes() as $node) {
    echo "{$node->nodeType()->value}: {$node->cost()->totalCost()}\n";
}
```

## Plan Nodes

Each node in the plan tree represents an operation PostgreSQL performs:

```php
<?php

$node = $plan->rootNode();

// Node type (Seq Scan, Index Scan, Hash Join, etc.)
$node->nodeType();           // PlanNodeType enum
$node->nodeType()->value;    // 'Seq Scan', 'Index Scan', etc.

// Cost estimates
$node->cost()->startupCost();    // Cost to return first row
$node->cost()->totalCost();      // Total cost

// Row estimates vs actual
$node->estimatedRows();      // Planner's row estimate
$node->actualRows();  // Actual rows (with ANALYZE)

// Timing information (requires ANALYZE)
$node->timing()?->startupTime();    // Time to first row
$node->timing()?->totalActualTime();  // Total execution time
$node->timing()?->loops();          // Number of iterations

// Buffer statistics (requires BUFFERS option)
$node->buffers()?->sharedHit();     // Blocks from shared cache
$node->buffers()?->sharedRead();    // Blocks read from disk

// Scan-specific info
$node->relationName();    // Table name for scans
$node->alias();           // Table alias
$node->indexName();       // Index name for index scans

// Child nodes
foreach ($node->children() as $child) {
    // Process child nodes
}
```

## Plan Analyzer

The `PlanAnalyzer` provides quick insights without manually traversing the plan tree:

```php
<?php

use function Flow\PostgreSql\DSL\{sql_analyze};

$plan = $client->explain($query);
$analyzer = sql_analyze($plan);

// Find performance issues
$insights = $analyzer->allInsights();

foreach ($insights as $insight) {
    echo "[{$insight->severity->value}] {$insight->type->value}: {$insight->description}\n";
}
```

### Finding Slow Nodes

Identify the slowest operations in your query:

```php
<?php

// Get top 3 slowest nodes
foreach ($analyzer->slowestNodes(limit: 3) as $insight) {
    echo "{$insight->node->nodeType()->value}: {$insight->metrics['time_ms']}ms\n";
}
```

### Sequential Scans

Find full table scans that might benefit from indexes:

```php
<?php

foreach ($analyzer->sequentialScans() as $insight) {
    echo "Sequential scan on '{$insight->node->relationName()}'\n";
    echo "  Estimated rows: {$insight->metrics['estimated_rows']}\n";
    // Consider adding an index if the table is large
}
```

### Estimate Mismatches

Detect where the planner's row estimates differ significantly from actual rows:

```php
<?php

// Find nodes where actual rows differ by more than 2x from estimates
foreach ($analyzer->estimateMismatches(threshold: 2.0) as $insight) {
    echo "Estimate mismatch on {$insight->node->nodeType()->value}\n";
    echo "  Expected: {$insight->metrics['estimated_rows']}, Actual: {$insight->metrics['actual_rows']}\n";
    echo "  Ratio: {$insight->metrics['mismatch_ratio']}x\n";
    // Consider running ANALYZE on the table or updating statistics
}
```

### External Sorts

Find sort operations that spill to disk:

```php
<?php

foreach ($analyzer->externalSorts() as $insight) {
    echo "Disk-based sort: {$insight->metrics['space_used_kb']}KB used\n";
    // Consider increasing work_mem or adding an index
}
```

### Inefficient Filters

Find filters that scan many rows but return few:

```php ignore
<?php

// Find filters removing more than 50% of rows
foreach ($analyzer->inefficientFilters(threshold: 0.5) as $insight) {
    echo "Inefficient filter: {$insight->metrics['removal_ratio'] * 100}% of rows discarded\n";
    // Consider adding an index to reduce rows scanned
}
```

### Buffer Analysis

Analyze cache efficiency and disk reads:

```php ignore
<?php

// Find nodes reading from disk
foreach ($analyzer->diskReads() as $insight) {
    echo "Disk reads: {$insight->metrics['blocks_read']} blocks\n";
}

// Find nodes with poor cache hit ratio
foreach ($analyzer->lowCacheHits(threshold: 0.9) as $insight) {
    echo "Low cache hit ratio: {$insight->metrics['hit_ratio'] * 100}%\n";
}
```

### Plan Summary

Get aggregate statistics for the entire plan:

```php
<?php

$summary = $analyzer->summary();

echo "Total cost: {$summary->totalCost}\n";
echo "Execution time: {$summary->executionTime}ms\n";
echo "Planning time: {$summary->planningTime}ms\n";
echo "Node count: {$summary->nodeCount}\n";
echo "Sequential scans: {$summary->sequentialScanCount}\n";
echo "Index scans: {$summary->indexScanCount}\n";

if ($summary->hasExternalSort) {
    echo "Warning: Query uses disk-based sorting\n";
}

if ($summary->hasDiskReads) {
    echo "Warning: Query reads from disk (not fully cached)\n";
}

if ($summary->overallCacheHitRatio !== null) {
    echo "Cache hit ratio: " . round($summary->overallCacheHitRatio * 100, 1) . "%\n";
}
```

## EXPLAIN Configuration

Customize what information is collected:

```php
<?php

use function Flow\PostgreSql\DSL\sql_explain_config;

// Default: ANALYZE with BUFFERS, TIMING, and FORMAT JSON
$plan = $client->explain($query);

// Without ANALYZE (no actual execution, estimates only)
$plan = $client->explain($query, config: sql_explain_config(analyze: false));

// Custom configuration
$plan = $client->explain($query, config: sql_explain_config(
    analyze: true,
    verbose: false,
    buffers: true,
    timing: true,
    costs: true,
));
```

### Configuration Options

| Option    | Description                              | Default |
|-----------|------------------------------------------|---------|
| `analyze` | Execute query and collect actual timings | `true`  |
| `verbose` | Include additional details               | `false` |
| `buffers` | Include buffer usage statistics          | `true`  |
| `timing`  | Include timing per node                  | `true`  |
| `costs`   | Include cost estimates                   | `true`  |
| `format`  | Output format (JSON, TEXT, XML, YAML)    | `JSON`  |

## Insight Types and Severity

### Insight Types

| Type                 | Description                                              |
|----------------------|----------------------------------------------------------|
| `SLOW_NODE`          | Node with high execution time                            |
| `ESTIMATE_MISMATCH`  | Significant difference between estimated and actual rows |
| `SEQUENTIAL_SCAN`    | Full table scan (potential missing index)                |
| `EXTERNAL_SORT`      | Sort operation spilling to disk                          |
| `INEFFICIENT_FILTER` | Filter removing large percentage of rows                 |
| `DISK_READ`          | Node reading data from disk                              |
| `LOW_CACHE_HIT`      | Node with poor cache hit ratio                           |

### Severity Levels

| Severity   | Description                                     |
|------------|-------------------------------------------------|
| `INFO`     | Informational, may not require action           |
| `WARNING`  | Potential performance issue worth investigating |
| `CRITICAL` | Significant performance problem                 |
