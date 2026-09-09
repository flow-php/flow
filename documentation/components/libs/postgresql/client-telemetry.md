# Telemetry

[DOC_LINK:/documentation/components/libs/postgresql.md]

[TOC]

Telemetry support for the PostgreSQL client provides observability through OpenTelemetry-compatible traces, metrics,
and logs. The `TraceableClient` decorator wraps your client and automatically instruments all operations.

## Quick Start

```php
<?php

use Psr\Clock\ClockInterface;

use function Flow\PostgreSql\DSL\{
    pgsql_client,
    pgsql_connection,
    traceable_postgresql_client,
    postgresql_telemetry_config,
    postgresql_telemetry_options
};
use function Flow\Telemetry\DSL\{telemetry, resource};

// Create base client
$client = pgsql_client(pgsql_connection('host=localhost dbname=mydb user=postgres'));

// Wrap with telemetry
$client = traceable_postgresql_client(
    $client,
    postgresql_telemetry_config(
        telemetry(resource(['service.name' => 'my-app'])),
        new SystemClock(), // PSR-20 ClockInterface implementation
    ),
);

// All operations are now traced
$users = $client->fetchAll('SELECT * FROM users WHERE active = $1', [true]);
```

## Configuration Options

Configure telemetry behavior with `postgresql_telemetry_options()`:

```php
<?php

use Flow\PostgreSql\Client\Telemetry\TransactionSpanMode;

use function Flow\PostgreSql\DSL\{postgresql_telemetry_config, postgresql_telemetry_options};

$config = postgresql_telemetry_config(
    $telemetry,
    $clock,
    postgresql_telemetry_options(
        traceQueries: true,       // Create spans for queries
        transactionSpans: TransactionSpanMode::GROUPED, // grouped | per_operation | off
        collectMetrics: true,     // Record duration and row count histograms
        logQueries: false,        // Log executed queries
        maxQueryLength: 1000,     // Truncate query text (null = unlimited)
        includeParameters: false, // Include query parameters (security risk)
        maxParameters: 10,        // Max parameters to include
        maxParameterLength: 100,  // Truncate parameter values
    ),
);
```

### Options Reference

| Option              | Default | Description                                                                 |
|---------------------|---------|-----------------------------------------------------------------------------|
| `traceQueries`      | `true`  | Create spans for each query operation                                       |
| `transactionSpans`  | `TransactionSpanMode::GROUPED` | `GROUPED` = one span per transaction; `PER_OPERATION` = a span per BEGIN/COMMIT/ROLLBACK; `OFF` = none |
| `collectMetrics`    | `true`  | Record duration and row count histograms                                    |
| `logQueries`        | `false` | Log executed queries via the telemetry logger                               |
| `maxQueryLength`    | `1000`  | Maximum query text length in span attributes (`null` = unlimited)           |
| `includeParameters` | `false` | Include query parameter values in span attributes                           |
| `maxParameters`     | `10`    | Maximum number of parameters to include when `includeParameters` is enabled |
| `maxParameterLength`| `100`   | Maximum length for each parameter value                                     |

### Fluent Configuration

Options can be configured fluently:

```php
<?php

use Flow\PostgreSql\Client\Telemetry\TransactionSpanMode;

use function Flow\PostgreSql\DSL\postgresql_telemetry_options;

$options = postgresql_telemetry_options()
    ->traceQueries(true)
    ->transactionSpans(TransactionSpanMode::GROUPED)
    ->collectMetrics(true)
    ->logQueries(true)
    ->maxQueryLength(500)
    ->includeParameters(true)
    ->maxParameters(5)
    ->maxParameterLength(50);
```

## Traced Operations

### Query Operations

All query methods are traced with individual spans:

- `fetch()`, `fetchOne()`, `fetchAll()` - SELECT queries
- `fetchScalar()`, `fetchScalarInt()`, `fetchScalarString()`, etc.
- `fetchInto()`, `fetchOneInto()`, `fetchAllInto()` - Object mapping queries
- `execute()` - INSERT, UPDATE, DELETE operations
- `explain()` - Query plan analysis

Span names follow the pattern: `{OPERATION} {table}` (e.g., `SELECT users`)

### Transaction Lifecycle

Transaction operations create hierarchical spans:

- `beginTransaction()` - Opens a span for the transaction
- `commit()` - Completes the transaction span with OK status
- `rollBack()` - Completes the transaction span (and all nested spans)
- Nested transactions create `SAVEPOINT` spans

```php
<?php

// Creates a transaction span containing query spans
$client->transaction(function ($client) {
    // Span: BEGIN TRANSACTION
    $client->execute('INSERT INTO users (name) VALUES ($1)', ['John']);
    // Span: INSERT users
    $client->execute('INSERT INTO logs (user_id, action) VALUES ($1, $2)', [1, 'created']);
    // Span: INSERT logs
    // Span completion: COMMIT TRANSACTION (on success)
});
```

### Cursor Iteration

Cursors create spans that track the entire iteration lifecycle:

```php
<?php

function processRow(array $row): void { /* your code */ }

$cursor = $client->cursor('SELECT * FROM large_table');
// Span: SELECT large_table (cursor)

foreach ($cursor->iterate() as $row) {
    processRow($row);
}
// Span completed with row count
```

## Collected Attributes

Spans include attributes following [OpenTelemetry database semantic conventions](https://opentelemetry.io/docs/specs/semconv/database/):

| Attribute                    | Description                                                  | Example                         |
|------------------------------|--------------------------------------------------------------|---------------------------------|
| `db.system.name`             | Database system identifier                                   | `postgresql`                    |
| `db.namespace`               | Database name                                                | `mydb`                          |
| `db.operation.name`          | SQL operation type                                           | `SELECT`, `INSERT`, `UPDATE`    |
| `db.collection.name`         | Target table name                                            | `users`                         |
| `db.query.text`              | SQL query text (may be truncated)                            | `SELECT * FROM users WHERE...`  |
| `db.query.parameter.{n}`     | Parameter values (when enabled)                              | `123`, `true`                   |
| `db.response.returned_rows`  | Number of rows returned                                      | `42`                            |
| `db.response.status_code`    | PostgreSQL SQLSTATE code (on error)                          | `23505`                         |
| `server.address`             | Database host                                                | `localhost`                     |
| `server.port`                | Database port (only if non-default)                          | `5433`                          |
| `error.type`                 | Exception class name (on error)                              | `QueryException`                |

### Transaction-Specific Attributes

| Attribute                            | Description                   | Example        |
|--------------------------------------|-------------------------------|----------------|
| `flow.db.transaction.nesting_level`  | Current nesting depth         | `1`, `2`       |
| `flow.db.transaction.savepoint`      | Savepoint name (nested txns)  | `savepoint_1`  |

## Security Considerations

**Query parameters are disabled by default** for security reasons. Parameter values may contain sensitive data
(passwords, tokens, PII) that should not appear in traces.

Enable parameters only in development or when you're certain the data is safe:

```php
<?php

// Development only - never in production with sensitive data
$options = postgresql_telemetry_options(
    includeParameters: true,
    maxParameterLength: 50, // Truncate to limit exposure
);
```

Query text is automatically truncated to `maxQueryLength` (default: 1000 characters) to prevent excessively large spans.

## Metrics

When `collectMetrics` is enabled, two histograms are recorded:

### `db.client.operation.duration`

Duration of database operations in seconds.

- **Unit**: `s` (seconds)
- **Attributes**: `db.system.name`, `db.namespace`, `db.operation.name`, `server.address`

### `db.client.response.returned_rows`

Number of rows returned by database operations.

- **Unit**: `{row}`
- **Attributes**: `db.system.name`, `db.namespace`, `db.operation.name`

## Examples

### Complete Transaction Tracing

```php
<?php

use Flow\PostgreSql\Client\Telemetry\TransactionSpanMode;

use function Flow\PostgreSql\DSL\{
    pgsql_client,
    pgsql_connection,
    traceable_postgresql_client,
    postgresql_telemetry_config,
};
use function Flow\Telemetry\DSL\{telemetry, resource};

$client = traceable_postgresql_client(
    pgsql_client(pgsql_connection('host=localhost dbname=shop')),
    postgresql_telemetry_config(
        telemetry(resource(['service.name' => 'order-service'])),
        new SystemClock(),
    ),
);

// Transaction with multiple operations
$orderId = $client->transaction(function ($client) use ($userId, $items) {
    // Insert order
    $client->execute(
        'INSERT INTO orders (user_id, status) VALUES ($1, $2)',
        [$userId, 'pending'],
    );

    $orderId = $client->fetchScalarInt('SELECT lastval()');

    // Insert order items
    foreach ($items as $item) {
        $client->execute(
            'INSERT INTO order_items (order_id, product_id, quantity) VALUES ($1, $2, $3)',
            [$orderId, $item['product_id'], $item['quantity']],
        );
    }

    // Update inventory
    $client->execute(
        'UPDATE products SET stock = stock - $1 WHERE id = $2',
        [$item['quantity'], $item['product_id']],
    );

    return $orderId;
});
```

This produces traces like:

```
BEGIN TRANSACTION
├── INSERT orders
├── SELECT
├── INSERT order_items
├── INSERT order_items
├── UPDATE products
└── COMMIT TRANSACTION
```

### Error Handling

Errors are automatically captured with span status and exception details:

```php
<?php

try {
    $client->execute('INSERT INTO users (email) VALUES ($1)', ['duplicate@example.com']);
} catch (QueryException $e) {
    // Span includes:
    // - status: ERROR
    // - error.type: Flow\PostgreSql\Client\Exception\QueryException
    // - db.response.status_code: 23505 (unique_violation)
    // - exception event with stack trace
}
```

### Cursor with Row Counting

```php
<?php

function processAuditEntry(array $row): void { /* your code */ }

$cursor = $client->cursor('SELECT * FROM audit_log WHERE created_at > $1', [$since]);

$processed = 0;
foreach ($cursor->iterate() as $row) {
    processAuditEntry($row);
    $processed++;
}

// Span completed with db.response.returned_rows = $processed
```

### Metrics-Only Mode

Disable tracing but keep metrics for performance monitoring:

```php
<?php

$client = traceable_postgresql_client(
    $baseClient,
    postgresql_telemetry_config(
        $telemetry,
        $clock,
        postgresql_telemetry_options(
            traceQueries: false,
            transactionSpans: TransactionSpanMode::OFF,
            collectMetrics: true,
        ),
    ),
);
```
