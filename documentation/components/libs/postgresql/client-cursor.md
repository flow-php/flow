# Cursors

[DOC_LINK:/documentation/components/libs/postgresql.md]

[TOC]

Cursors provide memory-efficient iteration over large result sets. Instead of loading all rows into memory, cursors
fetch rows one at a time using PostgreSQL's server-side cursor mechanism.

## Creating a Cursor

```php
<?php

use function Flow\PostgreSql\DSL\{pgsql_client, pgsql_connection};

$client = pgsql_client(pgsql_connection('host=localhost dbname=mydb'));

// Create cursor for a query
$cursor = $client->cursor('SELECT * FROM large_table WHERE active = $1', [true]);
```

## Iterating with iterate()

The `iterate()` method returns a generator that yields rows one at a time:

```php
<?php

function processEvent(array $row): void { /* your code */ }

$cursor = $client->cursor('SELECT * FROM events ORDER BY created_at');

foreach ($cursor->iterate() as $row) {
    processEvent($row);
}
```

You can also use the cursor directly as an iterator (implements `IteratorAggregate`):

```php
<?php

function processEvent(array $row): void { /* your code */ }

foreach ($cursor as $row) {
    processEvent($row);
}
```

## Object Mapping with map()

Map rows to objects while maintaining memory efficiency:

```php
<?php

readonly class Event
{
    public function __construct(
        public int $id,
        public string $type,
        public array $payload,
        public \DateTimeImmutable $createdAt,
    ) {}
}

$cursor = $client->cursor('SELECT id, type, payload, created_at AS createdAt FROM events');

foreach ($cursor->map(Event::class) as $event) {
    // Each iteration creates one object
    echo "{$event->type}: {$event->id}\n";
}
```

### Custom Mapper

Override the default mapper for specific mapping logic:

```php
<?php

function processEvent(array $event): void { /* your code */ }

use Flow\PostgreSql\Client\RowMapper;

readonly class EventMapper implements RowMapper
{
    public function map(string $class, array $row): object
    {
        return new Event(
            id: (int) $row['id'],
            type: $row['type'],
            payload: json_decode($row['payload'], true),
            createdAt: new \DateTimeImmutable($row['created_at']),
        );
    }
}

foreach ($cursor->map(Event::class, new EventMapper()) as $event) {
    processEvent($event);
}
```

## Row-by-Row with next()

Fetch rows manually with `next()`:

```php
<?php

$cursor = $client->cursor('SELECT * FROM users');

while (($row = $cursor->next()) !== null) {
    echo $row['name'] . "\n";

    // Can break early without fetching remaining rows
    if ($row['id'] > 1000) {
        break;
    }
}
```

## Counting Rows

Get the total count (available after cursor creation):

```php
<?php

$cursor = $client->cursor('SELECT * FROM users WHERE active = $1', [true]);

echo "Total rows: " . $cursor->count() . "\n";

foreach ($cursor->iterate() as $row) {
    // Process rows
}
```

## Memory Efficiency

Cursors are essential for processing large datasets:

```php
<?php

function processAuditEntry(array $row): void { /* your code */ }

// BAD: Loads all 1 million rows into memory
$rows = $client->fetchAll('SELECT * FROM audit_log');
foreach ($rows as $row) {
    processAuditEntry($row);
}

// GOOD: Processes one row at a time
$cursor = $client->cursor('SELECT * FROM audit_log');
foreach ($cursor->iterate() as $row) {
    processAuditEntry($row);
}
```

## Resource Cleanup

Cursors automatically clean up their server-side resources when:

1. Iteration completes (all rows consumed)
2. The cursor object is garbage collected
3. You explicitly call `free()`

For early termination, call `free()` to release resources immediately:

```php
<?php

$cursor = $client->cursor('SELECT * FROM large_table');

function foundWhat(array $row): bool { /* your code */ return true; }

foreach ($cursor->iterate() as $row) {
    if (foundWhat($row)) {
        $cursor->free();  // Release server resources now
        break;
    }
}
```

## Batched Processing

Process rows in batches for bulk operations:

```php
<?php

function processBatch(array $batch): void { /* your code */ }

$cursor = $client->cursor('SELECT * FROM products');

$batch = [];
$batchSize = 100;

foreach ($cursor->iterate() as $row) {
    $batch[] = $row;

    if (count($batch) >= $batchSize) {
        processBatch($batch);
        $batch = [];
    }
}

// Process remaining
if (count($batch) > 0) {
    processBatch($batch);
}
```

## Cursor vs fetchAll

| Method       | Memory Usage      | Best For                       |
|--------------|-------------------|--------------------------------|
| `fetchAll()` | Loads all rows    | Small result sets (< 10k rows) |
| `cursor()`   | One row at a time | Large result sets, streaming   |

Use cursors when:

- Processing millions of rows
- Result set size is unpredictable
- Memory constraints are tight
- You can process rows independently
