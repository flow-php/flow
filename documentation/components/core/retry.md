# Retry Mechanisms

[DOC_LINK:/documentation/components/core/core]

- [API Reference](/documentation/api/core)

[TOC]

The Flow ETL framework provides robust retry mechanisms to handle transient failures during data loading operations.
This is essential for building resilient data pipelines that can recover from temporary network issues, database
connection problems, or resource availability conflicts.

## Overview

The retry system focuses on **loader operations** - the final step where processed data is written to its destination.
When a loader encounters a temporary failure, the retry mechanism can automatically reattempt the operation according to
configurable strategies.

## Key Components

### RetryLoader

The `RetryLoader` is a decorator that wraps any existing loader with retry capabilities. It implements `Loader` and
`Loader\Closure`, and forwards `closure()` to the wrapped loader, so file loaders finalize and publish their
destination as they normally would.

Only `load()` is retried. A failure while closing is not retried, because closing publishes the destination and cannot
be resumed from a partial state.

```php
<?php

use function Flow\ETL\DSL\{
    data_frame,
    from_array,
    write_with_retries,
    retry_any_throwable,
    delay_fixed,
    duration_milliseconds
};

$dataFrame = data_frame()
    ->read(from_array([
        ['id' => 1, 'name' => 'John'],
        ['id' => 2, 'name' => 'Jane']
    ]))
    ->write(write_with_retries(
        to_some_service(...),
        retry_any_throwable(3),           // Retry up to 3 times
        delay_fixed(duration_milliseconds(500)) // Wait 500ms between retries
    ))
    ->run();
```

## Retry Strategies

Retry strategies determine **when** to retry an operation based on the type of exception thrown.

### AnyThrowable Strategy

Retries on any thrown exception up to the specified limit:

```php
use function Flow\ETL\DSL\retry_any_throwable;

$strategy = retry_any_throwable(5); // Retry up to 5 times on any exception
```

### Specific Exception Types Strategy

Retries only for specified exception types, allowing you to be selective about which failures should trigger retries:

```php
use function Flow\ETL\DSL\retry_on_exception_types;

$strategy = retry_on_exception_types([
    \PDOException::class,           // Database connection issues
    \RuntimeException::class,       // Runtime problems
    ConnectException::class,        // Network connectivity issues
], 3);
```

This is useful when you want to retry transient failures but immediately fail on logic errors or data validation issues.

### Any Throwable Except Strategy

Retries on any thrown exception except the listed types, which fail immediately:

```php
use function Flow\ETL\DSL\retry_any_throwable_except;

$strategy = retry_any_throwable_except([
    \Flow\ETL\Exception\InvalidLogicException::class,
], 3);
```

This is the default strategy for both `new RetryLoader($loader)` and `write_with_retries($loader)`:
`AnyThrowableExcept([InvalidLogicException::class], 3)` - every throwable is retried up to 3 times except
`InvalidLogicException`, which fails after a single attempt with no delay.

## Delay Factories

Delay factories determine **how long** to wait between retry attempts. Different strategies help avoid overwhelming
failing services while providing appropriate backoff behavior.

### Fixed Delay

Wait a consistent amount of time between each retry:

```php
use function Flow\ETL\DSL\{delay_fixed, duration_milliseconds, duration_seconds};

$delay = delay_fixed(duration_milliseconds(200)); // Wait 200ms between retries
$delay = delay_fixed(duration_seconds(1));        // Wait 1 second between retries
```

### Linear Backoff

Increase the delay by a fixed increment on each retry:

```php
use function Flow\ETL\DSL\delay_linear;

// Start with 100ms, add 50ms each retry: 100ms, 150ms, 200ms, 250ms...
$delay = delay_linear(
    duration_milliseconds(100),  // Initial delay
    duration_milliseconds(50)    // Increment per retry
);
```

### Exponential Backoff

Double (or multiply by a factor) the delay on each retry:

```php
use function Flow\ETL\DSL\delay_exponential;

// Start with 100ms, double each retry: 100ms, 200ms, 400ms, 800ms...
$delay = delay_exponential(
    duration_milliseconds(100),  // Base delay
    2,                          // Multiplier
    duration_seconds(5)         // Maximum delay cap
);
```

### Jitter

Add randomness to any delay strategy to prevent "thundering herd" problems when multiple processes retry simultaneously:

```php
use function Flow\ETL\DSL\delay_jitter;

// Add ±20% random variation to a fixed delay
$delay = delay_jitter(
    delay_fixed(duration_milliseconds(500)),
    0.2  // 20% jitter factor (0.0 to 1.0)
);
```

## Idempotent vs Non-Idempotent Operations

Understanding the difference between idempotent and non-idempotent operations is crucial for designing reliable retry
mechanisms.

### Idempotent Operations (Recommended)

Idempotent operations can be safely repeated without causing unintended side effects. The same operation executed
multiple times produces the same result.

**Examples of idempotent loader operations:**

- Database `UPSERT` (INSERT ON CONFLICT UPDATE)
- HTTP PUT requests
- Database UPDATE with specific WHERE clauses

```php
// Idempotent: Safe to retry
$loader = new DatabaseUpsertLoader($connection, 'users');
$retryLoader = write_with_retries($loader, retry_any_throwable(5));
```

### Non-Idempotent Operations (Use with Caution)

Non-idempotent operations may produce different results or unintended side effects when repeated.

**Examples of non-idempotent operations:**

- Database `INSERT` without conflict resolution
- File appends
- Counter increments

### File Loaders

Do not wrap file loaders such as `to_csv()`, `to_json()` or `to_parquet()` in `write_with_retries()`, regardless of the
save mode. A file loader appends each batch to a stream that stays open for the whole run, and a retry has nothing to
roll back, so a batch that fails after part of it reached the stream is written twice:

```php
data_frame()
    ->read(from_array([['id' => 1], ['id' => 2], ['id' => 3], ['id' => 4]]))
    ->batchSize(2)
    ->saveMode(overwrite())
    // a transient failure in the first batch leaves ids 1 and 2 in the file twice
    ->write(write_with_retries(to_csv($path)))
    ->run();
```

`overwrite()` replaces the destination once per run, not once per batch, so it does not undo a duplicated batch.

### Transformation Loaders

Wrapping `to_transformation(...)` or a `to_branch(...)` armed with `withTransformation(...)` in
`write_with_retries()` or `RetryLoader` throws `InvalidLogicException` at the first `load()`, at any nesting
depth. These loaders hold state across `load()` calls and cannot replay a failed batch. Retry the destination
instead:

```php
to_transformation($transformation, write_with_retries($loader));
to_branch($condition, write_with_retries($loader))->withTransformation($transformation);
```

When the destination is a transactional wrapper (`to_dbal_transaction()`, `to_pgsql_transaction()`), put the
wrapper inside `write_with_retries()`, not the other way around - a retry inside an aborted database
transaction can never succeed; wrapping the transaction gives every attempt a fresh one:

```php
to_transformation($transformation, write_with_retries(to_pgsql_transaction($client, $loader)));
```

## Advanced Configuration

### Custom Sleep Implementation

For testing or special requirements, you can provide a custom sleep implementation:

```php
use Flow\ETL\Time\FakeSleep;

$sleep = new FakeSleep(); // For testing - doesn't actually sleep
$retryLoader = write_with_retries(
    $loader,
    retry_any_throwable(3),
    delay_fixed(duration_milliseconds(100)),
    $sleep
);
```

### Complete Configuration Example

```php
<?php

use function Flow\ETL\DSL\{
    data_frame,
    from_array,
    write_with_retries,
    retry_on_exception_types,
    delay_jitter,
    delay_exponential,
    duration_milliseconds,
    duration_seconds
};

$result = data_frame()
    ->read(from_array($largeDataset))
    ->write(write_with_retries(
        to_database($connection, 'transactions'),

        // Only retry on specific transient failures
        retry_on_exception_types([
            \PDOException::class,
            \RuntimeException::class
        ], 5),

        // Exponential backoff with jitter
        delay_jitter(
            delay_exponential(
                duration_milliseconds(200),  // Start with 200ms
                2,                          // Double each time
                duration_seconds(10)        // Cap at 10 seconds
            ),
            0.3 // 30% jitter to prevent thundering herd
        )
    ))
    ->run();
```

## Error Information

When all retries are exhausted, a `FailedRetryException` is thrown containing detailed information about all attempts:

```php
use Flow\ETL\Exception\FailedRetryException;

try {
    $dataFrame->write($retryLoader)->run();
} catch (FailedRetryException $e) {
    echo "Failed after {$e->getRetriesRecord()->count()} attempts\n";

    // Access individual retry attempts
    foreach ($e->getRetriesRecord()->all() as $retry) {
        echo "Attempt {$retry->attempt()}: {$retry->exception()->getMessage()}\n";
        echo "Timestamp: {$retry->timestamp()->format('Y-m-d H:i:s')}\n";
    }
}
```