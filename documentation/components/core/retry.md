# Retry Mechanisms

- [⬅️ Back](core.md)
- [📚 API Reference](/documentation/api/core)

The Flow ETL framework provides robust retry mechanisms to handle transient failures during data loading operations.
This is essential for building resilient data pipelines that can recover from temporary network issues, database
connection problems, or resource availability conflicts.

## Overview

The retry system focuses on **loader operations** - the final step where processed data is written to its destination.
When a loader encounters a temporary failure, the retry mechanism can automatically reattempt the operation according to
configurable strategies.

## Key Components

### RetryLoader

The `RetryLoader` is a decorator that wraps any existing loader with retry capabilities. It implements the same `Loader`
interface, making it transparent to use in your data pipelines.

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
- File overwrites
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