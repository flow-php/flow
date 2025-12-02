# Execution Mode

[TOC]

Flow DataFrame supports two execution modes that control how scalar functions handle validation errors:

- **LENIENT** (default): Validation errors are ignored, processing continues with fallback values (null, false, [], '')
- **STRICT**: Validation errors throw `InvalidArgumentException` immediately, stopping execution

## Changing Execution Mode

```php
use Flow\ETL\Function\ExecutionMode;

(data_frame())
    ->read(from_array([
        ['name' => 'John', 'value' => '10'],
        ['name' => null, 'value' => '20'],
    ]))
    ->mode(ExecutionMode::STRICT)
    ->withEntry('upper', ref('name')->upper())
    ->write(to_stream(__DIR__ . '/output.csv', truncate: false))
    ->run();
```

## Behavior Comparison

### LENIENT Mode (Default)

When invalid data is encountered, functions return fallback values and processing continues:

```php
(data_frame())
    ->read(from_array([
        ['text' => 'hello'],
        ['text' => null],      // Invalid data
    ]))
    ->withEntry('upper', ref('text')->upper())
    ->fetch();

// Result:
// [
//   ['text' => 'hello', 'upper' => 'HELLO'],
//   ['text' => null, 'upper' => null],     // Returns null, continues processing
// ]
```

### STRICT Mode

When invalid data is encountered, an exception is thrown immediately:

```php
(data_frame())
    ->mode(ExecutionMode::STRICT)
    ->read(from_array([
        ['text' => 'hello'],
        ['text' => null],      // Invalid data
    ]))
    ->withEntry('upper', ref('text')->upper())
    ->fetch();

// Throws: InvalidArgumentException: Upper function requires non-null string
```