# Error Handling

[DOC_LINK:/documentation/components/core/core.md]

In case of any exception in transform/load steps, an ETL process will break, 
to change that behavior, please set custom [ErrorHandler](/src/core/etl/src/Flow/ETL/ErrorHandler.php).

Error Handler defines 3 behavior using 2 methods.

* `ErrorHandler::throw(\Throwable $error, Rows $rows) : bool`
* `ErrorHandler::skipRows(\Throwable $error, Rows $rows) : bool`

If `throw` returns true, ETL will simply throw an error.
If `skipRows' returns true, ETL will stop processing given rows, and it will try to move to the next batch.
If both methods return false, ETL will continue processing Rows using next transformers/loaders.

There are 3 build-in ErrorHandlers (look for more in adapters):

* [ignore error](/src/core/etl/src/Flow/ETL/ErrorHandler/IgnoreError.php)
* [skip rows](/src/core/etl/src/Flow/ETL/ErrorHandler/SkipRows.php)
* [throw error](/src/core/etl/src/Flow/ETL/ErrorHandler/ThrowError.php)

Error Handling can be set directly at ETL:

```php
<?php 

data_frame()
    ->read(from_csv(...))
    ->onError(ignore_error_handler())
    ->write(to_json(...))
    ->run();
```

## Tolerating invalid values per function

Scalar functions throw `InvalidArgumentException` when they receive a value they cannot
process, for example `ref('text')->upper()` over a null column. To tolerate the failure for
one function instead of the whole pipeline, wrap it in `optional()` - it evaluates the
wrapped function and returns null when it throws:

```php
<?php

use function Flow\ETL\DSL\{data_frame, from_array, optional, ref, to_stream};

data_frame()
    ->read(from_array([
        ['text' => 'hello'],
        ['text' => null],
    ]))
    ->withEntry('upper', optional(ref('text')->upper()))
    ->write(to_stream(__DIR__ . '/output.csv', truncate: false))
    ->run();

// Rows with a null 'text' get a null 'upper' column instead of stopping the pipeline.
```

## Row-level Error Handling

For fine-grained error handling during row processing operations:

```php
<?php

use Flow\ETL\Exception\InvalidArgumentException;

function validateAndProcess(Row $row): void { /* your code */ }
function logInvalidRow(Row $row, string $message): void { /* your code */ }
function logGeneralError(Row $row, Throwable $error): void { /* your code */ }

$successCount = 0;
$errorCount = 0;

data_frame()
    ->read($unreliableDataExtractor)
    ->forEach(function(Row $row) use (&$successCount, &$errorCount) {
        try {
            validateAndProcess($row);
            $successCount++;
        } catch (InvalidArgumentException $e) {
            logInvalidRow($row, $e->getMessage());
            $errorCount++;
        } catch (Exception $e) {
            logGeneralError($row, $e);
            $errorCount++;
        }
    });

echo "Success: {$successCount}, Errors: {$errorCount}";
```

> **Best Practice**: When processing unreliable data sources, implement row-level error handling to prevent entire pipeline failures and provide detailed error reporting.
