# Error Handling

[DOC_LINK:/documentation/components/core/core.md]

By default an exception thrown while reading, transforming or loading stops the pipeline. An
[ErrorHandler](/src/core/etl/src/Flow/ETL/ErrorHandler.php) decides per failure instead:

| Method | Error carries | Action other than `propagate` (rethrow) |
|---|---|---|
| `onExtraction(ExtractionError): ExtractionAction` | `cause`, `extractor` | `endSource` - stop reading; rows read so far still flow and the run completes |
| `onTransformation(TransformationError): TransformationAction` | `cause`, `transformer`, `rows` | `skipBatch` - drop the batch, continue with the next one |
| `onLoading(LoadingError): LoadingAction` | `cause`, `loader`, `rows` | `skipLoader` - the failing loader misses this batch, the remaining steps still run |

Built-in handlers:

| Handler | Extraction | Transformation | Loading |
|---|---|---|---|
| `throw_error_handler()` (default) | `propagate` | `propagate` | `propagate` |
| `ignore_error_handler()` | `endSource` | `skipBatch` | `skipLoader` |
| `skip_rows_handler()` | `endSource` | `skipBatch` | `propagate` |

```php
<?php

use Flow\ETL\ErrorHandler;
use Flow\ETL\ErrorHandler\{ExtractionAction, ExtractionError, LoadingAction, LoadingError, TransformationAction, TransformationError};

use function Flow\ETL\DSL\{data_frame, from_array, ignore_error_handler, to_stream};

final class SkipBrokenBatches implements ErrorHandler
{
    public function onExtraction(ExtractionError $error): ExtractionAction
    {
        return ExtractionAction::propagate;
    }

    public function onTransformation(TransformationError $error): TransformationAction
    {
        return TransformationAction::skipBatch;
    }

    public function onLoading(LoadingError $error): LoadingAction
    {
        return LoadingAction::propagate;
    }
}

data_frame()
    ->read(from_array([['id' => 1], ['id' => 2]]))
    ->onError(new SkipBrokenBatches())
    ->write(to_stream(__DIR__ . '/output.txt'))
    ->run();

data_frame()
    ->read(from_array([['id' => 1], ['id' => 2]]))
    ->onError(ignore_error_handler())
    ->write(to_stream(__DIR__ . '/output.txt'))
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
