# Error Handling

[DOC_LINK:/documentation/components/core/core.md]

By default, an exception during the read, the transformation or the load stops the pipeline. An
[ErrorHandler](/src/core/etl/src/Flow/ETL/Runtime/ErrorHandler.php) can decide for each failure instead:

| Method                                                        | Error carries                  | Action other than `propagate` (rethrow)                                            |
|---------------------------------------------------------------|--------------------------------|------------------------------------------------------------------------------------|
| `onExtraction(ExtractionError): ExtractionAction`             | `cause`, `extractor`           | `endSource`: stop the read. Rows read so far still flow, and the run completes     |
| `onTransformation(TransformationError): TransformationAction` | `cause`, `transformer`, `rows` | `skipBatch`: drop the batch, continue with the next one                            |
| `onLoading(LoadingError): LoadingAction`                      | `cause`, `loader`, `rows`      | `skipLoader`: the failing loader misses this batch, the remaining steps still run  |

A corrupted record never reaches the `ErrorHandler`. For more information, read [Corrupted Records](/documentation/components/core/corrupted-records.md).

Built-in handlers:

| Handler                           | Extraction  | Transformation | Loading      |
|-----------------------------------|-------------|----------------|--------------|
| `throw_error_handler()` (default) | `propagate` | `propagate`    | `propagate`  |
| `ignore_error_handler()`          | `endSource` | `skipBatch`    | `skipLoader` |
| `skip_rows_handler()`             | `endSource` | `skipBatch`    | `propagate`  |

```php
<?php

use Flow\ETL\Foundation\Describe\{Description, Rebuild};
use Flow\ETL\Runtime\ErrorHandler;
use Flow\ETL\Runtime\ErrorHandler\{ExtractionAction, ExtractionError, LoadingAction, LoadingError, TransformationAction, TransformationError};

use function Flow\ETL\DSL\{data_frame, from_array, ignore_error_handler, to_stream};

final class SkipBrokenBatches implements ErrorHandler
{
    public static function fromDescription(array $data, Rebuild $rebuild): static
    {
        return new self();
    }

    public function describe(): ?Description
    {
        return new Description(self::class, []);
    }

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

A scalar function that gets a value that it cannot process throws `EvaluationException` (an
`InvalidArgumentException`) with the failing row of the batch. An example is `ref('text')->upper()` over a null value:
`ToUpper function requires non-null value (row 1)`. The original exception is `getPrevious()`. To accept the failure
for one function, and not for the full pipeline, wrap the function in `optional()`. It evaluates the wrapped function.
For a function that throws, it returns null:

```php
<?php

use function Flow\ETL\DSL\{data_frame, from_array, optional, ref, to_stream};

data_frame()
    ->read(from_array([
        ['text' => 'hello'],
        ['text' => null],
    ]))
    ->withColumn('upper', optional(ref('text')->upper()))
    ->write(to_stream(__DIR__ . '/output.csv', truncate: false))
    ->run();

// Rows with a null 'text' get a null 'upper' column instead of stopping the pipeline.
```

## Row-level Error Handling

For detailed error handling during row processing operations:

```php
<?php

use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Values\Rows;

use function Flow\ETL\DSL\{data_frame, from_array};

function validateAndProcess(array $row): void { /* your code */ }
function logInvalidRow(array $row, string $message): void { /* your code */ }
function logGeneralError(array $row, Throwable $error): void { /* your code */ }

$successCount = 0;
$errorCount = 0;

data_frame()
    ->read(from_array([['id' => 1], ['id' => 2]]))
    ->forEach(function(Rows $rows) use (&$successCount, &$errorCount) {
        foreach ($rows->toArray() as $row) {
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
        }
    });

echo "Success: {$successCount}, Errors: {$errorCount}";
```

> Best Practice: For unreliable data sources, implement row-level error handling. It prevents failures of the full pipeline and gives detailed error reports.
