# Error Handling

- [⬅️️ Back](core.md)

In case of any exception in transform/load steps, an ETL process will break, 
to change that behavior, please set custom [ErrorHandler](../../../src/core/etl/src/Flow/ETL/ErrorHandler.php).

Error Handler defines 3 behavior using 2 methods.

* `ErrorHandler::throw(\Throwable $error, Rows $rows) : bool`
* `ErrorHandler::skipRows(\Throwable $error, Rows $rows) : bool`

If `throw` returns true, ETL will simply throw an error.
If `skipRows' returns true, ETL will stop processing given rows, and it will try to move to the next batch.
If both methods return false, ETL will continue processing Rows using next transformers/loaders.

There are 3 build-in ErrorHandlers (look for more in adapters):

* [ignore error](../../../src/core/etl/src/Flow/ETL/ErrorHandler/IgnoreError.php)
* [skip rows](../../../src/core/etl/src/Flow/ETL/ErrorHandler/SkipRows.php)
* [throw error](../../../src/core/etl/src/Flow/ETL/ErrorHandler/ThrowError.php)

Error Handling can be set directly at ETL:

```php
<?php 

data_frame()
    ->read(from_csv(...))
    ->onError(ignore_error_handler())
    ->write(to_json(...))
    ->run();
```
