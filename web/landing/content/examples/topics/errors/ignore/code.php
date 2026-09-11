<?php

declare(strict_types=1);

use function Flow\ETL\DSL\{data_frame, from_array, ignore_error_handler, int_schema, lit, ref, schema, to_output};

require __DIR__ . '/vendor/autoload.php';

// ignore_error_handler() keeps the row that threw, without the column that failed - so the
// batches no longer share a schema and cannot be collected into one table
data_frame()
    ->read(from_array([
        ['id' => 1, 'divisor' => 2],
        ['id' => 2, 'divisor' => 0],
        ['id' => 3, 'divisor' => 5],
    ], schema(int_schema('id'), int_schema('divisor'))))
    ->onError(ignore_error_handler())
    ->withEntry('result', lit(100)->divide(ref('divisor')))
    ->write(to_output(truncate: false))
    ->run();
