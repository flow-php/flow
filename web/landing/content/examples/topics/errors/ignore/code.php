<?php

declare(strict_types=1);

use function Flow\ETL\DSL\{data_frame, from_array, ignore_error_handler, int_schema, lit, ref, schema, to_output};

require __DIR__ . '/vendor/autoload.php';

// ignore_error_handler() drops the batch a transformation failed in and keeps going:
// with batchSize(1), only id 2 is lost
data_frame()
    ->read(from_array([
        ['id' => 1, 'divisor' => 2],
        ['id' => 2, 'divisor' => 0],
        ['id' => 3, 'divisor' => 5],
    ], schema(int_schema('id'), int_schema('divisor'))))
    ->batchSize(1)
    ->onError(ignore_error_handler())
    ->withEntry('result', lit(100)->divide(ref('divisor')))
    ->collect()
    ->write(to_output(truncate: false))
    ->run();
