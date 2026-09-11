<?php

declare(strict_types=1);

use function Flow\ETL\DSL\{data_frame, from_array, int_schema, lit, ref, schema, skip_rows_handler, to_output};

require __DIR__ . '/vendor/autoload.php';

// skip_rows_handler() drops the whole batch the failure landed in, not just the row:
// with batchSize(2), losing id 2 loses id 1 with it
data_frame()
    ->read(from_array([
        ['id' => 1, 'divisor' => 2],
        ['id' => 2, 'divisor' => 0],
        ['id' => 3, 'divisor' => 5],
        ['id' => 4, 'divisor' => 4],
    ], schema(int_schema('id'), int_schema('divisor'))))
    ->batchSize(2)
    ->onError(skip_rows_handler())
    ->withEntry('result', lit(100)->divide(ref('divisor')))
    ->collect()
    ->write(to_output(truncate: false))
    ->run();
