<?php

declare(strict_types=1);

use function Flow\ETL\DSL\{data_frame, from_array, int_schema, lit, ref, schema, throw_error_handler, to_output};

require __DIR__ . '/vendor/autoload.php';

// throw_error_handler() is the default: the pipeline stops and the exception reaches the caller
try {
    data_frame()
        ->read(from_array([
            ['id' => 1, 'divisor' => 2],
            ['id' => 2, 'divisor' => 0],
        ], schema(int_schema('id'), int_schema('divisor'))))
        ->onError(throw_error_handler())
        ->withEntry('result', lit(100)->divide(ref('divisor')))
        ->collect()
        ->write(to_output(truncate: false))
        ->run();
} catch (Throwable $e) {
    echo get_class($e) . ': ' . $e->getMessage() . "\n";
}
