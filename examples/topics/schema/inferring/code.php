<?php

declare(strict_types=1);

use function Flow\ETL\Adapter\CSV\from_csv;
use function Flow\ETL\DSL\{data_frame, schema_from_json, schema_to_json, to_stream};
use Flow\ETL\Loader\StreamLoader\Output;

require __DIR__ . '/../../../autoload.php';

if (!\file_exists(__DIR__ . '/output/schema.json')) {
    $schema = data_frame()
        ->read(from_csv(__DIR__ . '/input/dataset.csv'))
        ->limit(100) // Limiting the number of rows to read will speed up the process but might bring less accurate results
        ->autoCast()
        ->schema();

    \file_put_contents(__DIR__ . '/output/schema.json', schema_to_json($schema));
} else {
    /* @phpstan-ignore-next-line */
    $schema = schema_from_json(\file_get_contents(__DIR__ . '/output/schema.json'));
}

// Reading schemaless data formats with predefined schema can significantly improve performance
data_frame()
    ->read(from_csv(__DIR__ . '/input/dataset.csv', schema: $schema))
    ->collect()
    ->write(to_stream(__DIR__ . '/output.txt', truncate: false, output: Output::rows_and_schema))
    ->run();
