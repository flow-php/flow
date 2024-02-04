<?php

declare(strict_types=1);

use function Flow\ETL\Adapter\CSV\from_csv;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\schema_from_json;
use function Flow\ETL\DSL\schema_to_json;
use function Flow\ETL\DSL\to_output;
use Flow\ETL\Loader\StreamLoader\Output;

require __DIR__ . '/../../../autoload.php';

if (!\file_exists(__DIR__ . '/output/schema.json')) {
    $schema = df()
        ->read(from_csv(__DIR__ . '/input/dataset.csv'))
        ->limit(100) // Limiting the number of rows to read will speed up the process but might bring less accurate results
        ->autoCast()
        ->schema();

    \file_put_contents(__DIR__ . '/output/schema.json', schema_to_json($schema));
} else {
    $schema = schema_from_json(\file_get_contents(__DIR__ . '/output/schema.json'));
}

// Reading schemaless data formats with predefined schema can significantly improve performance
df()
    ->read(from_csv(__DIR__ . '/input/dataset.csv', schema: $schema))
    ->collect()
    ->write(to_output(truncate: false, output: Output::rows_and_schema))
    ->run();
