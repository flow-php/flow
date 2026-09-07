<?php

declare(strict_types=1);

use function Flow\ETL\Adapter\CSV\from_csv;
use function Flow\ETL\DSL\{data_frame, schema_from_json, schema_to_json, to_output};
use Flow\ETL\Loader\StreamLoader\Output;
use function Flow\Filesystem\DSL\fstab;
use function Flow\Filesystem\DSL\path;

require __DIR__ . '/vendor/autoload.php';

$fs = fstab()->for('file');

if ($fs->status(path(__DIR__ . '/output/schema.json')) === null) {
    $schema = data_frame()
        ->read(from_csv(__DIR__ . '/data/orders.csv'))
        ->limit(100) // Limiting the number of rows to read will speed up the process but might bring less accurate results
        ->schema();

    $fs->writeTo(path(__DIR__ . '/output/schema.json'))
        ->append(schema_to_json($schema))
        ->close();
} else {
    $schema = schema_from_json($fs->readFrom(path(__DIR__ . '/output/schema.json'))->content());
}

// Reading schemaless data formats with predefined schema can significantly improve performance
data_frame()
    ->read(from_csv(__DIR__ . '/data/orders.csv', schema: $schema))
    ->collect()
    ->write(to_output(truncate: false, output: Output::rows_and_schema))
    ->run();
