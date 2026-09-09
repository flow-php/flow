<?php

declare(strict_types=1);

use function Flow\ETL\Adapter\CSV\from_csv;
use function Flow\ETL\DSL\{data_frame, float_schema, schema, schema_to_ascii, uuid_schema};

require __DIR__ . '/vendor/autoload.php';

$path = __DIR__ . '/data/orders.csv';

// no schema given: Flow reads the file's own description of itself
echo "inferred:\n" . schema_to_ascii(
    data_frame()->read(from_csv($path))->select('order_id', 'discount')->schema(),
);

// declared up front: the reader is told, and never has to look
$declared = schema(uuid_schema('order_id', nullable: true), float_schema('discount', nullable: true));

echo "\ndeclared:\n" . schema_to_ascii(
    data_frame()->read(from_csv($path, schema: $declared))->select('order_id', 'discount')->schema(),
);
