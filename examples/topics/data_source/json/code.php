<?php

declare(strict_types=1);

use function Flow\ETL\Adapter\JSON\from_json;
use function Flow\ETL\DSL\{bool_schema, data_frame, int_schema, schema, str_schema, to_stream};

require __DIR__ . '/../../../autoload.php';

$schema = schema(
    int_schema('id'),
    str_schema('name'),
    str_schema('email'),
    bool_schema('active'),
);

data_frame()
    ->read(
        from_json(__DIR__ . '/input/dataset.json')
            ->withSchema($schema)
    )
    ->collect()
    ->write(to_stream(__DIR__ . '/output.txt', truncate: false))
    ->run();
