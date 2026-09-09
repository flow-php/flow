<?php

declare(strict_types=1);

use function Flow\ETL\DSL\{data_frame, from_array, int_schema, json_schema, ref, schema, to_output};

require __DIR__ . '/vendor/autoload.php';

data_frame()
    ->read(from_array(
        [['id' => 1, 'array' => ['a' => 1, 'b' => 2, 'c' => 3]]],
        schema(int_schema('id'), json_schema('array')),
    ))
    ->withEntry('array_size', ref('array')->size())
    ->write(to_output(truncate: false))
    ->run();
