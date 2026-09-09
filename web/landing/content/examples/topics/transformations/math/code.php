<?php

declare(strict_types=1);

use function Flow\ETL\DSL\{data_frame, from_array, int_schema, ref, schema, to_output};

require __DIR__ . '/vendor/autoload.php';

data_frame()
    ->read(from_array(
        [['a' => 100, 'b' => 200]],
        schema(int_schema('a'), int_schema('b')),
    ))
    ->withEntry('d', ref('b')->minus(ref('a')))
    ->write(to_output(truncate: false))
    ->run();
