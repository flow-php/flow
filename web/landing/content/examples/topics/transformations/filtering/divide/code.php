<?php

declare(strict_types=1);

use function Flow\ETL\DSL\{data_frame, from_array, int_schema, lit, ref, schema, to_output};

require __DIR__ . '/vendor/autoload.php';

data_frame()
    ->read(from_array(
        [['a' => 100, 'b' => 100], ['a' => 100, 'b' => 200]],
        schema(int_schema('a'), int_schema('b')),
    ))
    ->filter(ref('b')->divide(lit(2))->equals(ref('a')))
    ->withEntry('new_b', ref('b')->multiply(lit(2))->multiply(lit(5)))
    ->write(to_output(truncate: false))
    ->run();
