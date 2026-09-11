<?php

declare(strict_types=1);

use function Flow\ETL\DSL\{data_frame, from_array, int_schema, lit, ref, schema, to_output};

require __DIR__ . '/vendor/autoload.php';

data_frame()
    ->read(from_array(
        [['a' => 4, 'b' => 5], ['a' => 3, 'b' => 6]],
        schema(int_schema('a'), int_schema('b')),
    ))
    ->filter(ref('b')->mod(lit(2))->equals(lit(0)))
    ->write(to_output(truncate: false))
    ->run();
