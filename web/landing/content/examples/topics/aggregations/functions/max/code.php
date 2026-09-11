<?php

declare(strict_types=1);

use function Flow\ETL\DSL\{max, data_frame, from_array, int_schema, ref, schema, to_output};

require __DIR__ . '/vendor/autoload.php';

data_frame()
    ->read(from_array(
        [['a' => 100], ['a' => 100], ['a' => 200], ['a' => 400], ['a' => 400]],
        schema(int_schema('a')),
    ))
    ->aggregate([max(ref('a'))])
    ->write(to_output(truncate: false))
    ->run();
