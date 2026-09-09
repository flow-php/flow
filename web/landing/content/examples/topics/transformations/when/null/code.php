<?php

declare(strict_types=1);

use function Flow\ETL\DSL\{data_frame, from_array, int_schema, lit, ref, schema, to_output, when};

require __DIR__ . '/vendor/autoload.php';

data_frame()
    ->read(from_array(
        [
            ['id' => 1, 'value' => 1],
            ['id' => 2, 'value' => 1],
            ['id' => 3, 'value' => null],
            ['id' => 4, 'value' => 1],
            ['id' => 5, 'value' => null],
        ],
        schema(int_schema('id'), int_schema('value', nullable: true)),
    ))
    ->withEntry(
        'value',
        when(ref('value')->isNull(), then: lit(0))
    )
    ->write(to_output(truncate: false))
    ->run();
