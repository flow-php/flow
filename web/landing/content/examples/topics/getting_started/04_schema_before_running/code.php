<?php

declare(strict_types=1);

use function Flow\ETL\DSL\{data_frame, from_array, int_schema, lit, ref, schema, schema_to_ascii, str_schema};

require __DIR__ . '/vendor/autoload.php';

$frame = data_frame()
    ->read(from_array([
        ['id' => 1, 'name' => 'Norbert'],
        ['id' => 2, 'name' => 'Jane'],
    ], schema(int_schema('id'), str_schema('name'))))
    ->filter(ref('id')->greaterThan(lit(1)))
    ->withEntry('greeting', ref('name')->concat(lit('!')));

// answered from the plan: not one row has been read
echo schema_to_ascii($frame->schema());
