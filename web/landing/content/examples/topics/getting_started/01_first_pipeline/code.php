<?php

declare(strict_types=1);

use function Flow\ETL\DSL\{data_frame, from_array, int_schema, schema, str_schema, to_output};

require __DIR__ . '/vendor/autoload.php';

data_frame()
    ->read(from_array([
        ['id' => 1, 'name' => 'Norbert'],
        ['id' => 2, 'name' => 'Jane'],
        ['id' => 3, 'name' => 'John'],
    ], schema(int_schema('id'), str_schema('name'))))
    ->collect()
    ->write(to_output(truncate: false))
    ->run();
