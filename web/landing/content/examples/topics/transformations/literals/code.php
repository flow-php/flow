<?php

declare(strict_types=1);

use function Flow\ETL\DSL\{data_frame, from_array, lit, schema, str_schema, to_output};

require __DIR__ . '/vendor/autoload.php';

data_frame()
    ->read(from_array(
        [['name' => 'Norbert']],
        schema(str_schema('name')),
    ))
    ->withEntry('number', lit(1))
    ->write(to_output(truncate: false))
    ->run();
