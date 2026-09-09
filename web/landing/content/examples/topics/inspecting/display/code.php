<?php

declare(strict_types=1);

use function Flow\ETL\DSL\{data_frame, from_array, int_schema, schema, str_schema};

require __DIR__ . '/vendor/autoload.php';

// display() returns a string; printRows() and write(to_output()) print one
$table = data_frame()
    ->read(from_array([
        ['id' => 1, 'name' => 'Norbert'],
        ['id' => 2, 'name' => 'Jane'],
        ['id' => 3, 'name' => 'John'],
    ], schema(int_schema('id'), str_schema('name'))))
    ->collect()
    ->display($limit = 2);

echo 'the return value is ' . strlen($table) . " characters of text:\n";
echo $table;
