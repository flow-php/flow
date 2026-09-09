<?php

declare(strict_types=1);

use function Flow\ETL\DSL\{data_frame, from_array, int_schema, schema, str_schema};

require __DIR__ . '/vendor/autoload.php';

// fetch() brings Rows back into PHP, so a value can be read out of the pipeline
$rows = data_frame()
    ->read(from_array([
        ['id' => 1, 'name' => 'Norbert'],
        ['id' => 2, 'name' => 'Jane'],
        ['id' => 3, 'name' => 'John'],
    ], schema(int_schema('id'), str_schema('name'))))
    ->fetch(2);

echo 'fetched ' . $rows->count() . " rows\n";
echo 'first name: ' . $rows->first()->get('name') . "\n";

foreach ($rows as $row) {
    echo "  {$row->get('id')}: {$row->get('name')}\n";
}
