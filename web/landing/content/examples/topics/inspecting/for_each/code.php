<?php

declare(strict_types=1);

use Flow\ETL\Rows;

use function Flow\ETL\DSL\{data_frame, from_array, int_schema, schema, str_schema};

require __DIR__ . '/vendor/autoload.php';

// forEach() runs the pipeline and hands every batch to the callback, column by column
data_frame()
    ->read(from_array([
        ['id' => 1, 'name' => 'Norbert'],
        ['id' => 2, 'name' => 'Jane'],
        ['id' => 3, 'name' => 'John'],
        ['id' => 4, 'name' => 'Ann'],
        ['id' => 5, 'name' => 'Bob'],
    ], schema(int_schema('id'), str_schema('name'))))
    ->batchSize(2)
    ->forEach(static function (Rows $rows): void {
        echo $rows->count() . ' rows, names: ' . implode(', ', $rows->column('name')->values()) . "\n";
    });
