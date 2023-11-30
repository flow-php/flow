<?php

declare(strict_types=1);

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\bool_entry;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\str_entry;

require __DIR__ . '/../../bootstrap.php';

$rows = rows(
    row(int_entry('id', 1), str_entry('name', 'user_01'), bool_entry('active', true)),
    row(int_entry('id', 2), str_entry('name', 'user_02'), bool_entry('active', false)),
    row(int_entry('id', 3), str_entry('name', 'user_03'), bool_entry('active', true)),
    row(int_entry('id', 3), str_entry('name', 'user_04'), bool_entry('active', false)),
);

$rowsFromArray = array_to_rows([
    ['id' => 1, 'name' => 'user_01', 'active' => true],
    ['id' => 2, 'name' => 'user_02', 'active' => false],
    ['id' => 3, 'name' => 'user_03', 'active' => true],
    ['id' => 4, 'name' => 'user_04', 'active' => false],
]);
