<?php

declare(strict_types=1);

use function Flow\ETL\Adapter\CSV\from_csv;
use function Flow\ETL\DSL\data_frame;

require __DIR__ . '/vendor/autoload.php';

// a padded header, a blank header cell, an empty cell and a short line
file_put_contents(__DIR__ . '/input.csv', " id ,,name\n1,a,Norbert\n2,,Jane\n3\n");

// json_encode() keeps an empty string and a null apart, a table would not
foreach (data_frame()->read(from_csv(__DIR__ . '/input.csv', empty_to_null: false))->fetch()->toArray() as $row) {
    echo json_encode($row) . "\n";
}
