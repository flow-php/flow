<?php

declare(strict_types=1);

use function Flow\ETL\DSL\{data_frame, from_rows, int_entry, json_entry, ref, row, rows, to_stream};

require __DIR__ . '/vendor/autoload.php';

data_frame()
    ->read(from_rows(rows(
        row(int_entry('id', 1), json_entry('array', ['a' => 1, 'b' => 2, 'c' => 3])),
        row(int_entry('id', 2), json_entry('array', ['d' => 4, 'e' => 5, 'f' => 6])),
    )))
    ->withEntry('unpacked', ref('array')->unpack())
    ->write(to_stream(__DIR__ . '/output.txt', truncate: false))
    ->run();
