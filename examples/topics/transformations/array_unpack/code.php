<?php

declare(strict_types=1);

use function Flow\ETL\DSL\{array_entry, data_frame, from_rows, int_entry, ref, row, rows, to_output, to_stream};

require __DIR__ . '/../../../autoload.php';

data_frame()
    ->read(from_rows(rows(
        row(int_entry('id', 1), array_entry('array', ['a' => 1, 'b' => 2, 'c' => 3])),
        row(int_entry('id', 2), array_entry('array', ['d' => 4, 'e' => 5, 'f' => 6])),
    )))
    ->write(to_output(false))
    ->withEntry('unpacked', ref('array')->unpack())
    ->write(to_stream(__DIR__ . '/output.txt', truncate: false))
    ->run();
