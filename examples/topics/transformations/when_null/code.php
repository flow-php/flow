<?php

declare(strict_types=1);

use function Flow\ETL\DSL\{data_frame, from_rows, int_entry, lit, ref, row, rows, to_stream, when};

require __DIR__ . '/../../../autoload.php';

data_frame()
    ->read(from_rows(rows(
        row(int_entry('id', 1), int_entry('value', 1)),
        row(int_entry('id', 2), int_entry('value', 1)),
        row(int_entry('id', 3), int_entry('value', null)),
        row(int_entry('id', 4), int_entry('value', 1)),
        row(int_entry('id', 5), int_entry('value', null)),
    )))
    ->withEntry(
        'value',
        when(ref('value')->isNull(), then: lit(0))
    )
    ->write(to_stream(__DIR__ . '/output.txt', truncate: false))
    ->run();
