<?php

declare(strict_types=1);

use function Flow\ETL\DSL\{data_frame, from_rows, int_entry, lit, ref, row, rows, to_stream};

require __DIR__ . '/../../../autoload.php';

data_frame()
    ->read(from_rows(rows(
        row(int_entry('a', 4), int_entry('b', 5)),
        row(int_entry('a', 3), int_entry('b', 6))
    )))
    ->filter(ref('b')->mod(lit(2))->equals(lit(0)))
    ->write(to_stream(__DIR__ . '/output.txt', truncate: false))
    ->run();
