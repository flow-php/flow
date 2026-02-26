<?php

declare(strict_types=1);

use function Flow\ETL\DSL\{data_frame, from_rows, int_entry, lit, ref, row, rows, to_output};

require __DIR__ . '/vendor/autoload.php';

data_frame()
    ->read(from_rows(rows(
        row(int_entry('a', 4), int_entry('b', 5)),
        row(int_entry('a', 3), int_entry('b', 6))
    )))
    ->filter(ref('b')->mod(lit(2))->equals(lit(0)))
    ->write(to_output(truncate: false))
    ->run();
