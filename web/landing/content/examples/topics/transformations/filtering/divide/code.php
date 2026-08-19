<?php

declare(strict_types=1);

use function Flow\ETL\DSL\{data_frame, from_rows, int_entry, lit, ref, row, rows, to_output};

require __DIR__ . '/vendor/autoload.php';

data_frame()
    ->read(from_rows(rows(
        row(int_entry('a', 100), int_entry('b', 100)),
        row(int_entry('a', 100), int_entry('b', 200))
    )))
    ->filter(ref('b')->divide(lit(2))->equals(ref('a')))
    ->withEntry('new_b', ref('b')->multiply(lit(2))->multiply(lit(5)))
    ->write(to_output(truncate: false))
    ->run();
