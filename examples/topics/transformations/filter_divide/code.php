<?php

declare(strict_types=1);

use function Flow\ETL\DSL\{df, from_rows, int_entry, lit, ref, row, rows, to_output};

require __DIR__ . '/../../../autoload.php';

df()
    ->read(from_rows(rows(
        row(int_entry('a', 100), int_entry('b', 100)),
        row(int_entry('a', 100), int_entry('b', 200))
    )))
    ->filter(ref('b')->divide(lit(2))->equals(lit('a')))
    ->withEntry('new_b', ref('b')->multiply(lit(2))->multiply(lit(5)))
    ->write(to_output(false))
    ->run();
