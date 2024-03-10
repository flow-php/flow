<?php

declare(strict_types=1);

use function Flow\ETL\DSL\{array_entry, df, from_rows, int_entry, ref, row, rows, to_output};

require __DIR__ . '/../../../autoload.php';

df()
    ->read(from_rows(rows(
        row(int_entry('id', 1), array_entry('array', ['a' => 1, 'b' => 2, 'c' => 3])),
    )))
    ->withEntry('array_size', ref('array')->size())
    ->write(to_output(false))
    ->run();
