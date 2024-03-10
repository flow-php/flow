<?php

declare(strict_types=1);

use function Flow\ETL\DSL\{df, from_rows, int_entry, ref, row, rows, to_output};

require __DIR__ . '/../../../autoload.php';

df()
    ->read(from_rows(rows(
        row(int_entry('a', 100), int_entry('b', 200))
    )))
    ->write(to_output(false))
    ->withEntry('c', ref('a')->plus(ref('b')))
    ->withEntry('d', ref('b')->minus(ref('a')))
    ->write(to_output(false))
    ->run();
