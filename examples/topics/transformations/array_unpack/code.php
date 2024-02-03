<?php

declare(strict_types=1);

use function Flow\ETL\DSL\array_entry;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\to_output;

require __DIR__ . '/../../../autoload.php';

df()
    ->read(from_rows(rows(
        row(int_entry('id', 1), array_entry('array', ['a' => 1, 'b' => 2, 'c' => 3])),
        row(int_entry('id', 2), array_entry('array', ['d' => 4, 'e' => 5, 'f' => 6])),
    )))
    ->write(to_output(false))
    ->withEntry('unpacked', ref('array')->unpack())
    ->write(to_output(false))
    ->run();
