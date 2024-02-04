<?php

declare(strict_types=1);

use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\to_output;

require __DIR__ . '/../../../autoload.php';

df()
    ->read(from_rows(rows(
        row(int_entry('a', 4), int_entry('b', 5)),
        row(int_entry('a', 3), int_entry('b', 6))
    )))
    ->filter(ref('b')->mod(lit(2))->equals(lit(0)))
    ->write(to_output(false))
    ->run();
