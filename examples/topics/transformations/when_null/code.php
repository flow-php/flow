<?php

declare(strict_types=1);

use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\null_entry;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\to_output;
use function Flow\ETL\DSL\when;

require __DIR__ . '/../../../autoload.php';

df()
    ->read(from_rows(rows(
        row(int_entry('id', 1), int_entry('value', 1)),
        row(int_entry('id', 2), int_entry('value', 1)),
        row(int_entry('id', 3), null_entry('value')),
        row(int_entry('id', 4), int_entry('value', 1)),
        row(int_entry('id', 5), null_entry('value')),
    )))
    ->withEntry(
        'value',
        when(ref('value')->isNull(), then: lit(0))
    )
    ->write(to_output(false))
    ->run();
