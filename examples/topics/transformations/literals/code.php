<?php

declare(strict_types=1);

use function Flow\ETL\DSL\{df, from_rows, lit, row, rows, str_entry, to_output};

require __DIR__ . '/../../../autoload.php';

df()
    ->read(from_rows(rows(
        row(str_entry('name', 'Norbert'))
    )))
    ->withEntry('number', lit(1))
    ->write(to_output(false))
    ->run();
