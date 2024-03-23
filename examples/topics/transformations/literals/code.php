<?php

declare(strict_types=1);

use function Flow\ETL\DSL\{data_frame, from_rows, lit, row, rows, str_entry, to_stream};

require __DIR__ . '/../../../autoload.php';

data_frame()
    ->read(from_rows(rows(
        row(str_entry('name', 'Norbert'))
    )))
    ->withEntry('number', lit(1))
    ->write(to_stream(__DIR__ . '/output.txt', truncate: false))
    ->run();
