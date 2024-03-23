<?php

declare(strict_types=1);

use function Flow\ETL\DSL\{data_frame, from_rows, int_entry, max, ref, row, rows, to_stream};

require __DIR__ . '/../../../autoload.php';

data_frame()
    ->from(from_rows(rows(
        row(int_entry('a', 100)),
        row(int_entry('a', 100)),
        row(int_entry('a', 200)),
        row(int_entry('a', 400)),
        row(int_entry('a', 400))
    )))
    ->aggregate(max(ref('a')))
    ->write(to_stream(__DIR__ . '/output.txt', truncate: false))
    ->run();
