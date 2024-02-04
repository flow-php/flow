<?php

declare(strict_types=1);

use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\max;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\to_output;

require __DIR__ . '/../../../autoload.php';

df()
    ->from(from_rows(rows(
        row(int_entry('a', 100)),
        row(int_entry('a', 100)),
        row(int_entry('a', 200)),
        row(int_entry('a', 400)),
        row(int_entry('a', 400))
    )))
    ->aggregate(max(ref('a')))
    ->write(to_output(false))
    ->run();

// +-------+
// | a_max |
// +-------+
// |   400 |
// +-------+
// 1 rows
