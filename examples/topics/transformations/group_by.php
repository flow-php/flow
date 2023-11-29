<?php

declare(strict_types=1);

use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\to_output;
use Flow\ETL\Row;
use Flow\ETL\Rows;

require __DIR__ . '/../../bootstrap.php';

$df = df()
    ->read(from_rows(new Rows(
        Row::with(int_entry('a', 100)),
        Row::with(int_entry('a', 100)),
        Row::with(int_entry('a', 200)),
        Row::with(int_entry('a', 400)),
        Row::with(int_entry('a', 400))
    )))
    ->groupBy(ref('a'))
    ->write(to_output(false));

if ($_ENV['FLOW_PHAR_APP'] ?? false) {
    return $df;
}

$df->run();
