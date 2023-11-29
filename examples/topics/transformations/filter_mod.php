<?php

declare(strict_types=1);

use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\to_output;
use Flow\ETL\Row;
use Flow\ETL\Rows;

require __DIR__ . '/../../bootstrap.php';

$df = df()
    ->read(from_rows(new Rows(
        Row::with(int_entry('a', 4), int_entry('b', 5)),
        Row::with(int_entry('a', 3), int_entry('b', 6))
    )))
    ->filter(ref('b')->mod(lit(2))->equals(lit(0)))
    ->write(to_output(false));

if ($_ENV['FLOW_PHAR_APP'] ?? false) {
    return $df;
}

$df->run();
