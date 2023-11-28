<?php

declare(strict_types=1);

use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\read;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\to_output;
use Flow\ETL\DSL\Entry;
use Flow\ETL\Row;
use Flow\ETL\Rows;

require __DIR__ . '/../../bootstrap.php';

$df = read(
    from_rows(new Rows(
        Row::with(Entry::int('a', 4), Entry::int('b', 5)),
        Row::with(Entry::int('a', 3), Entry::int('b', 6))
    ))
)
    ->filter(ref('b')->mod(lit(2))->equals(lit(0)))
    ->write(to_output(false));

if ($_ENV['FLOW_PHAR_APP'] ?? false) {
    return $df;
}

$df->run();
