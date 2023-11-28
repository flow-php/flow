<?php

declare(strict_types=1);

use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\read;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\to_output;
use Flow\ETL\DSL\Entry;
use Flow\ETL\Row;
use Flow\ETL\Rows;

require __DIR__ . '/../../bootstrap.php';

$df = read(
    from_rows(new Rows(
        Row::with(Entry::int('a', 100)),
        Row::with(Entry::int('a', 100)),
        Row::with(Entry::int('a', 200)),
        Row::with(Entry::int('a', 400)),
        Row::with(Entry::int('a', 400))
    ))
)
    ->groupBy(ref('a'))
    ->write(to_output(false));

if ($_ENV['FLOW_PHAR_APP'] ?? false) {
    return $df;
}

$df->run();
