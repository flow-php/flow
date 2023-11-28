<?php

declare(strict_types=1);

use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\read;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\sum;
use function Flow\ETL\DSL\to_output;
use Flow\ETL\DSL\Entry;

require __DIR__ . '/../../bootstrap.php';

$df = read(
    from_rows(rows(
        row(Entry::int('a', 100)),
        row(Entry::int('a', 100)),
        row(Entry::int('a', 200)),
        row(Entry::int('a', 400)),
        row(Entry::int('a', 400))
    ))
)
    ->aggregate(sum(ref('a')))
    ->write(to_output(false));

if ($_ENV['FLOW_PHAR_APP'] ?? false) {
    return $df;
}

$df->run();
