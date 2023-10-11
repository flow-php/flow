<?php

declare(strict_types=1);

use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use Flow\ETL\DSL\Entry;
use Flow\ETL\DSL\From;
use Flow\ETL\DSL\To;
use Flow\ETL\Flow;
use Flow\ETL\Row;
use Flow\ETL\Rows;

require __DIR__ . '/../../bootstrap.php';

$flow = (new Flow())
    ->read(
        From::rows(new Rows(
            Row::with(Entry::int('a', 4), Entry::int('b', 5)),
            Row::with(Entry::int('a', 3), Entry::int('b', 6))
        ))
    )
    ->filter(ref('b')->mod(lit(2))->equals(lit(0)))
    ->write(To::output(false));

if ($_ENV['FLOW_PHAR_APP'] ?? false) {
    return $flow;
}

$flow->run();
