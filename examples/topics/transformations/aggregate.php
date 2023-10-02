<?php

declare(strict_types=1);

use function Flow\ETL\DSL\ref;
use Flow\ETL\DSL\Entry;
use Flow\ETL\DSL\From;
use Flow\ETL\DSL\To;
use Flow\ETL\Flow;
use Flow\ETL\GroupBy\Aggregation;
use Flow\ETL\Row;
use Flow\ETL\Rows;

require __DIR__ . '/../../bootstrap.php';

return (new Flow())
    ->read(
        From::rows(new Rows(
            Row::with(Entry::int('a', 100)),
            Row::with(Entry::int('a', 100)),
            Row::with(Entry::int('a', 200)),
            Row::with(Entry::int('a', 400)),
            Row::with(Entry::int('a', 400))
        ))
    )
    ->aggregate(Aggregation::sum(ref('a')))
    ->write(To::output(false));
