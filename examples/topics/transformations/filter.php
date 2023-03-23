<?php

declare(strict_types=1);

use function Flow\ETL\DSL\ref;
use Flow\ETL\DSL\Entry;
use Flow\ETL\DSL\From;
use Flow\ETL\DSL\To;
use Flow\ETL\Flow;
use Flow\ETL\Row;
use Flow\ETL\Rows;

require __DIR__ . '/../../bootstrap.php';

(new Flow())
    ->read(
        From::rows(new Rows(
            Row::create(Entry::integer('a', 100), Entry::integer('b', 100)),
            Row::create(Entry::integer('a', 100), Entry::integer('b', 200))
        ))
    )
    ->filter(ref('a')->equals('b'))
    ->write(To::output(false))
    ->run();
