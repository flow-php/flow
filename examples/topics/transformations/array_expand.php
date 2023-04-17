<?php

declare(strict_types=1);

use function Flow\ETL\DSL\array_expand;
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
            Row::with(Entry::int('id', 1), Entry::array('array', ['a' => 1, 'b' => 2, 'c' => 3])),
        ))
    )
    ->write(To::output(false))
    ->withEntry('expanded', array_expand(ref('array')))
    ->write(To::output(false))
    ->run();
