<?php

declare(strict_types=1);

use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\when;
use Flow\ETL\DSL\Entry;
use Flow\ETL\DSL\From;
use Flow\ETL\DSL\To;
use Flow\ETL\Flow;
use Flow\ETL\Row;
use Flow\ETL\Rows;

require __DIR__ . '/../../bootstrap.php';

return (new Flow())
    ->read(From::rows(new Rows(
        Row::with(Entry::int('id', 1), Entry::int('value', 1)),
        Row::with(Entry::int('id', 2), Entry::int('value', 1)),
        Row::with(Entry::int('id', 3), Entry::null('value')),
        Row::with(Entry::int('id', 4), Entry::int('value', 1)),
        Row::with(Entry::int('id', 5), Entry::null('value')),
    )))
    ->withEntry(
        'value',
        when(ref('value')->isNull(), then: lit(0))
    )
    ->write(To::output(false));
