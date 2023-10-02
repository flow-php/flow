<?php

declare(strict_types=1);

use function Flow\ETL\DSL\lit;
use Flow\ETL\DSL\Entry;
use Flow\ETL\DSL\From;
use Flow\ETL\DSL\To;
use Flow\ETL\Flow;
use Flow\ETL\Row;
use Flow\ETL\Rows;

require __DIR__ . '/../../bootstrap.php';

return (new Flow())
    ->read(
        From::rows(new Rows(
            Row::create(Entry::string('name', 'Norbert'))
        ))
    )
    ->withEntry('number', lit(1))
    ->write(To::output(false));
