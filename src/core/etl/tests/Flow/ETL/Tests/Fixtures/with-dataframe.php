<?php

declare(strict_types=1);

use Flow\ETL\DSL\Entry;
use Flow\ETL\Flow;
use Flow\ETL\Row;
use Flow\ETL\Rows;

return (new Flow())->process(
    new Rows(
        Row::create(Entry::string('code', 'PL'), Entry::string('name', 'Poland')),
        Row::create(Entry::string('code', 'US'), Entry::string('name', 'United States')),
        Row::create(Entry::string('code', 'GB'), Entry::string('name', 'Great Britain')),
    )
);
