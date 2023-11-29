<?php

declare(strict_types=1);

use function Flow\ETL\DSL\str_entry;
use Flow\ETL\Flow;
use Flow\ETL\Row;
use Flow\ETL\Rows;

return (new Flow())->process(
    new Rows(
        Row::create(str_entry('code', 'PL'), str_entry('name', 'Poland')),
        Row::create(str_entry('code', 'US'), str_entry('name', 'United States')),
        Row::create(str_entry('code', 'GB'), str_entry('name', 'Great Britain')),
    )
);
