<?php

declare(strict_types=1);

use function Flow\ETL\DSL\{row, rows, str_entry};
use Flow\ETL\{Flow};

return (new Flow())->process(
    rows(row(str_entry('code', 'PL'), str_entry('name', 'Poland')), row(str_entry('code', 'US'), str_entry('name', 'United States')), row(str_entry('code', 'GB'), str_entry('name', 'Great Britain')))
);
