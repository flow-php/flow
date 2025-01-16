<?php

declare(strict_types=1);

use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\{row, rows, str_entry};

return data_frame()->process(
    rows(row(str_entry('code', 'PL'), str_entry('name', 'Poland')), row(str_entry('code', 'US'), str_entry('name', 'United States')), row(str_entry('code', 'GB'), str_entry('name', 'Great Britain')))
);
