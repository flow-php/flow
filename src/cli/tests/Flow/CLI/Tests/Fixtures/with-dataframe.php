<?php

declare(strict_types=1);

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

return data_frame()->process(array_to_rows(
    [
        ['code' => 'PL', 'name' => 'Poland'],
        ['code' => 'US', 'name' => 'United States'],
        ['code' => 'GB', 'name' => 'Great Britain'],
    ],
    schema(str_schema('code'), str_schema('name')),
));
