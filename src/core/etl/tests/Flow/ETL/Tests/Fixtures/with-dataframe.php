<?php

declare(strict_types=1);

use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

return data_frame()->process(rows(
    schema(str_schema('code'), str_schema('name')),
    row(['code' => 'PL', 'name' => 'Poland']),
    row(['code' => 'US', 'name' => 'United States']),
    row(['code' => 'GB', 'name' => 'Great Britain']),
));
