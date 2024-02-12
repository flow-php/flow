<?php

declare(strict_types=1);

use function Flow\ETL\Adapter\CSV\from_csv;
use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\to_output;

require __DIR__ . '/../../../autoload.php';

data_frame()
    ->read(from_csv(__DIR__ . '/input/color=*/sku=*/*.csv'))
    ->collect()
    ->write(to_output(false))
    ->run();

// +----+-------+-----------+
// | id | color |       sku |
// +----+-------+-----------+
// |  5 | green | PRODUCT02 |
// |  6 | green | PRODUCT03 |
// |  4 | green | PRODUCT01 |
// |  2 |   red | PRODUCT02 |
// |  3 |   red | PRODUCT03 |
// |  1 |   red | PRODUCT01 |
// |  8 |  blue | PRODUCT02 |
// |  7 |  blue | PRODUCT01 |
// +----+-------+-----------+
// 8 rows