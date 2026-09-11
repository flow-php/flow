<?php

declare(strict_types=1);

use function Flow\ETL\Adapter\CSV\from_csv;
use function Flow\ETL\DSL\{data_frame, to_output};

require __DIR__ . '/vendor/autoload.php';

// one argument is enough
data_frame()
    ->read(from_csv(__DIR__ . '/data/orders.csv'))
    ->select('order_id', 'customer')
    ->limit(3)
    ->collect()
    ->write(to_output(truncate: false))
    ->run();
