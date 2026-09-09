<?php

declare(strict_types=1);

use function Flow\ETL\DSL\{data_frame, to_output};
use function Flow\Floe\DSL\from_floe;

require __DIR__ . '/vendor/autoload.php';

data_frame()
    ->read(from_floe(__DIR__ . '/data/orders.floe'))
    ->select('order_id', 'created_at', 'customer')
    ->limit(3)
    ->collect()
    ->write(to_output(truncate: false))
    ->run();
