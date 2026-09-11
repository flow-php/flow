<?php

declare(strict_types=1);

use function Flow\ETL\Adapter\Parquet\from_parquet;
use function Flow\ETL\DSL\{data_frame, to_output};

require __DIR__ . '/vendor/autoload.php';

data_frame()
    ->read(from_parquet(
        __DIR__ . '/data/orders.parquet',
    ))
    ->select('order_id', 'created_at', 'customer')
    ->limit(3)
    ->collect()
    ->write(to_output(truncate: false))
    ->run();
