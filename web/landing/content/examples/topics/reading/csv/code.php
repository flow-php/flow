<?php

declare(strict_types=1);

use function Flow\ETL\Adapter\CSV\from_csv;
use function Flow\ETL\DSL\{data_frame, to_output};

require __DIR__ . '/vendor/autoload.php';

data_frame()
    ->read(from_csv(
        __DIR__ . '/data/orders.csv',
        with_header: true,
        empty_to_null: true,
        separator: ',',
        enclosure: '"',
        escape: '\\'
    ))
    ->select('order_id', 'created_at', 'customer')
    ->limit(3)
    ->collect()
    ->write(to_output(truncate: false))
    ->run();
