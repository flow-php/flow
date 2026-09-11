<?php

declare(strict_types=1);

use function Flow\ETL\Adapter\CSV\{from_csv, to_csv};
use function Flow\ETL\DSL\{data_frame, overwrite, to_output};

require __DIR__ . '/vendor/autoload.php';

data_frame()
    ->read(from_csv(__DIR__ . '/data/orders.csv'))
    ->select('order_id', 'created_at', 'customer')
    ->limit(3)
    ->write(to_csv(__DIR__ . '/output/file.csv')->saveMode(overwrite()))
    ->collect()
    ->write(to_output(truncate: false))
    ->run();

data_frame()
    ->read(from_csv(__DIR__ . '/output/file.csv'))
    ->drop('customer')
    ->write(to_csv(__DIR__ . '/output/file.csv')->saveMode(overwrite()))
    ->collect()
    ->write(to_output(truncate: false))
    ->run();
