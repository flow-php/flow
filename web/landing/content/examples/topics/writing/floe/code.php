<?php

declare(strict_types=1);

use function Flow\ETL\Adapter\CSV\from_csv;
use function Flow\ETL\DSL\{data_frame, overwrite, to_output};
use function Flow\Floe\DSL\{from_floe, to_floe};

require __DIR__ . '/vendor/autoload.php';

data_frame()
    ->read(from_csv(__DIR__ . '/data/orders.csv'))
    ->select('order_id', 'created_at', 'customer')
    ->limit(3)
    ->collect()
    ->write(to_floe(__DIR__ . '/output.floe')->saveMode(overwrite()))
    ->run();

// the write is the lesson, so read it back through the floe reader and show it
data_frame()
    ->read(from_floe(__DIR__ . '/output.floe'))
    ->collect()
    ->write(to_output(truncate: false))
    ->run();
