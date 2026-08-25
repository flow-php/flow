<?php

declare(strict_types=1);

use function Flow\ETL\Adapter\CSV\from_csv;
use function Flow\ETL\DSL\{data_frame, overwrite};
use function Flow\Floe\DSL\to_floe;

require __DIR__ . '/vendor/autoload.php';

data_frame()
    ->read(from_csv(__DIR__ . '/data/orders.csv'))
    ->limit(10)
    ->collect()
    ->write(to_floe(__DIR__ . '/output.floe')->saveMode(overwrite()))
    ->run();
