<?php

declare(strict_types=1);

use function Flow\ETL\DSL\{data_frame, from_array, rename_replace, to_output};

require __DIR__ . '/vendor/autoload.php';

data_frame()
    ->read(from_array([
        ['order_id' => 1, 'order_status' => 'active'],
        ['order_id' => 2, 'order_status' => 'inactive'],
    ]))
    ->renameEach(rename_replace('order_', ''))
    ->collect()
    ->write(to_output(truncate: false))
    ->run();
