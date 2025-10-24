<?php

declare(strict_types=1);

use function Flow\ETL\DSL\{data_frame, from_array, to_stream};

require __DIR__ . '/vendor/autoload.php';

data_frame()
    ->read(from_array([
        ['order_id' => 1, 'item' => 'Widget', 'qty' => 2],
        ['order_id' => 1, 'item' => 'Gadget', 'qty' => 1],
        ['order_id' => 2, 'item' => 'Widget', 'qty' => 5],
        ['order_id' => 2, 'item' => 'Gizmo', 'qty' => 3],
        ['order_id' => 3, 'item' => 'Widget', 'qty' => 1],
    ]))
    ->batchBy('order_id')
    ->write(to_stream(__DIR__ . '/output.txt', truncate: false))
    ->run();
