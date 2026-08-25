<?php

declare(strict_types=1);

use function Flow\ETL\Adapter\CSV\{from_csv, to_csv};
use function Flow\ETL\DSL\{data_frame, overwrite, ref, to_branch, to_output};

require __DIR__ . '/vendor/autoload.php';

data_frame()
    ->read(from_csv(__DIR__ . '/data/orders.csv'))
    ->select('order_id', 'created_at', 'cancelled_at', 'discount', 'email')
    ->write(to_branch(ref('cancelled_at')->isNotNull(), to_csv(__DIR__ . '/output/cancelled.csv')->saveMode(overwrite())))
    ->write(to_branch(ref('cancelled_at')->isNull(), to_csv(__DIR__ . '/output/active.csv')))
    ->run();

data_frame()
    ->read(from_csv(__DIR__ . '/output/cancelled.csv'))
    ->collect()
    ->write(to_output(truncate: false))
    ->run();
