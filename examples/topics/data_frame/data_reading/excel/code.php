<?php

declare(strict_types=1);

use function Flow\ETL\Adapter\Excel\DSL\from_excel;
use function Flow\ETL\DSL\{data_frame, to_stream};

require __DIR__ . '/vendor/autoload.php';

data_frame()
    ->read(from_excel(
        __DIR__ . '/input/dataset.xlsx',
    ))
    ->collect()
    ->write(to_stream(__DIR__ . '/output.txt', truncate: false))
    ->run();
