<?php

declare(strict_types=1);

use function Flow\ETL\Adapter\Parquet\from_parquet;
use function Flow\ETL\DSL\{data_frame, to_stream};

require __DIR__ . '/../../../autoload.php';

data_frame()
    ->read(from_parquet(
        __DIR__ . '/input/dataset.parquet',
    ))
    ->collect()
    ->write(to_stream(__DIR__ . '/output.txt', truncate: false))
    ->run();
