<?php

declare(strict_types=1);

use function Flow\ETL\Adapter\Parquet\from_parquet;
use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\to_output;

require __DIR__ . '/../../../autoload.php';

data_frame()
    ->read(from_parquet(
        __DIR__ . '/input/dataset.parquet',
    ))
    ->write(to_output(false))
    ->run();
