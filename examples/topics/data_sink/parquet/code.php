<?php

declare(strict_types=1);

use function Flow\ETL\Adapter\Parquet\to_parquet;
use function Flow\ETL\DSL\{data_frame, from_array, overwrite};

require __DIR__ . '/../../../autoload.php';

data_frame()
    ->read(from_array([
        ['id' => 1],
        ['id' => 2],
        ['id' => 3],
        ['id' => 4],
        ['id' => 5],
    ]))
    ->collect()
    ->saveMode(overwrite())
    ->write(to_parquet(__DIR__ . '/output.parquet'))
    ->run();
