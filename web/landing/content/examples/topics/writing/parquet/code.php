<?php

declare(strict_types=1);

use function Flow\ETL\Adapter\Parquet\{from_parquet, to_parquet};
use function Flow\ETL\DSL\{data_frame, from_array, overwrite, to_output};

require __DIR__ . '/vendor/autoload.php';

data_frame()
    ->read(from_array([
        ['id' => 1],
        ['id' => 2],
        ['id' => 3],
        ['id' => 4],
        ['id' => 5],
    ]))
    ->collect()
    ->write(to_parquet(__DIR__ . '/output.parquet')->saveMode(overwrite()))
    ->run();

// the write is the lesson, so read it back through the parquet reader and show it
data_frame()
    ->read(from_parquet(__DIR__ . '/output.parquet'))
    ->collect()
    ->write(to_output(truncate: false))
    ->run();
