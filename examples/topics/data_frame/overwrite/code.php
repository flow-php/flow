<?php

declare(strict_types=1);

use function Flow\ETL\Adapter\CSV\{from_csv, to_csv};
use function Flow\ETL\DSL\{data_frame, overwrite, to_stream};

require __DIR__ . '/../../../autoload.php';

data_frame()
    ->read(from_csv(__DIR__ . '/input/file.csv'))
    ->saveMode(overwrite())
    ->write(to_csv(__DIR__ . '/output/file.csv'))
    ->collect()
    ->write(to_stream(__DIR__ . '/output.txt', truncate: false))
    ->run();

data_frame()
    ->read(from_csv(__DIR__ . '/output/file.csv'))
    ->saveMode(overwrite())
    ->drop('name')
    ->write(to_csv(__DIR__ . '/output/file.csv'))
    ->collect()
    ->write(to_stream(__DIR__ . '/output.txt', truncate: false))
    ->run();
