<?php

declare(strict_types=1);

use function Flow\ETL\Adapter\CSV\from_csv;
use function Flow\ETL\Adapter\CSV\to_csv;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\overwrite;

require __DIR__ . '/../../../autoload.php';

df()
    ->read(from_csv(__DIR__ . '/input/file.csv'))
    ->saveMode(overwrite())
    ->write(to_csv(__DIR__ . '/output/file.csv'))
    ->run();

df()
    ->read(from_csv(__DIR__ . '/output/file.csv'))
    ->saveMode(overwrite())
    ->drop('name')
    ->write(to_csv(__DIR__ . '/output/file.csv'))
    ->run();

// content of /output/file.csv:
// id
// 1
// 2
// 3
// 4
