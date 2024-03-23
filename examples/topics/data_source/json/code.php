<?php

declare(strict_types=1);

use function Flow\ETL\Adapter\JSON\from_json;
use function Flow\ETL\DSL\{data_frame, to_stream};

require __DIR__ . '/../../../autoload.php';

data_frame()
    ->read(from_json(
        __DIR__ . '/input/dataset.json',
    ))
    ->collect()
    ->write(to_stream(__DIR__ . '/output.txt', truncate: false))
    ->run();
