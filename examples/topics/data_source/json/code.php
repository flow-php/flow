<?php

declare(strict_types=1);

use function Flow\ETL\Adapter\JSON\from_json;
use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\to_output;

require __DIR__ . '/../../../autoload.php';

data_frame()
    ->read(from_json(
        __DIR__ . '/input/dataset.json',
    ))
    ->write(to_output(false))
    ->run();
