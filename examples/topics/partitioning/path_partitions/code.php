<?php

declare(strict_types=1);

use function Flow\ETL\DSL\{data_frame, from_path_partitions, ref, to_stream};

require __DIR__ . '/../../../autoload.php';

data_frame()
    ->read(from_path_partitions(__DIR__ . '/input/color=*/sku=*/*.csv'))
    ->withEntry('path', ref('path')->strReplace(__DIR__, '/__ABSOLUTE_PATH__'))
    ->collect()
    ->write(to_stream(__DIR__ . '/output.txt', truncate: false))
    ->run();
