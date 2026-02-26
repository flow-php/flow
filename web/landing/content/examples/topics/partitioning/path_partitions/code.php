<?php

declare(strict_types=1);

use function Flow\ETL\DSL\{data_frame, from_path_partitions, ref, to_output};

require __DIR__ . '/vendor/autoload.php';

data_frame()
    ->read(from_path_partitions(__DIR__ . '/input/color=*/sku=*/*.csv'))
    ->withEntry('path', ref('path')->strReplace(__DIR__, '/__ABSOLUTE_PATH__'))
    ->collect()
    ->write(to_output(truncate: false))
    ->run();
