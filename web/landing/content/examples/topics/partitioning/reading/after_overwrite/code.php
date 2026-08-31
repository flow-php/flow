<?php

declare(strict_types=1);

use function Flow\ETL\Adapter\CSV\{from_csv, to_csv};
use function Flow\ETL\DSL\{data_frame, from_array, overwrite, ref, to_output};

require __DIR__ . '/vendor/autoload.php';

$outputPath = __DIR__ . '/output';

data_frame()
    ->read(from_array(
        [
            ['id' => 1, 'color' => 'red', 'name' => 'Widget'],
            ['id' => 2, 'color' => 'blue', 'name' => 'Gadget'],
        ]
    ))
    ->write(to_csv($outputPath . '/products.csv')->saveMode(overwrite())->partitionBy(partition_by(ref('color'))))
    ->run();

data_frame()
    ->read(from_csv($outputPath . '/color=*/*.csv'))
    ->write(to_output(truncate: false))
    ->run();
