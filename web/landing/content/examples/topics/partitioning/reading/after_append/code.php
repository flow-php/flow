<?php

declare(strict_types=1);

use function Flow\ETL\Adapter\CSV\{from_csv, to_csv};
use function Flow\ETL\DSL\{append, data_frame, from_array, ref, to_output};

require __DIR__ . '/vendor/autoload.php';

$outputPath = __DIR__ . '/output';

data_frame()
    ->read(from_array(
        [
            ['id' => 1, 'color' => 'red', 'name' => 'Widget'],
            ['id' => 2, 'color' => 'blue', 'name' => 'Gadget'],
        ]
    ))
    ->partitionBy(ref('color'))
    ->write(to_csv($outputPath . '/products.csv')->saveMode(append()))
    ->run();

data_frame()
    ->read(from_array(
        [
            ['id' => 3, 'color' => 'red', 'name' => 'Sprocket'],
            ['id' => 4, 'color' => 'blue', 'name' => 'Gear'],
        ]
    ))
    ->partitionBy(ref('color'))
    ->write(to_csv($outputPath . '/products.csv')->saveMode(append()))
    ->run();

data_frame()
    ->read(from_csv($outputPath . '/color=*/*.csv'))
    ->write(to_output(truncate: false))
    ->run();
