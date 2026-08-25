<?php

declare(strict_types=1);

use function Flow\ETL\Adapter\CSV\{from_csv, to_csv};
use function Flow\ETL\DSL\{data_frame, from_array, overwrite, ref, to_output};

require __DIR__ . '/vendor/autoload.php';

data_frame()
    ->read(from_array(
        [
            ['id' => 1, 'color' => 'red', 'sku' => 'PRODUCT01'],
            ['id' => 2, 'color' => 'red', 'sku' => 'PRODUCT02'],
            ['id' => 3, 'color' => 'green', 'sku' => 'PRODUCT01'],
            ['id' => 4, 'color' => 'blue', 'sku' => 'PRODUCT02'],
        ]
    ))
    ->partitionBy(ref('color'), ref('sku'))
    ->write(to_csv(__DIR__ . '/output/{color}/{sku}.csv')->saveMode(overwrite()))
    ->run();

data_frame()
    ->read(from_csv(__DIR__ . '/output/{color}/{sku}.csv'))
    ->write(to_output(truncate: false))
    ->run();
