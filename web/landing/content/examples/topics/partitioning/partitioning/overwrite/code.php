<?php

declare(strict_types=1);

use function Flow\ETL\Adapter\CSV\{from_csv, to_csv};
use function Flow\ETL\DSL\{data_frame, from_array, overwrite, partition_by, ref, to_output};

require __DIR__ . '/vendor/autoload.php';

data_frame()
    ->read(from_array(
        [
            ['id' => 1, 'color' => 'red', 'sku' => 'PRODUCT01'],
            ['id' => 2, 'color' => 'red', 'sku' => 'PRODUCT02'],
            ['id' => 3, 'color' => 'red', 'sku' => 'PRODUCT03'],
            ['id' => 4, 'color' => 'green', 'sku' => 'PRODUCT01'],
            ['id' => 5, 'color' => 'green', 'sku' => 'PRODUCT02'],
            ['id' => 6, 'color' => 'green', 'sku' => 'PRODUCT03'],
            ['id' => 7, 'color' => 'blue', 'sku' => 'PRODUCT01'],
            ['id' => 8, 'color' => 'blue', 'sku' => 'PRODUCT02'],
        ]
    ))
    ->write(to_csv(__DIR__ . '/output/products.csv')->saveMode(overwrite())->partitionBy(partition_by(ref('color'), ref('sku'))))
    ->run();

// the directory layout is the lesson, so read the partitions back and show them
data_frame()
    ->read(from_csv(__DIR__ . '/output/color=*/sku=*/*.csv'))
    ->collect()
    ->write(to_output(truncate: false))
    ->run();
