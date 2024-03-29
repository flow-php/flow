<?php

declare(strict_types=1);

use function Flow\ETL\Adapter\CSV\to_csv;
use function Flow\ETL\DSL\{data_frame, from_array, overwrite, ref};

require __DIR__ . '/../../../autoload.php';

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
    ->partitionBy(ref('color'), ref('sku'))
    ->mode(overwrite())
    ->write(to_csv(__DIR__ . '/output/products.csv'))
    ->run();
