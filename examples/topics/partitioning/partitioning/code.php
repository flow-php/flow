<?php

declare(strict_types=1);

use function Flow\ETL\Adapter\CSV\to_csv;
use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\to_output;
use Flow\ETL\Join\Expression;
use Flow\ETL\Join\Join;

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
            ['id' => 8, 'color' => 'blue', 'sku' => 'PRODUCT02']
        ]
    ))
    ->partitionBy(ref('color'), ref('sku'))
    ->write(to_csv(__DIR__ . '/output')) // do not provider extension, partitions are anyway written to separate folders
    ->run();

// output
// ├── color=blue
// │   ├── sku=PRODUCT01
// │   │   └── 65c7e9bc4460a568233195.csv
// │   └── sku=PRODUCT02
// │       └── 65c7e9bc446c2326068326.csv
// ├── color=green
// │   ├── sku=PRODUCT01
// │   │   └── 65c7e9bc44305321518126.csv
// │   ├── sku=PRODUCT02
// │   │   └── 65c7e9bc44421020940545.csv
// │   └── sku=PRODUCT03
// │       └── 65c7e9bc44515031584752.csv
// └── color=red
//     ├── sku=PRODUCT01
//     │   └── 65c7e9bc4386f958078278.csv
//     ├── sku=PRODUCT02
//     │   └── 65c7e9bc440fa083889144.csv
//     └── sku=PRODUCT03
//         └── 65c7e9bc44209401416287.csv
//
// 12 directories, 8 files

