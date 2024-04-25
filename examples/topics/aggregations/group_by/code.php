<?php

declare(strict_types=1);

use function Flow\ETL\DSL\{data_frame, from_array, ref, to_stream};

require __DIR__ . '/../../../autoload.php';

data_frame()
    ->read(from_array([
        ['id' => 1, 'group' => 'A'],
        ['id' => 2, 'group' => 'B'],
        ['id' => 3, 'group' => 'A'],
        ['id' => 4, 'group' => 'B'],
        ['id' => 5, 'group' => 'A'],
        ['id' => 6, 'group' => 'B'],
        ['id' => 7, 'group' => 'A'],
        ['id' => 8, 'group' => 'B'],
        ['id' => 9, 'group' => 'A'],
        ['id' => 10, 'group' => 'B'],
    ]))
    ->groupBy(ref('group')) // GroupedDataFrame
    ->toDF() // DataFrame
    ->write(to_stream(__DIR__ . '/output.txt', truncate: false))
    ->run();
