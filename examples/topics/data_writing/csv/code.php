<?php

declare(strict_types=1);

use function Flow\ETL\Adapter\CSV\to_csv;
use function Flow\ETL\DSL\{data_frame, from_array, overwrite};

require __DIR__ . '/vendor/autoload.php';

data_frame()
    ->read(
        from_array([
            ['id' => 1, 'name' => 'John', 'age' => 30],
            ['id' => 2, 'name' => 'Jane', 'age' => 25],
            ['id' => 3, 'name' => 'Bob', 'age' => 35],
            ['id' => 4, 'name' => 'Alice', 'age' => 28],
            ['id' => 5, 'name' => 'Charlie', 'age' => 32],
        ])
    )
    ->collect()
    ->mode(overwrite())
    ->write(to_csv(__DIR__ . '/output.csv'))
    ->run();
