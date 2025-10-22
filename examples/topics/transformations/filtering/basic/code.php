<?php

declare(strict_types=1);

use function Flow\ETL\DSL\{data_frame, from_array, ref, to_stream};

require __DIR__ . '/vendor/autoload.php';

data_frame()
    ->read(from_array([
        ['id' => 1, 'name' => 'Alice', 'age' => 25, 'active' => true],
        ['id' => 2, 'name' => 'Bob', 'age' => 32, 'active' => false],
        ['id' => 3, 'name' => 'Charlie', 'age' => 28, 'active' => true],
        ['id' => 4, 'name' => 'Diana', 'age' => 35, 'active' => true],
        ['id' => 5, 'name' => 'Eve', 'age' => 22, 'active' => false],
    ]))
    ->collect()
    ->filter(ref('active')->isTrue())
    ->write(to_stream(__DIR__ . '/output.txt', truncate: false))
    ->run();
