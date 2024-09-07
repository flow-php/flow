<?php

declare(strict_types=1);

use function Flow\ETL\DSL\{data_frame, from_array, to_stream};

require __DIR__ . '/../../../autoload.php';

data_frame()
    ->read(from_array([
        ['id' => 1, 'name' => 'John'],
        ['id' => 2, 'name' => 'Doe'],
        ['id' => 3, 'name' => 'Jane'],
        ['id' => 4, 'name' => 'Smith'],
        ['id' => 5, 'name' => 'Alice'],
    ]))
    ->collect() // alternatively we can also use ->batchSize(-1)
    ->write(to_stream(__DIR__ . '/output.txt', truncate: false))
    ->run();
