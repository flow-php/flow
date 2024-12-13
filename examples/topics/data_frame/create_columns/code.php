<?php

declare(strict_types=1);

use function Flow\ETL\DSL\{data_frame, from_array, lit, ref, to_stream};

require __DIR__ . '/../../../autoload.php';

data_frame()
    ->read(from_array([
        ['id' => 1, 'name' => 'Norbert'],
        ['id' => 2, 'name' => 'John'],
        ['id' => 3, 'name' => 'Jane'],
    ]))
    ->withEntry('active', ref('id')->isOdd())
    ->withEntry('number', lit(5))
    ->collect()
    ->write(to_stream(__DIR__ . '/output.txt', truncate: false))
    ->run();
