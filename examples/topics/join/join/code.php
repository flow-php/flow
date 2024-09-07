<?php

declare(strict_types=1);

use function Flow\ETL\DSL\{data_frame, from_array, join_on, to_stream};
use Flow\ETL\Join\{Join};

require __DIR__ . '/../../../autoload.php';

$users = [
    ['id' => 1, 'name' => 'John'],
    ['id' => 2, 'name' => 'Jane'],
    ['id' => 3, 'name' => 'Doe'],
    ['id' => 4, 'name' => 'Bruno'],
];

$emails = [
    ['id' => 2, 'email' => 'john@email.com'],
    ['id' => 3, 'email' => 'jane@emial.com'],
    ['id' => 4, 'email' => 'bruno@email.com'],
];

data_frame()
    ->read(from_array($users))
    ->join(
        data_frame()->read(from_array($emails)),
        join_on(['id' => 'id']),
        Join::left
    )
    ->collect()
    ->write(to_stream(__DIR__ . '/output.txt', truncate: false))
    ->run();
