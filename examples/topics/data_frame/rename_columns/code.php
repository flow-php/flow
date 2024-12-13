<?php

declare(strict_types=1);

use function Flow\ETL\DSL\{data_frame, from_array, to_stream};

require __DIR__ . '/../../../autoload.php';

data_frame()
    ->read(from_array([
        ['id' => 1, 'name' => 'Norbert', 'joined_id' => 1, 'joined_status' => 'active'],
        ['id' => 2, 'name' => 'John', 'joined_id' => 2, 'joined_status' => 'inactive'],
        ['id' => 3, 'name' => 'Jane', 'joined_id' => 3, 'joined_status' => 'active'],
    ]))
    ->rename('id', 'user_id')
    ->renameAll('joined_', '')
    ->collect()
    ->write(to_stream(__DIR__ . '/output.txt', truncate: false))
    ->run();
