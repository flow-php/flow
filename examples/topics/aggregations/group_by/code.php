<?php

declare(strict_types=1);

use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\to_output;

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
    ->groupBy(ref('group'))
    ->write(to_output(truncate: false))
    ->run();

// +-------+
// | group |
// +-------+
// |     A |
// |     B |
// +-------+
// 2 rows
