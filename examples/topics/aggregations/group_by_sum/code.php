<?php

declare(strict_types=1);

use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\sum;
use function Flow\ETL\DSL\to_output;

require __DIR__ . '/../../../autoload.php';

data_frame()
    ->read(from_array([
        ['id' => 1, 'group' => 'A', 'value' => 100],
        ['id' => 2, 'group' => 'B', 'value' => 200],
        ['id' => 3, 'group' => 'A', 'value' => 300],
        ['id' => 4, 'group' => 'B', 'value' => 100],
        ['id' => 5, 'group' => 'A', 'value' => 200],
        ['id' => 6, 'group' => 'B', 'value' => 100],
        ['id' => 7, 'group' => 'A', 'value' => 400],
        ['id' => 8, 'group' => 'B', 'value' => 20],
        ['id' => 9, 'group' => 'A', 'value' => 800],
        ['id' => 10, 'group' => 'B', 'value' => 40],
    ]))
    ->groupBy(ref('group'))
    ->aggregate(sum(ref('value')))
    ->write(to_output(truncate: false))
    ->run();

// +-------+-----------+
// | group | value_sum |
// +-------+-----------+
// |     A |      1800 |
// |     B |       460 |
// +-------+-----------+
// 2 rows
