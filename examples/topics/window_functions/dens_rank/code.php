<?php

declare(strict_types=1);

use function Flow\ETL\DSL\{data_frame, dense_rank, from_array, ref, to_stream, window};

require __DIR__ . '/../../../autoload.php';

$df = data_frame()
    ->read(
        from_array([
            ['id' => 1, 'name' => 'Greg', 'department' => 'IT', 'salary' => 6000],
            ['id' => 2, 'name' => 'Michal', 'department' => 'IT', 'salary' => 5000],
            ['id' => 3, 'name' => 'Tomas', 'department' => 'Finances', 'salary' => 11_000],
            ['id' => 4, 'name' => 'John', 'department' => 'Finances', 'salary' => 9000],
            ['id' => 5, 'name' => 'Jane', 'department' => 'Finances', 'salary' => 14_000],
            ['id' => 6, 'name' => 'Janet', 'department' => 'Finances', 'salary' => 4000],
        ])
    )
    ->withEntry('rank', dense_rank()->over(window()->partitionBy(ref('department'))->orderBy(ref('salary')->desc())))
    ->sortBy(ref('department'), ref('rank'))
    ->collect()
    ->write(to_stream(__DIR__ . '/output.txt', truncate: false))
    ->run();
