<?php

declare(strict_types=1);

use function Flow\ETL\DSL\{
    data_frame,
    from_array,
    to_stream};

data_frame()
    ->read(
        from_array(
            [
                ['id' => 1, 'array' => ['a' => 1, 'b' => 2, 'c' => 3]],
                ['id' => 2, 'array' => ['a' => 4, 'b' => 5, 'c' => 6]],
                ['id' => 3, 'array' => ['a' => 7, 'b' => 8, 'c' => 9]],
            ],
        )
    )
    ->collect()
    ->write(to_stream(__DIR__ . '/output.txt', truncate: false))
    ->run();
