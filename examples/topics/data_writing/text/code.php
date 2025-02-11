<?php

declare(strict_types=1);

use function Flow\ETL\Adapter\Text\to_text;
use function Flow\ETL\DSL\{concat_ws, data_frame, from_array, overwrite, ref};

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
    ->withEntry('line', concat_ws('_', ref('id'), ref('name'), ref('age')))
    ->select('line')
    ->collect()
    ->mode(overwrite())
    ->write(to_text(__DIR__ . '/output.txt'))
    ->run();
