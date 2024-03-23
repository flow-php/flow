<?php

declare(strict_types=1);

use function Flow\ETL\DSL\{data_frame, from_array, to_stream};
use Flow\ETL\Loader\StreamLoader\Output;

require __DIR__ . '/../../../autoload.php';

data_frame()
    ->read(from_array([
        ['id' => 1, 'name' => 'Product 1', 'active' => true, 'tags' => ['tag1', 'tag2']],
        ['id' => 2, 'name' => 'Product 2', 'active' => false, 'address' => ['city' => 'London', 'country' => 'UK']],
        ['id' => 3, 'name' => 'Product 3', 'active' => true, 'tags' => ['tag1', 'tag2']],
        ['id' => 3, 'name' => 'Product 3', 'active' => true],
    ]))
    ->collect()
    ->write(to_stream(__DIR__ . '/output.txt', truncate: false, output: Output::schema))
    ->run();
