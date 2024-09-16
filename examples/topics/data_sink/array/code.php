<?php

declare(strict_types=1);

use function Flow\ETL\DSL\{data_frame, from_array, to_array};

require __DIR__ . '/../../../autoload.php';

$array = [];

data_frame()
    ->read(from_array([
        ['id' => 1],
        ['id' => 2],
        ['id' => 3],
        ['id' => 4],
        ['id' => 5],
    ]))
    ->collect()
    ->write(to_array($array))
    ->run();

\file_put_contents(__DIR__ . '/output.txt', \var_export($array, true));
