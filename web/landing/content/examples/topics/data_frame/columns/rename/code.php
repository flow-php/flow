<?php

declare(strict_types=1);

use function Flow\ETL\DSL\{data_frame, from_array, to_output};

require __DIR__ . '/vendor/autoload.php';

data_frame()
    ->read(from_array([
        ['id' => 1, 'first_name' => 'Norbert', 'last_name' => 'Orzechowicz'],
        ['id' => 2, 'first_name' => 'John', 'last_name' => 'Doe'],
    ]))
    ->rename('id', 'user_id')
    ->collect()
    ->write(to_output(truncate: false))
    ->run();
