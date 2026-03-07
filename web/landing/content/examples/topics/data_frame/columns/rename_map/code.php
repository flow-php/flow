<?php

declare(strict_types=1);

use function Flow\ETL\DSL\{data_frame, from_array, rename_map, to_output};

require __DIR__ . '/vendor/autoload.php';

data_frame()
    ->read(from_array([
        ['id' => 1, 'first_name' => 'Norbert', 'last_name' => 'Orzechowicz'],
        ['id' => 2, 'first_name' => 'John', 'last_name' => 'Doe'],
    ]))
    ->renameEach(rename_map([
        'id' => 'user_id',
        'first_name' => 'name',
        'last_name' => 'surname',
    ]))
    ->collect()
    ->write(to_output(truncate: false))
    ->run();
