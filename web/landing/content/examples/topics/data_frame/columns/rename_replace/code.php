<?php

declare(strict_types=1);

use function Flow\ETL\DSL\{data_frame, from_array, rename_replace, to_output};

require __DIR__ . '/vendor/autoload.php';

data_frame()
    ->read(from_array([
        ['id' => 1, 'name' => 'Norbert', 'joined_id' => 1, 'joined_status' => 'active'],
        ['id' => 2, 'name' => 'John', 'joined_id' => 2, 'joined_status' => 'inactive'],
    ]))
    ->renameEach(rename_replace('joined_', ''))
    ->collect()
    ->write(to_output(truncate: false))
    ->run();
