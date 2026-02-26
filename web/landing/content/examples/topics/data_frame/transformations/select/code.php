<?php

declare(strict_types=1);

use function Flow\ETL\DSL\{data_frame, from_array, select, to_output};

require __DIR__ . '/vendor/autoload.php';

data_frame()
    ->read(
        from_array([
            ['id' => 1, 'first_name' => 'John', 'last_name' => 'Doe'],
            ['id' => 2, 'first_name' => 'Jane', 'last_name' => 'Smith'],
            ['id' => 3, 'first_name' => 'Bob', 'last_name' => 'Johnson'],
            ['id' => 4, 'first_name' => 'Alice', 'last_name' => 'Williams'],
        ])
    )
    ->with(select('id'))
    ->collect()
    ->write(to_output(truncate: false))
    ->run();
