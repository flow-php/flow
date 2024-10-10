<?php

declare(strict_types=1);

use function Flow\ETL\DSL\{df, from_array, to_output};

return df()
    ->read(from_array([
        ['id' => 1, 'name' => 'User 01', 'active' => true],
        ['id' => 2, 'name' => 'User 02', 'active' => false],
        ['id' => 3, 'name' => 'User 03', 'active' => true],
    ]))
    ->collect()
    ->write(to_output());
