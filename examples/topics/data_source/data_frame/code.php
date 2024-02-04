<?php

declare(strict_types=1);

use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\from_data_frame;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\to_output;

require __DIR__ . '/../../../autoload.php';

data_frame()
    ->read(
        from_data_frame(
            data_frame()
                ->read(from_array(
                    [
                        ['id' => 1],
                        ['id' => 2],
                        ['id' => 3],
                        ['id' => 4],
                        ['id' => 5],
                    ]
                ))
                ->withEntry('timestamp', lit(\time()))
        )
    )
    ->write(to_output(false))
    ->run();
