<?php

declare(strict_types=1);

use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\from_sequence_number;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\to_output;

require __DIR__ . '/../../../autoload.php';

data_frame()
    ->read(from_sequence_number('id', 0, 10))
    ->sortBy(ref('id')->desc())
    ->collect()
    ->write(to_output(false))
    ->run();
