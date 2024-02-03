<?php

declare(strict_types=1);

use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\str_entry;
use function Flow\ETL\DSL\to_output;

require __DIR__ . '/../../../autoload.php';

df()
    ->read(from_rows(rows(
        row(str_entry('name', 'Norbert'))
    )))
    ->withEntry('number', lit(1))
    ->write(to_output(false))
    ->run();
