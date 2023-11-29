<?php

declare(strict_types=1);

use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\read;
use function Flow\ETL\DSL\str_entry;
use function Flow\ETL\DSL\to_output;
use Flow\ETL\Row;
use Flow\ETL\Rows;

require __DIR__ . '/../../bootstrap.php';

$df = read(
    from_rows(new Rows(
        Row::create(str_entry('name', 'Norbert'))
    ))
)
    ->withEntry('number', lit(1))
    ->write(to_output(false));

if ($_ENV['FLOW_PHAR_APP'] ?? false) {
    return $df;
}

$df->run();
