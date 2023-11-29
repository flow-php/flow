<?php

declare(strict_types=1);

use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\to_output;
use Flow\ETL\Row;
use Flow\ETL\Rows;

require __DIR__ . '/../../bootstrap.php';

$df = df()
    ->read(from_rows(new Rows(
        Row::create(int_entry('a', 100), int_entry('b', 200))
    )))
    ->write(to_output(false))
    ->withEntry('c', ref('a')->plus(ref('b')))
    ->withEntry('d', ref('b')->minus(ref('a')))
    ->write(to_output(false));

if ($_ENV['FLOW_PHAR_APP'] ?? false) {
    return $df;
}

$df->run();
