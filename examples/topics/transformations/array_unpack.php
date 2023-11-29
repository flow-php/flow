<?php

declare(strict_types=1);

use function Flow\ETL\DSL\array_entry;
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
        Row::with(int_entry('id', 1), array_entry('array', ['a' => 1, 'b' => 2, 'c' => 3])),
        Row::with(int_entry('id', 2), array_entry('array', ['d' => 4, 'e' => 5, 'f' => 6])),
    )))
    ->write(to_output(false))
    ->withEntry('unpacked', ref('array')->unpack())
    ->write(to_output(false));

if ($_ENV['FLOW_PHAR_APP'] ?? false) {
    return $df;
}

$df->run();
