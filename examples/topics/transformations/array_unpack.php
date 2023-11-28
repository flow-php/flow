<?php

declare(strict_types=1);

use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\read;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\to_output;
use Flow\ETL\DSL\Entry;
use Flow\ETL\Row;
use Flow\ETL\Rows;

require __DIR__ . '/../../bootstrap.php';

$df = read(
    from_rows(new Rows(
        Row::with(Entry::int('id', 1), Entry::array('array', ['a' => 1, 'b' => 2, 'c' => 3])),
        Row::with(Entry::int('id', 2), Entry::array('array', ['d' => 4, 'e' => 5, 'f' => 6])),
    ))
)
    ->write(to_output(false))
    ->withEntry('unpacked', ref('array')->unpack())
    ->write(to_output(false));

if ($_ENV['FLOW_PHAR_APP'] ?? false) {
    return $df;
}

$df->run();
