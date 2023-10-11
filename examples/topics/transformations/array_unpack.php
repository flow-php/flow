<?php

declare(strict_types=1);

use function Flow\ETL\DSL\ref;
use Flow\ETL\DSL\Entry;
use Flow\ETL\DSL\From;
use Flow\ETL\DSL\To;
use Flow\ETL\Flow;
use Flow\ETL\Row;
use Flow\ETL\Rows;

require __DIR__ . '/../../bootstrap.php';

$flow = (new Flow())
    ->read(
        From::rows(new Rows(
            Row::with(Entry::int('id', 1), Entry::array('array', ['a' => 1, 'b' => 2, 'c' => 3])),
            Row::with(Entry::int('id', 2), Entry::array('array', ['d' => 4, 'e' => 5, 'f' => 6])),
        ))
    )
    ->write(To::output(false))
    ->withEntry('unpacked', ref('array')->unpack())
    ->write(To::output(false));

if ('' !== \Phar::running(false)) {
    return $flow;
}

$flow->run();
