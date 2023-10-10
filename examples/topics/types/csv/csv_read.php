<?php

declare(strict_types=1);

use function Flow\ETL\DSL\col;
use function Flow\ETL\DSL\ref;
use Aeon\Calendar\Stopwatch;
use Flow\ETL\DSL\CSV;
use Flow\ETL\Flow;

require __DIR__ . '/../../../bootstrap.php';

$csvFileSize = \round(\filesize(__FLOW_OUTPUT__ . '/dataset.csv') / 1024 / 1024);
print "Reading CSV {$csvFileSize}Mb file...\n";

$stopwatch = new Stopwatch();
$stopwatch->start();
$total = 0;

(new Flow())
    ->read(CSV::from(__FLOW_OUTPUT__ . '/dataset.csv', 1000))
    ->withEntry('unpacked', ref('row')->unpack())
    ->renameAll('unpacked.', '')
    ->drop(col('row'))
    ->limit(10_000)
    ->run();

$stopwatch->stop();

print "Total elapsed time: {$stopwatch->totalElapsedTime()->inSecondsPrecise()}s\n";
