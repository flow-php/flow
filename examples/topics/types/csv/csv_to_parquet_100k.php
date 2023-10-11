<?php

declare(strict_types=1);

use function Flow\ETL\DSL\col;
use function Flow\ETL\DSL\ref;
use Aeon\Calendar\Stopwatch;
use Flow\ETL\DSL\CSV;
use Flow\ETL\DSL\Parquet;
use Flow\ETL\Flow;

require __DIR__ . '/../../../bootstrap.php';

$flow = (new Flow())
    ->read(CSV::from(__FLOW_OUTPUT__ . '/dataset.csv', 10_000))
    ->withEntry('unpacked', ref('row')->unpack())
    ->renameAll('unpacked.', '')
    ->drop(col('row'))
    ->write(Parquet::to(__FLOW_OUTPUT__ . '/dataset_100k.parquet', 100_000));

if ($_ENV['FLOW_PHAR_APP'] ?? false) {
    return $flow;
}

$csvFileSize = \round(\filesize(__FLOW_OUTPUT__ . '/dataset.csv') / 1024 / 1024);
print "Converting CSV {$csvFileSize}Mb file into parquet...\n";

$stopwatch = new Stopwatch();
$stopwatch->start();

$flow->run();

$stopwatch->stop();

print "Total elapsed time: {$stopwatch->totalElapsedTime()->inSecondsPrecise()}s\n";

$csvFileSize = \round(\filesize(__FLOW_OUTPUT__ . '/dataset_100k.parquet') / 1024 / 1024);
print "Output parquet file size {$csvFileSize}Mb\n";
