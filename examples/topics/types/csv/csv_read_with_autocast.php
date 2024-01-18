<?php

declare(strict_types=1);

use function Flow\ETL\Adapter\CSV\from_csv;
use function Flow\ETL\DSL\to_output;
use Aeon\Calendar\Stopwatch;
use Flow\ETL\Flow;
use Flow\ETL\Loader\StreamLoader\Output;

require __DIR__ . '/../../../bootstrap.php';

if (!\file_exists(__FLOW_OUTPUT__ . '/dataset.csv')) {
    include __DIR__ . '/../../../setup/php_to_csv.php';
}

$flow = (new Flow())
    ->read(from_csv(__FLOW_OUTPUT__ . '/dataset.csv'))
    ->limit(1000)
    ->autoCast()
    ->collect()
    ->write(to_output(false, Output::rows_and_schema));

if ($_ENV['FLOW_PHAR_APP'] ?? false) {
    return $flow;
}

$csvFileSize = \round(\filesize(__FLOW_OUTPUT__ . '/dataset.csv') / 1024 / 1024);
print "Reading CSV {$csvFileSize}Mb file...\n";

$stopwatch = new Stopwatch();
$stopwatch->start();

$flow->run();

$stopwatch->stop();

print "Total elapsed time: {$stopwatch->totalElapsedTime()->inSecondsPrecise()}s\n";
