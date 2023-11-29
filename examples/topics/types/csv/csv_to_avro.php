<?php

declare(strict_types=1);

use function Flow\ETL\Adapter\CSV\from_csv;
use function Flow\ETL\DSL\Adapter\Avro\to_avro;
use Aeon\Calendar\Stopwatch;
use Flow\ETL\Flow;

require __DIR__ . '/../../../bootstrap.php';

if (!\file_exists(__FLOW_OUTPUT__ . '/dataset.csv')) {
    include __DIR__ . '/../../../setup/php_to_csv.php';
}

$flow = (new Flow())
    ->read(from_csv(__FLOW_OUTPUT__ . '/dataset.csv'))
    ->rename('last name', 'last_name')
    ->write(to_avro(__FLOW_OUTPUT__ . '/dataset.avro'));

if ($_ENV['FLOW_PHAR_APP'] ?? false) {
    return $flow;
}

$csvFileSize = \round(\filesize(__FLOW_OUTPUT__ . '/dataset.csv') / 1024 / 1024);
print "Converting CSV {$csvFileSize}Mb file into avro...\n";

$stopwatch = new Stopwatch();
$stopwatch->start();

$flow->run();

$stopwatch->stop();

print "Total elapsed time: {$stopwatch->totalElapsedTime()->inSecondsPrecise()}s\n\n";

$arvoFileSize = \round(\filesize(__FLOW_OUTPUT__ . '/dataset.avro') / 1024 / 1024);
print "Output avro file size {$arvoFileSize}Mb\n";
