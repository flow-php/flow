<?php

declare(strict_types=1);

use function Flow\ETL\DSL\from_csv;
use function Flow\ETL\DSL\to_parquet;
use Aeon\Calendar\Stopwatch;
use Flow\ETL\Flow;

require __DIR__ . '/../../../bootstrap.php';

if (!\file_exists(__FLOW_OUTPUT__ . '/dataset.csv')) {
    include __DIR__ . '/../../../setup/php_to_csv.php';
}

$flow = (new Flow())
    ->read(from_csv(__FLOW_OUTPUT__ . '/dataset.csv'))
    ->write(to_parquet(__FLOW_OUTPUT__ . '/dataset_10k.parquet'));

if ($_ENV['FLOW_PHAR_APP'] ?? false) {
    return $flow;
}

$csvFileSize = \round(\filesize(__FLOW_OUTPUT__ . '/dataset.csv') / 1024 / 1024);
print "Converting CSV {$csvFileSize}Mb file into parquet...\n";

$stopwatch = new Stopwatch();
$stopwatch->start();

$flow->run();

$stopwatch->stop();

print "Total elapsed time: {$stopwatch->totalElapsedTime()->inSecondsPrecise()}s\n\n";

$parquetFileSize = \round(\filesize(__FLOW_OUTPUT__ . '/dataset_10k.parquet') / 1024 / 1024);
print "Output parquet file size {$parquetFileSize}Mb\n";
