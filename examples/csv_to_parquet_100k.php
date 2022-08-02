<?php

declare(strict_types=1);

use Aeon\Calendar\Stopwatch;
use Flow\ETL\DSL\CSV;
use Flow\ETL\DSL\Parquet;
use Flow\ETL\DSL\Transform;
use Flow\ETL\Flow;

require __DIR__ . '/../vendor/autoload.php';

$csvFileSize = \round(\filesize(__DIR__ . '/output/dataset.csv') / 1024 / 1024);
print "Converting CSV {$csvFileSize}Mb file into parquet...\n";

$stopwatch = new Stopwatch();
$stopwatch->start();
$total = 0;

(new Flow())
    ->read(CSV::from(__DIR__ . '/output/dataset.csv', 10_000))
    ->rows(Transform::array_unpack('row'))
    ->drop('row')
    ->write(Parquet::to(__DIR__ . '/output/dataset_100k.parquet', 100_000))
    ->run();

$stopwatch->stop();

print "Total elapsed time: {$stopwatch->totalElapsedTime()->inSecondsPrecise()}s\n";

$csvFileSize = \round(\filesize(__DIR__ . '/output/dataset_100k.parquet') / 1024 / 1024);
print "Output parquet file size {$csvFileSize}Mb\n";
