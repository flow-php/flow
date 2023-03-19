<?php

declare(strict_types=1);

use function Flow\ETL\DSL\col;
use Aeon\Calendar\Stopwatch;
use Flow\ETL\DSL\CSV;
use Flow\ETL\DSL\Parquet;
use Flow\ETL\DSL\Transform;
use Flow\ETL\Flow;

require __DIR__ . '/../../../bootstrap.php';

$csvFileSize = \round(\filesize(__FLOW_OUTPUT__ . '/dataset.csv') / 1024 / 1024);
print "Converting CSV {$csvFileSize}Mb file into parquet...\n";

$stopwatch = new Stopwatch();
$stopwatch->start();
$total = 0;

(new Flow())
    ->read(CSV::from(__FLOW_OUTPUT__ . '/dataset.csv', 10_000))
    ->rows(Transform::array_unpack('row'))
    ->drop(col('row'))
    ->write(Parquet::to(__FLOW_OUTPUT__ . '/dataset_10k.parquet', 10_000))
    ->run();

$stopwatch->stop();

print "Total elapsed time: {$stopwatch->totalElapsedTime()->inSecondsPrecise()}s\n\n";

$parquetFileSize = \round(\filesize(__FLOW_OUTPUT__ . '/dataset_10k.parquet') / 1024 / 1024);
print "Output parquet file size {$parquetFileSize}Mb\n";
