<?php

declare(strict_types=1);

/**
 * Compare reading CSV, Parquet files, 100k rows in batch.
 */

use Aeon\Calendar\Stopwatch;
use Flow\ETL\DSL\CSV;
use Flow\ETL\DSL\Parquet;
use Flow\ETL\DSL\To;
use Flow\ETL\DSL\Transform;
use Flow\ETL\Flow;
use Flow\ETL\Row;

require __DIR__ . '/../vendor/autoload.php';

// CVS

$csvFileSize = \round(\filesize(__DIR__ . '/output/dataset.csv') / 1024 / 1024);
print "Reading CSV file: {$csvFileSize}Mb...\n";
$stopwatch = new Stopwatch();
$stopwatch->start();

(new Flow())
        ->read(CSV::from(__DIR__ . '/output/dataset.csv', 100_000))
        ->rows(Transform::array_unpack('row'))
        ->drop('row')
        ->filter(function (Row $r) {
            return (int) $r->valueOf('id') === 987512;
        })
        ->write(To::output())
        ->run();

$stopwatch->stop();

print "Total reading CSV: {$stopwatch->totalElapsedTime()->inSecondsPrecise()}s\n\n";

// PARQUET

$parquetFileSize = \round(\filesize(__DIR__ . '/data/dataset_100k.parquet') / 1024 / 1024);
print "Reading Parquet file: {$parquetFileSize}Mb...\n";

$stopwatch = new Stopwatch();
$stopwatch->start();

(new Flow())
    ->read(Parquet::from(__DIR__ . '/data/dataset_100k.parquet', 'row', ['id']))
    ->rows(Transform::array_unpack('row'))
    ->drop('row')
    ->filter(function (Row $r) {
        return (int) $r->valueOf('id') === 987512;
    })
    ->write(To::output())
    ->run();

$stopwatch->stop();

print "Total reading Parquet: {$stopwatch->totalElapsedTime()->inSecondsPrecise()}s\n\n";
