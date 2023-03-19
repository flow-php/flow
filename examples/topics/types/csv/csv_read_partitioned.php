<?php

declare(strict_types=1);

use function Flow\ETL\DSL\col;
use function Flow\ETL\DSL\ref;
use Aeon\Calendar\Stopwatch;
use Flow\ETL\DSL\CSV;
use Flow\ETL\DSL\Partitions;
use Flow\ETL\DSL\To;
use Flow\ETL\DSL\Transform;
use Flow\ETL\Flow;

require __DIR__ . '/../../../bootstrap.php';

print "Reading partitioned CSV dataset...\n";

$stopwatch = new Stopwatch();
$stopwatch->start();
$total = 0;

(new Flow())
    ->read(CSV::from(__FLOW_DATA__ . '/partitioned'))
    ->rows(Transform::array_unpack(\col('row')))
    ->drop(col('row'))
    ->collect()
    ->sortBy(ref('id'))
    ->write(To::output())
    ->run();

$stopwatch->lap();

print "Total elapsed time: {$stopwatch->elapsedTime(1)->inSecondsPrecise()}s\n\n";

print "Reading partitioned CSV dataset with partition filtering...\n";

(new Flow())
    ->read(CSV::from(__FLOW_DATA__ . '/partitioned'))
    ->rows(Transform::array_unpack('row'))
    ->drop('row')
    ->collect()
    ->filterPartitions(Partitions::only('t_shirt_color', 'green'))
    ->sortBy(col('id'))
    ->write(To::output())
    ->run();

$stopwatch->lap();

print "Total elapsed time: {$stopwatch->elapsedTime(2)->inSecondsPrecise()}s\n";

$stopwatch->stop();
