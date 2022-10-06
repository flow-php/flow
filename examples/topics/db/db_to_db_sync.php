<?php declare(strict_types=1);

use Aeon\Calendar\Stopwatch;
use Flow\ETL\Adapter\Doctrine\Order;
use Flow\ETL\Adapter\Doctrine\OrderBy;
use Flow\ETL\DSL\Dbal;
use Flow\ETL\DSL\Transform;
use Flow\ETL\Flow;

require __DIR__ . '/../../bootstrap.php';

// target db connection
$dbConnection = require __DIR__ . '/db_clean.php';

// source db connection
print "Loading source data into postgresql...\n";
[$sourceDbConnection, $rows] = require __DIR__ . '/db_source.php';

$stopwatch = new Stopwatch();
$stopwatch->start();

print "Loading {$rows} rows into postgresql...\n";

(new Flow())
    ->read(
        Dbal::from_limit_offset(
            $sourceDbConnection,
            'source_dataset_table',
            new OrderBy('id', Order::DESC)
        )
    )
    ->rows(Transform::array_unpack('row'))
    ->drop('row')
    ->rows(Transform::to_integer('id'))
    ->rows(Transform::string_concat(['name', 'last_name'], ' ', 'name'))
    ->drop('last_name')
    ->write(Dbal::to_table_insert($dbConnection, 'flow_dataset_table'))
    ->run();

$stopwatch->stop();

print 'Flow PHP - Elapsed time: ' . $stopwatch->totalElapsedTime()->inSecondsPrecise() . "s \n";
$dbRows = \current($dbConnection->executeQuery('SELECT COUNT(*) FROM flow_dataset_table')->fetchNumeric());
print "Total inserted rows: {$dbRows}\n";
