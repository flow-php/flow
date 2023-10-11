<?php declare(strict_types=1);

if ($_ENV['FLOW_PHAR_APP'] ?? false) {
    print "This example cannot be run in PHAR, please use CLI approach.\n";

    exit(1);
}

use function Flow\ETL\DSL\concat;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use Aeon\Calendar\Stopwatch;
use Flow\ETL\Adapter\Doctrine\Order;
use Flow\ETL\Adapter\Doctrine\OrderBy;
use Flow\ETL\DSL\Dbal;
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
    ->withEntry('unpacked', ref('row')->unpack())
    ->renameAll('unpacked.', '')
    ->drop('row')
    ->withEntry('id', ref('id')->cast('int'))
    ->withEntry('name', concat(ref('name'), lit(' '), ref('last name')))
    ->drop('last_name')
    ->write(Dbal::to_table_insert($dbConnection, 'flow_dataset_table'))
    ->run();

$stopwatch->stop();

print 'Flow PHP - Elapsed time: ' . $stopwatch->totalElapsedTime()->inSecondsPrecise() . "s \n";
$dbRows = \current($dbConnection->executeQuery('SELECT COUNT(*) FROM flow_dataset_table')->fetchNumeric());
print "Total inserted rows: {$dbRows}\n";
