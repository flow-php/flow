
<?php

use function Flow\ETL\DSL\concat;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use Aeon\Calendar\Stopwatch;
use Flow\ETL\Adapter\Doctrine\DbalLoader;
use Flow\ETL\DSL\CSV;
use Flow\ETL\DSL\Transform;
use Flow\ETL\Flow;
use Monolog\Handler\StreamHandler;
use Monolog\Logger;

require __DIR__ . '/bootstrap.php';

$dbConnection = require __DIR__ . '/../db/db_clean.php';

$logger = new Logger('server');
$logger->pushHandler(new StreamHandler(__FLOW_VAR__ . '/logs/server.log', Logger::DEBUG));
$logger->pushHandler(new StreamHandler(__FLOW_VAR__ . '/logs/server_error.log', Logger::ERROR, false));

$stopwatch = new Stopwatch();
$stopwatch->start();

$csvFileSize = \round(\filesize(__FLOW_OUTPUT__ . '/dataset.csv') / 1024 / 1024);
print "Loading CSV {$csvFileSize}Mb file into postgresql...\n";

(new Flow())
    ->read(CSV::from($path = __FLOW_OUTPUT__ . '/dataset.csv', 10_000))
    ->withEntry('unpacked', ref('row')->unpack())
    ->renameAll('unpacked.', '')
    ->drop('row')
    ->rows(Transform::to_integer('id'))
    ->withEntry('name', concat(ref('name'), lit(' '), ref('last name')))
    ->drop('last name')
    ->load(DbalLoader::fromConnection($dbConnection, 'flow_dataset_table', 1000))
    ->run();

$stopwatch->stop();

print 'Flow PHP - Elapsed time: ' . $stopwatch->totalElapsedTime()->inSecondsPrecise() . "s \n";
$dbRows = \current($dbConnection->executeQuery('SELECT COUNT(*) FROM flow_dataset_table')->fetchNumeric());
print "Total inserted rows: {$dbRows}\n";
