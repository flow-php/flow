#!/usr/bin/env php
<?php

use Aeon\Calendar\Stopwatch;
use Flow\ETL\Async\ReactPHP\Server\SocketServer;
use Flow\ETL\Async\ReactPHP\Worker\ChildProcessLauncher;
use Flow\ETL\DSL\CSV;
use Flow\ETL\DSL\Json;
use Flow\ETL\DSL\Transform;
use Flow\ETL\Filesystem\SaveMode;
use Flow\ETL\Flow;
use Flow\ETL\Pipeline\LocalSocketPipeline;
use Monolog\Handler\StreamHandler;
use Monolog\Logger;
use Psr\Log\LogLevel;

require __DIR__ . '/../../bootstrap.php';

$dbConnection = require __DIR__ . '/../db/db_clean.php';

\putenv('FLOW_PHP_ASYNC_AUTOLOAD=' . __FLOW_AUTOLOAD__);

$logger = new Logger('server');
$logger->pushHandler(new StreamHandler('php://stdout', LogLevel::DEBUG, false));
$logger->pushHandler(new StreamHandler('php://stderr', LogLevel::ERROR, false));

$stopwatch = new Stopwatch();
$stopwatch->start();

$csvFileSize = \round(\filesize(__FLOW_OUTPUT__ . '/dataset.csv') / 1024 / 1024);
print "Loading CSV {$csvFileSize}Mb file into json file...\n";

(new Flow())
    ->read(CSV::from($path = __FLOW_OUTPUT__ . '/dataset.csv', 100_000))
    ->pipeline(
        new LocalSocketPipeline(
            SocketServer::unixDomain(__FLOW_VAR_RUN__, $logger),
            //SocketServer::tcp(6651, $logger),
            new ChildProcessLauncher(__FLOW_SRC__ . '/adapter/etl-adapter-reactphp/bin/worker-reactphp', $logger),
            $workers = 8
        )
    )
    ->rows(Transform::array_unpack('row'))
    ->drop('row')
    ->rows(Transform::to_integer('id'))
    ->rows(Transform::string_concat(['name', 'last name'], ' ', 'name'))
    ->drop('last name')
    ->mode(SaveMode::Overwrite)
    ->partitionBy('country_code')
    ->write(Json::to(__FLOW_OUTPUT__ . '/async/dataset.csv'))
    ->run();

$stopwatch->stop();

print 'Flow PHP - Elapsed time: ' . $stopwatch->totalElapsedTime()->inSecondsPrecise() . "s \n";
