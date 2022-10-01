#!/usr/bin/env php
<?php

use Aeon\Calendar\Stopwatch;
use Flow\ETL\Async\ReactPHP\Server\SocketServer;
use Flow\ETL\Async\ReactPHP\Worker\ChildProcessLauncher;
use Flow\ETL\DSL\CSV;
use Flow\ETL\DSL\Text;
use Flow\ETL\Filesystem\SaveMode;
use Flow\ETL\Flow;
use Flow\ETL\Pipeline\LocalSocketPipeline;
use Monolog\Handler\StreamHandler;
use Monolog\Logger;
use Psr\Log\LogLevel;

require __DIR__ . '/../vendor/autoload.php';

if (!\is_dir(__DIR__ . '/var')) {
    \mkdir(__DIR__ . '/var');
}

if (!\is_dir(__DIR__ . '/var/run/')) {
    \mkdir(__DIR__ . '/var/run/');
}

\putenv('FLOW_PHP_ASYNC_AUTOLOAD=' . __DIR__ . '/../vendor/autoload.php');

$logger = new Logger('server');
$logger->pushHandler(new StreamHandler('php://stdout', LogLevel::DEBUG, false));
$logger->pushHandler(new StreamHandler('php://stderr', LogLevel::ERROR, false));

$stopwatch = new Stopwatch();
$stopwatch->start();

$csvFileSize = \round(\filesize(__DIR__ . '/output/dataset.csv') / 1024 / 1024);
print "Loading CSV {$csvFileSize}Mb file into json file...\n";

(new Flow())
    ->read(CSV::from($path = __DIR__ . '/output/dataset.csv', 100_000))
    ->pipeline(
        new LocalSocketPipeline(
            SocketServer::unixDomain(__DIR__ . '/var/run/', $logger),
            //SocketServer::tcp(6651, $logger),
            new ChildProcessLauncher(__DIR__ . '/../src/adapter/etl-adapter-reactphp/bin/worker-reactphp', $logger),
            $workers = 8
        )
    )
    ->mode(SaveMode::Overwrite)
    ->write(Text::to(__DIR__ . '/output/async/dataset.txt'))
    ->run();

$stopwatch->stop();

print 'Flow PHP - Elapsed time: ' . $stopwatch->totalElapsedTime()->inSecondsPrecise() . "s \n";
