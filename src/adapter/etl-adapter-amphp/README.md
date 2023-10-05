# Flow Adapter: [AMP](https://amphp.org/) 

This adapter providers async local pipeline server/worker elements implemented on [amphp](https://amphp.org/).

Following communication protocols are supported:

- TCP/IP (only local) - `127.0.0.1:6651`
- Unix Domain Socket - `uinx:///var/run/etl.sock`

# Contributing

This repo is **READ ONLY**, in order to contribute to Flow PHP project, please 
open PR against [flow](https://github.com/flow-php/flow) monorepo. 

Changes merged to monorepo are automatically propagated into sub repositories.

# Installation

```
composer require flow-php/etl-adapter-amphp:1.x@dev
```

Example usage: 

```php
<?php

use function Flow\ETL\DSL\concat;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use Flow\ETL\Adapter\CSV\League\CSVExtractor;
use Flow\ETL\Adapter\Doctrine\DbalLoader;
use Flow\ETL\Monitoring\Memory\Consumption;
use Flow\ETL\Pipeline\LocalSocketPipeline;
use Flow\ETL\Async\ReactPHP\Worker\ChildProcessLauncher;
use Flow\ETL\Async\ReactPHP\Server\SocketServer;
use Flow\ETL\DSL\Transform;
use Flow\ETL\Flow;
use Monolog\Handler\StreamHandler;
use Monolog\Logger;
use Psr\Log\LogLevel;

require __DIR__ . '/vendor/autoload.php';

$logger = new Logger('server');
$logger->pushHandler(new StreamHandler("php://stdout", LogLevel::DEBUG, false));
$logger->pushHandler(new StreamHandler("php://stderr", LogLevel::ERROR, false));

(new Flow)
    ->read(new CSVExtractor(
        $path = __DIR__ . '/data/dataset.csv',
        10_000,
        0
    ))
    ->pipeline(
        new LocalSocketPipeline(
            SocketServer::unixDomain(__DIR__ . "/var/run/", $logger),
            new ChildProcessLauncher(__DIR__ . "/vendor/bin/worker-amp", $logger),
            $workers = 8
        )
    )
    ->rows(Transform::array_unpack('row'))
    ->drop('row')
    ->withEntry('id', ref('id')->cast('int'))
    ->withEntry('name', concat(ref('name'), lit(' '), ref('last name')))
    ->drop('last_name')
    ->load(new DbalLoader($tableName, $chunkSize = 1000, $dbConnectionParams))
    ->run();
```

This adapter comes with built-in [worker](bin/worker-amp) CLI application
but feel free to create custom.
Customization of the works will let you adjust logger or autoloader. 
