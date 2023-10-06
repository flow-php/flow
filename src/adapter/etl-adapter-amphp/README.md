# Flow Adapter: [AMP](https://amphp.org/) 

Flow PHP's Adapter AmpHP is a meticulously crafted library aimed at integrating the asynchronous capabilities of AmpHP
within your ETL (Extract, Transform, Load) workflows. This adapter is instrumental for developers aspiring to execute
non-blocking data operations, thereby optimizing performance and responsiveness in their data transformation workflows.
By leveraging the Adapter AmpHP library, developers can access a robust suite of features engineered for precise
asynchronous data operations, simplifying complex data transformations while enhancing operational efficiency. The
Adapter AmpHP library encapsulates a rich set of functionalities, offering a streamlined API for managing asynchronous
tasks, which is indispensable in modern data processing and transformation landscapes. This library mirrors Flow PHP's
commitment to delivering versatile and efficient data processing solutions, making it a superior choice for developers
dealing with asynchronous operations in large-scale and data-intensive environments. With Flow PHP's Adapter AmpHP,
embracing asynchronous data processing within your ETL workflows becomes a seamless and efficient endeavor, aligning
harmoniously with the robust and adaptable framework of the Flow PHP ecosystem.

Following communication protocols are supported:

- TCP/IP (only local) - `127.0.0.1:6651`
- Unix Domain Socket - `uinx:///var/run/etl.sock`

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

## Contributing

This repo is **READ ONLY**, in order to contribute to Flow PHP project, please
open PR against [flow](https://github.com/flow-php/flow) monorepo.

Changes merged to monorepo are automatically propagated into sub repositories.
