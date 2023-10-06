# ETL Adapter: [ReactPHP](https://reactphp.org/)

Flow PHP's Adapter ReactPHP is a distinguished library, meticulously designed to integrate the asynchronous capabilities
of ReactPHP with Flow PHP’s ETL (Extract, Transform, Load) processes. This adapter is instrumental for developers
striving to conduct non-blocking data operations, thus ensuring optimal performance and responsiveness in data
transformation workflows. By utilizing the Adapter ReactPHP library, developers can leverage a robust suite of features
tailored for handling asynchronous data operations with precision, thereby simplifying complex data transformations
while boosting operational efficiency. The Adapter ReactPHP library encapsulates a rich set of functionalities,
providing a streamlined API for managing asynchronous tasks, which is vital in modern data processing and transformation
landscapes. This library signifies Flow PHP’s dedication to offering versatile and efficient data processing solutions,
making it an ideal choice for developers dealing with asynchronous operations in large-scale and data-intensive
environments. With Flow PHP's Adapter ReactPHP, embracing asynchronous data processing within your ETL workflows becomes
a seamless and efficient endeavor, harmoniously aligning with the adaptable and robust framework of the Flow PHP
ecosystem.

Following communication protocols are supported: 

- TCP/IP (only local) - `127.0.0.1:6651`
- Unix Domain Socket - `uinx:///var/run/etl.sock`

## Installation

```
composer require flow-php/etl-adapter-reactphp:1.x@dev
```

Working example:

```php
<?php

use function Flow\ETL\DSL\concat;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use Aeon\Calendar\Stopwatch;
use Doctrine\DBAL\DriverManager;
use Doctrine\DBAL\Schema\Column;
use Doctrine\DBAL\Schema\Table;
use Doctrine\DBAL\Types\Type;
use Doctrine\DBAL\Types\Types;
use Flow\ETL\Adapter\CSV\League\CSVExtractor;
use Flow\ETL\Adapter\Doctrine\DbalLoader;
use Flow\ETL\Cache\InMemoryCache;
use Flow\ETL\Config;
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

$stopwatch = new Stopwatch();
$stopwatch->start();

$memory = new Consumption();

(new Flow)
    ->read(new CSVExtractor(
        $path = __DIR__ . '/data/dataset.csv',
        10_000,
        0
    ))
    ->pipeline(
        new LocalSocketPipeline(
            SocketServer::unixDomain(__DIR__ . "/var/run/", $logger),
            new ChildProcessLauncher(__DIR__ . "/vendor/bin/worker-reactphp", $logger),
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

This adapter comes with built-in [worker](bin/worker-reactphp) CLI application
but feel free to create custom.
Customization of the works will let you adjust logger or autoloader.
