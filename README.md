# ETL - Asynchronous Pipeline ReactPHP Implementation

This repository provides implementation for [flow-php/etl-async](https://github.com/flow-php/etl-async) based on [ReactPHP](https://reactphp.org/) components.

Working example:

```php
<?php

use Flow\ETL\Async\LocalPipeline;
use Flow\ETL\Async\ReactPHP\Worker\ChildProcessLauncher;
use Flow\ETL\Async\ReactPHP\Server\TCPServer;
use Flow\ETL\ETL;
use Flow\ETL\Transformer\ArrayUnpackTransformer;
use Flow\ETL\Transformer\Cast\CastToInteger;
use Flow\ETL\Transformer\CastTransformer;
use Flow\ETL\Transformer\RemoveEntriesTransformer;
use Flow\ETL\Transformer\Rename\EntryRename;
use Flow\ETL\Transformer\RenameEntriesTransformer;
use Flow\ETL\Transformer\StringConcatTransformer;
use Monolog\Handler\StreamHandler;
use Monolog\Logger;

$extractor = new SomeComplexExtractor();

$logger = new Logger('server');
$logger->pushHandler(new StreamHandler(__DIR__ . '/var/logs/error.log', Logger::ERROR, false));

$pipeline = new LocalPipeline(
    new TCPServer($port = 6651, $logger),
    new ChildProcessLauncher(__DIR__ . "/bin/worker", $port, $logger),
    $workers = 6
);

ETL::extract($extractor, $pipeline)
    ->transform(new ArrayUnpackTransformer('row'))
    ->transform(new RemoveEntriesTransformer('row'))
    ->transform(new CastTransformer(new CastToInteger(['id'])))
    ->transform(new StringConcatTransformer(['name', 'last name'], ' ', '_name'))
    ->transform(new RemoveEntriesTransformer('name', 'last name'))
    ->transform(new RenameEntriesTransformer(new EntryRename('_name', 'name')))
    ->run();
```

## Worker CLI

In most cases [bin/worker-reactphp](bin/worker-reactphp) should work just fine, however sometimes you might
want to add some custom logic to the worker, in that case you need to create your own worker script. 

Please find minimalistic loader worker example below: 

`my-worker.php`

```php
#!/usr/bin/env php
<?php

use Flow\ETL\Async\ReactPHP\Worker\TCPClient;
use Flow\ETL\Async\Client\CLI;
use Flow\ETL\Async\Client\CLI\Input;
use Flow\Serializer\CompressingSerializer;
use Flow\Serializer\NativePHPSerializer;
use Monolog\Handler\StreamHandler;
use Monolog\Logger;
use Psr\Log\LogLevel;

require __DIR__ . "/vendor/autoload.php";

$logger = new Logger('worker');
$logger->pushHandler(new StreamHandler(__DIR__ . "/var/logs/worker.log", LogLevel::DEBUG));

$serializer = new CompressingSerializer(new NativePHPSerializer());

$cli = new CLI($logger, new TCPClient($logger, $serializer));

exit($cli->run(new Input($argv)));
```

Don't forget to setup proper permissions by:

```
chmod +x my-worker
```