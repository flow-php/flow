# ETL - Asynchronous Pipeline ReactPHP Implementation

This repository provides implementation for [flow-php/etl-async](https://github.com/flow-php/etl-async) based on [ReactPHP](https://reactphp.org/) components.

Working example:

```php
<?php

use Flow\ETL\Async\LocalPipeline;
use Flow\ETL\Async\ReactPHP\Worker\ChildProcessLauncher;
use Flow\ETL\Async\ReactPHP\Server\TCPServer;
use Flow\ETL\ETL;
use Flow\ETL\Loader;
use Flow\ETL\Rows;
use Flow\ETL\Transformer\ArrayUnpackTransformer;
use Flow\ETL\Transformer\Cast\CastToInteger;
use Flow\ETL\Transformer\CastTransformer;
use Flow\ETL\Transformer\RemoveEntriesTransformer;
use Flow\ETL\Transformer\Rename\EntryRename;
use Flow\ETL\Transformer\RenameEntriesTransformer;
use Flow\ETL\Transformer\StringConcatTransformer;
use Monolog\Handler\StreamHandler;
use Monolog\Logger;

return new class implements Extractor {
    public function extract(): \Generator
    {
        $rows = [];
        for ($i = 0; $i <= 10_000_000; $i++) {
            $rows[] = Row::create(
                new ArrayEntry(
                    'row', ['id' => $i, 'name' => 'Name', 'last name' => 'Last Name', 'phone' => '123 123 123']
                ),
            );

            if (\count($rows) >= 1000) {
                echo "extracted " . $i . "\n ";
                yield new Rows(...$rows);

                $rows = [];
            }
        }

        if (\count($rows) >= 0) {
            yield new Rows(...$rows);
        }
    }
};


$logger = new Logger('server');
$logger->pushHandler(new StreamHandler(__DIR__ . '/var/logs/server.log', Logger::DEBUG));
$logger->pushHandler(new StreamHandler(__DIR__ . '/var/logs/server_error.log', Logger::ERROR, false));

$pipeline = new LocalPipeline(
    new TCPServer($port = 6651, $logger),
    new ChildProcessLauncher(__DIR__ . "/bin/worker", $port, $logger),
    $workers = 10
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