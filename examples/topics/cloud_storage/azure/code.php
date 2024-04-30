<?php

declare(strict_types=1);

use function Flow\ETL\Adapter\CSV\to_csv;
use function Flow\ETL\DSL\{data_frame, from_array, overwrite};
use Flow\ETL\Adapter\Filesystem\AzureBlobStream;
use Flow\ETL\Filesystem\Path;
use Symfony\Component\Dotenv\Dotenv;

require __DIR__ . '/../../../autoload.php';

if (!\file_exists(__DIR__ . '/.env')) {
    print 'Example skipped. Please create .env file with Azure Storage Account credentials.' . PHP_EOL;

    return;
}

$dotenv = new Dotenv();
$dotenv->load(__DIR__ . '/.env');

$azure_client_option = [
    'connection-string' => $_ENV['AZURE_CONNECTION_STRING'],
    'container' => $_ENV['AZURE_CONTAINER'],
];

AzureBlobStream::register();

data_frame()
    ->read(from_array([
        ['id' => 1, 'name' => 'test'],
        ['id' => 2, 'name' => 'test'],
        ['id' => 3, 'name' => 'test'],
        ['id' => 4, 'name' => 'test'],
    ]))
    ->saveMode(overwrite())
    ->write(to_csv(new Path('flow-azure-blob://test.csv', $azure_client_option)))
    ->run();
