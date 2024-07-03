<?php

declare(strict_types=1);

use function Flow\Azure\SDK\DSL\{azure_blob_service, azure_blob_service_config, azure_shared_key_authorization_factory};
use function Flow\ETL\Adapter\CSV\to_csv;
use function Flow\ETL\DSL\{config_builder, data_frame, from_array, overwrite};
use function Flow\Filesystem\Bridge\Azure\DSL\azure_filesystem;
use function Flow\Filesystem\DSL\path;
use Symfony\Component\Dotenv\Dotenv;

require __DIR__ . '/../../../autoload.php';

if (!\file_exists(__DIR__ . '/.env')) {
    print 'Example skipped. Please create .env file with Azure Storage Account credentials.' . PHP_EOL;

    return;
}

$dotenv = new Dotenv();
$dotenv->load(__DIR__ . '/.env');

$config = config_builder()
    ->mount(
        azure_filesystem(
            azure_blob_service(
                azure_blob_service_config(
                    $_ENV['AZURE_ACCOUNT'],
                    $_ENV['AZURE_CONTAINER']
                ),
                azure_shared_key_authorization_factory(
                    $_ENV['AZURE_ACCOUNT'],
                    $_ENV['AZURE_ACCOUNT_KEY']
                ),
            )
        )
    );

data_frame($config)
    ->read(from_array([
        ['id' => 1, 'name' => 'test'],
        ['id' => 2, 'name' => 'test'],
        ['id' => 3, 'name' => 'test'],
        ['id' => 4, 'name' => 'test'],
    ]))
    ->saveMode(overwrite())
    ->write(to_csv(path('azure-blob://test.csv')))
    ->run();
