<?php

declare(strict_types=1);

use function Flow\Azure\SDK\DSL\{azure_blob_service, azure_blob_service_config, azure_shared_key_authorization_factory};
use function Flow\Filesystem\DSL\fstab;
use function Flow\Filesystem\DSL\protocol;
use function Flow\ETL\Adapter\Parquet\{from_parquet, to_parquet};
use function Flow\ETL\DSL\{config_builder, data_frame, from_array, overwrite, to_output};
use function Flow\Filesystem\Bridge\Azure\DSL\azure_filesystem;
use function Flow\Filesystem\DSL\path;
use Symfony\Component\Dotenv\Dotenv;

require __DIR__ . '/vendor/autoload.php';

$fs = fstab()->for(protocol('file'));

if ($fs->status(path(__DIR__ . '/.env')) === null) {
    print 'Example skipped. Please create .env file with Azure Storage Account credentials.' . PHP_EOL;

    return;
}

$dotenv = new Dotenv();
$dotenv->load(__DIR__ . '/.env');

$azureAccount = $_ENV['AZURE_ACCOUNT'];
$azureContainer = $_ENV['AZURE_CONTAINER'];
$azureAccountKey = $_ENV['AZURE_ACCOUNT_KEY'];

if (!\is_string($azureAccount) || !\is_string($azureContainer) || !\is_string($azureAccountKey)) {
    print 'Example skipped. Azure credentials must be strings.' . PHP_EOL;

    return;
}

$config = config_builder()
    ->mount(
        azure_filesystem(
            azure_blob_service(
                azure_blob_service_config(
                    $azureAccount,
                    $azureContainer
                ),
                azure_shared_key_authorization_factory(
                    $azureAccount,
                    $azureAccountKey
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
    ->write(to_parquet(path('azure-blob://test.parquet')))
    ->run();

data_frame($config)
    ->read(from_parquet(path('azure-blob://test.parquet')))
    ->write(to_output(truncate: false))
    ->run();
