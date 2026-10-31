<?php

declare(strict_types=1);

use function Flow\Azure\SDK\DSL\{azure_blob_service, azure_blob_service_config, azure_shared_key_authorization_factory};
use function Flow\Filesystem\DSL\fstab;
use function Flow\ETL\Adapter\Parquet\{from_parquet, to_parquet};
use function Flow\ETL\DSL\{data_frame, from_array, overwrite, to_output};
use function Flow\Filesystem\Bridge\Azure\DSL\azure_filesystem;
use function Flow\Filesystem\DSL\path;
use Symfony\Component\Dotenv\Dotenv;

require __DIR__ . '/vendor/autoload.php';

$fs = fstab()->for('file');

if ($fs->status(path(__DIR__ . '/.env')) === null) {
    print 'Example skipped. Please create .env file with Azure Storage Account credentials.' . PHP_EOL;

    return;
}

$dotenv = new Dotenv();
$dotenv->load(__DIR__ . '/.env');

$azureAccount = $_ENV['AZURE_ACCOUNT'] ?? null;
$azureContainer = $_ENV['AZURE_CONTAINER'] ?? null;
$azureAccountKey = $_ENV['AZURE_ACCOUNT_KEY'] ?? null;

if ($azureAccount === null || $azureContainer === null || $azureAccountKey === null) {
    print 'Example skipped. Please set AZURE_ACCOUNT, AZURE_CONTAINER and AZURE_ACCOUNT_KEY in the .env file.' . PHP_EOL;

    return;
}

$azure = azure_filesystem(
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
);

data_frame()
    ->read(from_array([
        ['id' => 1, 'name' => 'test'],
        ['id' => 2, 'name' => 'test'],
        ['id' => 3, 'name' => 'test'],
        ['id' => 4, 'name' => 'test'],
    ]))
    ->write(to_parquet(path('azure-blob://test.parquet'), filesystem: $azure)->saveMode(overwrite()))
    ->run();

data_frame()
    ->read(from_parquet(path('azure-blob://test.parquet'), filesystem: $azure))
    ->write(to_output(truncate: false))
    ->run();
