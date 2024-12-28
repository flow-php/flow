# Filesystem Azure Bridge

- [⬅️️ Back](../../introduction.md)

The Filesystem Azure Bridge is a bridge that allows you to use the Azure Blob Storage as a filesystem in your application.

## Installation

```bash
composer require flow-php/filesystem-azure-bridge
```

## Usage

> [!NOTE]  
> Since the Azure SDK is not providing any http client or factories, you need to install them manually.
> The following example uses the `php-http/discovery` package to find the factories in your project existing dependencies.
> Use below links to find the implementations for client and the factories:

- [Http Client](https://packagist.org/providers/psr/http-client-implementation)
- [Http Factories](https://packagist.org/providers/psr/http-factory-implementation)


```php
<?php

use Http\Discovery\Psr17FactoryDiscovery;
use Http\Discovery\Psr18ClientDiscovery;
use function Flow\Azure\SDK\DSL\azure_blob_service;
use function Flow\Azure\SDK\DSL\azure_blob_service_config;
use function Flow\Azure\SDK\DSL\azure_http_factory;
use function Flow\Azure\SDK\DSL\azure_shared_key_authorization_factory;
use function Flow\Azure\SDK\DSL\azure_url_factory;

$sdk = azure_blob_service(
    $config = azure_blob_service_config($account, $container),
    Psr18ClientDiscovery::find(),
    azure_http_factory(Psr17FactoryDiscovery::findRequestFactory(), Psr17FactoryDiscovery::findStreamFactory()),
    azure_url_factory(),
    azure_shared_key_authorization_factory($config, $accountKey),
    $logger
);
```

## Usage with Flow

To use the Azure Blob filesystem with Flow, you need to mount the filesystem to the configuration.
This operation will mount the Azure Blob filesystem to fstab instance available in the DataFrame runtime.

```php
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
    ->read(from_csv(path('azure-blob://test.csv')))
    ->write(to_stream(__DIR__ . '/output.txt', truncate: false))
    ->run();    
```