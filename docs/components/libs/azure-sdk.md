# Azure SDK

- [⬅️️ Back](../../introduction.md)

Simple, lightweight, dependency-free and efficient Azure SDK for PHP.

## Installation

```bash
composer require flow-php/azure-sdk
```

> [!NOTE]  
> Since the Azure SDK is not providing any http client or factories, you need to install them manually.
> The following example uses the `php-http/discovery` package to find the factories in your project existing dependencies.
> Use below links to find the implementations for client and the factories:

- [Http Client](https://packagist.org/providers/psr/http-client-implementation)
- [Http Factories](https://packagist.org/providers/psr/http-factory-implementation)

> [!TIP]
> To fully benefit from SDK features, you need to install the following packages:
> `composer require flow-php/monolog-http-bridge` that will normalize request/response objects from logs context

> [!WARNING]
> This implementation is not fully covering Azure SDK, only Storage related services are implemented. 
> Feel free to contribute to the project and add more services. 

## Usage

The absolute minimum configuration to start using the SDK is to provide the account name and the account key.

```php
<?php

use function Flow\Azure\SDK\DSL\azure_blob_service;
use function Flow\Azure\SDK\DSL\azure_blob_service_config;
use function Flow\Azure\SDK\DSL\azure_shared_key_authorization_factory;

$sdk = azure_blob_service(
    azure_blob_service_config($account, $container),
    azure_shared_key_authorization_factory($account, $accountKey),
);
```

#### Advanced

Since our goal is to not depend on any specific http client implementation, the library will use [php-http/discovery](https://packagist.org/packages/php-http/discovery)
to find the factories for the http client and the request/response objects. To gain full control over this library 
it need to be initialized like this: 

```php
<?php

use function Flow\Azure\SDK\DSL\azure_blob_service;
use function Flow\Azure\SDK\DSL\azure_blob_service_config;
use function Flow\Azure\SDK\DSL\azure_http_factory;
use function Flow\Azure\SDK\DSL\azure_shared_key_authorization_factory;
use function Flow\Azure\SDK\DSL\azure_url_factory;
use Http\Discovery\Psr17Factory;
use Http\Discovery\Psr18Client;
use Psr\Log\NullLogger;

$sdk = azure_blob_service(
    azure_blob_service_config($account, $container),
    azure_shared_key_authorization_factory($account, $accountKey),
    new Psr18Client(),
    azure_http_factory(request_factory: new Psr17Factory(), stream_factory: new Psr17Factory()),
    azure_url_factory(),
    new NullLogger(),
);
```