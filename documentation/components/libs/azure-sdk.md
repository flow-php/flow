---
package: flow-php/azure-sdk
---

# Azure SDK

[PACKAGE_NAV]

[TOC]

A small Azure SDK for PHP without dependencies.

## Installation

For the installation instructions, read the [installation page](/documentation/installation/packages/azure-sdk.md).

> [!NOTE]  
> The Azure SDK does not include an http client or factories, so you must install them yourself.
> The example below uses the `php-http/discovery` package to find the factories in the existing dependencies of your project.
> Use these links to find implementations of the client and the factories:

- [Http Client](https://packagist.org/providers/psr/http-client-implementation)
- [Http Factories](https://packagist.org/providers/psr/http-factory-implementation)

> [!TIP]
> For all SDK features, install this package:
> `composer require flow-php/monolog-http-bridge`. It normalizes the request/response objects in the log context.

> [!WARNING]
> This implementation does not cover the full Azure SDK. It implements only the Storage services.
> You can contribute to the project and add more services.

## Usage

The minimum configuration for the SDK is the account name and the account key.

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

The library does not depend on a specific http client implementation. It uses [php-http/discovery](https://packagist.org/packages/php-http/discovery)
to find the factories for the http client and the request/response objects. For full control over this library,
initialize it like this:

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