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
