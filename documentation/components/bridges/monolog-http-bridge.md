---
package: flow-php/monolog-http-bridge
---

# Monolog HTTP Bridge

[PACKAGE_NAV]

[TOC]

Monolog HTTP Bridge goes through the LogRecord context and normalizes all [PSR7](https://www.php-fig.org/psr/psr-7/) `Request` and `Response` instances in it.

## Installation

For the installation instructions, read the [installation page](/documentation/installation/packages/monolog-http-bridge.md).

## Usage

To normalize Request/Response objects in Monolog, register the monolog Processor.

```php
<?php

use Flow\Bridge\Monolog\Http\Config;
use Flow\Bridge\Monolog\Http\Config\RequestConfig;
use Flow\Bridge\Monolog\Http\Config\ResponseConfig;
use Flow\Bridge\Monolog\Http\PSR7Processor;
use Monolog\Logger;
use Monolog\Handler\StreamHandler;
use Psr\Log\LogLevel;

$logger = new Logger('logs-test');
$logger->pushProcessor(
    new PSR7Processor(
        new Config(
            request: new RequestConfig(withBody: true, bodySizeLimit: 200),
            response: new ResponseConfig(withBody: true)
        )
    )
);
$logger->pushHandler(new StreamHandler(__DIR__ . '/logs.txt', LogLevel::DEBUG));
```

## Configuration

You can configure the Processor to normalize Request/Response objects in different ways.

For more details, read these classes:

 - `Flow\Bridge\Monolog\Http\Config\RequestConfig`
 - `Flow\Bridge\Monolog\Http\Config\ResponseConfig`

## Sanitization

The bridge can sanitize sensitive data in the normalized Request/Response objects. Use it to mask sensitive information such as passwords, tokens, and other credentials.

### Using Sanitizers

You can configure sanitizers for request and response objects:

```php
<?php

use Flow\Bridge\Monolog\Http\Config;
use Flow\Bridge\Monolog\Http\Config\RequestConfig;
use Flow\Bridge\Monolog\Http\Config\ResponseConfig;
use Flow\Bridge\Monolog\Http\PSR7Processor;
use function Flow\Bridge\Monolog\Http\DSL\mask;

$logger = new Logger('logs-test');
$logger->pushProcessor(
    new PSR7Processor(
        new Config(
            request: new RequestConfig(
                withBody: true,
                sanitizers: [
                    'password' => mask(), // Mask the entire value with asterisks (*)
                    'access_token' => mask('#'), // Mask the entire value with hash symbols (#)
                    'key' => mask('*', 2), // Keep the first 2 characters, mask the rest with asterisks
                ]
            ),
            response: new ResponseConfig(
                withBody: true,
                sanitizers: [
                    'credentials' => mask(), // Mask the entire value with asterisks
                    'access_token' => mask('#', 3), // Keep the first 3 characters, mask the rest with hash symbols
                ]
            )
        )
    )
);
```

### Sanitizer Types

At this time, the bridge supports these sanitizer types:

#### Mask Sanitizer

The Mask sanitizer replaces the characters of a string with a specified character. You can configure:

- `character`: the mask character (default is '*')
- `offset`: the position where the mask starts (default is 0, which masks the full string)

You can create a Mask sanitizer in two ways:

1. With the DSL function:

```php
use function Flow\Bridge\Monolog\Http\DSL\mask;

// Mask the entire value with asterisks
$sanitizer = mask();

// Mask the entire value with hash symbols
$sanitizer = mask('#');

// Keep the first 2 characters, mask the rest with asterisks
$sanitizer = mask('*', 2);
```

2. With an array configuration:

```php
$sanitizers = [
    'password' => ['type' => 'mask', 'character' => '*', 'offset' => 0],
    'access_token' => ['type' => 'mask', 'character' => '#', 'offset' => 3],
];
```

### Example

You log a request with sensitive data:

```php
$logger->info('HTTP Request', [
    'request' => $request, // PSR-7 request with sensitive data in the body
]);
```

The log shows the sensitive fields masked:

```json
{
    "username": "john_doe",
    "password": "***************",
    "email": "john@example.com",
    "access_token": "###############",
    "data": {
        "key": "se***********",
        "value": "public_value"
    }
}
```
