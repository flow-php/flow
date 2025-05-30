# Monolog HTTP Bridge

- [⬅️️ Back](../../introduction.md)
- [📚API Reference](/documentation/api/bridge/monolog/http)
- [📁Files](/documentation/api/bridge/monolog/http/indices/files.html)

Monolog HTTP Bridge is a package that iterates through LogRecord context and normalizes all instances of `Request` and `Response` from [PSR7](https://www.php-fig.org/psr/psr-7/).

## Installation

```
composer require flow-php/monolog-http-bridge:~--FLOW_PHP_VERSION--
```

## Usage

To normalize Request/Response objects in Monolog you need to register monolog Processor. 

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

The Processor can be configured to normalize Request/Response objects in different ways.

For more details, please refer to the following classes: 

 - `Flow\Bridge\Monolog\Http\Config\RequestConfig`
 - `Flow\Bridge\Monolog\Http\Config\ResponseConfig`

## Sanitization

The bridge provides a way to sanitize sensitive data in the normalized Request/Response objects. This is useful for masking sensitive information like passwords, tokens, and other credentials.

### Using Sanitizers

You can configure sanitizers for both request and response objects:

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

Currently, the bridge supports the following sanitizer types:

#### Mask Sanitizer

The Mask sanitizer replaces characters in a string with a specified character. You can configure:

- `character`: The character used for masking (default is '*')
- `offset`: The position from which to start masking (default is 0, meaning mask the entire string)

You can create a Mask sanitizer in two ways:

1. Using the DSL function:

```php
use function Flow\Bridge\Monolog\Http\DSL\mask;

// Mask the entire value with asterisks
$sanitizer = mask();

// Mask the entire value with hash symbols
$sanitizer = mask('#');

// Keep the first 2 characters, mask the rest with asterisks
$sanitizer = mask('*', 2);
```

2. Using an array configuration:

```php
$sanitizers = [
    'password' => ['type' => 'mask', 'character' => '*', 'offset' => 0],
    'access_token' => ['type' => 'mask', 'character' => '#', 'offset' => 3],
];
```

### Example

When you log a request with sensitive data:

```php
$logger->info('HTTP Request', [
    'request' => $request, // PSR-7 request with sensitive data in the body
]);
```

The sensitive fields will be masked in the log:

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
