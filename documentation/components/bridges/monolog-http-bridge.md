# Monolog HTTP Bridge

- [⬅️️ Back](../../introduction.md)

Monolog HTTP Bridge is a package that iterates through LogRecord context and normalizes all instances of `Request` and `Response` from [PSR7](https://www.php-fig.org/psr/psr-7/).

## Installation

```
composer require flow-php/monolog-http-bridge
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
