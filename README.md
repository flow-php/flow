# ETL Adapter: HTTP

[![Minimum PHP Version](https://img.shields.io/badge/php-%3E%3D%207.4-8892BF.svg)](https://php.net/)

## Description

ETL Adapter that provides memory safe HTML support for ETL.

Following implementation are available:
- [PSR Http Client](https://github.com/php-fig/http-client)

## Extractor - PsrHttpClientExtractor

```php
<?php

use Flow\ETL\Adapter\Http\NextRequestFactory;
use Flow\ETL\Adapter\Http\PsrHttpClientExtractor;
use Http\Client\Curl\Client;
use Nyholm\Psr7\Factory\Psr17Factory;
use Psr\Http\Message\RequestInterface;
use Psr\Http\Message\ResponseInterface;

$psr17Factory = new Psr17Factory();
$psr18Client = new Client($psr17Factory, $psr17Factory);

$extractor = new PsrHttpClientExtractor($psr18Client, new class implements NextRequestFactory {
    public function create(?ResponseInterface $previousResponse = null): ?RequestInterface
    {
        $psr17Factory = new Psr17Factory();

        if ($previousResponse === null) {
            return $psr17Factory
                ->createRequest('GET', 'https://api.github.com/orgs/flow-php')
                ->withHeader('Accept', 'application/vnd.github.v3+json')
                ->withHeader('User-Agent', 'flow-php/etl');
        }

        return null;
    }
});

$rows = $extractor->extract();

$body = \json_decode($rows->current()->first()->valueOf('body'), true, 512, JSON_THROW_ON_ERROR);

$this->assertSame(1, $rows->current()->count());
$this->assertSame("flow-php", $body['login']);
$this->assertSame(73495297, $body['id']);
$this->assertSame(["GitHub.com"], $rows->current()->first()->valueOf('headers')['Server']);
$this->assertSame(200, $rows->current()->first()->valueOf('status_code'));
$this->assertSame('1.1', $rows->current()->first()->valueOf('protocol_version'));
$this->assertSame('OK', $rows->current()->first()->valueOf('reason_phrase'));
```

## Development

In order to install dependencies please, launch following commands:

```bash
composer install
```

## Run Tests

In order to execute full test suite, please launch following command:

```bash
composer build
```

It's recommended to use [pcov](https://pecl.php.net/package/pcov) for code coverage however you can also use
xdebug by setting `XDEBUG_MODE=coverage` env variable.