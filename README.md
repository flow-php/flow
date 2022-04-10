# ETL Adapter: HTTP

[![Minimum PHP Version](https://img.shields.io/badge/php-~8.1-8892BF.svg)](https://php.net/)

## Description

ETL Adapter that provides memory safe HTML support for ETL.

Following implementation are available:
- [PSR Http Client](https://github.com/php-fig/http-client)

## Extractor - PsrHttpClientDynamicExtractor

Extract Rows using NextRequestFactory that can dynamically parse previous response and generate next request.

```php
<?php

use Flow\ETL\Adapter\Http\DynamicExtractor\NextRequestFactory;
use Flow\ETL\Adapter\Http\PsrHttpClientExtractor;
use Http\Client\Curl\Client;
use Nyholm\Psr7\Factory\Psr17Factory;
use Psr\Http\Message\RequestInterface;
use Psr\Http\Message\ResponseInterface;

$psr17Factory = new Psr17Factory();
$psr18Client = new Client($psr17Factory, $psr17Factory);

$extractor = new PsrHttpClientDynamicExtractor($psr18Client, new class implements NextRequestFactory {
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

## Extractor - PsrHttpClientStaticExtractor

Extract Rows from predefined collection of requests. 

```php 

use Flow\ETL\Adapter\Http\PsrHttpClientStaticExtractor;
use Flow\ETL\Rows;
use Http\Client\Curl\Client;
use Nyholm\Psr7\Factory\Psr17Factory;

$psr17Factory = new Psr17Factory();
$psr18Client = new Client($psr17Factory, $psr17Factory);

$requests = function () use ($psr17Factory) : \Generator {
    yield $psr17Factory
        ->createRequest('GET', 'https://api.github.com/users/norberttech')
        ->withHeader('Accept', 'application/vnd.github.v3+json')
        ->withHeader('User-Agent', 'flow-php/etl');

    yield $psr17Factory
        ->createRequest('GET', 'https://api.github.com/users/tomaszhanc')
        ->withHeader('Accept', 'application/vnd.github.v3+json')
        ->withHeader('User-Agent', 'flow-php/etl');
};

$extractor = new PsrHttpClientStaticExtractor($psr18Client, $requests());

$rowsGenerator = $extractor->extract();

/** @var Rows $norbertRows */
$norbertRows = $rowsGenerator->current();

$rowsGenerator->next();

/** @var Rows $tomekRows */
$tomekRows = $rowsGenerator->current();

$norbertResponseBody = \json_decode($norbertRows->first()->valueOf('body'), true, 512, JSON_THROW_ON_ERROR);
$tomekResponseBody = \json_decode($tomekRows->first()->valueOf('body'), true, 512, JSON_THROW_ON_ERROR);

\var_dump($norbertResponseBody);

$this->assertSame('norberttech', $norbertResponseBody['login']);
$this->assertSame('tomaszhanc', $tomekResponseBody['login']);
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