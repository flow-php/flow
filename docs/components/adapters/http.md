# ETL Adapter: HTTP

- [⬅️️ Back](../../introduction.md)

Flow PHP's Adapter HTTP is a finely crafted library designed to enable seamless interaction with HTTP protocols within
your ETL (Extract, Transform, Load) workflows. This adapter is vital for developers aiming to effortlessly send or
receive HTTP requests, ensuring a streamlined and reliable data transformation process. By harnessing the Adapter HTTP
library, developers can access a robust suite of features engineered for precise HTTP communication, simplifying complex
data exchange operations while enhancing overall data processing efficiency. The Adapter HTTP library encapsulates an
extensive range of functionalities, offering a streamlined API for managing HTTP tasks, which is crucial in contemporary
data processing and transformation endeavors. This library epitomizes Flow PHP's commitment to providing versatile and
efficient data processing solutions, making it an excellent choice for developers dealing with HTTP communication in
large-scale and data-intensive environments. With Flow PHP's Adapter HTTP, navigating HTTP tasks within your ETL
workflows becomes a more refined and efficient endeavor, harmoniously aligning with the robust and adaptable framework
of the Flow PHP ecosystem.

## Installation

```
composer require flow-php/etl-adapter-http
```

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