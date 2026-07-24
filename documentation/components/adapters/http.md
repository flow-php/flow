---
package: flow-php/etl-adapter-http
---

# ETL Adapter: HTTP

[PACKAGE_NAV]

[TOC]

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

For detailed installation instructions, see the [installation page](/documentation/installation/packages/etl-adapter-http.md).

## Extractor - PsrHttpClientDynamicExtractor

Extract Rows using NextRequestFactory that can dynamically parse previous response and generate next request.

```php
<?php

use Flow\ETL\Adapter\Http\DynamicExtractor\NextRequestFactory;
use Flow\ETL\Adapter\Http\PsrHttpClientDynamicExtractor;
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

// The body is decoded once (content-type aware) and hydrated into native, typed entries.
$body = $rows->current()->first()->valueOf('response_body');

$this->assertSame(1, $rows->current()->count());
$this->assertSame("flow-php", $body['login']);
$this->assertSame(73495297, $body['id']);
$this->assertSame(["GitHub.com"], $rows->current()->first()->valueOf('response_headers')['Server']);
$this->assertSame(200, $rows->current()->first()->valueOf('response_status_code'));
$this->assertSame('1.1', $rows->current()->first()->valueOf('response_protocol_version'));
$this->assertSame('OK', $rows->current()->first()->valueOf('response_reason_phrase'));
```

Each emitted row carries the response entries `response_body`, `response_headers`, `response_status_code`,
`response_protocol_version`, `response_reason_phrase`, plus the request entries `request_body`, `request_uri`,
`request_headers`, `request_protocol_version`, `request_method`. The body is decoded once, content-type aware — JSON
via `json_decode`, XML via the types library's `XMLConverter` — into a **navigable structure** that the DataFrame
Hydrator types (a JSON/XML object becomes a `structure`/`map`; other content types stay a string). Because the body is
a navigable structure regardless of format, JSON and XML paginate through the identical DSL below.

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

$norbertResponseBody = $norbertRows->first()->valueOf('response_body');
$tomekResponseBody = $tomekRows->first()->valueOf('response_body');

\var_dump($norbertResponseBody);

$this->assertSame('norberttech', $norbertResponseBody['login']);
$this->assertSame('tomaszhanc', $tomekResponseBody['login']);
```

## Extractor - PsrHttpClientPaginatedExtractor

`from_http_paginated($client, $baseRequest, $paginator, ?$schema)` paginates declaratively — no hand-written
`NextRequestFactory`. A paginator is a **strategy** (how the next page is derived) + a **stop condition**; strategies
that inject a token also take an **injection** (where it goes). It stops on the strategy's safe default, on HTTP `>= 400`
(the row is still emitted), and on a repeated request (loop guard). Every `http_*` helper below lives in
`Flow\ETL\Adapter\Http`.

```php
$extractor = from_http_paginated($psr18Client, $request, $strategy);   // $strategy = one of:
```

### Supported strategies

```php
// page number — ?per_page=100&page=1,2,3…          default stop: empty page at records_path
http_pagination_page_number(http_request_option_query('page'),
    page_size: 100, size_option: http_request_option_query('per_page'), records_path: 'items');

// offset / limit — ?limit=100&offset=0,100,200…    default stop: total_path reached, else empty page
http_pagination_offset(http_request_option_query('offset'), http_request_option_query('limit'),
    limit: 100, total_path: 'meta.total');

// cursor in body — ?cursor=<meta.next_cursor>      default stop: cursor missing
http_pagination_cursor('meta.next_cursor', http_request_option_query('cursor'));

// next URL in body — follows the URL (\. escapes a literal dot, e.g. OData @odata.nextLink)
http_pagination_next_url('@odata\.nextLink');

// Link header (RFC 8288) — follows rel="next", stops when absent
http_pagination_link_header('next');

// last record cursor — ?since=<last of data.*.id>
http_pagination_last_record_cursor('data.*.id', http_request_option_query('since'),
    stop_when: http_stop_when_flag_false('has_more'));
```

### Injection — where the token goes (`http_request_option_*`)

```php
http_request_option_query('page');                  // set/override a query param
http_request_option_header('X-Cursor');             // set a header
http_request_option_body('cursor', $psr17Factory);  // set a JSON body path, re-encoding the body (e.g. POST search)
http_request_option_uri();                          // the token *is* the next URL, resolved relative to the base
```

### Stop conditions — `http_stop_when_*`, composable with `->or()` / `->and()`

```php
// path_missing · empty_path · flag_false · flag_true · total_reached · max_pages · max_results
stop_when: http_stop_when_flag_false('has_more')->or(http_stop_when_max_pages(1000));
```

### Typing the row with a schema

All three extractors expose `withSchema(Schema)` (also the optional last argument of the `from_*` DSL functions). The
schema is passed straight to the DataFrame Hydrator's `cast()` — it describes the **row**, so to type the body you
declare `response_body` as a `structure`. As with every extractor, `cast()` keeps only the columns the schema mentions,
so include any envelope columns you want to keep.

```php
<?php

use function Flow\ETL\Adapter\Http\from_http_paginated;
use function Flow\ETL\Adapter\Http\http_pagination_cursor;
use function Flow\ETL\Adapter\Http\http_request_option_query;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\structure_schema;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;

$extractor = from_http_paginated(
    $psr18Client,
    $psr17Factory->createRequest('GET', 'https://api.example.com/items'),
    http_pagination_cursor('meta.next_cursor', http_request_option_query('cursor')),
)->withSchema(schema(
    structure_schema('response_body', type_structure([
        'id' => type_integer(),
        'name' => type_string(),
    ])),
    int_schema('response_status_code'),
));
```

The `response_body` structure can be derived from an OpenAPI specification with the OpenAPI specification bridge
(`schema_from_openapi_specification($spec)` returns the body's field definitions), so the HTTP adapter takes on no
extra dependency.