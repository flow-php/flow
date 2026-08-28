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

For detailed installation instructions, see
the [installation page](/documentation/installation/packages/etl-adapter-http.md).

## Which extractor to use

- `from_http_paginated()` - reading from an API. Start here, it covers every common pagination scheme.
- `from_static_http_requests()` - a fixed list of URLs known upfront, no pagination.
- `from_dynamic_http_requests()` - last resort, when no built-in paginator can express how the next request is built.

## Row structure

Every extractor in this adapter emits the same columns, so the sections below apply to all three.

| Column                      | Type                        | Description                                             |
|-----------------------------|-----------------------------|---------------------------------------------------------|
| `response_body`             | `?string`                   | Raw response body text                                  |
| `response_headers`          | `map<string, list<string>>` | Response headers, one list of values per header name    |
| `response_status_code`      | `integer`                   | HTTP status code, e.g. `200`                            |
| `response_protocol_version` | `string`                    | e.g. `1.1`                                              |
| `response_reason_phrase`    | `string`                    | e.g. `OK`                                               |
| `request_body`              | `?string`                   | Raw body of the request that produced this response     |
| `request_uri`               | `string`                    | Full URI the request was sent to, query string included |
| `request_headers`           | `map<string, list<string>>` | Request headers, one list of values per header name     |
| `request_protocol_version`  | `string`                    | e.g. `1.1`                                              |
| `request_method`            | `string`                    | e.g. `GET`                                              |

### Body handling

`response_body` and `request_body` hold the raw body text, whatever the content type is. An empty or unreadable body
becomes `null`. The adapter never decodes the body into the row - to type it as a structure, declare a schema (see
*Typing the row with a schema* below).

Paginators decode the body internally, content-type aware, to navigate into `records_path` and cursor fields, so JSON
and XML paginate through the identical DSL. That decoding never reaches the row columns.

## Extractor - PsrHttpClientPaginatedExtractor

`from_http_paginated($client, $baseRequest, $paginator, ?$schema)` paginates declaratively - no hand-written
`NextRequestFactory`. A paginator is a **strategy** (how the next page is derived) + a **stop condition**; strategies
that inject a token also take an **injection** (where it goes). It stops on the strategy's safe default, on HTTP
`>= 400`
(the row is still emitted), and on a repeated request (loop guard). Every `http_*` helper below lives in
`Flow\ETL\Adapter\Http`.

```php
<?php

use Http\Client\Curl\Client;
use Nyholm\Psr7\Factory\Psr17Factory;

use function Flow\ETL\Adapter\Http\{from_http_paginated, http_pagination_page_number, http_request_option_query};
use function Flow\ETL\DSL\{data_frame, to_output};

$psr17Factory = new Psr17Factory();
$psr18Client = new Client($psr17Factory, $psr17Factory);

data_frame()
    ->read(from_http_paginated(
        $psr18Client,
        $psr17Factory
            ->createRequest('GET', 'https://api.example.com/items')
            ->withHeader('Accept', 'application/json'),
        http_pagination_page_number(
            http_request_option_query('page'),
            page_size: 100,
            size_option: http_request_option_query('per_page'),
            records_path: 'items',
        ),
    ))
    ->write(to_output())
    ->run();
```

This walks `?per_page=100&page=1,2,3...` until the `items` path of a response comes back empty.

### Supported strategies

```php
// page number - ?per_page=100&page=1,2,3...          default stop: empty page at records_path
http_pagination_page_number(http_request_option_query('page'),
    page_size: 100, size_option: http_request_option_query('per_page'), records_path: 'items');

// offset / limit - ?limit=100&offset=0,100,200...    default stop: total_path reached, else empty page
http_pagination_offset(http_request_option_query('offset'), http_request_option_query('limit'),
    limit: 100, total_path: 'meta.total');

// cursor in body - ?cursor=<meta.next_cursor>      default stop: cursor missing
http_pagination_cursor('meta.next_cursor', http_request_option_query('cursor'));

// next URL in body - follows the URL (\. escapes a literal dot, e.g. OData @odata.nextLink)
http_pagination_next_url('@odata\.nextLink');

// Link header (RFC 8288) - follows rel="next", stops when absent
http_pagination_link_header('next');

// last record cursor - ?since=<last of data.*.id>
http_pagination_last_record_cursor('data.*.id', http_request_option_query('since'),
    stop_when: http_stop_when_flag_false('has_more'));
```

### Injection - where the token goes (`http_request_option_*`)

```php
http_request_option_query('page');                  // set/override a query param
http_request_option_header('X-Cursor');             // set a header
http_request_option_body('cursor', $psr17Factory);  // set a JSON body path, re-encoding the body (e.g. POST search)
http_request_option_uri();                          // the token *is* the next URL, resolved relative to the base
```

### Stop conditions - `http_stop_when_*`, composable with `->or()` / `->and()`

```php
// path_missing · empty_path · flag_false · flag_true · total_reached · max_pages · max_results
stop_when: http_stop_when_flag_false('has_more')->or(http_stop_when_max_pages(1000));
```

## Extractor - PsrHttpClientStaticExtractor

`from_static_http_requests($client, $requests, ?$schema)` sends a predefined collection of requests. Use it when every
URL is known upfront and nothing needs to be derived from a response.

```php
<?php

use Http\Client\Curl\Client;
use Nyholm\Psr7\Factory\Psr17Factory;

use function Flow\ETL\Adapter\Http\from_static_http_requests;
use function Flow\ETL\DSL\{data_frame, to_output};

$psr17Factory = new Psr17Factory();
$psr18Client = new Client($psr17Factory, $psr17Factory);

$requests = function () use ($psr17Factory) : \Generator {
    foreach (['norberttech', 'tomaszhanc'] as $username) {
        yield $psr17Factory
            ->createRequest('GET', 'https://api.github.com/users/' . $username)
            ->withHeader('Accept', 'application/vnd.github.v3+json')
            ->withHeader('User-Agent', 'flow-php/etl');
    }
};

data_frame()
    ->read(from_static_http_requests($psr18Client, $requests()))
    ->write(to_output())
    ->run();
```

## Extractor - PsrHttpClientDynamicExtractor

> Reach for this only when no built-in paginator fits. It is the lowest-level extractor of the three - you hand-write
> the request loop, so loop guards, stop conditions and token extraction all become your responsibility. If the API
> paginates in any of the shapes listed above, use `from_http_paginated()` instead.

`from_dynamic_http_requests($client, $requestFactory, ?$schema)` drives the loop through a `NextRequestFactory` that
receives the previous response and returns the next request, or `null` to stop.

```php
<?php

use Flow\ETL\Adapter\Http\DynamicExtractor\NextRequestFactory;
use Http\Client\Curl\Client;
use Nyholm\Psr7\Factory\Psr17Factory;
use Psr\Http\Message\{RequestInterface, ResponseInterface};

use function Flow\ETL\Adapter\Http\from_dynamic_http_requests;
use function Flow\ETL\DSL\{data_frame, to_output};

$psr17Factory = new Psr17Factory();
$psr18Client = new Client($psr17Factory, $psr17Factory);

data_frame()
    ->read(from_dynamic_http_requests($psr18Client, new class($psr17Factory) implements NextRequestFactory {
        public function __construct(private readonly Psr17Factory $psr17Factory)
        {
        }

        public function create(?ResponseInterface $previousResponse = null) : ?RequestInterface
        {
            if ($previousResponse !== null) {
                return null;
            }

            return $this->psr17Factory
                ->createRequest('GET', 'https://api.github.com/orgs/flow-php')
                ->withHeader('Accept', 'application/vnd.github.v3+json')
                ->withHeader('User-Agent', 'flow-php/etl');
        }
    }))
    ->write(to_output())
    ->run();
```

## Typing the row with a schema

All three extractors expose `withSchema(Schema)` (also the optional last argument of the `from_*` DSL functions). The
schema is passed straight to the DataFrame Hydrator's `cast()` - it describes the **row**, so to type the body you
declare `response_body` as a `structure`. As with every extractor, `cast()` keeps only the columns the schema mentions,
so include any envelope columns you want to keep.

```php
<?php

use function Flow\ETL\Adapter\Http\{from_http_paginated, http_pagination_cursor, http_request_option_query};
use function Flow\ETL\DSL\{int_schema, schema, structure_schema};
use function Flow\Types\DSL\{type_integer, type_string, type_structure};

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
(`schema_from_openapi_specification($spec)` returns the body's field definitions), so the HTTP adapter takes on no extra
dependency.
