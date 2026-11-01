---
package: flow-php/etl-adapter-http
---

# ETL Adapter: HTTP

[PACKAGE_NAV]

[TOC]

The HTTP adapter sends HTTP requests and reads the responses as rows in ETL (Extract, Transform, Load) pipelines.

## Installation

For the installation instructions, read
the [installation page](/documentation/installation/packages/etl-adapter-http.md).

## Which extractor to use

- `from_http_paginated()`: reads from an API. Start here. It covers all common pagination schemes.
- `from_static_http_requests()`: a fixed list of URLs that you know before the run, no pagination.
- `from_dynamic_http_requests()`: the last option, for a next request that no built-in paginator can describe.

## Row structure

All extractors in this adapter emit the same columns, so the sections below apply to all three.

| Column                      | Type                        | Description                                             |
|-----------------------------|-----------------------------|---------------------------------------------------------|
| `response_body`             | `?string`                   | Raw response body text                                  |
| `response_headers`          | `map<string, list<string>>` | Response headers, one list of values per header name    |
| `response_status_code`      | `integer`                   | HTTP status code, for example `200`                     |
| `response_protocol_version` | `string`                    | for example `1.1`                                       |
| `response_reason_phrase`    | `string`                    | for example `OK`                                        |
| `request_body`              | `?string`                   | Raw body of the request that produced this response     |
| `request_uri`               | `string`                    | Full URI the request was sent to, query string included |
| `request_headers`           | `map<string, list<string>>` | Request headers, one list of values per header name     |
| `request_protocol_version`  | `string`                    | for example `1.1`                                       |
| `request_method`            | `string`                    | for example `GET`                                       |

### Body handling

`response_body` and `request_body` hold the raw body text, for each content type. An empty or unreadable body
becomes `null`. The adapter never decodes the body into the row. To type it as a structure, declare a schema (read
*Typing the row with a schema* below).

Paginators decode the body internally, by its content type, to find `records_path` and the cursor fields. JSON and
XML therefore paginate through the same DSL. That decoding never reaches the row columns.

## Extractor - PsrHttpClientPaginatedExtractor

`from_http_paginated($client, $baseRequest, $paginator, ?$schema)` paginates declaratively. You do not write a
`NextRequestFactory`. A paginator is a strategy (how to get the next page) + a stop condition. Strategies that inject
a token also take an injection (where the token goes). The extractor stops on the safe default of the strategy, on
HTTP `>= 400`
(the row is still emitted), and on a repeated request (loop guard). All `http_*` helpers below are in
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

This goes through `?per_page=100&page=1,2,3...` until the `items` path of a response is empty.

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

### Stop conditions - `http_stop_when_*`, composed with `http_stop_when_any_of()` / `http_stop_when_all_of()`

```php
// path_missing · empty_path · flag_false · flag_true · total_reached · max_pages · max_results
stop_when: http_stop_when_any_of(http_stop_when_flag_false('has_more'), http_stop_when_max_pages(1000));
```

## Extractor - PsrHttpClientStaticExtractor

`from_static_http_requests($client, $requests, ?$schema)` sends a predefined collection of requests. It is for
URLs that you know before the run. No request depends on a response.

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

Each response is one row and, by default, one batch. Group responses into batches with `withBatchSize()`. A pushed
limit stops the requests at the limit:

```php
<?php

data_frame()
    ->read(from_static_http_requests($psr18Client, $requests())->withBatchSize(50))
    ->limit(10) // 10 requests sent
    ->write(to_output())
    ->run();
```

## Extractor - PsrHttpClientDynamicExtractor

> Use this only for an API that no built-in paginator fits. It is the lowest-level extractor of the three. You write the request
> loop yourself, so the loop guards, stop conditions and token extraction are your task. If the API paginates in one
> of the shapes above, use `from_http_paginated()` instead.

`from_dynamic_http_requests($client, $requestFactory, ?$schema)` runs the loop through a `NextRequestFactory`. The
factory receives the previous response and returns the next request, or `null` to stop.

```php
<?php

use Flow\ETL\Adapter\Http\IO\Extractor\NextRequestFactory;
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

All three extractors have `withSchema(Schema)` (also the optional last argument of the `from_*` DSL functions). The
schema describes the row. To type the body, declare `response_body` as a `structure`. As with all extractors, only
the columns in the schema stay, so add the envelope columns that you want to keep.

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

The OpenAPI specification bridge can make the `response_body` structure from an OpenAPI specification.
`schema_from_openapi_specification($spec)` returns the field definitions of the body. The HTTP adapter therefore needs
no extra dependency.
